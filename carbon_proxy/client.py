import asyncio
import builtins
import io
import logging
import os
import pickle
import socket
import struct
import sys
from http import HTTPStatus
from pathlib import Path
from setproctitle import setproctitle
from threading import RLock

import aiohttp
import async_timeout
import forkme
import msgpack
from aiomisc.log import basic_config, LogFormat
from aiomisc.thread_pool import threaded
from aiomisc.utils import bind_socket, new_event_loop
from configargparse import ArgumentParser
from yarl import URL


log = logging.getLogger()
parser = ArgumentParser(auto_env_var_prefix="APP_")

parser.add_argument('-f', '--forks', type=int, default=4)
parser.add_argument('--pool-size', default=4, type=int)
parser.add_argument('-D', '--debug', action='store_true')

parser.add_argument('--log-level', default='info',
                    choices=('debug', 'info', 'warning', 'error', 'fatal'))

parser.add_argument('--log-format', choices=LogFormat.choices(),
                    default='color')


group = parser.add_argument_group('TCP receiver settings')
group.add_argument('--tcp-listen', type=str, default='0.0.0.0')
group.add_argument('--tcp-port', type=int, default=2003)

group = parser.add_argument_group('UDP receiver settings')
group.add_argument('--udp-listen', type=str, default='0.0.0.0')
group.add_argument('--udp-port', type=int, default=2003)

group = parser.add_argument_group('Pickle receiver settings')
group.add_argument('--pickle-listen', type=str, default='0.0.0.0')
group.add_argument('--pickle-port', type=int, default=2004)

group = parser.add_argument_group('Carbon proxy server settings')
group.add_argument('-U', '--carbon-proxy-url', type=URL, required=True)
group.add_argument('-S', '--carbon-proxy-secret', type=str, required=True)

group = parser.add_argument_group('Storage settings')
group.add_argument('-s', '--storage', type=Path, required=True)


class Storage:
    def __init__(self, path, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.path = path

        self._lock = RLock()

        open(self.path, 'a').close()

        self.packer = msgpack.Packer(use_bin_type=True, encoding='utf-8')

    @threaded
    def _write(self, obj):
        log.debug("Writing to %r", self.path)

        with open(self.path, 'ab') as fp:
            with self._lock:
                payload = self.packer.pack(obj)
                fp.write(struct.pack("!I", len(payload)))
                fp.write(payload)

    def write(self, obj):
        return self.loop.create_task(self._write(obj))

    @threaded
    def _read(self, max_size=1000):
        with self._lock:
            with open(self.path, 'rb+') as fp:
                result = []

                while True:
                    length = fp.read(4)
                    if len(length) < 4:
                        break

                    length = struct.unpack('!I', length)[0]

                    payload = fp.read(length)

                    if len(payload) < length:
                        break

                    result.append(
                        msgpack.unpackb(payload, encoding='utf-8')
                    )

                    if len(result) >= max_size:
                        break

                fp.truncate(0)

            return result

    async def read(self, count=1, timeout=1):
        result = []
        deadline = self.loop.time() + timeout

        while True:
            for item in await self._read():
                result.append(item)

            if not result:
                await asyncio.sleep(timeout / 4, loop=self.loop)

            if len(result) > count or self.loop.time() >= deadline:
                break

        return result


STORAGE = None      # type: Storage


def parse_line(line):
    try:
        metric = line.decode()
        name, value, timestamp = metric.split(" ", 3)
        timestamp = float(timestamp)

        if value == 'nan':
            value = None
        else:
            value = float(value) if '.' in value else int(value)

        return STORAGE.write((name, (timestamp, value)))
    except:
        log.warning('Cannot process received line')


async def tcp_handler(reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter):
    addr = writer.get_extra_info('peername')
    log.info("Client connected %r", addr)

    while not reader.at_eof():
        try:
            async with async_timeout.timeout(5):
                line = await reader.readline()
            if line:
                parse_line(line)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            log.info('Client connection closed after timeout')
            break
        except ConnectionResetError:
            log.warning('Client connection reset')
            reader.feed_eof()
            break

    log.info("Client disconnected %r", addr)


class RestrictedUnpickler(pickle.Unpickler):
    safe_builtins = frozenset({
        'int',
        'float',
        'str',
        'tuple',
    })

    def find_class(self, module, name):
        # Only allow safe classes from builtins.
        if module == "builtins" and name in self.safe_builtins:
            return getattr(builtins, name)

        # Forbid everything else.
        raise pickle.UnpicklingError(
            "global '%s.%s' is forbidden" % (module, name))


async def pickle_handler(reader: asyncio.StreamReader, *_):
    while not reader.at_eof():
        try:
            header = await reader.readexactly(4)
            size = struct.unpack("!L", header)[0]

            log.info("Receiving %d bytes through Pickle", size)

            data = RestrictedUnpickler(
                io.BytesIO(await reader.readexactly(size))
            ).load()

            for metric in data:
                try:
                    name, ts_value = metric
                    ts, value = ts_value

                    await STORAGE.write((name, (ts, value)))
                except:
                    continue
        except asyncio.IncompleteReadError:
            return


class UDPServerProtocol(asyncio.DatagramProtocol):

    def __init__(self):
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        log.debug("Received %d bytes through UDP", len(data))

        for line in data.split(b'\n'):
            if line:
                parse_line(line)


async def sender(proxy_url: URL, secret, send_deadline=5):
    headers = {
        "Authorization": "Bearer %s" % secret
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            payload = await STORAGE.read(count=10000, timeout=send_deadline)

            if not payload:
                log.debug('Skipping sending attempt because no data.')
                continue

            data = msgpack.packb(payload)

            while True:
                try:
                    request = session.post(
                        proxy_url,
                        data=data,
                        timeout=30,
                        headers={
                            'Content-Type': 'application/octet-stream'
                        }
                    )
                    log.debug("Sending %d bytes to %s", len(data), proxy_url)
                    async with request as resp:  # type: aiohttp.ClientResponse
                        if resp.status == HTTPStatus.ACCEPTED:
                            log.debug("Sent %d bytes", len(data))
                            break
                        else:
                            await asyncio.sleep(1)
                except:
                    log.exception("Data sending error")
                    await asyncio.sleep(1)
                    continue


async def amain(loop: asyncio.AbstractEventLoop,
                tcp_sock, pickle_sock, udp_sock):

    log.info("Starting TCP server on %s:%d", *tcp_sock.getsockname()[:2])
    await asyncio.start_server(
        tcp_handler, None, None, sock=tcp_sock, loop=loop
    )

    log.info("Starting Pickle server on %s:%d", *pickle_sock.getsockname()[:2])
    await asyncio.start_server(
        pickle_handler, None, None, sock=pickle_sock, loop=loop
    )

    log.info("Starting UDP server on %s:%d", *udp_sock.getsockname()[:2])
    await loop.create_datagram_endpoint(
        UDPServerProtocol, sock=udp_sock
    )


def main():
    global STORAGE

    arguments = parser.parse_args()

    basic_config(level=arguments.log_level,
                 log_format=arguments.log_format,
                 buffered=False)

    setproctitle(os.path.basename("[Master] %s" % sys.argv[0]))

    tcp_sock = bind_socket(
        address=arguments.tcp_listen,
        port=arguments.tcp_port
    )

    pickle_sock = bind_socket(
        address=arguments.pickle_listen,
        port=arguments.pickle_port
    )

    udp_sock = bind_socket(
        socket.AF_INET6 if ':' in arguments.udp_listen else socket.AF_INET,
        socket.SOCK_DGRAM,
        address=arguments.udp_listen,
        port=arguments.udp_port
    )

    test_path = os.path.join(arguments.storage, '.test')
    with open(test_path, 'ab+') as fp:
        assert fp.truncate(0) == 0, (
            "Truncating not supported on %r" % arguments.storage
        )

    forkme.fork(arguments.forks)

    setproctitle(os.path.basename("[Worker] %s" % sys.argv[0]))

    loop = new_event_loop(arguments.pool_size)
    loop.set_debug(arguments.debug)

    basic_config(level=arguments.log_level,
                 log_format=arguments.log_format,
                 buffered=True, loop=loop)

    loop.create_task(amain(loop, tcp_sock, pickle_sock, udp_sock))
    loop.create_task(
        sender(
            arguments.carbon_proxy_url,
            arguments.carbon_proxy_secret
        )
    )

    STORAGE = Storage(
        os.path.join(
            arguments.storage,
            "%03d.bin" % (forkme.get_id() or 0)
        ), loop=loop
    )

    try:
        loop.run_forever()
    finally:
        loop.stop()
        loop.run_until_complete(loop.shutdown_asyncgens())


if __name__ == '__main__':
    main()
