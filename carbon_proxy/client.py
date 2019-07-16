import asyncio
import builtins
import io
import logging
import os
import pickle
import socket
import struct
import sys
from contextlib import suppress
from contextvars import ContextVar
from http import HTTPStatus
from pathlib import Path
from setproctitle import setproctitle
from threading import RLock
from types import MappingProxyType

import aiohttp
import async_timeout
import forklib
import msgpack
from aiomisc import entrypoint, threaded, bind_socket
from aiomisc.service import TCPServer, UDPServer
from aiomisc.service.periodic import PeriodicService
from aiomisc.log import basic_config, LogFormat
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


Registry = ContextVar("Registry")


class Storage:
    ROTATE_SIZE = 2 ** 20

    def _write_to(self, fp, data, seek=None, truncate=False):
        if seek is not None:
            fp.seek(seek)

        if truncate:
            fp.truncate()

        return fp.write(self.packer.pack(data))

    @staticmethod
    def _read_from(fp, seek=None):
        if seek is not None:
            fp.seek(seek)

        unpacker = msgpack.Unpacker(fp, raw=False)

        while True:
            pos = fp.tell()

            try:
                result = unpacker.unpack()
                cur = fp.tell()

                yield result, cur
            except Exception:
                fp.seek(pos)
                raise

    def __init__(self, path):
        self.write_lock = RLock()
        self.meta_lock = RLock()

        self.path = path
        self.write_fp = open(path, "ab")
        self.pos_fp = open("%s.pos" % path, "ab+")

        self.packer = msgpack.Packer(use_bin_type=True)

    def write(self, obj):
        with self.write_lock:
            return self._write_to(self.write_fp, obj)

    def read(self):
        with open(self.path, 'rb') as fp:
            self.write_fp.flush()

            try:
                pos, cur = next(self._read_from(self.pos_fp, seek=0))
            except msgpack.OutOfData:
                pos = 0

            try:
                for item, pos in self._read_from(fp, seek=pos):
                    self._write_to(self.pos_fp, pos, seek=0, truncate=True)
                    yield item
            except msgpack.OutOfData:
                if self.size() > self.ROTATE_SIZE:
                    self.clear()

                return
            finally:
                self.pos_fp.flush()

    def size(self):
        return os.stat(self.path).st_size

    def clear(self):
        with self.write_lock:

            self.write_fp.seek(0)
            self.write_fp.truncate(0)

            self._write_to(self.pos_fp, 0, 0, truncate=True)

            log.info(
                'Truncating file %r new size is %s',
                self.path,
                self.size()
            )

    write_async = threaded(write)
    clear_async = threaded(clear)
    size_async = threaded(size)

    @threaded
    def read_async(self, chunk_size=2000):
        result = []
        for item in self.read():
            if len(result) > chunk_size:
                break

            result.append(item)

        return result


class StorageBase:
    ...


class CarbonTCPServer(TCPServer, StorageBase):

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        log.info("Client connected %r", addr)

        while not reader.at_eof():
            try:
                async with async_timeout.timeout(5):
                    line = await reader.readline()

                if line:
                    metric = line.decode()
                    name, value, timestamp = metric.split(" ", 3)
                    timestamp = float(timestamp)

                    if value == 'nan':
                        value = None
                    else:
                        value = float(value) if '.' in value else int(value)

                    await self.storage.write_async((name, (timestamp, value)))
            except asyncio.CancelledError:
                log.info('Client connection closed after timeout')
                break
            except:  # noqa
                continue

        log.info("Client disconnected %r", addr)


class CarbonPickleServer(TCPServer, StorageBase):
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
                "global '%s.%s' is forbidden" % (
                    module, name
                )
            )

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter):
        while not reader.at_eof():
            try:
                header = await reader.readexactly(4)
                size = struct.unpack("!L", header)[0]

                log.info("Receiving %d bytes through Pickle", size)

                data = self.RestrictedUnpickler(
                    io.BytesIO(await reader.readexactly(size))
                ).load()

                for metric in data:
                    with suppress(Exception):
                        name, ts_value = metric
                        ts, value = ts_value

                        await self.storage.write_async((name, (ts, value)))
            except asyncio.IncompleteReadError:
                return


class CarbonUDPServer(UDPServer, StorageBase):

    async def handle_datagram(self, data: bytes, addr):
        log.debug("Received %d bytes through UDP", len(data))

        for line in data.split(b'\n'):
            if not line:
                continue

            metric = line.decode()
            name, value, timestamp = metric.split(" ", 3)
            timestamp = float(timestamp)

            if value == 'nan':
                value = None
            else:
                value = float(value) if '.' in value else int(value)

            await self.storage.write_async((name, (timestamp, value)))
            log.debug("Data has been written to storage")


class SenderService(PeriodicService, StorageBase):

    __required__ = ('proxy_url', 'secret',)

    proxy_url: URL
    secret: str
    interval: int = 10
    send_timeout: int = 30
    headers: dict = None
    http_session: aiohttp.ClientSession

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "Authorization": "Bearer %s" % self.secret,
            'Content-Type': 'application/octet-stream'
        }

    async def start(self):
        self.http_session = aiohttp.ClientSession(headers=self.headers)
        await super().start()

    async def stop(self, *args, **kwargs):
        await self.http_session.close()
        await super().stop(*args, **kwargs)

    async def callback(self):
        payload = await self.storage.read_async()

        if not payload:
            return

        data = msgpack.packb(payload)

        while True:
            request = self.http_session.post(
                self.proxy_url,
                data=data,
                timeout=self.send_timeout,
            )

            log.info("Sending %d bytes to %s", len(data), self.proxy_url)

            async with request as resp:
                if resp.status == HTTPStatus.ACCEPTED:
                    log.debug("Sent %d bytes", len(payload))
                    break
                elif resp.status == HTTPStatus.BAD_REQUEST:
                    log.warning("Bad request %r", payload)
                    break
                else:
                    log.warning("Wrong response %s. Retrying...", resp.status)


def main():
    arguments = parser.parse_args()

    basic_config(level=arguments.log_level,
                 log_format=arguments.log_format,
                 buffered=False)

    setproctitle(os.path.basename("[Master] %s" % sys.argv[0]))

    Registry.set(dict())

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

    services = [
        CarbonTCPServer(sock=tcp_sock),
        CarbonPickleServer(sock=pickle_sock),
        CarbonUDPServer(sock=udp_sock),
        SenderService(
            proxy_url=arguments.carbon_proxy_url,
            secret=arguments.carbon_proxy_secret,
        )
    ]

    def run():
        setproctitle(os.path.basename("[Worker] %s" % sys.argv[0]))

        registry = {'storage': Storage(
            os.path.join(arguments.storage, "%03d.db" % forklib.get_id())
        )}

        Registry.set(MappingProxyType(registry))

        with entrypoint(*services,
                        log_level=arguments.log_level,
                        log_format=arguments.log_format,
                        pool_size=arguments.pool_size) as loop:
            loop.set_debug(arguments.debug)
            loop.run_forever()

    if arguments.debug:
        run()
    else:
        forklib.fork(arguments.forks, run)


if __name__ == '__main__':
    main()
