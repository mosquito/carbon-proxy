import asyncio
import logging
import os
import pickle
import socket
import struct
import sys

import aiohttp
from http import HTTPStatus

import async_timeout
import forkme
from collections import deque

import msgpack
from aiomisc.log import basic_config, LogFormat
from aiomisc.utils import chunk_list, bind_socket, new_event_loop
from configargparse import ArgumentParser
from setproctitle import setproctitle
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


QUEUE = deque()


async def tcp_handler(reader: asyncio.StreamReader,
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

                QUEUE.append((name, (timestamp, value)))
        except asyncio.CancelledError:
            log.info('Client connection closed after timeout')
            break
        except:
            continue

    log.info("Client disconnected %r", addr)


async def pickle_handler(reader: asyncio.StreamReader, *_):
    while not reader.at_eof():
        try:
            header = await reader.readexactly(4)
            size = struct.unpack("!L", header)[0]

            log.info("Receiving %d bytes through Pickle", size)

            for metric in pickle.loads(await reader.readexactly(size)):
                try:
                    name, ts_value = metric
                    ts, value = ts_value

                    QUEUE.append((name, (ts, value)))
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
            if not line:
                continue

            metric = line.decode()
            name, value, timestamp = metric.split(" ", 3)
            timestamp = float(timestamp)

            if value == 'nan':
                value = None
            else:
                value = float(value) if '.' in value else int(value)

            QUEUE.append((name, (timestamp, value)))


async def sender(proxy_url: URL, secret):
    headers = {
        "Authorization": "Bearer %s" % secret
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            if not len(QUEUE):
                await asyncio.sleep(1)
                continue

            log.info("Sending metrics. Queue size = %s", len(QUEUE))

            metrics = []
            while QUEUE:
                metrics.append(QUEUE.popleft())

            for payload in chunk_list(metrics, 10000):
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

                await asyncio.sleep(1)


async def amain(loop: asyncio.AbstractEventLoop,
                tcp_sock, pickle_sock, udp_sock):

    log.info("Starting TCP server on %s:%d", *tcp_sock.getsockname())
    await asyncio.start_server(
        tcp_handler, None, None, sock=tcp_sock, loop=loop
    )

    log.info("Starting Pickle server on %s:%d", *pickle_sock.getsockname())
    await asyncio.start_server(
        pickle_handler, None, None, sock=pickle_sock, loop=loop
    )

    log.info("Starting UDP server on %s:%d", *udp_sock.getsockname())
    await loop.create_datagram_endpoint(
        UDPServerProtocol, sock=udp_sock
    )


def main():
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

    try:
        loop.run_forever()
    finally:
        loop.stop()
        loop.run_until_complete(loop.shutdown_asyncgens())


if __name__ == '__main__':
    main()
