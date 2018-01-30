import asyncio
import logging
import logging.handlers
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
from configargparse import ArgumentParser
from setproctitle import setproctitle
from yarl import URL

from .thread_pool import ThreadPoolExecutor
from .utils import (
    get_stream_handler,
    get_syslog_handler,
    bind_socket,
    chunk_list,
)

try:
    import uvloop
    asyncio.get_event_loop().close()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


log = logging.getLogger()
parser = ArgumentParser()

parser.add_argument('-f', '--forks', type=int, env_var="FORKS", default=4)
parser.add_argument('--pool-size', default=4, type=int, env_var='POOL_SIZE')
parser.add_argument('-D', '--debug', action='store_true')

parser.add_argument(
    '--log-level',
    choices=('debug', 'info', 'warning', 'error', 'fatal'),
    default='info', env_var='LOG_LEVEL',
)

parser.add_argument(
    '--log-format',
    choices=('syslog', 'stderr'),
    default='stderr',
    env_var='LOG_FORMAT',
)


group = parser.add_argument_group('TCP receiver settings')
group.add_argument('--tcp-listen', type=str,
                   default='0.0.0.0', env_var='TCP_LISTEN')
group.add_argument('--tcp-port', type=int,
                   default=2003, env_var='TCP_PORT')

group = parser.add_argument_group('UDP receiver settings')
group.add_argument('--udp-listen', type=str,
                   default='0.0.0.0', env_var='UDP_LISTEN')
group.add_argument('--udp-port', type=int,
                   default=2003, env_var='UDP_PORT')

group = parser.add_argument_group('Pickle receiver settings')
group.add_argument('--pickle-listen', type=str,
                   default='0.0.0.0', env_var='PICKLE_LISTEN')
group.add_argument('--pickle-port', type=int,
                   default=2004, env_var='PICKLE_PORT')

group = parser.add_argument_group('Carbon proxy server settings')
group.add_argument(
    '-U', '--carbon-proxy-url', type=URL,
    required=True, env_var='CARBON_PROXY',
)

group.add_argument(
    '-S', '--carbon-proxy-secret', type=str,
    required=True, env_var='CARBON_PROXY_SECRET',
)


QUEUE = deque()


async def tcp_handler(reader: asyncio.StreamReader, *_):
    log.info("Client connected")

    while not reader.at_eof():
        try:
            log.info("Receiving data through TCP")

            async with async_timeout.timeout(5):
                line = await reader.readline()

            if line:
                metric = line.decode()
                name, value, timestamp = metric.split(" ", 3)
                timestamp = float(timestamp)
                value = float(value) if '.' in value else int(value)
                QUEUE.append((name, (timestamp, value)))
        except asyncio.CancelledError:
            log.info('Client connection closed after timeout')
            break
        except:
            continue


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
        self.queue = asyncio.Queue()
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        payload = []

        log.info("Received %d bytes through UDP", len(data))

        for line in data.split(b'\n'):
            if not line:
                continue

            metric = line.decode()
            name, value, timestamp = metric.split(" ", 3)
            timestamp = float(timestamp)
            value = float(value) if '.' in value else int(value)
            payload.append((name, (timestamp, value)))

        if payload:
            QUEUE.put_nowait(payload)

    def close(self):
        self.queue.put_nowait(None)


async def sender(proxy_url: URL, secret):
    headers = {
        "Authorization": "Bearer %s" % secret
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            if not QUEUE:
                await asyncio.sleep(1)
                continue

            metrics = []
            while QUEUE:
                metrics.append(QUEUE.popleft())

            for payload in chunk_list(metrics, 1000):
                data = msgpack.packb(payload)

                while True:
                    request = session.post(proxy_url, data=data, headers={
                        'Content-Type': 'application/octet-stream'
                    })
                    async with request as resp:  # type: aiohttp.ClientResponse
                        if resp.status == HTTPStatus.ACCEPTED:
                            log.info("Sent %d bytes", len(data))
                            break
                        else:
                            await asyncio.sleep(1)

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
    logging.basicConfig(level=logging.INFO)

    arguments = parser.parse_args()

    setproctitle(os.path.basename("[Master] %s" % sys.argv[0]))

    tcp_sock = bind_socket(
        socket.AF_INET if ':' in arguments.tcp_listen else socket.AF_INET6,
        socket.SOCK_STREAM,
        address=arguments.tcp_listen,
        port=arguments.tcp_port
    )

    pickle_sock = bind_socket(
        socket.AF_INET if ':' in arguments.pickle_listen else socket.AF_INET6,
        socket.SOCK_STREAM,
        address=arguments.pickle_listen,
        port=arguments.pickle_port
    )

    udp_sock = bind_socket(
        socket.AF_INET if ':' in arguments.udp_listen else socket.AF_INET6,
        socket.SOCK_DGRAM,
        address=arguments.udp_listen,
        port=arguments.udp_port
    )

    forkme.fork(arguments.forks)

    setproctitle(os.path.basename("[Worker] %s" % sys.argv[0]))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_debug(arguments.debug)

    pool = ThreadPoolExecutor(arguments.pool_size, loop=loop)
    loop.set_default_executor(pool)

    if arguments.log_format == 'syslog':
        log_handler = get_syslog_handler(loop)
    elif arguments.log_format == 'stderr':
        log_handler = get_stream_handler(loop)
    else:
        raise ValueError('Invalid log format')

    logging.basicConfig(
        level=getattr(logging, arguments.log_level.upper(), logging.INFO),
        handlers=[log_handler],
    )

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
