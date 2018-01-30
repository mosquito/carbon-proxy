import asyncio
import logging
import logging.handlers
import socket
from functools import wraps, partial
from typing import Iterable, Any

import itertools


log = logging.getLogger(__name__)


def threaded(func):
    @wraps(func)
    async def wrap(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

    return wrap


class AsyncMemoryHandler(logging.handlers.MemoryHandler):
    def __init__(self, capacity, target=None, flush_interval: float=0.1,
                 loop: asyncio.AbstractEventLoop = None, **kwargs):
        super().__init__(capacity, target=target, **kwargs)
        self.loop = loop or asyncio.get_event_loop()
        self.periodic_task = self.loop.create_task(
            self.periodic(flush_interval)
        )

    async def periodic(self, period: float):
        while True:
            await asyncio.sleep(period, loop=self.loop)

            try:
                await self.loop.run_in_executor(None, self.flush)
            except Exception as e:
                print(e)

    def close(self):
        super().close()
        self.periodic_task.cancel()


def get_syslog_handler(loop):
    handler = logging.handlers.SysLogHandler(address='/dev/log')
    handler.setLevel(logging.getLogger().getEffectiveLevel())

    formatter = logging.Formatter('%(module)s.%(funcName)s: %(message)s')
    handler.setFormatter(formatter)

    return AsyncMemoryHandler(
        target=handler,
        capacity=1000,
        loop=loop
    )


def get_stream_handler(loop):
    handler = logging.StreamHandler()
    handler.setLevel(logging.getLogger().getEffectiveLevel())

    return AsyncMemoryHandler(
        target=handler,
        capacity=1000,
        loop=loop,
    )


def chunk_list(iterable: Iterable[Any], size: int):
    iterable = iter(iterable)

    item = list(itertools.islice(iterable, size))

    while item:
        yield item
        item = list(itertools.islice(iterable, size))


def bind_socket(*args, address: str, port: int):
    sock = socket.socket(*args)
    sock.setblocking(0)

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    sock_addr = address, port
    log.info('Listening tcp://%s:%s' % sock_addr)

    try:
        sock.bind(sock_addr)
    finally:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 0)

    return sock
