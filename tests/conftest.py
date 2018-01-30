import asyncio
from types import SimpleNamespace

import uvloop
import time

from aiohttp.web import Application

from carbon_proxy.server import make_app

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import asyncio
import aiohttp.test_utils
import pytest

from contextlib import contextmanager
from aiohttp import CookieJar


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

pytest_plugins = (
    'aiohttp.pytest_plugin',
)


@pytest.fixture
def timer():
    @contextmanager
    def timer(expected_time=0, *, dispersion=0.5):
        expected_time = float(expected_time)
        dispersion_value = expected_time * dispersion

        now = time.monotonic()

        yield

        delta = time.monotonic() - now

        lower_bound = expected_time - dispersion_value
        upper_bound = expected_time + dispersion_value

        assert lower_bound < delta < upper_bound

    return timer


@pytest.fixture()
def arguments():
    return SimpleNamespace(
        debug=True,
        log_level='debug',
        log_format='stderr',
        pool_size=4,

        # should set after TestServer initialization
        http_address=None,
        http_port=None,
    )


@pytest.fixture()
async def app(event_loop: asyncio.AbstractEventLoop,
              arguments: SimpleNamespace) -> Application:
    asyncio.set_event_loop(event_loop)

    app = make_app(event_loop, arguments)

    try:
        yield app
    finally:
        await app.shutdown()


@pytest.fixture()
async def http_client(arguments: SimpleNamespace,
                      event_loop: asyncio.AbstractEventLoop,
                      app: Application):

    server = aiohttp.test_utils.TestServer(app, loop=event_loop)
    client = aiohttp.test_utils.TestClient(
        server,
        loop=event_loop,
        cookie_jar=CookieJar(unsafe=True)
    )     # type: aiohttp.test_utils.TestClient

    await client.start_server()

    try:
        arguments.http_host = server.host
        arguments.http_port = server.port
        yield client
    finally:
        await client.close()
        await server.close()
        arguments.http_host = None
        arguments.http_port = None
