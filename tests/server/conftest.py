import asyncio
import msgpack

import aiohttp
import pytest
from yarl import URL

from carbon_proxy.server import (
    parser,
    API,
    Sender,
)


@pytest.fixture
def pack():
    def do_pack(data):
        return msgpack.packb(data, use_bin_type=True)

    return do_pack


@pytest.fixture
def http_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def carbon_host():
    return 'localhost'


@pytest.fixture
def carbon_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def foo_bar_carbon_host():
    return 'localhost'


@pytest.fixture
def foo_bar_carbon_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def proxy_secret():
    return 'foo-bar'


@pytest.fixture
def routes(carbon_host, carbon_port, foo_bar_carbon_host, foo_bar_carbon_port):
    return (
        f'foo.bar={foo_bar_carbon_host}:{foo_bar_carbon_port},'
        f'={carbon_host}:{carbon_port}'
    )


@pytest.fixture
def arguments(http_port, proxy_secret, routes):
    return parser.parse_args([
        '--forks', '1',
        '--log-level', 'debug',
        '--pool-size', '4',
        '--http-address', 'localhost',
        '--http-port', str(http_port),
        '--http-secret', proxy_secret,
        '--routes', routes,
    ])


@pytest.fixture
def services(api_service, sender_service):
    return filter(None, [api_service, sender_service])


@pytest.fixture
def api_service(arguments):
    return API(
        address=arguments.http_address,
        port=arguments.http_port,
        secret=arguments.http_secret,
    )


@pytest.fixture
def sender_service(arguments):
    return Sender(
        routes=arguments.routes,
        interval=arguments.sender_interval,
    )


@pytest.fixture
async def http_session():
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
async def http_session_auth(arguments):
    headers = {'Authorization': f'Bearer {arguments.http_secret}'}
    async with aiohttp.ClientSession(headers=headers) as session:
        yield session


@pytest.fixture
def base_url(arguments):
    return URL(f'http://{arguments.http_address}:{arguments.http_port}/')


@pytest.fixture
def stat_url(base_url):
    return base_url / 'stat'


class Server:
    def __init__(self, loop, host, port):
        self.loop = loop
        self.task = self.loop.create_task(
            asyncio.start_server(self.handler, host, port, loop=loop))

        self.host = host
        self.port = port
        self.data = b''
        self.event = asyncio.Event(loop=self.loop)

    async def handler(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter):
        while not reader.at_eof():
            self.data += await reader.read(1)

        if self.data:
            self.event.set()

    async def wait_data(self):
        await self.event.wait()
        self.event = asyncio.Event(loop=self.loop)


@pytest.fixture
def tcp_server(loop, carbon_host, carbon_port):
    server = Server(host=carbon_host, port=carbon_port, loop=loop)
    try:
        yield server
    finally:
        server.task.cancel()


@pytest.fixture
def foo_bar_tcp_server(loop, foo_bar_carbon_host, foo_bar_carbon_port):
    server = Server(
        host=foo_bar_carbon_host,
        port=foo_bar_carbon_port,
        loop=loop,
    )
    try:
        yield server
    finally:
        server.task.cancel()
