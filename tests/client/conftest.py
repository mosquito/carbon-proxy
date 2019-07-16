import asyncio

import aiohttp
from aiohttp import test_utils
from aiocarbon.storage import RawStorage
from aiocarbon.protocol import TCPClient
import pytest

from carbon_proxy.client import (
    parser,
    CarbonTCPServer,
    CarbonUDPServer,
    CarbonPickleServer,
    SenderService,
    Storage,
)


@pytest.fixture
def tcp_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def udp_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def pickle_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def server_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def proxy_secret():
    return 'foo-bar'


@pytest.fixture
def arguments(tcp_port, udp_port, pickle_port, server_port, proxy_secret,
              tmpdir):
    return parser.parse_args([
        '--forks', '1',
        '--log-level', 'debug',
        '--tcp-port', str(tcp_port),
        '--tcp-listen', 'localhost',
        '--udp-port', str(udp_port),
        '--pickle-port', str(pickle_port),
        '--carbon-proxy-url', f'http://localhost:{server_port}/',
        '--carbon-proxy-secret', proxy_secret,
        '--storage', str(tmpdir.mkdir('client')),
    ])


@pytest.fixture
def services(carbon_tcp_server, carbon_udp_server, carbon_pickle_server,
             sender_service):
    return filter(None, [
        carbon_tcp_server,
        carbon_udp_server,
        carbon_pickle_server,
        sender_service,
    ])


@pytest.fixture
def carbon_tcp_server(arguments, storage):
    return CarbonTCPServer(
        address=arguments.tcp_listen,
        port=arguments.tcp_port,
        storage=storage,
    )


@pytest.fixture
def carbon_udp_server(arguments, storage):
    return CarbonUDPServer(
        address=arguments.udp_listen,
        port=arguments.udp_port,
        storage=storage,
    )


@pytest.fixture
def carbon_pickle_server(arguments, storage):
    return CarbonPickleServer(
        address=arguments.pickle_listen,
        port=arguments.pickle_port,
        storage=storage,
    )


@pytest.fixture
def sender_service(arguments, storage):
    return SenderService(
        proxy_url=arguments.carbon_proxy_url,
        secret=arguments.carbon_proxy_secret,
        storage=storage,
    )


@pytest.fixture
def storage(arguments):
    return Storage(arguments.storage / 'test.db')


@pytest.fixture
async def proxy_carbon_client(arguments, loop):
    carbon_client = TCPClient(
        arguments.tcp_listen,
        arguments.tcp_port,
        namespace='',
        storage=RawStorage(),
        loop=loop,
    )
    periodic_send_task = loop.create_task(carbon_client.run())
    try:
        yield carbon_client
    finally:
        periodic_send_task.cancel()


class UDPClientProtocol:

    def __init__(self, message, loop):
        self.message = message
        self.on_close = loop.create_future()

    def connection_made(self, transport):
        transport.sendto(self.message.encode())
        transport.close()

    def connection_lost(self, err):
        self.on_close.set_result(True)


@pytest.fixture
async def send_to_udp(arguments, loop):
    async def send_data(message):
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPClientProtocol(message, loop),
            remote_addr=(arguments.udp_listen, arguments.udp_port))
        try:
            await protocol.on_close
        finally:
            transport.close()
    return send_data


async def proxy_server_handler_mock(request):
    request.app['calls'].append(await request.read())
    return aiohttp.web.Response(status=202)


@pytest.fixture
async def proxy_server_mock(server_port, loop):
    app = aiohttp.web.Application()
    app['calls'] = []
    app.router.add_route('post', '/', proxy_server_handler_mock)

    server = test_utils.TestServer(app, port=server_port)
    client = test_utils.TestClient(server)
    await server.start_server(loop)
    try:
        yield app
    finally:
        await client.close()
        await server.close()
