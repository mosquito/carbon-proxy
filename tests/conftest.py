from asyncio import Event

import aiohttp
import msgpack
import pytest
from aiohttp import BasicAuth, test_utils
from aiohttp.web_app import Application
from aiohttp.web_request import Request
from aiohttp.web_response import Response
from yarl import URL

from jaeger_proxy.main import API, Sender, parser


@pytest.fixture
def pack():
    def do_pack(data):
        return msgpack.packb(data, use_bin_type=True)

    return do_pack


@pytest.fixture
def http_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def jaeger_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def proxy_secret():
    return "foo-bar"


@pytest.fixture
def arguments(http_port, proxy_secret, jaeger_route):
    return parser.parse_args(
        [
            "--forks=1",
            "--log-level=debug",
            f"--http-address=localhost",
            f"--http-port={str(http_port)}",
            f"--http-password={proxy_secret}",
            f"--jaeger-route={str(jaeger_route)}",
            f"--sender-interval=0.1",
        ]
    )


@pytest.fixture
def services(api_service, sender_service):
    return [api_service, sender_service]


@pytest.fixture
def api_service(arguments):
    return API(
        address=arguments.http_address,
        port=arguments.http_port,
        password=arguments.http_password,
        login=arguments.http_login,
    )


@pytest.fixture
def sender_service(arguments):
    return Sender(
        jaeger_route=arguments.jaeger_route,
        interval=arguments.sender_interval,
    )


@pytest.fixture
async def http_session():
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
async def http_session_auth(arguments):
    auth = BasicAuth(
        password=arguments.http_password, login=arguments.http_login
    )
    async with aiohttp.ClientSession(auth=auth) as session:
        yield session


@pytest.fixture
def base_url(arguments):
    return URL(f"http://{arguments.http_address}:{arguments.http_port}/")


@pytest.fixture
def stat_url(base_url):
    return base_url / "api/traces"


@pytest.fixture
def jaeger_route(jaeger_port):
    return URL.build(
        scheme="http",
        port=jaeger_port,
        host="127.0.0.1",
        path="/api/traces",
    )


class WaitingApp(Application):
    def __init__(self):
        super().__init__()
        self['data'] = None
        self['event'] = Event()

    async def wait(self):
        await self['event'].wait()
        self['event'] = Event()
        return self['data']


async def handler(request: Request):
    data = await request.read()
    request.app['event'].set()
    request.app['data'] = data
    return Response(status=204)


@pytest.fixture
async def jaeger_mock_server(jaeger_port):
    app = WaitingApp()

    app.router.add_route("POST", "/api/traces", handler)

    server = test_utils.TestServer(app, port=jaeger_port)
    client = test_utils.TestClient(server)

    await client.start_server()
    try:
        yield app
    finally:
        await client.close()
        await server.close()
