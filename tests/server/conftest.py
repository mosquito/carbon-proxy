import aiohttp
import pytest
from yarl import URL

from carbon_proxy.server import (
    parser,
    API,
    Sender,
)


@pytest.fixture
def http_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def carbon_port(aiomisc_unused_port_factory):
    return aiomisc_unused_port_factory()


@pytest.fixture
def proxy_secret():
    return 'foo-bar'


@pytest.fixture
def arguments(http_port, carbon_port, proxy_secret):
    return parser.parse_args([
        '--forks', '1',
        '--log-level', 'debug',
        '--pool-size', '4',
        '--http-address', 'localhost',
        '--http-port', str(http_port),
        '--http-secret', proxy_secret,
        '--carbon-host', 'localhost',
        '--carbon-port', str(carbon_port),
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
        host=arguments.carbon_host,
        port=arguments.carbon_port,
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
def api_url(arguments):
    return URL(f'http://{arguments.http_address}:{arguments.http_port}/')
