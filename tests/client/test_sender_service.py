import pytest
from aiohttp import ClientSession
import msgpack

from carbon_proxy.client import SenderService


@pytest.fixture
def sender_service():
    return


@pytest.fixture
async def do_send(arguments, storage):
    sender = SenderService(
        proxy_url=arguments.carbon_proxy_url,
        secret=arguments.carbon_proxy_secret,
        storage=storage,
        http_session=ClientSession(),
    )

    async def send():
        await sender.callback()

    try:
        yield send
    finally:
        await sender.http_session.close()


async def test_sender_service(storage, do_send, proxy_server_mock):
    metrics = [
        ['foo.bar.spam', [1548934966.0, 42]],
        ['foo.bar.spam', [1548934967.0, 33]],
        ['foo.bar.spam', [1548934968.0, 55]],
    ]
    for metric in metrics:
        storage.write(metric)

    await do_send()

    assert len(proxy_server_mock['calls']) == 1
    data = msgpack.unpackb(proxy_server_mock['calls'][0], raw=False)
    assert data == metrics
