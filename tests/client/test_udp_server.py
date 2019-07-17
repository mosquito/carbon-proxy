import asyncio

import pytest


@pytest.fixture
def storage(storage):
    storage.mock_storage = mock_storage = []

    async def write_async_mocked(obj):
        mock_storage.append(obj)
    storage.write_async = write_async_mocked

    return storage


async def test_accepts_metrics_from_udp(send_to_udp, storage):
    await send_to_udp('foo.bar.spam 42 1548934966')
    await send_to_udp('foo.bar.spam 33 1548934967')
    await send_to_udp('foo.bar.spam 55 1548934968')

    await asyncio.sleep(1)

    assert set(storage.mock_storage) == set([
        ('foo.bar.spam', (1548934966.0, 42)),
        ('foo.bar.spam', (1548934967.0, 33)),
        ('foo.bar.spam', (1548934968.0, 55)),
    ])
