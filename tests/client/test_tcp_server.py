import asyncio

from aiocarbon.metric import Metric
import pytest


@pytest.fixture
def storage(storage):
    storage.mock_storage = mock_storage = []

    async def write_async_mocked(obj):
        mock_storage.append(obj)
    storage.write_async = write_async_mocked

    return storage


async def test_accepts_metrics(proxy_carbon_client, storage):
    metrics = [
        Metric('foo.bar.spam', 42, 1548934966),
        Metric('foo.bar.spam', 33, 1548934967),
        Metric('foo.bar.spam', 55, 1548934968)]
    for metric in metrics:
        proxy_carbon_client.add(metric)

    await asyncio.sleep(1)
    assert storage.mock_storage == [
        ('foo.bar.spam', (1548934966.0, 42)),
        ('foo.bar.spam', (1548934967.0, 33)),
        ('foo.bar.spam', (1548934968.0, 55)),
    ]
