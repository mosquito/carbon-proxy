import asyncio
import pytest
import time

from carbon_proxy.utils import threaded


pytestmark = pytest.mark.asyncio


async def test_simple(event_loop, timer):
    sleep = threaded(time.sleep)

    with timer(1):
        await asyncio.gather(
            sleep(1),
            sleep(1),
            sleep(1),
            sleep(1),
            sleep(1),
        )
