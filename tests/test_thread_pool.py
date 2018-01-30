import asyncio
from contextlib import suppress
from functools import partial

import pytest
import time

from carbon_proxy.utils import threaded
from carbon_proxy.thread_pool import ThreadPoolExecutor


@pytest.fixture
async def executor(event_loop: asyncio.AbstractEventLoop):
    thread_pool = ThreadPoolExecutor(8, loop=event_loop)
    event_loop.set_default_executor(thread_pool)
    try:
        yield thread_pool
    finally:
        with suppress(Exception):
            thread_pool.shutdown(wait=True)

        event_loop.set_default_executor(None)


@pytest.mark.asyncio
async def test_threaded(executor: ThreadPoolExecutor, timer):
    assert executor

    sleep = threaded(time.sleep)

    with timer(1):
        await asyncio.gather(
            sleep(1),
            sleep(1),
            sleep(1),
            sleep(1),
            sleep(1),
        )


@pytest.mark.asyncio
async def test_threaded_exc(executor: ThreadPoolExecutor):
    assert executor

    @threaded
    def worker():
        raise Exception

    number = 90

    done, _ = await asyncio.wait([worker() for _ in range(number)])

    for task in done:
        with pytest.raises(Exception):
            task.result()


@pytest.mark.asyncio
async def test_future_already_done(executor: ThreadPoolExecutor):
    futures = []

    for _ in range(10):
        futures.append(executor.submit(time.sleep, 0.1))

    for future in futures:
        future.set_exception(asyncio.CancelledError())

    await asyncio.wait(futures)


@pytest.mark.asyncio
async def test_future_when_pool_shutting_down(executor: ThreadPoolExecutor):
    futures = []

    for _ in range(10):
        futures.append(executor.submit(time.sleep, 0.1))

    executor.shutdown(wait=False)

    done, _ = await asyncio.wait(futures)

    for task in done:
        with pytest.raises(RuntimeError) as e:
            task.result()

        assert 'shutting down' in str(e)


@pytest.mark.asyncio
async def test_failed_future_already_done(executor: ThreadPoolExecutor):
    futures = []

    def exc():
        time.sleep(0.1)
        raise Exception

    for _ in range(10):
        futures.append(executor.submit(exc))

    for future in futures:
        future.set_exception(asyncio.CancelledError())

    await asyncio.wait(futures)


@pytest.mark.asyncio
async def test_cancel(executor: ThreadPoolExecutor, event_loop, timer):
    assert executor

    sleep = threaded(time.sleep)

    with timer(1, dispersion=2):
        tasks = [event_loop.create_task(sleep(1)) for _ in range(1000)]

        await asyncio.sleep(1)

        for task in tasks:
            task.cancel()

        executor.shutdown(wait=True)


def test_imap_func(executor: ThreadPoolExecutor, timer):
    with timer(1):
        counter = 0
        func_list = [partial(time.sleep, 1) for _ in range(8)]

        for _ in executor.imap_func(*func_list):
            counter += 1

        assert counter == 8


def test_imap_func_exc(executor: ThreadPoolExecutor):
    def worker():
        raise Exception

    with pytest.raises(Exception):
        for _ in executor.imap_func(*[worker for _ in range(9)]):
            pass
