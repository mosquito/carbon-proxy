import asyncio
from typing import Callable

from concurrent.futures._base import Executor
from functools import partial
from multiprocessing.pool import ThreadPool as ThreadPoolBase, RUN


class ThreadPoolExecutor(ThreadPoolBase, Executor):
    def __init__(self, processes=None, initializer=None, initargs=(),
                 loop: asyncio.AbstractEventLoop=None):

        ThreadPoolBase.__init__(self, processes, initializer, initargs)
        self._loop = loop
        self._futures = set()

    @staticmethod
    def _in_thread(future: asyncio.Future, func: Callable,
                   loop: asyncio.AbstractEventLoop):
        try:
            result = func()
        except Exception as e:
            if future.done():
                return

            loop.call_soon_threadsafe(future.set_exception, e)
        else:
            if future.done():
                return

            loop.call_soon_threadsafe(future.set_result, result)

    def submit(self, fn, *args, **kwargs):
        future = self._loop.create_future()     # type: asyncio.Future
        self._futures.add(future)

        future.add_done_callback(self._futures.remove)

        self._loop.call_soon_threadsafe(
            partial(
                self.apply_async,
                self._in_thread,
                kwds=dict(
                    future=future,
                    func=partial(fn, *args, **kwargs),
                    loop=self._loop
                )
            )
        )

        return future

    def shutdown(self, wait=True):
        if self._state != RUN:
            return

        for future in self._futures:
            if future.done():
                continue

            future.set_exception(
                RuntimeError('%s shutting down' % self.__class__.__name__)
            )

        self.terminate()

        if wait:
            self.join()

    def imap_func(self, *func):
        return self.imap(func=lambda x: x(), iterable=func)
