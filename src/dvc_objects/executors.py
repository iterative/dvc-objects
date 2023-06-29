import asyncio
import queue
import sys
from concurrent import futures
from itertools import islice
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
)

from .fs.callbacks import Callback

_T = TypeVar("_T")


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    _max_workers: int

    def __init__(
        self, max_workers: int = None, cancel_on_error: bool = False, **kwargs
    ):
        super().__init__(max_workers=max_workers, **kwargs)
        self._cancel_on_error = cancel_on_error

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def imap_unordered(
        self, fn: Callable[..., _T], *iterables: Iterable[Any]
    ) -> Iterator[_T]:
        """Lazier version of map that does not preserve ordering of results.

        It does not create all the futures at once to reduce memory usage.
        """

        def create_taskset(n: int) -> Set[futures.Future]:
            return {self.submit(fn, *args) for args in islice(it, n)}

        it = zip(*iterables)
        tasks = create_taskset(self.max_workers * 5)
        while tasks:
            done, tasks = futures.wait(tasks, return_when=futures.FIRST_COMPLETED)
            for fut in done:
                yield fut.result()
            tasks.update(create_taskset(len(done)))

    def shutdown(  # pylint: disable=arguments-differ
        self, wait=True, *, cancel_futures=False
    ):
        if sys.version_info > (3, 9):
            # pylint: disable=unexpected-keyword-arg
            return super().shutdown(wait=wait, cancel_futures=cancel_futures)

        with self._shutdown_lock:
            self._shutdown = True
            if cancel_futures:
                # Drain all work items from the queue, and then cancel their
                # associated futures.
                while True:
                    try:
                        work_item = self._work_queue.get_nowait()
                    except queue.Empty:
                        break
                    if work_item is not None:
                        work_item.future.cancel()

            # Send a wake-up to prevent threads calling
            # _work_queue.get(block=True) from permanently blocking.
            self._work_queue.put(None)
        if wait:
            for t in self._threads:
                t.join()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._cancel_on_error:
            self.shutdown(wait=True, cancel_futures=exc_val is not None)
        else:
            self.shutdown(wait=True)
        return False


async def batch_coros(
    coros: Sequence[Coroutine],
    batch_size: Optional[int] = None,
    callback: Optional[Callback] = None,
    timeout: Optional[int] = None,
    return_exceptions: bool = False,
    nofiles: bool = False,
) -> List[Any]:
    """Run the given in coroutines in parallel.

    The asyncio loop will be kept saturated with up to `batch_size` tasks in
    the loop at a time.

    Tasks are not guaranteed to run in order, but results are returned in the
    original order.
    """
    from fsspec.asyn import _get_batch_size

    if batch_size is None:
        batch_size = _get_batch_size(nofiles=nofiles)
    if batch_size == -1:
        batch_size = len(coros)
    assert batch_size > 0

    def create_taskset(n: int) -> Dict[Awaitable, int]:
        return {asyncio.create_task(coro): i for i, coro in islice(it, n)}

    it: Iterator[Tuple[int, Coroutine]] = enumerate(coros)
    tasks = create_taskset(batch_size)
    results: Dict[int, Any] = {}
    while tasks:
        done, pending = await asyncio.wait(
            tasks.keys(), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if not done and timeout:
            for pending_fut in pending:
                pending_fut.cancel()
            raise TimeoutError
        for fut in done:
            try:
                result = fut.result()
            except Exception as exc:  # pylint: disable=broad-except
                if not return_exceptions:
                    for pending_fut in pending:
                        pending_fut.cancel()
                    raise
                result = exc
            index = tasks.pop(fut)
            results[index] = result
            if callback is not None:
                callback.relative_update()

        tasks.update(create_taskset(len(done)))

    return [results[k] for k in sorted(results)]
