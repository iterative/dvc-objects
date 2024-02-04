import asyncio
from functools import wraps
from typing import TYPE_CHECKING, Any, BinaryIO, Callable, Dict, Optional, TypeVar, cast

import fsspec
from fsspec.callbacks import DEFAULT_CALLBACK, Callback, NoOpCallback

if TYPE_CHECKING:
    from typing import Union

    from dvc_objects._tqdm import Tqdm

F = TypeVar("F", bound=Callable)

__all__ = ["Callback", "NoOpCallback", "TqdmCallback", "DEFAULT_CALLBACK"]


class CallbackStream:
    def __init__(self, stream, callback: Callback):
        self.stream = stream

        @wraps(stream.read)
        def read(*args, **kwargs):
            data = stream.read(*args, **kwargs)
            callback.relative_update(len(data))
            return data

        self.read = read

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


class TqdmCallback(fsspec.callbacks.TqdmCallback):
    def __init__(
        self,
        size: Optional[int] = None,
        value: int = 0,
        progress_bar: Optional["Tqdm"] = None,
        **tqdm_kwargs,
    ):
        from dvc_objects._tqdm import Tqdm

        tqdm_kwargs.pop("total", None)
        super().__init__(tqdm_kwargs=tqdm_kwargs, tqdm_cls=Tqdm, size=size, value=value)
        if progress_bar:
            self.tqdm = progress_bar

    def branched(
        self,
        path_1: "Union[str, BinaryIO]",
        path_2: str,
        kwargs: Dict[str, Any],
    ):
        desc = path_1 if isinstance(path_1, str) else path_2
        return TqdmCallback(bytes=True, desc=desc)


def wrap_fn(callback: fsspec.Callback, fn: F) -> F:
    @wraps(fn)
    async def async_wrapper(*args, **kwargs):
        res = await fn(*args, **kwargs)
        callback.relative_update()
        return res

    @wraps(fn)
    def sync_wrapper(*args, **kwargs):
        res = fn(*args, **kwargs)
        callback.relative_update()
        return res

    return async_wrapper if asyncio.iscoroutinefunction(fn) else sync_wrapper  # type: ignore[return-value]


def branch_callback(callback: fsspec.Callback, fn: F) -> F:
    @wraps(fn)
    def sync_wrapper(path1: "Union[str, BinaryIO]", path2: str, **kwargs):
        with callback.branched(path1, path2):
            return fn(path1, path2, **kwargs)

    return callback.branch_coro(fn) if asyncio.iscoroutinefunction(fn) else sync_wrapper  # type: ignore[return-value]


def wrap_and_branch_callback(callback: fsspec.Callback, fn: F) -> F:
    branch_wrapper = branch_callback(callback, fn)
    return wrap_fn(callback, branch_wrapper)


def wrap_file(file, callback: fsspec.Callback) -> BinaryIO:
    return cast(BinaryIO, CallbackStream(file, callback))
