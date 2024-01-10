import asyncio
from functools import wraps
from typing import TYPE_CHECKING, Any, BinaryIO, Callable, Dict, Optional, TypeVar, cast

import fsspec

if TYPE_CHECKING:
    from typing import Union

    from dvc_objects._tqdm import Tqdm

F = TypeVar("F", bound=Callable)


class CallbackStream:
    def __init__(self, stream, callback: fsspec.Callback):
        self.stream = stream

        @wraps(stream.read)
        def read(*args, **kwargs):
            data = stream.read(*args, **kwargs)
            callback.relative_update(len(data))
            return data

        self.read = read

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


class ScopedCallback(fsspec.Callback):
    def __enter__(self):
        return self

    def __exit__(self, *exc_args):
        self.close()

    def close(self):
        """Handle here on exit."""

    def branch(
        self,
        path_1: "Union[str, BinaryIO]",
        path_2: str,
        kwargs: Dict[str, Any],
        child: Optional["Callback"] = None,
    ) -> "Callback":
        child = kwargs["callback"] = child or DEFAULT_CALLBACK
        return child


class Callback(ScopedCallback):
    def absolute_update(self, value: int) -> None:
        value = value if value is not None else self.value
        return super().absolute_update(value)

    @classmethod
    def as_callback(
        cls, maybe_callback: Optional[fsspec.Callback] = None
    ) -> "Callback":
        if maybe_callback is None:
            return DEFAULT_CALLBACK
        if isinstance(maybe_callback, Callback):
            return maybe_callback
        return FsspecCallbackWrapper(maybe_callback)


class NoOpCallback(Callback, fsspec.callbacks.NoOpCallback):
    pass


class TqdmCallback(Callback):
    def __init__(
        self,
        size: Optional[int] = None,
        value: int = 0,
        progress_bar: Optional["Tqdm"] = None,
        **tqdm_kwargs,
    ):
        from dvc_objects._tqdm import Tqdm

        tqdm_kwargs.pop("total", None)
        self._tqdm_kwargs = tqdm_kwargs
        self._tqdm_cls = Tqdm
        self.tqdm = progress_bar
        super().__init__(size=size, value=value)

    def close(self):
        if self.tqdm is not None:
            self.tqdm.close()
        self.tqdm = None

    def call(self, hook_name=None, **kwargs):
        if self.tqdm is None:
            self.tqdm = self._tqdm_cls(**self._tqdm_kwargs, total=self.size or -1)
        self.tqdm.update_to(self.value, total=self.size)

    def branch(
        self,
        path_1: "Union[str, BinaryIO]",
        path_2: str,
        kwargs: Dict[str, Any],
        child: Optional[Callback] = None,
    ):
        desc = path_1 if isinstance(path_1, str) else path_2
        child = child or TqdmCallback(bytes=True, desc=desc)
        return super().branch(path_1, path_2, kwargs, child=child)


class FsspecCallbackWrapper(Callback):
    def __init__(self, callback: fsspec.Callback):
        object.__setattr__(self, "_callback", callback)

    def __getattr__(self, name: str):
        return getattr(self._callback, name)

    def __setattr__(self, name: str, value: Any):
        setattr(self._callback, name, value)

    def absolute_update(self, value: int) -> None:
        value = value if value is not None else self.value
        return self._callback.absolute_update(value)

    def branch(
        self,
        path_1: "Union[str, BinaryIO]",
        path_2: str,
        kwargs: Dict[str, Any],
        child: Optional["Callback"] = None,
    ) -> "Callback":
        if not child:
            self._callback.branch(path_1, path_2, kwargs)
            child = self.as_callback(kwargs.get("callback"))
        return super().branch(path_1, path_2, kwargs, child=child)


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
    callback = Callback.as_callback(callback)

    @wraps(fn)
    async def async_wrapper(path1: "Union[str, BinaryIO]", path2: str, **kwargs):
        with callback.branch(path1, path2, kwargs):
            return await fn(path1, path2, **kwargs)

    @wraps(fn)
    def sync_wrapper(path1: "Union[str, BinaryIO]", path2: str, **kwargs):
        with callback.branch(path1, path2, kwargs):
            return fn(path1, path2, **kwargs)

    return async_wrapper if asyncio.iscoroutinefunction(fn) else sync_wrapper  # type: ignore[return-value]


def wrap_and_branch_callback(callback: fsspec.Callback, fn: F) -> F:
    branch_wrapper = branch_callback(callback, fn)
    return wrap_fn(callback, branch_wrapper)


def wrap_file(file, callback: fsspec.Callback) -> BinaryIO:
    return cast(BinaryIO, CallbackStream(file, callback))


DEFAULT_CALLBACK = NoOpCallback()
