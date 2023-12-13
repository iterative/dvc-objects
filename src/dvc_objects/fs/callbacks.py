from contextlib import ExitStack
from functools import wraps
from typing import TYPE_CHECKING, Any, Dict, Optional

import fsspec

from dvc_objects.utils import cached_property

if TYPE_CHECKING:
    from typing import BinaryIO, Callable, TypeVar, Union

    from typing_extensions import ParamSpec

    from dvc_objects._tqdm import Tqdm

    _P = ParamSpec("_P")
    _R = TypeVar("_R")


class CallbackStream:
    def __init__(self, stream, callback, method="read"):
        self.stream = stream
        if method == "write":

            @wraps(stream.write)
            def write(data, *args, **kwargs):
                res = stream.write(data, *args, **kwargs)
                callback.relative_update(len(data))
                return res

            self.write = write
        else:

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
    def relative_update(self, inc: int = 1) -> None:
        inc = inc if inc is not None else 0
        return super().relative_update(inc)

    def absolute_update(self, value: int) -> None:
        value = value if value is not None else self.value
        return super().absolute_update(value)

    @classmethod
    def as_callback(
        cls, maybe_callback: Optional[fsspec.callbacks.Callback] = None
    ) -> "Callback":
        if maybe_callback is None:
            return DEFAULT_CALLBACK
        if isinstance(maybe_callback, Callback):
            return maybe_callback
        return FsspecCallbackWrapper(maybe_callback)

    @classmethod
    def as_tqdm_callback(
        cls,
        callback: Optional[fsspec.callbacks.Callback] = None,
        **tqdm_kwargs: Any,
    ) -> "Callback":
        if callback is None:
            return TqdmCallback(**tqdm_kwargs)
        return cls.as_callback(callback)


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
        tqdm_kwargs["total"] = size or -1
        self._tqdm_kwargs = tqdm_kwargs
        self._progress_bar = progress_bar
        self._stack = ExitStack()
        super().__init__(size=size, value=value)

    @cached_property
    def progress_bar(self):
        from dvc_objects._tqdm import Tqdm

        progress_bar = (
            self._progress_bar
            if self._progress_bar is not None
            else Tqdm(**self._tqdm_kwargs)
        )
        return self._stack.enter_context(progress_bar)

    def __enter__(self):
        return self

    def close(self):
        self._stack.close()

    def set_size(self, size):
        # Tqdm tries to be smart when to refresh,
        # so we try to force it to re-render.
        super().set_size(size)
        self.progress_bar.refresh()

    def call(self, hook_name=None, **kwargs):
        self.progress_bar.update_to(self.value, total=self.size)

    def branch(
        self,
        path_1: "Union[str, BinaryIO]",
        path_2: str,
        kwargs: Dict[str, Any],
        child: Optional[Callback] = None,
    ):
        child = child or TqdmCallback(
            bytes=True, desc=path_1 if isinstance(path_1, str) else path_2
        )
        return super().branch(path_1, path_2, kwargs, child=child)


class FsspecCallbackWrapper(Callback):
    def __init__(self, callback: fsspec.callbacks.Callback):
        object.__setattr__(self, "_callback", callback)

    def __getattr__(self, name: str):
        return getattr(self._callback, name)

    def __setattr__(self, name: str, value: Any):
        setattr(self._callback, name, value)

    def relative_update(self, inc: int = 1) -> None:
        inc = inc if inc is not None else 0
        return self._callback.relative_update(inc)

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


def wrap_fn(callback: "Callback", fn: "Callable[_P, _R]") -> "Callable[_P, _R]":
    @wraps(fn)
    def wrapped(*args: "_P.args", **kwargs: "_P.kwargs") -> "_R":
        res = fn(*args, **kwargs)
        callback.relative_update()
        return res

    return wrapped


def wrap_and_branch_callback(callback: "Callback", fn: "Callable") -> "Callable":
    """
    Wraps a function, and pass a new child callback to it.
    When the function completes, we increment the parent callback by 1.
    """

    @wraps(fn)
    def func(path1: "Union[str, BinaryIO]", path2: str, **kwargs):
        with callback.branch(path1, path2, kwargs):
            res = fn(path1, path2, **kwargs)
            callback.relative_update()
            return res

    return func


def wrap_and_branch_coro(callback: "Callback", fn: "Callable") -> "Callable":
    """
    Wraps a coroutine, and pass a new child callback to it.
    When the coroutine completes, we increment the parent callback by 1.
    """

    @wraps(fn)
    async def func(path1: "Union[str, BinaryIO]", path2: str, **kwargs):
        with callback.branch(path1, path2, kwargs):
            res = await fn(path1, path2, **kwargs)
            callback.relative_update()
            return res

    return func


DEFAULT_CALLBACK = NoOpCallback()
