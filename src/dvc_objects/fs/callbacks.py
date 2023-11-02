from contextlib import ExitStack
from functools import wraps
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol, TypeVar, cast, overload

import fsspec

from dvc_objects.utils import cached_property

if TYPE_CHECKING:
    from typing import Awaitable, BinaryIO, Callable, TextIO, Union

    from typing_extensions import ParamSpec

    from .._tqdm import Tqdm

    _P = ParamSpec("_P")
    _R = TypeVar("_R")


class _CallbackProtocol(Protocol):
    def relative_update(self, inc: int = 1) -> None:
        ...

    def branch(
        self,
        path_1: "Union[str, BinaryIO]",
        path_2: str,
        kwargs: Dict[str, Any],
        child: Optional["Callback"] = None,
    ) -> "Callback":
        ...


class _DVCCallbackMixin(_CallbackProtocol):
    @overload
    def wrap_attr(self, fobj: "BinaryIO", method: str = "read") -> "BinaryIO":
        ...

    @overload
    def wrap_attr(self, fobj: "TextIO", method: str = "read") -> "TextIO":
        ...

    def wrap_attr(
        self, fobj: "Union[TextIO, BinaryIO]", method: str = "read"
    ) -> "Union[TextIO, BinaryIO]":
        from tqdm.utils import CallbackIOWrapper

        wrapped = CallbackIOWrapper(self.relative_update, fobj, method)
        return cast("Union[TextIO, BinaryIO]", wrapped)

    def wrap_fn(self, fn: "Callable[_P, _R]") -> "Callable[_P, _R]":
        @wraps(fn)
        def wrapped(*args: "_P.args", **kwargs: "_P.kwargs") -> "_R":
            res = fn(*args, **kwargs)
            self.relative_update()
            return res

        return wrapped

    def wrap_coro(
        self, fn: "Callable[_P, Awaitable[_R]]"
    ) -> "Callable[_P, Awaitable[_R]]":
        @wraps(fn)
        async def wrapped(*args: "_P.args", **kwargs: "_P.kwargs") -> "_R":
            res = await fn(*args, **kwargs)
            self.relative_update()
            return res

        return wrapped

    def wrap_and_branch(self, fn: "Callable") -> "Callable":
        """
        Wraps a function, and pass a new child callback to it.
        When the function completes, we increment the parent callback by 1.
        """
        wrapped = self.wrap_fn(fn)

        @wraps(fn)
        def func(path1: "Union[str, BinaryIO]", path2: str, **kwargs):
            kw: Dict[str, Any] = dict(kwargs)
            with self.branch(path1, path2, kw):  # pylint: disable=not-context-manager
                return wrapped(path1, path2, **kw)

        return func

    def wrap_and_branch_coro(self, fn: "Callable") -> "Callable":
        """
        Wraps a coroutine, and pass a new child callback to it.
        When the coroutine completes, we increment the parent callback by 1.
        """
        wrapped = self.wrap_coro(fn)

        @wraps(fn)
        async def func(path1: "Union[str, BinaryIO]", path2: str, **kwargs):
            kw: Dict[str, Any] = dict(kwargs)
            with self.branch(path1, path2, kw):  # pylint: disable=not-context-manager
                return await wrapped(path1, path2, **kw)

        return func

    def __enter__(self):
        return self

    def __exit__(self, *exc_args):
        self.close()

    def close(self):
        """Handle here on exit."""

    @classmethod
    def as_tqdm_callback(
        cls,
        callback: Optional[fsspec.callbacks.Callback] = None,
        **tqdm_kwargs: Any,
    ) -> "Callback":
        if callback is None:
            return TqdmCallback(**tqdm_kwargs)
        if isinstance(callback, Callback):
            return callback
        return cast("Callback", _FsspecCallbackWrapper(callback))


class Callback(fsspec.Callback, _DVCCallbackMixin):
    """Callback usable as a context manager, and a few helper methods."""

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
        return _FsspecCallbackWrapper(maybe_callback)

    def branch(  # pylint: disable=arguments-differ
        self,
        path_1: "Union[str, BinaryIO]",
        path_2: str,
        kwargs: Dict[str, Any],
        child: Optional["Callback"] = None,
    ) -> "Callback":
        child = kwargs["callback"] = child or DEFAULT_CALLBACK
        return child


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
        from .._tqdm import Tqdm

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


class _FsspecCallbackWrapper(fsspec.callbacks.Callback, _DVCCallbackMixin):
    def __init__(  # pylint: disable=super-init-not-called
        self, callback: fsspec.callbacks.Callback
    ):
        object.__setattr__(self, "_callback", callback)

    def __getattr__(self, name: str):
        return getattr(self._callback, name)

    def __setattr__(self, name: str, value: Any):
        setattr(self._callback, name, value)

    def branch(self, *args, **kwargs):
        return _FsspecCallbackWrapper(self._callback.branch(*args, **kwargs))


DEFAULT_CALLBACK = NoOpCallback()
