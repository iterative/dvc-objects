from functools import wraps
from typing import TYPE_CHECKING, BinaryIO, Optional, Type, cast

import fsspec
from fsspec.callbacks import DEFAULT_CALLBACK, Callback, NoOpCallback

if TYPE_CHECKING:
    from typing import Union

    from tqdm import tqdm


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
        progress_bar: Optional["tqdm"] = None,
        tqdm_cls: Optional[Type["tqdm"]] = None,
        **tqdm_kwargs,
    ):
        from dvc_objects._tqdm import Tqdm

        tqdm_kwargs.pop("total", None)
        tqdm_cls = tqdm_cls or Tqdm
        super().__init__(
            tqdm_kwargs=tqdm_kwargs, tqdm_cls=tqdm_cls, size=size, value=value
        )
        if progress_bar is None:
            self.tqdm = progress_bar

    def branched(self, path_1: "Union[str, BinaryIO]", path_2: str, **kwargs):
        desc = path_1 if isinstance(path_1, str) else path_2
        return TqdmCallback(bytes=True, desc=desc)


def wrap_file(file, callback: Callback) -> BinaryIO:
    return cast(BinaryIO, CallbackStream(file, callback))
