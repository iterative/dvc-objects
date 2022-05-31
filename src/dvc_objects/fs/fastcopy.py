import os
import posix
import sys
from shutil import _fastcopy_fcopyfile  # type: ignore[attr-defined]
from shutil import _fastcopy_sendfile  # type: ignore[attr-defined]
from shutil import _GiveupOnFastCopy  # type: ignore[attr-defined]
from shutil import copyfileobj

_WINDOWS = os.name == "nt"
COPY_BUFSIZE = 1024 * 1024 if _WINDOWS else 2**20
_USE_CP_SENDFILE = hasattr(os, "sendfile") and sys.platform.startswith("linux")
_HAS_FCOPYFILE = posix and hasattr(posix, "_fcopyfile")  # macOS


def _copyfileobj_readinto(fsrc, fdst, callback, length=COPY_BUFSIZE):
    """readinto()/memoryview() based variant of copyfileobj().
    *fsrc* must support readinto() method and both files must be
    open in binary mode.
    """
    # Localize variable access to minimize overhead.
    fsrc_readinto = fsrc.readinto
    fdst_write = fdst.write
    with memoryview(bytearray(length)) as mv:
        while n := fsrc_readinto(mv):
            callback.relative_update(n)
            if n >= length:
                fdst_write(mv)
                continue

            with mv[:n] as smv:
                fdst.write(smv)


def _copyfileobj(fsrc, fdst, callback, length=COPY_BUFSIZE):
    file_size = os.fstat(fsrc.fileno()).st_size
    callback.set_size(file_size)

    if _WINDOWS and file_size > 0:
        # Windows, see:
        # https://github.com/python/cpython/pull/7160#discussion_r195405230
        return _copyfileobj_readinto(
            fsrc, fdst, callback, min(file_size, length)
        )

    wrapped = callback.wrap_attr(fsrc)
    copyfileobj(wrapped, fdst, length=length)


def _copyfile(fsrc, fdst, callback):
    if _HAS_FCOPYFILE:  # macOS
        try:
            return _fastcopy_fcopyfile(fsrc, fdst, posix._COPYFILE_DATA)
        except _GiveupOnFastCopy:
            pass

    if _USE_CP_SENDFILE:
        try:
            return _fastcopy_sendfile(fsrc, fdst)
        except _GiveupOnFastCopy:
            pass

    return _copyfileobj(fsrc, fdst, callback)


def copyfile(src, dst, *, callback=None):
    from .callbacks import Callback

    with open(src, "rb") as fsrc:
        try:
            with open(dst, "wb") as fdst, Callback.as_callback(callback) as cb:
                _copyfile(fsrc, fdst, cb)
                return dst
        except IsADirectoryError as e:
            if os.path.exists(dst):
                raise
            raise FileNotFoundError(f"Directory does not exist: {dst}") from e
