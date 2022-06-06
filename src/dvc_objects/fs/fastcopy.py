import errno
import os
import sys
from shutil import _fastcopy_fcopyfile as _fcopyfile  # type: ignore
from shutil import _fastcopy_sendfile as _sendfile  # type: ignore
from shutil import _GiveupOnFastCopy  # type: ignore
from shutil import copyfileobj as _copyfileobj_shutil

try:
    import posix  # type:ignore
except ImportError:
    posix = None  # type: ignore


_WINDOWS = os.name == "nt"
COPY_BUFSIZE = 1024 * 1024 if _WINDOWS else 2**20
_USE_CP_SENDFILE = hasattr(os, "sendfile") and sys.platform.startswith("linux")
_HAS_FCOPYFILE = posix and hasattr(posix, "_fcopyfile")  # macOS
_USE_CP_COPY_FILE_RANGE = hasattr(os, "copy_file_range")


def _determine_linux_fastcopy_blocksize(infd):
    """Determine blocksize for fastcopying on Linux.
    Hopefully the whole file will be copied in a single call.
    The copying itself should be performed in a loop 'till EOF is
    reached (0 return) so a blocksize smaller or bigger than the actual
    file size should not make any difference, also in case the file
    content changes while being copied.
    """
    try:
        blocksize = max(os.fstat(infd).st_size, 2**23)  # min 8 MiB
    except OSError:
        blocksize = 2**27  # 128 MiB
    # On 32-bit architectures truncate to 1 GiB to avoid OverflowError,
    # see gh-82500.
    if sys.maxsize < 2**32:
        blocksize = min(blocksize, 2**30)
    return blocksize


def _copy_file_range(fsrc, fdst):
    """Copy data from one regular mmap-like fd to another by using
    a high-performance copy_file_range(2) syscall that gives filesystems
    an opportunity to implement the use of reflinks or server-side copy.
    This should work on Linux >= 4.5 only.

    See https://github.com/python/cpython/pull/93152.
    """
    try:
        infd = fsrc.fileno()
        outfd = fdst.fileno()
    except Exception as err:
        raise _GiveupOnFastCopy(err)  # not a regular file

    blocksize = _determine_linux_fastcopy_blocksize(infd)
    offset = 0
    while True:
        try:
            n_copied = os.copy_file_range(  # pylint: disable=no-member
                infd, outfd, blocksize, offset_dst=offset
            )
        except OSError as err:
            # ...in order to have a more informative exception.
            err.filename = fsrc.name
            err.filename2 = fdst.name

            if err.errno == errno.ENOSPC:  # filesystem is full
                raise err from None

            # Give up on first call and if no data was copied.
            if offset == 0 and os.lseek(outfd, 0, os.SEEK_CUR) == 0:
                raise _GiveupOnFastCopy(err)

            raise err
        else:
            if n_copied == 0:
                # If no bytes have been copied yet, copy_file_range
                # might silently fail.
                # https://lore.kernel.org/linux-fsdevel/20210126233840.GG4626@dread.disaster.area/T/#m05753578c7f7882f6e9ffe01f981bc223edef2b0
                if offset == 0:
                    raise _GiveupOnFastCopy()
                break
            offset += n_copied


def _copyfileobj_readinto(fsrc, fdst, callback=None, length=COPY_BUFSIZE):
    """readinto()/memoryview() based variant of copyfileobj().
    *fsrc* must support readinto() method and both files must be
    open in binary mode.
    """
    # Localize variable access to minimize overhead.
    fsrc_readinto = fsrc.readinto
    fdst_write = fdst.write
    with memoryview(bytearray(length)) as mv:
        while n := fsrc_readinto(mv):
            if callback:
                callback.relative_update(n)
            if n >= length:
                fdst_write(mv)
                continue

            with mv[:n] as smv:
                fdst.write(smv)


def _copyfileobj(fsrc, fdst, callback=None, length=COPY_BUFSIZE):
    file_size = os.fstat(fsrc.fileno()).st_size
    if callback:
        callback.set_size(file_size)

    if _WINDOWS and file_size > 0:
        # Windows, see:
        # https://github.com/python/cpython/pull/7160#discussion_r195405230
        return _copyfileobj_readinto(
            fsrc, fdst, callback, min(file_size, length)
        )

    wrapped = callback.wrap_attr(fsrc) if callback else fsrc
    _copyfileobj_shutil(wrapped, fdst, length=length)


def _copyfile(fsrc, fdst, callback):
    if _HAS_FCOPYFILE:  # macOS
        try:
            # pylint: disable=protected-access, no-member
            return _fcopyfile(
                fsrc,
                fdst,
                posix._COPYFILE_DATA,
            )
        except _GiveupOnFastCopy:
            pass

    if _USE_CP_COPY_FILE_RANGE:
        try:
            return _copy_file_range(fsrc, fdst)
        except _GiveupOnFastCopy:
            pass

    if _USE_CP_SENDFILE:
        try:
            return _sendfile(fsrc, fdst)
        except _GiveupOnFastCopy:
            pass

    return _copyfileobj(fsrc, fdst, callback)


def copyfile(src, dst, *, callback=None, copy_function=_copyfile):
    from .callbacks import Callback

    with open(src, "rb") as fsrc:
        try:
            with open(dst, "wb") as fdst, Callback.as_callback(callback) as cb:
                copy_function(fsrc, fdst, cb)
                return dst
        except IsADirectoryError as e:
            if os.path.exists(dst):
                raise
            raise FileNotFoundError(f"Directory does not exist: {dst}") from e
