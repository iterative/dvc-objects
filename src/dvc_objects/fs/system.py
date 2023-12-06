import ctypes
import errno
import logging
import os
import stat
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import AnyFSPath

logger = logging.getLogger(__name__)


def hardlink(source: "AnyFSPath", link_name: "AnyFSPath") -> None:
    # NOTE: we should really be using `os.link()` here with
    # `follow_symlinks=True`, but unfortunately the implementation is
    # buggy across platforms, so until it is fixed, we just dereference
    # the symlink ourselves here.
    #
    # See https://bugs.python.org/issue41355 for more info.
    st = os.lstat(source)
    if stat.S_ISLNK(st.st_mode):
        src = os.path.realpath(source)
    else:
        src = source

    os.link(src, link_name)


def symlink(source: "AnyFSPath", link_name: "AnyFSPath") -> None:
    os.symlink(source, link_name)


def _clonefile():
    def _cdll(name):
        return ctypes.CDLL(name, use_errno=True)

    LIBC = "libc.dylib"
    LIBC_FALLBACK = "/usr/lib/libSystem.dylib"
    try:
        clib = _cdll(LIBC)
    except OSError as exc:
        logger.debug(
            "unable to access '%s' (errno '%d'). Falling back to '%s'.",
            LIBC,
            exc.errno,
            LIBC_FALLBACK,
        )
        if exc.errno != errno.ENOENT:
            raise
        # NOTE: trying to bypass System Integrity Protection (SIP)
        clib = _cdll(LIBC_FALLBACK)

    clonefile = getattr(clib, "clonefile", None)
    if clonefile is None:
        raise OSError(
            errno.ENOTSUP,
            "'clonefile' not supported by the standard library",
        )

    def errcheck(ret, _func, _args):
        if ret:
            err = ctypes.get_errno()
            msg = os.strerror(err)
            raise OSError(err, msg)
        return ret

    clonefile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
    clonefile.restype = ctypes.c_int
    clonefile.errcheck = errcheck

    return clonefile


# NOTE: reflink may (macos) or may not (linux) clone permissions,
# so the user needs to handle those himself.
if sys.platform == "darwin":
    clonefile = _clonefile()

    def reflink(src, dst):
        clonefile(
            ctypes.c_char_p(os.fsencode(src)),
            ctypes.c_char_p(os.fsencode(dst)),
            ctypes.c_int(0),
        )

elif sys.platform == "linux":
    import fcntl  # pylint: disable=import-error

    FICLONE = 0x40049409

    def reflink(src, dst):
        try:
            with open(src, "rb") as s, open(dst, "wb+") as d:
                fcntl.ioctl(d.fileno(), FICLONE, s.fileno())
        except OSError:
            try:
                os.unlink(dst)
            except OSError:
                pass
            raise
else:

    def reflink(src, dst):
        raise OSError(
            errno.ENOTSUP,
            f"reflink is not supported on '{sys.platform}'",
        )


def inode(path: "AnyFSPath") -> int:
    return os.lstat(path).st_ino


def is_symlink(path: "AnyFSPath") -> bool:
    return os.path.islink(path)


def is_hardlink(path: "AnyFSPath") -> bool:
    return os.stat(path).st_nlink > 1
