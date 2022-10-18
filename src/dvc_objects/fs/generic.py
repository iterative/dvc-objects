import errno
import logging
import os
from contextlib import suppress
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, List, Optional

from .callbacks import DEFAULT_CALLBACK
from .local import LocalFileSystem, localfs
from .utils import as_atomic

if TYPE_CHECKING:
    from .base import AnyFSPath, FileSystem
    from .callbacks import Callback

logger = logging.getLogger(__name__)


def log_exceptions(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(
        from_fs: "FileSystem",
        from_path: "AnyFSPath",
        to_fs: "FileSystem",
        to_path: "AnyFSPath",
        *args: Any,
        **kwargs: Any,
    ) -> int:
        try:
            func(from_fs, from_path, to_fs, to_path, *args, **kwargs)
            return 0
        except Exception as exc:  # pylint: disable=broad-except
            # NOTE: this means we ran out of file descriptors and there is no
            # reason to try to proceed, as we will hit this error anyways.
            # pylint: disable=no-member
            if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
                raise
            logger.exception("failed to transfer '%s'", from_path)
            return 1

    return wrapper


def _link(
    link: "str",
    from_fs: "FileSystem",
    from_path: "AnyFSPath",
    to_fs: "FileSystem",
    to_path: "AnyFSPath",
) -> None:
    if not isinstance(from_fs, type(to_fs)):
        raise OSError(errno.EXDEV, "can't link across filesystems")

    func = getattr(to_fs, link)
    func(from_path, to_path)


def copy(
    from_fs: "FileSystem",
    from_path: "AnyFSPath",
    to_fs: "FileSystem",
    to_path: "AnyFSPath",
    callback: "Callback" = DEFAULT_CALLBACK,
) -> None:
    get_file = callback.wrap_and_branch(from_fs.get_file)
    put_file = callback.wrap_and_branch(to_fs.put_file)

    if isinstance(from_fs, LocalFileSystem):
        return put_file(from_path, to_path, callback=callback)

    if isinstance(to_fs, LocalFileSystem):
        with as_atomic(localfs, to_path, create_parents=True) as tmp_file:
            return get_file(from_path, tmp_file, callback=callback)

    with from_fs.open(from_path, mode="rb") as fobj:
        size = from_fs.size(from_path)
        return put_file(fobj, to_path, size=size, callback=callback)


def _try_links(
    links: List["str"],
    from_fs: "FileSystem",
    from_path: "AnyFSPath",
    to_fs: "FileSystem",
    to_path: "AnyFSPath",
    callback: "Callback" = DEFAULT_CALLBACK,
) -> None:
    error = None
    while links:
        link = links[0]

        if link == "copy":
            return copy(from_fs, from_path, to_fs, to_path, callback=callback)

        try:
            _link(link, from_fs, from_path, to_fs, to_path)
            callback.relative_update()
            return
        except OSError as exc:
            if exc.errno not in (
                errno.EPERM,
                errno.ENOTSUP,
                errno.EXDEV,
                errno.ENOTTY,
            ):
                raise
            error = exc

        del links[0]

    raise OSError(
        errno.ENOTSUP, "no more link types left to try out"
    ) from error


def transfer(
    from_fs: "FileSystem",
    from_path: "AnyFSPath",
    to_fs: "FileSystem",
    to_path: "AnyFSPath",
    hardlink: bool = False,
    links: Optional[List["str"]] = None,
    callback: "Callback" = DEFAULT_CALLBACK,
) -> None:
    try:
        assert not (hardlink and links)
        if hardlink:
            links = links or ["reflink", "hardlink", "copy"]
        else:
            links = links or ["reflink", "copy"]

        _try_links(
            links, from_fs, from_path, to_fs, to_path, callback=callback
        )
    except OSError as exc:
        # If the target file already exists, we are going to simply
        # ignore the exception (#4992).
        #
        # On Windows, it is not always guaranteed that you'll get
        # FileExistsError (it might be PermissionError or a bare OSError)
        # but all of those exceptions raised from the original
        # FileExistsError so we have a separate check for that.
        if isinstance(exc, FileExistsError) or (
            os.name == "nt"
            and exc.__context__
            and isinstance(exc.__context__, FileExistsError)
        ):
            logger.debug("'%s' file already exists, skipping", to_path)
            return None

        raise


def _test_link(
    link: "str",
    from_fs: "FileSystem",
    from_file: "AnyFSPath",
    to_fs: "FileSystem",
    to_file: "AnyFSPath",
) -> bool:
    try:
        _try_links([link], from_fs, from_file, to_fs, to_file)
    except OSError as exc:
        logger.debug("link type %s is not available (%s)", link, exc)
        return False

    try:
        _is_link_func = getattr(to_fs, f"is_{link}")
        return _is_link_func(to_file)
    except AttributeError:
        pass

    return True


def test_links(
    links: List["str"],
    from_fs: "FileSystem",
    from_path: "AnyFSPath",
    to_fs: "FileSystem",
    to_path: "AnyFSPath",
) -> List["AnyFSPath"]:
    from .utils import tmp_fname

    from_file = from_fs.path.join(from_path, tmp_fname())
    to_file = to_fs.path.join(
        to_fs.path.parent(to_path),
        tmp_fname(),
    )

    from_fs.makedirs(from_fs.path.parent(from_file))
    with from_fs.open(from_file, "wb") as fobj:
        fobj.write(b"test")
    to_fs.makedirs(to_fs.path.parent(to_file))

    ret = []
    try:
        for link in links:
            try:
                if _test_link(link, from_fs, from_file, to_fs, to_file):
                    ret.append(link)
            finally:
                with suppress(FileNotFoundError):
                    to_fs.remove(to_file)
    finally:
        from_fs.remove(from_file)

    return ret
