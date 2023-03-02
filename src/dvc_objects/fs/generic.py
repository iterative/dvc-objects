import asyncio
import errno
import logging
import os
from contextlib import suppress
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union

from fsspec.asyn import get_loop

from ..executors import ThreadPoolExecutor, batch_coros
from .callbacks import DEFAULT_CALLBACK
from .local import LocalFileSystem, localfs
from .utils import as_atomic

if TYPE_CHECKING:
    from .base import AnyFSPath, FileSystem
    from .callbacks import Callback

logger = logging.getLogger(__name__)


TransferErrorHandler = Callable[["AnyFSPath", "AnyFSPath", BaseException], None]


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
    from_path: Union["AnyFSPath", List["AnyFSPath"]],
    to_fs: "FileSystem",
    to_path: Union["AnyFSPath", List["AnyFSPath"]],
    callback: "Callback" = DEFAULT_CALLBACK,
    batch_size: Optional[int] = None,
    on_error: Optional[TransferErrorHandler] = None,
) -> None:
    # NOTE: We intentionally do not use fs.get()/fs.put() here.
    # get/put support batching but also include fsspec overhead from doing path
    # and recursive directory expension that we don't want in copy/transfer

    if isinstance(from_path, str):
        from_path = [from_path]
    if isinstance(to_path, str):
        to_path = [to_path]

    if isinstance(from_fs, LocalFileSystem):
        return _put(
            from_path,
            to_fs,
            to_path,
            callback=callback,
            batch_size=batch_size,
            on_error=on_error,
        )
    if isinstance(to_fs, LocalFileSystem):
        return _get(
            from_fs,
            from_path,
            to_path,
            callback=callback,
            batch_size=batch_size,
            on_error=on_error,
        )

    put_file = callback.wrap_and_branch(to_fs.put_file)

    def _copy_one(from_p: "AnyFSPath", to_p: "AnyFSPath"):
        try:
            with from_fs.open(from_p, mode="rb") as fobj:
                size = from_fs.size(from_p)
                return put_file(fobj, to_p, size=size, callback=callback)
        except Exception as exc:  # pylint: disable=broad-except
            if on_error is not None:
                on_error(from_p, to_p, exc)
            else:
                raise

    if len(from_path) == 1:
        return _copy_one(from_path[0], to_path[0])

    jobs = batch_size or to_fs.jobs
    executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
    with executor:
        list(executor.imap_unordered(_copy_one, from_path, to_path))


def _put(
    from_paths: List["AnyFSPath"],
    to_fs: "FileSystem",
    to_paths: List["AnyFSPath"],
    callback: "Callback" = DEFAULT_CALLBACK,
    batch_size: Optional[int] = None,
    on_error: Optional[TransferErrorHandler] = None,
) -> None:
    put_file = callback.wrap_and_branch(to_fs.put_file)

    def _put_one(from_path: "AnyFSPath", to_path: "AnyFSPath"):
        try:
            return put_file(from_path, to_path, callback=callback)
        except Exception as exc:  # pylint: disable=broad-except
            if on_error is not None:
                on_error(from_path, to_path, exc)
            else:
                raise

    if len(from_paths) == 1:
        return _put_one(from_paths[0], to_paths[0])

    jobs = batch_size or to_fs.jobs
    if to_fs.fs.async_impl:
        put_coro = callback.wrap_and_branch_coro(
            to_fs.fs._put_file  # pylint: disable=protected-access
        )
        loop = get_loop()
        fut = asyncio.run_coroutine_threadsafe(
            batch_coros(
                [
                    put_coro(from_path, to_path, callback=callback)
                    for from_path, to_path in zip(from_paths, to_paths)
                ],
                batch_size=jobs,
                return_exceptions=True,
            ),
            loop,
        )
        for i, result in enumerate(fut.result()):
            if isinstance(result, BaseException):
                if on_error is not None:
                    on_error(from_paths[i], to_paths[i], result)
                else:
                    raise result
        return

    executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
    with executor:
        list(executor.imap_unordered(_put_one, from_paths, to_paths))


def _get(
    from_fs: "FileSystem",
    from_paths: List["AnyFSPath"],
    to_paths: List["AnyFSPath"],
    callback: "Callback" = DEFAULT_CALLBACK,
    batch_size: Optional[int] = None,
    on_error: Optional[TransferErrorHandler] = None,
) -> None:
    get_file = callback.wrap_and_branch(from_fs.get_file)

    def _get_one(from_path: "AnyFSPath", to_path: "AnyFSPath"):
        with as_atomic(localfs, to_path, create_parents=True) as tmp_file:
            try:
                return get_file(from_path, tmp_file, callback=callback)
            except Exception as exc:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(from_path, to_path, exc)
                else:
                    raise

    if len(from_paths) == 1:
        return _get_one(from_paths[0], to_paths[0])

    jobs = batch_size or from_fs.jobs
    if from_fs.fs.async_impl:

        async def _get_one_coro(from_path: "AnyFSPath", to_path: "AnyFSPath"):
            get_coro = callback.wrap_and_branch_coro(
                from_fs.fs._get_file  # pylint: disable=protected-access
            )
            with as_atomic(localfs, to_path, create_parents=True) as tmp_file:
                return await get_coro(from_path, tmp_file, callback=callback)

        loop = get_loop()
        fut = asyncio.run_coroutine_threadsafe(
            batch_coros(
                [
                    _get_one_coro(from_path, to_path)
                    for from_path, to_path in zip(from_paths, to_paths)
                ],
                batch_size=jobs,
                return_exceptions=True,
            ),
            loop,
        )
        for i, result in enumerate(fut.result()):
            if isinstance(result, BaseException):
                if on_error is not None:
                    on_error(from_paths[i], to_paths[i], result)
                else:
                    raise result
        return

    executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
    with executor:
        list(executor.imap_unordered(_get_one, from_paths, to_paths))


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

    raise OSError(errno.ENOTSUP, "no more link types left to try out") from error


def transfer(
    from_fs: "FileSystem",
    from_path: Union["AnyFSPath", List["AnyFSPath"]],
    to_fs: "FileSystem",
    to_path: Union["AnyFSPath", List["AnyFSPath"]],
    hardlink: bool = False,
    links: Optional[List["str"]] = None,
    callback: "Callback" = DEFAULT_CALLBACK,
    batch_size: Optional[int] = None,
    on_error: Optional[TransferErrorHandler] = None,
) -> None:
    if isinstance(from_path, str):
        from_path = [from_path]
    if isinstance(to_path, str):
        to_path = [to_path]
    assert len(from_path) == len(to_path)

    assert not (hardlink and links)
    if hardlink:
        links = links or ["reflink", "hardlink", "copy"]
    else:
        links = links or ["reflink", "copy"]

    callback.set_size(len(from_path))
    # Try to link files sequentially. If/when the only remaining link type is
    # copy, the remaining copy operations will be batched.
    for i, (from_p, to_p) in enumerate(zip(from_path, to_path)):
        if links[0] == "copy":
            copy(
                from_fs,
                from_path[i:],
                to_fs,
                to_path[i:],
                callback=callback,
                batch_size=batch_size,
                on_error=on_error,
            )
            return
        try:
            _try_links(
                links,
                from_fs,
                from_p,
                to_fs,
                to_p,
                callback=callback,
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
                continue

            if on_error is not None:
                on_error(from_p, to_p, exc)
            else:
                raise
        except Exception as exc:  # pylint: disable=broad-except
            if on_error is not None:
                on_error(from_p, to_p, exc)
            else:
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
