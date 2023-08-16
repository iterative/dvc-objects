import asyncio
import datetime
import logging
import os
import shutil
from functools import partial
from multiprocessing import cpu_count
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
    overload,
)

from fsspec.asyn import get_loop
from funcy import cached_property, once_per_args

from ..executors import ThreadPoolExecutor, batch_coros
from .callbacks import DEFAULT_CALLBACK, Callback
from .errors import RemoteMissingDepsError

if TYPE_CHECKING:
    from typing import BinaryIO, TextIO

    from fsspec.spec import AbstractFileSystem

    from .path import Path


logger = logging.getLogger(__name__)


FSPath = str
AnyFSPath = str

# An info() entry, might evolve to a TypedDict
# in the future (e.g for properly type 'size' etc).
Entry = Dict[str, Any]


class LinkError(OSError):
    def __init__(self, link: str, fs: "FileSystem", path: str) -> None:
        import errno

        super().__init__(
            errno.EPERM,
            f"{link} is not supported for {fs.protocol} by {type(fs)}",
            path,
        )


@once_per_args
def check_required_version(
    pkg: str, dist: str = "dvc_objects", log_level=logging.WARNING
):
    from importlib import metadata

    from packaging.requirements import InvalidRequirement, Requirement

    try:
        reqs = {
            r.name: r.specifier for r in map(Requirement, metadata.requires(dist) or [])
        }
        version = metadata.version(pkg)
    except (metadata.PackageNotFoundError, InvalidRequirement):
        return

    specifier = reqs.get(pkg)
    if specifier and version and version not in specifier:
        logger.log(
            log_level,
            "'%s%s' is required, but you have %r installed which is incompatible.",
            pkg,
            specifier,
            version,
        )


class FileSystem:
    sep = "/"

    protocol = "base"
    REQUIRES: ClassVar[Dict[str, str]] = {}
    _JOBS = 4 * cpu_count()

    HASH_JOBS = max(1, min(4, cpu_count() // 2))
    LIST_OBJECT_PAGE_SIZE = 1000
    TRAVERSE_WEIGHT_MULTIPLIER = 5
    TRAVERSE_PREFIX_LEN = 2
    TRAVERSE_THRESHOLD_SIZE = 500000
    CAN_TRAVERSE = True

    PARAM_CHECKSUM: ClassVar[Optional[str]] = None

    def __init__(self, fs=None, **kwargs: Any):
        self._check_requires(**kwargs)

        self.jobs = kwargs.get("jobs") or self._JOBS
        self.hash_jobs = kwargs.get("checksum_jobs") or self.HASH_JOBS
        self._config = kwargs
        if fs:
            self.fs = fs

    @cached_property
    def fs_args(self) -> Dict[str, Any]:
        ret = {"skip_instance_cache": True}
        ret.update(self._prepare_credentials(**self._config))
        return ret

    @property
    def config(self) -> Dict[str, Any]:
        return self._config

    @property
    def root_marker(self) -> str:
        return self.fs.root_marker

    @cached_property
    def path(self) -> "Path":
        from .path import Path

        def _getcwd():
            return self.fs.root_marker

        return Path(self.sep, getcwd=_getcwd)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        return path

    def unstrip_protocol(self, path: str) -> str:
        return path

    @cached_property
    def fs(self) -> "AbstractFileSystem":  # pylint: disable=method-hidden
        raise NotImplementedError

    @cached_property
    def version_aware(self) -> bool:
        return self._config.get("version_aware", False)

    @staticmethod
    def _get_kwargs_from_urls(urlpath: str) -> "Dict[str, Any]":
        from fsspec.utils import infer_storage_options

        options = infer_storage_options(urlpath)
        options.pop("path", None)
        options.pop("protocol", None)
        return options

    def _prepare_credentials(
        self, **config: Dict[str, Any]  # pylint: disable=unused-argument
    ) -> Dict[str, Any]:
        """Prepare the arguments for authentication to the
        host filesystem"""
        return {}

    @classmethod
    def get_missing_deps(cls) -> List[str]:
        from importlib.util import find_spec

        return [pkg for pkg, mod in cls.REQUIRES.items() if not find_spec(mod)]

    def _check_requires(self, **kwargs):
        from .scheme import Schemes

        check_required_version(pkg="fsspec")
        missing = self.get_missing_deps()
        if not missing:
            return

        proto = self.protocol
        if proto == Schemes.WEBDAVS:
            proto = Schemes.WEBDAV

        url = kwargs.get("url", f"{self.protocol}://")
        raise RemoteMissingDepsError(self, proto, url, missing)

    def isdir(self, path: AnyFSPath) -> bool:
        return self.fs.isdir(path)

    def isfile(self, path: AnyFSPath) -> bool:
        return self.fs.isfile(path)

    def is_empty(self, path: AnyFSPath) -> bool:
        entry = self.info(path)
        if entry["type"] == "directory":
            return not self.fs.ls(path)
        return entry["size"] == 0

    @overload
    def open(
        self,
        path: AnyFSPath,
        mode: Literal["rb", "br", "wb"],
        **kwargs: Any,
    ) -> "BinaryIO":  # pylint: disable=arguments-differ
        return self.open(path, mode, **kwargs)

    @overload
    def open(
        self,
        path: AnyFSPath,
        mode: Literal["r", "rt", "w"] = "r",
        **kwargs: Any,
    ) -> "TextIO":  # pylint: disable=arguments-differ
        ...

    def open(
        self,
        path: AnyFSPath,
        mode: str = "r",
        **kwargs: Any,
    ) -> "IO[Any]":  # pylint: disable=arguments-differ
        if "b" in mode:
            kwargs.pop("encoding", None)
        return self.fs.open(path, mode=mode, **kwargs)

    def read_block(
        self,
        path: AnyFSPath,
        offset: int,
        length: int,
        delimiter: bytes = None,
    ) -> bytes:
        return self.fs.read_block(path, offset, length, delimiter=delimiter)

    def cat(
        self,
        path: Union[AnyFSPath, List[AnyFSPath]],
        recursive: bool = False,
        on_error: Literal["raise", "omit", "return"] = "raise",
        **kwargs: Any,
    ) -> Union[bytes, Dict[AnyFSPath, bytes]]:
        return self.fs.cat(path, recursive=recursive, on_error=on_error, **kwargs)

    def cat_ranges(
        self,
        paths: List[AnyFSPath],
        starts: List[int],
        ends: List[int],
        max_gap: int = None,
        **kwargs,
    ) -> List[bytes]:
        return self.fs.cat_ranges(paths, starts, ends, max_gap=max_gap, **kwargs)

    def cat_file(
        self,
        path: AnyFSPath,
        start: int = None,
        end: int = None,
        **kwargs: Any,
    ) -> bytes:
        return self.fs.cat_file(path, start=start, end=end, **kwargs)

    def head(self, path: AnyFSPath, size: int = 1024) -> bytes:
        return self.fs.head(path, size=size)

    def tail(self, path: AnyFSPath, size: int = 1024) -> bytes:
        return self.fs.tail(path, size=size)

    def pipe_file(self, path: AnyFSPath, value: bytes, **kwargs: Any) -> None:
        return self.fs.pipe_file(path, value, **kwargs)

    write_bytes = pipe_file
    read_bytes = cat_file

    def read_text(
        self,
        path: AnyFSPath,
        encoding: str = None,
        errors: str = None,
        newline: str = None,
        **kwargs: Any,
    ) -> str:
        return self.fs.read_text(
            path, encoding=encoding, errors=errors, newline=newline, **kwargs
        )

    def write_text(
        self,
        path: AnyFSPath,
        value: str,
        encoding: str = None,
        errors: str = None,
        newline: str = None,
        **kwargs: Any,
    ) -> None:
        self.fs.write_text(
            path,
            value,
            encoding=encoding,
            errors=errors,
            newline=newline,
            **kwargs,
        )

    def pipe(
        self,
        path: Union[AnyFSPath, Dict[AnyFSPath, bytes]],
        value: Optional[bytes] = None,
        **kwargs: Any,
    ) -> None:
        return self.fs.pipe(path, value=value, **kwargs)

    def touch(self, path: AnyFSPath, truncate: bool = True, **kwargs: Any) -> None:
        return self.fs.touch(path, truncate=truncate, **kwargs)

    def checksum(self, path: AnyFSPath) -> str:
        return self.fs.checksum(path)

    def copy(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        self.makedirs(self.path.parent(to_info))
        self.fs.copy(from_info, to_info)

    def cp_file(self, from_info: AnyFSPath, to_info: AnyFSPath, **kwargs: Any) -> None:
        self.fs.cp_file(from_info, to_info, **kwargs)

    @overload
    def exists(
        self,
        path: AnyFSPath,
        callback: "Callback" = ...,
        batch_size: Optional[int] = ...,
    ) -> bool:
        ...

    @overload
    def exists(
        self,
        path: List[AnyFSPath],
        callback: "Callback" = ...,
        batch_size: Optional[int] = ...,
    ) -> List[bool]:
        ...

    def exists(
        self,
        path: Union[AnyFSPath, List[AnyFSPath]],
        callback: "Callback" = DEFAULT_CALLBACK,
        batch_size: Optional[int] = None,
    ) -> Union[bool, List[bool]]:
        if isinstance(path, str):
            return self.fs.exists(path)
        callback.set_size(len(path))
        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            loop = get_loop()
            fut = asyncio.run_coroutine_threadsafe(
                batch_coros(
                    [
                        self.fs._exists(p)  # pylint: disable=protected-access
                        for p in path
                    ],
                    batch_size=jobs,
                    callback=callback,
                ),
                loop,
            )
            return fut.result()
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
        with executor:
            return list(executor.map(callback.wrap_fn(self.fs.exists), path))

    def lexists(self, path: AnyFSPath) -> bool:
        return self.fs.lexists(path)

    def symlink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.symlink(from_info, to_info)
        except AttributeError:
            raise LinkError("symlink", self, from_info)

    def link(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.link(from_info, to_info)
        except AttributeError:
            raise LinkError("hardlink", self, from_info)

    hardlink = link

    def reflink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.reflink(from_info, to_info)
        except AttributeError:
            raise LinkError("reflink", self, from_info)

    def islink(self, path: AnyFSPath) -> bool:
        try:
            return self.fs.islink(path)
        except AttributeError:
            return False

    is_symlink = islink

    def is_hardlink(self, path: AnyFSPath) -> bool:
        try:
            return self.fs.is_hardlink(path)
        except AttributeError:
            return False

    def iscopy(self, path: AnyFSPath) -> bool:
        return not (self.is_symlink(path) or self.is_hardlink(path))

    @overload
    def ls(self, path: AnyFSPath, detail: Literal[True], **kwargs) -> "Iterator[Entry]":
        ...

    @overload
    def ls(self, path: AnyFSPath, detail: Literal[False], **kwargs) -> Iterator[str]:
        ...

    def ls(self, path, detail=False, **kwargs):
        return self.fs.ls(path, detail=detail, **kwargs)

    def find(
        self,
        path: Union[AnyFSPath, List[AnyFSPath]],
        prefix: bool = False,  # pylint: disable=unused-argument
        batch_size: Optional[int] = None,
        **kwargs,
    ) -> Iterator[str]:
        if isinstance(path, str):
            yield from self.fs.find(path)
            return
        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            loop = get_loop()
            fut = asyncio.run_coroutine_threadsafe(
                batch_coros(
                    [
                        self.fs._find(p)  # pylint: disable=protected-access
                        for p in path
                    ],
                    batch_size=jobs,
                ),
                loop,
            )
            for result in fut.result():
                yield from result
            return
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
        with executor:
            find = partial(self.fs.find)
            for result in executor.imap_unordered(find, path):
                yield from result

    def mv(self, from_info: AnyFSPath, to_info: AnyFSPath, **kwargs: Any) -> None:
        self.fs.mv(from_info, to_info)

    move = mv

    def rmdir(self, path: AnyFSPath) -> None:
        self.fs.rmdir(path)

    def rm_file(self, path: AnyFSPath) -> None:
        self.fs.rm_file(path)

    def rm(
        self,
        path: Union[AnyFSPath, List[AnyFSPath]],
        recursive: bool = False,
        **kwargs,
    ) -> None:
        self.fs.rm(path, recursive=recursive, **kwargs)

    remove = rm

    @overload
    def info(
        self,
        path: AnyFSPath,
        callback: "Callback" = ...,
        batch_size: Optional[int] = ...,
        **kwargs,
    ) -> "Entry":
        ...

    @overload
    def info(
        self,
        path: List[AnyFSPath],
        callback: "Callback" = ...,
        batch_size: Optional[int] = ...,
    ) -> List["Entry"]:
        ...

    def info(self, path, callback=DEFAULT_CALLBACK, batch_size=None, **kwargs):
        if isinstance(path, str):
            return self.fs.info(path, **kwargs)
        callback.set_size(len(path))
        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            loop = get_loop()
            fut = asyncio.run_coroutine_threadsafe(
                batch_coros(
                    [
                        self.fs._info(p, **kwargs)  # pylint: disable=protected-access
                        for p in path
                    ],
                    batch_size=jobs,
                    callback=callback,
                ),
                loop,
            )
            return fut.result()
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
        with executor:
            func = partial(self.fs.info, **kwargs)
            return list(executor.map(callback.wrap_fn(func), path))

    def mkdir(
        self, path: AnyFSPath, create_parents: bool = True, **kwargs: Any
    ) -> None:
        self.fs.mkdir(path, create_parents=create_parents, **kwargs)

    def makedirs(self, path: AnyFSPath, **kwargs: Any) -> None:
        self.fs.makedirs(path, exist_ok=kwargs.pop("exist_ok", True))

    def put_file(
        self,
        from_file: Union[AnyFSPath, "BinaryIO"],
        to_info: AnyFSPath,
        callback: Callback = DEFAULT_CALLBACK,
        size: int = None,
        **kwargs,
    ) -> None:
        if size:
            callback.set_size(size)
        if hasattr(from_file, "read"):
            stream = callback.wrap_attr(cast("BinaryIO", from_file))
            self.upload_fobj(stream, to_info, size=size)
        else:
            assert isinstance(from_file, str)
            self.fs.put_file(os.fspath(from_file), to_info, callback=callback, **kwargs)
        self.fs.invalidate_cache(self.path.parent(to_info))

    def get_file(
        self,
        from_info: AnyFSPath,
        to_info: AnyFSPath,
        callback: Callback = DEFAULT_CALLBACK,
        **kwargs,
    ) -> None:
        self.fs.get_file(from_info, to_info, callback=callback, **kwargs)

    def upload_fobj(self, fobj: IO, to_info: AnyFSPath, **kwargs) -> None:
        self.makedirs(self.path.parent(to_info))
        with self.open(to_info, "wb") as fdest:
            shutil.copyfileobj(
                fobj,
                fdest,
                length=getattr(fdest, "blocksize", None),  # type: ignore
            )

    def walk(self, path: AnyFSPath, **kwargs: Any):
        return self.fs.walk(path, **kwargs)

    def glob(self, path: AnyFSPath, **kwargs: Any):
        return self.fs.glob(path, **kwargs)

    def size(self, path: AnyFSPath) -> Optional[int]:
        return self.fs.size(path)

    def sizes(self, paths: List[AnyFSPath]) -> List[Optional[int]]:
        return self.fs.sizes(paths)

    def du(
        self,
        path: AnyFSPath,
        total: bool = True,
        maxdepth: int = None,
        **kwargs: Any,
    ) -> Union[int, Dict[AnyFSPath, int]]:
        return self.fs.du(path, total=total, maxdepth=maxdepth, **kwargs)

    def put(
        self,
        from_info: Union[AnyFSPath, List[AnyFSPath]],
        to_info: Union[AnyFSPath, List[AnyFSPath]],
        callback: "Callback" = DEFAULT_CALLBACK,
        recursive: bool = False,  # pylint: disable=unused-argument
        batch_size: int = None,
    ):
        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            return self.fs.put(
                from_info,
                to_info,
                callback=callback,
                batch_size=jobs,
                recursive=recursive,
            )

        assert not recursive, "not implemented yet"
        from_infos = [from_info] if isinstance(from_info, str) else from_info
        to_infos = [to_info] if isinstance(to_info, str) else to_info

        callback.set_size(len(from_infos))
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
        with executor:
            put_file = callback.wrap_and_branch(self.put_file)
            list(executor.imap_unordered(put_file, from_infos, to_infos))

    def get(
        self,
        from_info: Union[AnyFSPath, List[AnyFSPath]],
        to_info: Union[AnyFSPath, List[AnyFSPath]],
        callback: "Callback" = DEFAULT_CALLBACK,
        recursive: bool = False,  # pylint: disable=unused-argument
        batch_size: int = None,
    ) -> None:
        # Currently, the implementation is non-recursive if the paths are
        # provided as a list, and recursive if it's a single path.
        from .local import localfs

        def get_file(rpath, lpath, **kwargs):
            localfs.makedirs(localfs.path.parent(lpath), exist_ok=True)
            self.fs.get_file(rpath, lpath, **kwargs)

        get_file = callback.wrap_and_branch(get_file)

        if isinstance(from_info, list) and isinstance(to_info, list):
            from_infos: List[AnyFSPath] = from_info
            to_infos: List[AnyFSPath] = to_info
        else:
            assert isinstance(from_info, str)
            assert isinstance(to_info, str)

            if not self.isdir(from_info):
                callback.set_size(1)
                return get_file(from_info, to_info)

            from_infos = list(self.find(from_info))
            if not from_infos:
                return localfs.makedirs(to_info, exist_ok=True)

            to_infos = [
                localfs.path.join(to_info, *self.path.relparts(info, from_info))
                for info in from_infos
            ]

        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            return self.fs.get(
                from_infos,
                to_infos,
                callback=callback,
                batch_size=jobs,
            )

        callback.set_size(len(from_infos))
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
        with executor:
            list(executor.imap_unordered(get_file, from_infos, to_infos))

    def ukey(self, path: AnyFSPath) -> str:
        return self.fs.ukey(path)

    def created(self, path: AnyFSPath) -> datetime.datetime:
        return self.fs.created(path)

    def modified(self, path: AnyFSPath) -> datetime.datetime:
        return self.fs.modified(path)

    def sign(self, path: AnyFSPath, expiration: int = 100, **kwargs: Any) -> str:
        return self.fs.sign(path, expiration=expiration, **kwargs)


class ObjectFileSystem(FileSystem):  # pylint: disable=abstract-method
    TRAVERSE_PREFIX_LEN = 3

    def makedirs(self, path: AnyFSPath, **kwargs: Any) -> None:
        # For object storages make this method a no-op. The original
        # fs.makedirs() method will only check if the bucket exists
        # and create if it doesn't though we don't want to support
        # that behavior, and the check will cost some time so we'll
        # simply ignore all mkdir()/makedirs() calls.
        return None

    def mkdir(
        self, path: AnyFSPath, create_parents: bool = True, **kwargs: Any
    ) -> None:
        return None

    def find(
        self,
        path: Union[AnyFSPath, List[AnyFSPath]],
        prefix: bool = False,
        batch_size: Optional[int] = None,  # pylint: disable=unused-argument
        **kwargs,
    ) -> Iterator[str]:
        if isinstance(path, str):
            paths = [path]
        else:
            paths = path

        def _make_args(paths: List[AnyFSPath]) -> Iterator[Tuple[str, str]]:
            for path in paths:
                if prefix and not path.endswith(self.path.flavour.sep):
                    parent = self.path.parent(path)
                    yield parent, self.path.parts(path)[-1]
                else:
                    yield path, ""

        args = list(_make_args(paths))
        if len(args) == 1:
            path, prefix_str = args[0]
            yield from self.fs.find(path, prefix=prefix_str)
            return

        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            loop = get_loop()
            fut = asyncio.run_coroutine_threadsafe(
                batch_coros(
                    [
                        self.fs._find(  # pylint: disable=protected-access
                            path, prefix=prefix_str
                        )
                        for path, prefix_str in args
                    ],
                    batch_size=jobs,
                ),
                loop,
            )
            for result in fut.result():
                yield from result
            return
        # NOTE: this is not parallelized yet since imap_unordered does not
        # handle kwargs. We do not actually support any non-async object
        # storages, so this can be addressed when it is actually needed
        for path, prefix_str in args:
            yield from self.fs.find(path, prefix=prefix_str)
