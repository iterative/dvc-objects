import itertools
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from functools import partial
from io import BytesIO
from typing import TYPE_CHECKING, BinaryIO, Optional, Tuple, Union, cast

from .errors import ObjectDBPermissionError
from .obj import Object

if TYPE_CHECKING:
    from .fs.base import AnyFSPath, FileSystem
    from .fs.callbacks import Callback


logger = logging.getLogger(__name__)


def noop(*args, **kwargs):
    pass


def wrap_iter(iterable, callback):
    for index, item in enumerate(iterable, start=1):
        yield item
        callback(index)


class ObjectDB:
    def __init__(self, fs: "FileSystem", path: str, **config):
        self.fs = fs
        self.path = path
        self.read_only = config.get("read_only", False)
        self._initialized = False

    def __eq__(self, other):
        return (
            self.fs == other.fs
            and self.path == other.path
            and self.read_only == other.read_only
        )

    def __hash__(self):
        return hash((self.fs.protocol, self.path))

    def _init(self):
        if self.read_only:
            return

        if self._initialized:
            return

        dnames = {f"{num:02x}" for num in range(0, 256)}
        existing = set()
        with suppress(FileNotFoundError):
            existing = {
                self.fs.path.name(path)
                for path in self.fs.ls(self.path, detail=False)
            }

        for dname in dnames.difference(existing):
            self.makedirs(self.fs.path.join(self.path, dname))

        self._initialized = True

    def exists(self, oid: str) -> bool:
        return self.fs.isfile(self.oid_to_path(oid))

    def exists_prefix(self, short_oid: str) -> str:
        path = self.oid_to_path(short_oid)
        if len(short_oid) <= 2:
            raise ValueError(short_oid, [])

        if self.exists(path):
            return short_oid

        prefix, _ = self._oid_parts(short_oid)
        ret = [
            oid
            for oid in self._list_oids(prefix=prefix)
            if oid.startswith(short_oid)
        ]
        if not ret:
            raise KeyError(short_oid)
        if len(ret) == 1:
            return ret[0]
        raise ValueError(short_oid, ret)

    def move(self, from_info, to_info):
        self.fs.move(from_info, to_info)

    def makedirs(self, path):
        self.fs.makedirs(path)

    def get(self, oid: str) -> Object:
        return Object(
            self.oid_to_path(oid),
            self.fs,
            oid,
        )

    def add_bytes(self, oid: str, data: Union[bytes, BinaryIO]) -> None:
        if self.read_only:
            raise ObjectDBPermissionError("Cannot add to read-only ODB")

        if isinstance(data, bytes):
            fobj: "BinaryIO" = BytesIO(data)
            size: Optional[int] = len(data)
        else:
            fobj = data
            size = cast("Optional[int]", getattr(fobj, "size", None))

        self._init()

        path = self.oid_to_path(oid)
        self.fs.put_file(fobj, path, size=size)

    def add(
        self,
        path: "AnyFSPath",
        fs: "FileSystem",
        oid: str,
        hardlink: bool = False,
        callback: "Callback" = None,
        **kwargs,
    ):
        from dvc_objects.fs import generic
        from dvc_objects.fs.callbacks import Callback

        if self.read_only:
            raise ObjectDBPermissionError("Cannot add to read-only ODB")

        if self.exists(oid):
            return

        self._init()

        cache_path = self.oid_to_path(oid)
        with Callback.as_tqdm_callback(
            callback,
            desc=fs.path.name(path),
            bytes=True,
        ) as cb:
            generic.transfer(
                fs,
                path,
                self.fs,
                cache_path,
                hardlink=hardlink,
                callback=Callback.as_callback(cb),
            )

    def delete(self, oid: str):
        self.fs.remove(self.oid_to_path(oid))

    def clear(self):
        for oid in self.all():
            self.delete(oid)

    def _oid_parts(self, oid: str) -> Tuple[str, str]:
        return oid[:2], oid[2:]

    def oid_to_path(self, oid) -> str:
        return self.fs.path.join(self.path, *self._oid_parts(oid))

    def _list_paths(self, prefix: str = None):
        prefix = prefix or ""
        parts: "Tuple[str, ...]" = (self.path,)
        if prefix:
            parts = *parts, prefix[:2]
        if len(prefix) > 2:
            parts = *parts, prefix[2:]
        yield from self.fs.find(self.fs.path.join(*parts), prefix=bool(prefix))

    def path_to_oid(self, path) -> str:
        parts = self.fs.path.parts(path)[-2:]

        if not (len(parts) == 2 and parts[0] and len(parts[0]) == 2):
            raise ValueError(f"Bad cache file path '{path}'")

        return "".join(parts)

    def _list_oids(self, prefix=None):
        """Iterate over oids in this fs.

        If `prefix` is specified, only oids which begin with `prefix`
        will be returned.
        """
        for path in self._list_paths(prefix):
            try:
                yield self.path_to_oid(path)
            except ValueError:
                logger.debug(
                    "'%s' doesn't look like a cache file, skipping", path
                )

    def _oids_with_limit(self, limit, prefix=None):
        count = 0
        for oid in self._list_oids(prefix):
            yield oid
            count += 1
            if count > limit:
                logger.debug(
                    "`_list_oids()` returned max '{}' oids, "
                    "skipping remaining results".format(limit)
                )
                return

    def _max_estimation_size(self, oids):
        # Max remote size allowed for us to use traverse method
        return max(
            self.fs.TRAVERSE_THRESHOLD_SIZE,
            len(oids)
            / self.fs.TRAVERSE_WEIGHT_MULTIPLIER
            * self.fs.LIST_OBJECT_PAGE_SIZE,
        )

    def _estimate_remote_size(self, oids=None, progress=noop):
        """Estimate fs size based on number of entries beginning with
        "00..." prefix.

        Takes a progress callback that returns current_estimated_size.
        """
        prefix = "0" * self.fs.TRAVERSE_PREFIX_LEN
        total_prefixes = pow(16, self.fs.TRAVERSE_PREFIX_LEN)
        if oids:
            max_oids = self._max_estimation_size(oids)
        else:
            max_oids = None

        def iter_with_pbar(oids):
            total = 0
            for oid in oids:
                total += total_prefixes
                progress(total)
                yield oid

        if max_oids:
            oids = self._oids_with_limit(max_oids / total_prefixes, prefix)
        else:
            oids = self._list_oids(prefix)

        remote_oids = set(iter_with_pbar(oids))
        if remote_oids:
            remote_size = total_prefixes * len(remote_oids)
        else:
            remote_size = total_prefixes
        logger.debug(f"Estimated remote size: {remote_size} files")
        return remote_size, remote_oids

    def _list_oids_traverse(self, remote_size, remote_oids, jobs=None):
        """Iterate over all oids found in this fs.
        Hashes are fetched in parallel according to prefix, except in
        cases where the remote size is very small.

        All oids from the remote (including any from the size
        estimation step passed via the `remote_oids` argument) will be
        returned.

        NOTE: For large remotes the list of oids will be very
        big(e.g. 100M entries, md5 for each is 32 bytes, so ~3200Mb list)
        and we don't really need all of it at the same time, so it makes
        sense to use a generator to gradually iterate over it, without
        keeping all of it in memory.
        """
        num_pages = remote_size / self.fs.LIST_OBJECT_PAGE_SIZE
        if num_pages < 256 / self.fs.jobs:
            # Fetching prefixes in parallel requires at least 255 more
            # requests, for small enough remotes it will be faster to fetch
            # entire cache without splitting it into prefixes.
            #
            # NOTE: this ends up re-fetching oids that were already
            # fetched during remote size estimation
            traverse_prefixes = [None]
        else:
            yield from remote_oids
            traverse_prefixes = [f"{i:02x}" for i in range(1, 256)]
            if self.fs.TRAVERSE_PREFIX_LEN > 2:
                traverse_prefixes += [
                    "{0:0{1}x}".format(i, self.fs.TRAVERSE_PREFIX_LEN)
                    for i in range(1, pow(16, self.fs.TRAVERSE_PREFIX_LEN - 2))
                ]

        with ThreadPoolExecutor(max_workers=jobs or self.fs.jobs) as executor:
            in_remote = executor.map(self._list_oids, traverse_prefixes)
            yield from itertools.chain.from_iterable(in_remote)

    def all(self, jobs=None):
        """Iterate over all oids in this fs.

        Hashes will be fetched in parallel threads according to prefix
        (except for small remotes) and a progress bar will be displayed.
        """
        if not self.fs.CAN_TRAVERSE:
            return self._list_oids()

        remote_size, remote_oids = self._estimate_remote_size()
        return self._list_oids_traverse(remote_size, remote_oids, jobs=jobs)

    def list_oids_exists(self, oids, jobs=None):
        """Return list of the specified oids which exist in this fs.
        Hashes will be queried individually.
        """
        logger.debug(f"Querying {len(oids)} oids via object_exists")
        with ThreadPoolExecutor(max_workers=jobs or self.fs.jobs) as executor:
            paths = map(self.oid_to_path, oids)
            in_remote = executor.map(self.fs.exists, paths)
            yield from itertools.compress(oids, in_remote)

    def oids_exist(self, oids, jobs=None, progress=noop):
        """Check if the given oids are stored in the remote.

        There are two ways of performing this check:

        - Traverse method: Get a list of all the files in the remote
            (traversing the cache directory) and compare it with
            the given oids. Cache entries will be retrieved in parallel
            threads according to prefix (i.e. entries starting with, "00...",
            "01...", and so on) and a progress bar will be displayed.

        - Exists method: For each given oid, run the `exists`
            method and filter the oids that aren't on the remote.
            This is done in parallel threads.
            It also shows a progress bar when performing the check.

        The reason for such an odd logic is that most of the remotes
        take much shorter time to just retrieve everything they have under
        a certain prefix (e.g. s3, gs, ssh, hdfs). Other remotes that can
        check if particular file exists much quicker, use their own
        implementation of oids_exist (see ssh, local).

        Which method to use will be automatically determined after estimating
        the size of the remote cache, and comparing the estimated size with
        len(oids). To estimate the size of the remote cache, we fetch
        a small subset of cache entries (i.e. entries starting with "00...").
        Based on the number of entries in that subset, the size of the full
        cache can be estimated, since the cache is evenly distributed according
        to oid.

        Takes a callback that returns value in the format of:
        (phase, total, current). The phase can be {"estimating, "querying"}.

        Returns:
            A list with oids that were found in the remote
        """
        # Remotes which do not use traverse prefix should override
        # oids_exist() (see ssh, local)
        assert self.fs.TRAVERSE_PREFIX_LEN >= 2

        # During the tests, for ensuring that the traverse behavior
        # is working we turn on this option. It will ensure the
        # _list_oids_traverse() is called.
        always_traverse = getattr(self.fs, "_ALWAYS_TRAVERSE", False)

        oids = set(oids)
        if (
            len(oids) == 1 or not self.fs.CAN_TRAVERSE
        ) and not always_traverse:
            remote_oids = self.list_oids_exists(oids, jobs)
            callback = partial(progress, "querying", len(oids))
            return list(wrap_iter(remote_oids, callback))

        # Max remote size allowed for us to use traverse method

        estimator_cb = partial(progress, "estimating", None)
        remote_size, remote_oids = self._estimate_remote_size(
            oids, progress=estimator_cb
        )

        traverse_pages = remote_size / self.fs.LIST_OBJECT_PAGE_SIZE
        # For sufficiently large remotes, traverse must be weighted to account
        # for performance overhead from large lists/sets.
        # From testing with S3, for remotes with 1M+ files, object_exists is
        # faster until len(oids) is at least 10k~100k
        if remote_size > self.fs.TRAVERSE_THRESHOLD_SIZE:
            traverse_weight = (
                traverse_pages * self.fs.TRAVERSE_WEIGHT_MULTIPLIER
            )
        else:
            traverse_weight = traverse_pages
        if len(oids) < traverse_weight and not always_traverse:
            logger.debug(
                "Large remote ('{}' oids < '{}' traverse weight), "
                "using object_exists for remaining oids".format(
                    len(oids), traverse_weight
                )
            )
            remaining_oids = oids - remote_oids
            ret = list(oids & remote_oids)
            callback = partial(progress, "querying", len(remaining_oids))
            ret.extend(
                wrap_iter(
                    self.list_oids_exists(remaining_oids, jobs), callback
                )
            )
            return ret

        logger.debug(f"Querying '{len(oids)}' oids via traverse")
        remote_oids = self._list_oids_traverse(
            remote_size, remote_oids, jobs=jobs
        )
        callback = partial(progress, "querying", remote_size)
        return list(oids & set(wrap_iter(remote_oids, callback)))
