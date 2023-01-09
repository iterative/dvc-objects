import ntpath
import posixpath
from types import ModuleType
from typing import Callable, Iterable, Iterator, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit


class Path:
    def __init__(
        self,
        sep: str,
        getcwd: Optional[Callable[[], str]] = None,
        realpath: Optional[Callable[[str], str]] = None,
    ):
        def _getcwd() -> str:
            return ""

        self.getcwd: Callable[[], str] = getcwd or _getcwd
        self.realpath = realpath or self.abspath

        if sep == posixpath.sep:
            self.flavour: ModuleType = posixpath
        elif sep == ntpath.sep:
            self.flavour = ntpath
        else:
            raise ValueError(f"unsupported separator '{sep}'")

    def chdir(self, path: str):
        def _getcwd() -> str:
            return path

        self.getcwd = _getcwd

    def join(self, *parts: str) -> str:
        return self.flavour.join(*parts)

    def split(self, path: str) -> Tuple[str, str]:
        return self.flavour.split(path)

    def splitext(self, path: str) -> Tuple[str, str]:
        return self.flavour.splitext(path)

    def normpath(self, path: str) -> str:
        if self.flavour == ntpath:
            return self.flavour.normpath(path)

        parts = list(urlsplit(path))
        parts[2] = self.flavour.normpath(parts[2])
        return urlunsplit(parts)

    def isabs(self, path: str) -> bool:
        return self.flavour.isabs(path)

    def abspath(self, path: str) -> str:
        if not self.isabs(path):
            path = self.join(self.getcwd(), path)
        return self.normpath(path)

    def commonprefix(self, paths: Sequence[str]) -> str:
        return self.flavour.commonprefix(paths)

    def commonpath(self, paths: Iterable[str]) -> str:
        return self.flavour.commonpath(paths)

    def parts(self, path: str) -> Tuple[str, ...]:
        drive, path = self.flavour.splitdrive(path.rstrip(self.flavour.sep))

        ret = []
        while True:
            path, part = self.flavour.split(path)

            if part:
                ret.append(part)
                continue

            if path:
                ret.append(path)

            break

        ret.reverse()

        if drive:
            ret = [drive] + ret

        return tuple(ret)

    def parent(self, path: str) -> str:
        return self.flavour.dirname(path)

    def dirname(self, path: str) -> str:
        return self.parent(path)

    def parents(self, path: str) -> Iterator[str]:
        while True:
            parent = self.flavour.dirname(path)
            if parent == path:
                break
            yield parent
            path = parent

    def name(self, path: str) -> str:
        return self.flavour.basename(path)

    def suffix(self, path: str) -> str:
        name = self.name(path)
        _, dot, suffix = name.partition(".")
        return dot + suffix

    def with_name(self, path: str, name: str) -> str:
        return self.join(self.parent(path), name)

    def with_suffix(self, path: str, suffix: str) -> str:
        return self.splitext(path)[0] + suffix

    def isin(self, left: str, right: str) -> bool:
        if left == right:
            return False
        try:
            common = self.commonpath([left, right])
        except ValueError:
            # Paths don't have the same drive
            return False
        return common == right

    def isin_or_eq(self, left: str, right: str) -> bool:
        return left == right or self.isin(left, right)

    def overlaps(self, left: str, right: str) -> bool:
        # pylint: disable=arguments-out-of-order
        return self.isin_or_eq(left, right) or self.isin(right, left)

    def relpath(self, path: str, start: Optional[str] = None) -> str:
        if start is None:
            start = "."
        return self.flavour.relpath(
            self.abspath(path), start=self.abspath(start)
        )

    def relparts(
        self, path: str, start: Optional[str] = None
    ) -> Tuple[str, ...]:
        return self.parts(self.relpath(path, start=start))

    def as_posix(self, path: str) -> str:
        return path.replace(self.flavour.sep, posixpath.sep)


class LocalFileSystemPath(Path):
    def normpath(self, path: str) -> str:
        return self.flavour.normpath(path)
