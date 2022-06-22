import errno

from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFile
from fsspec.implementations.memory import MemoryFileSystem as MemFS


class MemFS2(AbstractFileSystem):  # pylint: disable=abstract-method
    """In-Memory Object Storage FileSystem."""

    protocol = "memory"
    root_marker = "/"
    _strip_protocol = MemFS._strip_protocol  # pylint: disable=protected-access

    def __init__(self, *args, **storage_options):
        import pygtrie

        super().__init__(*args, **storage_options)
        self.trie = self.store = pygtrie.StringTrie()

    def ls(self, path, detail=False, **kwargs):
        path = self._strip_protocol(path)
        if path not in ("", "/"):
            info = self.info(path)
            if info["type"] != "directory":
                return [info] if detail else [path]

        ret = []

        def node_factory(path_conv, paths, children, *args):
            node_path = path_conv(paths)
            if path == node_path:
                list(filter(None, children))
            else:
                ret.append(node_path)

        try:
            self.trie.traverse(node_factory, prefix=path)
        except KeyError as exc:
            if path in ("", "/"):
                return []
            raise FileNotFoundError(
                errno.ENOENT, "No such file", path
            ) from exc

        if not detail:
            return ret

        return [self.info(_path) for _path in ret]

    def _rm(self, path):
        path = self._strip_protocol(path)
        if self.isdir(path):
            raise IsADirectoryError(errno.EISDIR, "Is a directory", path)

        try:
            del self.trie[path]
        except KeyError as e:
            raise FileNotFoundError(errno.ENOENT, "No such file", path) from e

    def _open(  # pylint: disable=arguments-differ
        self, path, mode="rb", **kwargs
    ):
        path = self._strip_protocol(path)
        if self.isdir(path):
            raise IsADirectoryError(errno.EISDIR, "Is a directory", path)
        short = self.trie.shortest_prefix(path)
        if short and short.key != path:
            raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)

        if mode in ["rb", "ab", "rb+"]:
            if filelike := self.trie.get(path):
                if mode == "ab":
                    # position at the end of file
                    filelike.seek(0, 2)
                else:
                    # position at the beginning of file
                    filelike.seek(0)
                return filelike
            raise FileNotFoundError(errno.ENOENT, "No such file", path)
        if mode == "wb":
            filelike = MemoryFile(self, path)
            if not self._intrans:
                filelike.commit()
            return filelike

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        if path in ("", "/") or self.trie.has_subtrie(path):
            return {
                "name": path,
                "size": 0,
                "type": "directory",
            }

        if filelike := self.trie.get(path):
            return {
                "name": path,
                "size": filelike.size
                if hasattr(filelike, "size")
                else filelike.getbuffer().nbytes,
                "type": "file",
                "created": getattr(filelike, "created", None),
            }
        raise FileNotFoundError(errno.ENOENT, "No such file", path)

    def cp_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        if self.isdir(path1):
            return

        with self.open(path1, "rb") as src, self.open(path2, "wb") as dst:
            dst.write(src.getbuffer())

    def created(self, path):
        return self.info(path).get("created")
