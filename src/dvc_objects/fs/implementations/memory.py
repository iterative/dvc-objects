from ..base import FileSystem


class MemoryFileSystem(FileSystem):  # pylint:disable=abstract-method
    protocol = "memory"
    PARAM_CHECKSUM = "md5"

    def __init__(self, global_store=True, trie_based=False, fs=None, **kwargs):
        super().__init__(fs=fs, **kwargs)
        if fs is None:
            from ._memory import MemFS, MemFS2

            fs_cls = MemFS2 if trie_based else MemFS
            self.fs = fs_cls(**self.fs_args)
            if not global_store and not trie_based:
                self.fs.store = {}
                self.fs.pseudo_dirs = [""]

    def __eq__(self, other):
        return (
            isinstance(other, type(self)) and self.fs.store is other.fs.store
        )

    __hash__ = FileSystem.__hash__
