from ..base import FileSystem


class MemoryFileSystem(FileSystem):  # pylint:disable=abstract-method
    protocol = "memory"
    PARAM_CHECKSUM = "md5"

    def __init__(self, global_store=True, **kwargs):
        from fsspec.implementations.memory import MemoryFileSystem as MemFS

        super().__init__(**kwargs)
        self.fs = fs = MemFS(**self.fs_args)
        if not global_store:
            fs.store = {}
            fs.pseudo_dirs = [""]

    def __eq__(self, other):
        return (
            isinstance(other, type(self)) and self.fs.store is other.fs.store
        )

    __hash__ = FileSystem.__hash__
