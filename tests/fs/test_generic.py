import pytest
from fsspec import Callback
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.memory import MemoryFileSystem as MemoryFS

from dvc_objects.fs.generic import copy, transfer
from dvc_objects.fs.local import LocalFileSystem
from dvc_objects.fs.memory import MemoryFileSystem


def awrap(fn):
    async def inner(self, *args, **kwargs):
        return fn(self.fs, *args, **kwargs)

    return inner


class AsyncMemoryFS(AsyncFileSystem):
    cachable = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fs = MemoryFS()
        self.fs.store = {}
        self.fs.pseudo_dirs = [""]

    def _open(self, *args, **kwargs):
        return self.fs.open(*args, **kwargs)

    _info = awrap(MemoryFS.info)
    _ls = awrap(MemoryFS.ls)
    _mkdir = awrap(MemoryFS.mkdir)
    _makedirs = awrap(MemoryFS.makedirs)
    _get_file = awrap(MemoryFS.get_file)
    _put_file = awrap(MemoryFS.put_file)
    _cat_file = awrap(MemoryFS.cat_file)
    _pipe_file = awrap(MemoryFS.pipe_file)
    _cp_file = awrap(MemoryFS.cp_file)
    _rm_file = awrap(MemoryFS.rm_file)


fs_clses = [
    LocalFileSystem,
    lambda: MemoryFileSystem(global_store=False),
    lambda: MemoryFileSystem(fs=AsyncMemoryFS()),
]
fs_clses[1].__name__ = MemoryFileSystem.__name__  # type: ignore[attr-defined]
fs_clses[2].__name__ = "Async" + MemoryFileSystem.__name__  # type: ignore[attr-defined]


@pytest.mark.parametrize("files", [{"foo": b"foo"}, {"foo": b"foo", "bar": b"barr"}])
@pytest.mark.parametrize("fs_cls1", fs_clses)
@pytest.mark.parametrize("fs_cls2", fs_clses)
def test_copy(tmp_path, files, fs_cls1, fs_cls2, mocker):
    fs1, fs2 = fs_cls1(), fs_cls2()
    fs1_root = tmp_path if isinstance(fs1, LocalFileSystem) else fs1.root_marker
    fs2_root = tmp_path if isinstance(fs2, LocalFileSystem) else fs2.root_marker
    src_root = fs1.join(fs1_root, "src")
    dest_root = fs2.join(fs2_root, "dest")

    src_files = {fs1.join(src_root, f): c for f, c in files.items()}
    dest_files = {fs2.join(dest_root, f): c for f, c in files.items()}
    fs1.mkdir(src_root)
    fs1.pipe(src_files)

    callback = Callback()
    spy_close = mocker.spy(Callback, "close")
    child_callbacks = [Callback() for _ in files]

    branched = mocker.patch.object(callback, "branched", side_effect=child_callbacks)
    copy(fs1, list(src_files), fs2, list(dest_files), callback=callback)

    assert fs2.cat(list(dest_files)) == dest_files

    n = len(files)
    # assert main callback works
    assert callback.value == n
    assert callback.size is None  # does not set sizes
    # assert child callbacks are handled correctly
    assert branched.call_count == n, f"expected branched to be called {n} times"
    assert spy_close.call_count == n, f"expected close to be called {n} times"

    if isinstance(fs1, LocalFileSystem) and isinstance(fs2, LocalFileSystem):
        # localfs copy avoids calling set_size or update if fs supports reflink
        # or, file size is less than 1GB
        return
    assert {c.size for c in child_callbacks} == {len(c) for c in files.values()}
    assert {c.value for c in child_callbacks} == {len(c) for c in files.values()}


@pytest.mark.parametrize("files", [{"foo": b"foo"}, {"foo": b"foo", "bar": b"barr"}])
@pytest.mark.parametrize(
    "link_type",
    [
        pytest.param("reflink", marks=pytest.mark.xfail(reason="unsupported")),
        "symlink",
        "hardlink",
        "copy",
    ],
)
def test_transfer_between_local_fses(mocker, tmp_path, files, link_type):
    fs = LocalFileSystem()
    fs.mkdir(fs.join(tmp_path, "src"))
    fs.mkdir(fs.join(tmp_path, "dest"))

    src_files = {fs.join(tmp_path, "src", f): c for f, c in files.items()}
    dest_files = {fs.join(tmp_path, "dest", f): c for f, c in files.items()}

    fs.pipe(src_files)

    callback = Callback()

    branched = mocker.patch.object(callback, "branched")
    transfer(
        fs, list(src_files), fs, list(dest_files), callback=callback, links=[link_type]
    )
    assert fs.cat([str(tmp_path / "dest" / file) for file in files]) == dest_files
    assert callback.value == len(files)
    assert callback.size is None  # does not set sizes
    assert branched.call_count == (len(files) if link_type == "copy" else 0)
