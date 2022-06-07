import os
from os import fspath

from dvc_objects.fs.system import (
    hardlink,
    inode,
    is_hardlink,
    is_symlink,
    symlink,
)


def test_inode(tmp_path):
    file = tmp_path / "foo"
    file.write_text("foo content", encoding="utf8")
    assert inode(fspath(file)) == inode(fspath(file))


def test_symlink(tmp_path):
    (tmp_path / "source").write_bytes(b"source")
    symlink(fspath(tmp_path / "source"), fspath(tmp_path / "dest"))

    assert os.path.islink(tmp_path / "dest")
    assert is_symlink(fspath(tmp_path / "dest"))


def test_hardlink(tmp_path):
    src = tmp_path / "source"
    dest = tmp_path / "dest"
    src.write_bytes(b"source")

    hardlink(fspath(src), fspath(dest))
    assert inode(fspath(src)) == inode(fspath(dest))
    assert is_hardlink(fspath(dest))


def test_hardlink_follows_symlink(tmp_path):
    src = tmp_path / "source"
    inter = tmp_path / "inter"
    dest = tmp_path / "dest"
    src.write_bytes(b"source")

    symlink(fspath(src), fspath(inter))
    hardlink(fspath(inter), fspath(dest))

    assert inode(fspath(src)) == inode(fspath(dest))
    assert is_hardlink(fspath(dest))
