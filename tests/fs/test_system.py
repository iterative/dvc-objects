from os import fspath

from dvc_objects.fs.system import inode


def test_inode(tmp_path):
    file = tmp_path / "foo"
    file.write_text("foo content", encoding="utf8")
    assert inode(fspath(file)) == inode(fspath(file))
