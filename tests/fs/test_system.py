from os import fspath

from dvc_objects.fs.system import inode


def test_inode(tmp_path):
    file = tmp_path / "foo"
    file.write_text("foo content")
    assert inode(fspath(file)) == inode(fspath(file))
