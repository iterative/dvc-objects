import filecmp
import os
import re
from os import fspath

from dvc_objects.fs import utils


def test_tmp_fname():
    file = os.path.join("path", "to", "file")

    def pattern(path):
        return r"^" + re.escape(path) + r"\.[a-z0-9]{22}\.tmp$"

    assert re.search(pattern(file), utils.tmp_fname(file), re.IGNORECASE)
    assert re.search(
        pattern(file),
        utils.tmp_fname(file),
        re.IGNORECASE,
    )


def test_move(tmp_path):
    src = tmp_path / "foo"
    src.write_text("foo content", encoding="utf8")

    dest = tmp_path / "some" / "directory"
    dest.mkdir(parents=True)
    utils.move(fspath(src), fspath(dest))
    assert not os.path.isfile(src)
    assert len(os.listdir(dest)) == 1


def test_copyfile(tmp_path):
    src = tmp_path / "foo"
    src.write_text("foo content", encoding="utf8")
    dest = tmp_path / "bar"

    utils.copyfile(fspath(src), fspath(dest))
    assert filecmp.cmp(src, dest, shallow=False)


def test_copyfile_existing_dir(tmp_path):
    src = tmp_path / "foo"
    src.write_text("foo content", encoding="utf8")
    dest = tmp_path / "dir"
    dest.mkdir()

    utils.copyfile(fspath(src), fspath(dest))
    assert filecmp.cmp(src, dest / "foo", shallow=False)
