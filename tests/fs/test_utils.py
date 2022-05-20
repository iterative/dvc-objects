import filecmp
import os
import re
from os import fspath

import pytest

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


KB = 1024
MB = KB**2
GB = KB**3
TB = KB**4


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("10", 10),
        ("10   ", 10),
        ("1kb", 1 * KB),
        ("2kb", 2 * KB),
        ("1000mib", 1000 * MB),
        ("20gB", 20 * GB),
        ("10Tib", 10 * TB),
    ],
)
def test_conversions_human_readable_to_bytes(test_input, expected):
    assert utils.human_readable_to_bytes(test_input) == expected


@pytest.mark.parametrize("invalid_input", ["foo", "10XB", "1000Pb", "fooMiB"])
def test_conversions_human_readable_to_bytes_invalid(invalid_input):
    with pytest.raises(ValueError):
        utils.human_readable_to_bytes(invalid_input)
