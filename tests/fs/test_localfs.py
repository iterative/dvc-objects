import os
from os import fspath

import pytest

from dvc_objects.fs.local import LocalFileSystem


@pytest.mark.parametrize("path, contents", [("file", "foo"), ("тест", "проверка")])
def test_local_fs_open(tmp_path, path, contents):
    file = tmp_path / path
    file.write_text(contents, encoding="utf8")
    fs = LocalFileSystem()

    with fs.open(fspath(file), encoding="utf-8") as fobj:
        assert fobj.read() == contents


def test_local_fs_exists(tmp_path):
    (tmp_path / "file").write_text("file", encoding="utf8")
    (tmp_path / "тест").write_text("проверка", encoding="utf8")

    fs = LocalFileSystem()
    assert fs.exists(fspath(tmp_path / "file"))
    assert fs.exists(fspath(tmp_path / "тест"))
    assert not fs.exists(fspath(tmp_path / "not-existing-file"))


def test_local_fs_isdir(tmp_path):
    (tmp_path / "file").write_text("file", encoding="utf8")
    (tmp_path / "data_dir").mkdir()

    fs = LocalFileSystem()

    assert fs.isdir(fspath(tmp_path / "data_dir"))
    assert not fs.isdir(fspath(tmp_path / "file"))
    assert not fs.isdir(fspath(tmp_path / "not-existing-file"))


def test_local_fs_isfile(tmp_path):
    (tmp_path / "file").write_text("file", encoding="utf8")
    (tmp_path / "data_dir").mkdir()

    fs = LocalFileSystem()

    assert fs.isfile(fspath(tmp_path / "file"))
    assert not fs.isfile(fspath(tmp_path / "data_dir"))
    assert not fs.isfile(fspath(tmp_path / "not-existing-file"))


def test_local_fs_rm(tmp_path):
    (tmp_path / "file").write_text("file", encoding="utf8")
    (tmp_path / "file2").write_text("file2", encoding="utf8")

    fs = LocalFileSystem()
    fs.remove([tmp_path / "file", tmp_path / "file2"])
    assert not fs.exists(fspath(tmp_path / "file"))
    assert not fs.exists(fspath(tmp_path / "file2"))


def convert_to_sets(walk_results):
    return [(root, set(dirs), set(nondirs)) for root, dirs, nondirs in walk_results]


@pytest.fixture
def dir_path(tmp_path):
    for file, contents in [
        ("foo", "foo"),
        ("bar", "bar"),
        ("тест", "проверка"),
        (
            "code.py",
            "import sys\nimport shutil\n" "shutil.copyfile(sys.argv[1], sys.argv[2])",
        ),
    ]:
        (tmp_path / file).write_text(contents, encoding="utf8")
    (tmp_path / "data" / "sub").mkdir(parents=True)
    (tmp_path / "data" / "file").write_text("file", encoding="utf8")
    (tmp_path / "data" / "sub" / "file").write_text("sub_file", encoding="utf8")
    return tmp_path


def test_walk(dir_path):
    fs = LocalFileSystem()
    walk_results = fs.walk(fspath(dir_path))
    assert convert_to_sets(walk_results) == [
        (str(dir_path), {"data"}, {"code.py", "bar", "тест", "foo"}),
        (str(dir_path / "data"), {"sub"}, {"file"}),
        (str(dir_path / "data" / "sub"), set(), {"file"}),
    ]

    walk_results = fs.walk(fspath(dir_path / "data" / "sub"))
    assert convert_to_sets(walk_results) == [
        (fspath(dir_path / "data" / "sub"), set(), {"file"}),
    ]


def test_walk_detail(dir_path):
    fs = LocalFileSystem()
    walk_results = list(fs.walk(fspath(dir_path), detail=True))

    expected = [
        (str(dir_path), {"data"}, {"code.py", "bar", "тест", "foo"}),
        (str(dir_path / "data"), {"sub"}, {"file"}),
        (str(dir_path / "data" / "sub"), set(), {"file"}),
    ]

    assert len(walk_results) == len(expected)
    for entry, expected_entry in zip(walk_results, expected):
        root, dirs, files = entry
        exp_root, exp_dirs, exp_files = expected_entry
        assert root == exp_root
        assert len(dirs) == len(exp_dirs)
        assert len(files) == len(exp_files)
        for basename in exp_dirs:
            assert fs.path.normpath(dirs[basename]["name"]) == os.path.join(
                exp_root, basename
            )
            assert dirs[basename]["type"] == "directory"
        for basename in exp_files:
            assert fs.path.normpath(files[basename]["name"]) == os.path.join(
                exp_root, basename
            )
            assert files[basename]["type"] == "file"


@pytest.mark.skipif(
    os.name == "nt", reason="A file name can't contain newlines on Windows"
)
def test_normpath_with_newlines():
    fs = LocalFileSystem()
    newline_path = os.path.join("one", "two\nthree")
    assert fs.path.normpath(newline_path) == newline_path
