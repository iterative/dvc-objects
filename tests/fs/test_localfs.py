from os import fspath

import pytest

from dvc_objects.fs.local import LocalFileSystem


@pytest.mark.parametrize(
    "path, contents", [("file", "foo"), ("тест", "проверка")]
)
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


def convert_to_sets(walk_results):
    return [
        (root, set(dirs), set(nondirs)) for root, dirs, nondirs in walk_results
    ]


def test_walk(tmp_path):
    for file, contents in [
        ("foo", "foo"),
        ("bar", "bar"),
        ("тест", "проверка"),
        (
            "code.py",
            "import sys\nimport shutil\n"
            "shutil.copyfile(sys.argv[1], sys.argv[2])",
        ),
    ]:
        (tmp_path / file).write_text(contents, encoding="utf8")
    (tmp_path / "data" / "sub").mkdir(parents=True)
    (tmp_path / "data" / "file").write_text("file", encoding="utf8")
    (tmp_path / "data" / "sub" / "file").write_text(
        "sub_file", encoding="utf8"
    )

    fs = LocalFileSystem()
    walk_results = fs.walk(fspath(tmp_path))
    assert convert_to_sets(walk_results) == [
        (str(tmp_path), {"data"}, {"code.py", "bar", "тест", "foo"}),
        (str(tmp_path / "data"), {"sub"}, {"file"}),
        (str(tmp_path / "data" / "sub"), set(), {"file"}),
    ]

    walk_results = fs.walk(fspath(tmp_path / "data" / "sub"))
    assert convert_to_sets(walk_results) == [
        (fspath(tmp_path / "data" / "sub"), set(), {"file"}),
    ]
