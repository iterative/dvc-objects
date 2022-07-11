import math
from io import BytesIO

import pytest

from dvc_objects.db import ObjectDB
from dvc_objects.errors import ObjectDBPermissionError
from dvc_objects.fs import as_filesystem
from dvc_objects.fs.base import FileSystem


@pytest.fixture(params=["local", "memory"])
def odb_path(request, tmp_upath_factory):
    yield tmp_upath_factory.mktemp(request.param)


@pytest.fixture
def fs(odb_path):
    yield as_filesystem(odb_path.fs)


@pytest.fixture
def odb(odb_path, fs):
    yield ObjectDB(fs, odb_path.path)


def test_odb(odb_path, fs, odb):
    assert odb.fs is fs
    assert odb.path == odb_path.path
    assert odb.read_only is False
    assert odb == ObjectDB(fs, odb_path.path)
    assert hash(odb) == hash(odb)


@pytest.mark.parametrize(
    "data, expected",
    [(b"contents", b"contents"), (BytesIO(b"contents"), b"contents")],
)
def test_add_bytes(odb_path, odb, fs, data, expected):
    if isinstance(data, BytesIO):
        data.seek(0)

    odb.add_bytes("1234", data)
    assert (odb_path / "12" / "34").read_bytes() == expected


def test_odb_readonly(fs, odb_path):
    odb = ObjectDB(fs, odb_path, read_only=True)
    with pytest.raises(ObjectDBPermissionError):
        odb.add((odb_path / "foo").path, odb.fs, "1234")

    with pytest.raises(ObjectDBPermissionError):
        odb.add_bytes("1234", b"contents")


def test_odb_add(odb_path, odb, fs):
    foo = odb_path / "foo"
    foo.write_bytes(b"foo")
    bar = odb_path / "bar"
    bar.write_bytes(b"bar")

    odb.add(foo.path, fs, "1234")
    assert odb.exists("1234")

    # should not allow writing to an already existing object
    odb.add(bar.path, fs, "1234")
    assert (odb_path / "12" / "34").read_bytes() == b"foo"


def test_exists(odb):
    odb.add_bytes("1234", b"content")
    assert odb.exists("1234")


def test_exists_prefix(odb):
    with pytest.raises(KeyError):
        assert odb.exists_prefix("123")

    odb.add_bytes("123456", b"content")
    assert odb.exists_prefix("123") == "123456"


def test_exists_prefix_ambiguous(odb):
    odb.add_bytes("123456", b"content")
    odb.add_bytes("123450", b"content")
    with pytest.raises(ValueError) as exc:
        assert odb.exists_prefix("123")
    assert exc.value.args == ("123", ["123450", "123456"])


def test_move(odb, odb_path):
    odb.add_bytes("1234", b"content")
    src = odb_path / "12" / "34"
    dst = odb_path / "45" / "67"
    odb.move(src.path, dst.path)
    assert list(odb.fs.find(odb_path.path)) == [dst.path]


def test_makedirs(odb):
    odb.makedirs("12")
    assert odb.fs.isdir("12")


def test_get(odb, fs, odb_path):
    obj = odb.get("1234")
    assert obj.fs == fs
    assert obj.path == (odb_path / "12" / "34").path
    assert obj.oid == "1234"
    assert len(obj) == 1


def test_path_to_oid():
    odb = ObjectDB(FileSystem(), "/odb")

    assert odb.path_to_oid("/12/34") == "1234"
    assert odb.path_to_oid("/odb/12/34") == "1234"
    assert odb.path_to_oid("/odb/12/34/56") == "3456"
    assert odb.path_to_oid("/odb/12/34/abcde12") == "34abcde12"

    with pytest.raises(ValueError):
        odb.path_to_oid("bar")

    with pytest.raises(ValueError):
        odb.path_to_oid("/b/ar")


def test_oid_to_path():
    odb = ObjectDB(FileSystem(), "/odb")
    assert odb.oid_to_path("1234") == "/odb/12/34"


@pytest.mark.parametrize("traverse", [True, False])
def test_listing_oids(odb, mocker, traverse):
    mocker.patch.object(odb.fs, "CAN_TRAVERSE", traverse)

    oids = ["123456", "345678", "567890"]
    assert not list(odb.all())
    assert not list(odb.list_oids_exists(oids))
    assert not odb.oids_exist(oids)

    odb.add_bytes("123456", b"content")
    assert list(odb.all()) == ["123456"]
    assert list(odb.list_oids_exists(oids))
    assert set(odb.oids_exist(oids)) == {"123456"}


def test_oids_exist_non_traverse_fs(mocker):
    odb = ObjectDB(FileSystem(), "/odb")

    object_exists = mocker.patch.object(odb, "list_oids_exists")
    traverse = mocker.patch.object(odb, "_list_oids_traverse")
    mocker.patch.object(odb.fs, "CAN_TRAVERSE", False)

    oids = set(range(100))
    odb.oids_exist(oids)
    object_exists.assert_called_with(oids, None)
    traverse.assert_not_called()


def test_oids_exist_less_oids_larger_fs(mocker):
    # large remote, small local
    odb = ObjectDB(FileSystem(), "/odb")

    object_exists = mocker.patch.object(odb, "list_oids_exists")
    traverse = mocker.patch.object(odb, "_list_oids_traverse")
    mocker.patch.object(odb.fs, "CAN_TRAVERSE", True)
    mocker.patch.object(odb, "_list_oids", return_value=list(range(2048)))

    oids = list(range(1000))
    odb.oids_exist(oids)
    # verify that _odb_paths_with_max() short circuits
    # before returning all 2048 remote oids
    max_oids = math.ceil(
        odb._max_estimation_size(oids) / pow(16, odb.fs.TRAVERSE_PREFIX_LEN)
    )
    assert max_oids < 2048
    object_exists.assert_called_with(frozenset(range(max_oids, 1000)), None)
    traverse.assert_not_called()


def test_oids_exist_large_oids_larger_fs(mocker):
    # large remote, large local
    odb = ObjectDB(FileSystem(), "/odb")

    object_exists = mocker.patch.object(odb, "list_oids_exists")
    traverse = mocker.patch.object(odb, "_list_oids_traverse")
    mocker.patch.object(odb.fs, "CAN_TRAVERSE", True)
    mocker.patch.object(odb.fs, "TRAVERSE_THRESHOLD_SIZE", 1000)
    mocker.patch.object(odb, "_list_oids", return_value=list(range(256)))

    oids = list(range(2000))
    odb.oids_exist(oids)
    object_exists.assert_not_called()
    traverse.assert_called_with(
        256 * pow(16, odb.fs.TRAVERSE_PREFIX_LEN),
        set(range(256)),
        jobs=None,
    )
    object_exists.assert_not_called()


def test_list_paths(mocker):
    odb = ObjectDB(FileSystem(), "/odb")

    walk_mock = mocker.patch.object(odb.fs, "find", return_value=[])
    for _ in odb._list_paths():
        pass  # pragma: no cover
    walk_mock.assert_called_with("/odb", prefix=False)

    for _ in odb._list_paths(prefix="000"):
        pass  # pragma: no cover
    walk_mock.assert_called_with("/odb/00/0", prefix=True)


def test_list_oids(mocker):
    # large remote, large local
    odb = ObjectDB(FileSystem(), "/odb")
    mocker.patch.object(odb, "_list_paths", return_value=["12/34", "bar"])
    assert list(odb._list_oids()) == ["1234"]


@pytest.mark.parametrize("prefix_len", [2, 3])
def test_list_oids_traverse(mocker, prefix_len):
    odb = ObjectDB(FileSystem(), "/odb")

    list_oids = mocker.patch.object(odb, "_list_oids", return_value=[])
    mocker.patch.object(
        odb, "path_to_oid", side_effect=lambda x: x
    )  # pragma: no cover
    mocker.patch.object(odb.fs, "TRAVERSE_PREFIX_LEN", prefix_len)

    # parallel traverse
    size = 256 / odb.fs._JOBS * odb.fs.LIST_OBJECT_PAGE_SIZE
    list(odb._list_oids_traverse(size, {0}))
    for i in range(1, 16):
        list_oids.assert_any_call(f"{i:0{odb.fs.TRAVERSE_PREFIX_LEN}x}")
    for i in range(1, 256):
        list_oids.assert_any_call(f"{i:02x}")

    # default traverse (small remote)
    size -= 1
    list_oids.reset_mock()
    list(odb._list_oids_traverse(size - 1, {0}))
    list_oids.assert_called_with(None)
