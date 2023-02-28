import math
from io import BytesIO

import pytest

from dvc_objects.db import ObjectDB
from dvc_objects.errors import ObjectDBPermissionError
from dvc_objects.fs.base import FileSystem


def test_odb(memfs):
    odb = ObjectDB(memfs, "/odb")
    assert odb.fs is memfs
    assert odb.path == "/odb"
    assert odb.read_only is False
    assert odb == odb == ObjectDB(memfs, "/odb")
    assert hash(odb) == hash(odb)


@pytest.mark.parametrize(
    "data, expected",
    [(b"contents", b"contents"), (BytesIO(b"contents"), b"contents")],
)
def test_add_bytes(memfs, data, expected):
    odb = ObjectDB(memfs, memfs.root_marker)
    odb.add_bytes("1234", data)
    assert memfs.cat_file("/12/34") == expected


def test_odb_readonly():
    odb = ObjectDB(FileSystem(), "/odb", read_only=True)
    with pytest.raises(ObjectDBPermissionError):
        odb.add("/odb/foo", odb.fs, "1234")

    with pytest.raises(ObjectDBPermissionError):
        odb.add_bytes("1234", b"contents")


def test_odb_add(memfs):
    memfs.pipe({"foo": b"foo", "bar": b"bar"})

    odb = ObjectDB(memfs, "/odb")
    odb.add("/foo", memfs, "1234")
    assert odb.exists("1234")

    # should not allow writing to an already existing object
    odb.add("/bar", memfs, "1234")
    assert memfs.cat_file("/odb/12/34") == b"foo"


def test_delete(memfs):
    memfs.pipe({"foo": b"foo", "bar": b"bar"})

    odb = ObjectDB(memfs, "/odb")
    odb.add("/foo", memfs, "1234")
    odb.add("/bar", memfs, "4321")
    assert odb.exists("1234")
    assert odb.exists("4321")

    odb.delete("1234")
    assert memfs.isdir("/odb/12")
    assert memfs.isdir("/odb/43")
    assert not odb.exists("1234")
    assert odb.exists("4321")

    odb.delete("4321")
    assert memfs.isdir("/odb/12")
    assert memfs.isdir("/odb/43")
    assert not odb.exists("1234")
    assert not odb.exists("4321")


def test_clear(memfs):
    memfs.pipe({"foo": b"foo", "bar": b"bar"})

    odb = ObjectDB(memfs, "/odb")
    odb.add("/foo", memfs, "1234")
    odb.add("/bar", memfs, "4321")

    odb.clear()
    assert memfs.isdir("/odb/12")
    assert memfs.isdir("/odb/43")
    assert not odb.exists("1234")
    assert not odb.exists("4321")


def test_exists(memfs):
    odb = ObjectDB(memfs, "/odb")
    odb.add_bytes("1234", b"content")
    assert odb.exists("1234")


def test_exists_prefix(memfs):
    odb = ObjectDB(memfs, "/odb")
    with pytest.raises(KeyError):
        assert odb.exists_prefix("123")

    odb.add_bytes("123456", b"content")
    assert odb.exists_prefix("123") == "123456"


@pytest.mark.parametrize(
    "oid, found",
    [
        ("", []),
        ("1", []),
        ("12", []),
        ("123", ["123450", "123456"]),
    ],
)
def test_exists_prefix_ambiguous(memfs, oid, found):
    odb = ObjectDB(memfs, "/odb")
    odb.add_bytes("123456", b"content")
    odb.add_bytes("123450", b"content")

    with pytest.raises(ValueError) as exc:
        assert odb.exists_prefix(oid)
    assert exc.value.args == (oid, found)


def test_move(memfs):
    odb = ObjectDB(memfs, "/")
    odb.add_bytes("1234", b"content")
    odb.move("/12/34", "/45/67")
    assert list(memfs.find("")) == ["/45/67"]


def test_makedirs(memfs):
    odb = ObjectDB(memfs, "/")
    odb.makedirs("12")
    assert memfs.isdir("12")


def test_get(memfs):
    odb = ObjectDB(memfs, "/odb")
    obj = odb.get("1234")
    assert obj.fs == memfs
    assert obj.path == "/odb/12/34"
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
def test_listing_oids(memfs, mocker, traverse):
    mocker.patch.object(memfs, "CAN_TRAVERSE", traverse)
    odb = ObjectDB(memfs, "/odb")

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
    object_exists.assert_called_with(oids, jobs=None)
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
    object_exists.assert_called_with(frozenset(range(max_oids, 1000)), jobs=None)
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


def test_list_prefixes(mocker):
    odb = ObjectDB(FileSystem(), "/odb")

    walk_mock = mocker.patch.object(odb.fs, "find", return_value=[])
    for _ in odb._list_prefixes():
        pass  # pragma: no cover
    walk_mock.assert_called_with("/odb", batch_size=None, prefix=False)

    for _ in odb._list_prefixes(["000"]):
        pass  # pragma: no cover
    walk_mock.assert_called_with("/odb/00/0", batch_size=None, prefix=True)


def test_list_oids(mocker):
    # large remote, large local
    odb = ObjectDB(FileSystem(), "/odb")
    mocker.patch.object(odb, "_list_prefixes", return_value=["12/34", "bar"])
    assert list(odb._list_oids()) == ["1234"]


@pytest.mark.parametrize(
    "prefix_len, extra_prefixes",
    [
        (2, []),
        (3, [f"{i:03x}" for i in range(1, 16)]),
    ],
)
def test_list_oids_traverse(mocker, prefix_len, extra_prefixes):
    odb = ObjectDB(FileSystem(), "/odb")

    list_oids = mocker.patch.object(odb, "_list_oids", return_value=[])
    mocker.patch.object(odb, "path_to_oid", side_effect=lambda x: x)  # pragma: no cover
    mocker.patch.object(odb.fs, "TRAVERSE_PREFIX_LEN", prefix_len)

    # parallel traverse
    size = 256 / odb.fs._JOBS * odb.fs.LIST_OBJECT_PAGE_SIZE
    list(odb._list_oids_traverse(size, {0}))
    prefixes = [f"{i:02x}" for i in range(1, 256)] + extra_prefixes
    list_oids.assert_any_call(prefixes=prefixes, jobs=None)

    # default traverse (small remote)
    size -= 1
    list_oids.reset_mock()
    list(odb._list_oids_traverse(size - 1, {0}))
    list_oids.assert_called_with(prefixes=None, jobs=None)
