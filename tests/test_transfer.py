from dvc_objects.db import ObjectDB
from dvc_objects.transfer import transfer


def test_transfer(memfs):
    src = ObjectDB(memfs, "/odb1")
    dest = ObjectDB(memfs, "/odb2")

    src.add_bytes("1234", b"content")
    assert transfer(src, dest, {"1234"}) == {"1234"}
    assert dest.exists("1234")
