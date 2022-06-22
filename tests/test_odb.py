from io import BytesIO

import pytest

from dvc_objects.db import ObjectDB


@pytest.mark.parametrize(
    "data, expected",
    [(b"content", b"content"), (BytesIO(b"content"), b"content")],
)
def test_add_bytes(memfs, data, expected):
    odb = ObjectDB(memfs, memfs.root_marker)
    odb.add_bytes("1234", data)
    assert memfs.cat_file("/12/34") == expected
