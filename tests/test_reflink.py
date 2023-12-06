import errno
import os
import stat
from os import fspath, umask

import pytest

from dvc_objects.fs.system import reflink


@pytest.mark.xfail(raises=OSError, strict=False)
def test_reflink(tmp_dir_pytest_cache):
    src = tmp_dir_pytest_cache / "source"
    dest = tmp_dir_pytest_cache / "dest"

    src.write_bytes(b"content")
    reflink(fspath(src), fspath(dest))

    assert dest.is_file()
    assert dest.read_bytes() == b"content"

    stat_mode = src.stat().st_mode & (stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    assert stat_mode == (0o666 & ~umask(0))


@pytest.mark.skipif(os.name != "nt", reason="only run in Windows")
def test_reflink_unsupported_on_windows(tmp_dir_pytest_cache):
    src = tmp_dir_pytest_cache / "source"
    dest = tmp_dir_pytest_cache / "dest"
    src.write_bytes(b"content")

    with pytest.raises(OSError) as exc:
        reflink(fspath(src), fspath(dest))

    assert exc.value.errno == errno.ENOTSUP
    assert not dest.exists()
