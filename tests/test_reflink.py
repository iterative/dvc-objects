import errno
import stat
from os import fspath, umask
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from dvc_objects.fs.system import reflink


@pytest.fixture
def test_dir(request):
    """Create a test directory within cache directory.

    The cache directory by default, is in the root of the repo, where reflink
    may be supported.
    """
    cache = request.config.cache
    path = cache.mkdir("reflink_test")
    with TemporaryDirectory(dir=path) as tmp_dir:
        yield Path(tmp_dir)


@pytest.mark.xfail(raises=OSError, strict=False)
def test_reflink(test_dir):
    src = test_dir / "source"
    dest = test_dir / "dest"

    src.write_bytes(b"content")
    reflink(fspath(src), fspath(dest))

    assert dest.is_file()
    assert dest.read_bytes() == b"content"

    stat_mode = src.stat().st_mode & (
        stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO
    )
    assert stat_mode == (0o666 & ~umask(0))


def test_reflink_unsupported_on_windows(test_dir, mocker):
    src = test_dir / "source"
    dest = test_dir / "dest"
    src.write_bytes(b"content")

    mocker.patch("platform.system", mocker.MagicMock(return_value="Windows"))
    with pytest.raises(OSError) as exc:
        reflink(fspath(src), fspath(dest))

    assert exc.value.errno == errno.ENOTSUP
    assert not dest.exists()
