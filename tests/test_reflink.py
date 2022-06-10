import os
import subprocess
import sys
from os import fspath

import pytest

from dvc_objects.fs.system import reflink


@pytest.fixture
def btrfs_mount(tmp_path):
    if sys.platform != "linux":
        pytest.skip("not supported on other platforms")

    volume = tmp_path / "volume"
    with volume.open("wb") as f:
        f.truncate(256 * 1024**2)  # 256 MB

    try:
        subprocess.check_call(["mkfs.btrfs", volume])
    except subprocess.CalledProcessError:
        pytest.skip("no btrfs")

    mount = tmp_path / "mount"
    os.mkdir(mount)
    try:
        assert subprocess.check_call(["mount", volume, mount]) == 0
    except subprocess.CalledProcessError:
        pytest.skip("no permission to mount")

    yield mount
    assert subprocess.call(["umount", volume]) == 0


@pytest.fixture
def mount(request):
    if sys.platform not in ("linux", "darwin"):
        pytest.skip(f"unsupported on platform {sys.platform=}")

    return request.getfixturevalue(
        "btrfs_mount" if sys.platform == "linux" else "tmp_path"
    )


def test_reflink(mount):
    src = mount / "source"
    dest = mount / "dest"

    src.write_bytes(b"content")
    reflink(fspath(src), fspath(dest))

    assert os.path.isfile(mount / "dest")
