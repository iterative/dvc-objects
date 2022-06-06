import os
import shutil
from functools import partial
from pathlib import Path

import pytest

from dvc_objects.fs import fastcopy, system
from dvc_objects.fs.utils import human_readable_to_bytes, remove

try:
    import posix  # type: ignore
except ImportError:
    posix = None  # type: ignore


TEST_DIR = Path(__file__).resolve().parents[1] / "copy-test-dir"
FILE_SIZES = [
    "1KB",
    "10KB",
    "100KB",
    "1MB",
    "10MB",
    "100MB",
    "1GB",
    # "2GB",
    # "5GB",
    # "10GB",
]


def write_random_data(
    path: Path, size: int, chunk_size: int = 1024 * 1024 * 1024
) -> None:
    TEST_DIR.mkdir(exist_ok=True, parents=True)
    try:
        with path.open("wb") as fobj:
            while size > chunk_size:
                fobj.write(os.urandom(chunk_size))
                size -= chunk_size
            fobj.write(os.urandom(size))
    except:  # noqa: E722, B001
        remove(os.fspath(path))
        raise


def copyfile_length(src, dst, length=0):
    with open(src, "rb") as fsrc, open(dst, "wb+") as fdst:
        shutil.copyfileobj(fsrc, fdst, length=0)


def copyfile_read(src, dst):
    with open(src, "rb") as fsrc, open(dst, "wb+") as fdst:
        fastcopy._copyfileobj(fsrc, fdst)


def copyfile_readinto(src, dst):
    with open(src, "rb") as fsrc, open(dst, "wb+") as fdst:
        file_size = os.fstat(fsrc.fileno()).st_size
        return fastcopy._copyfileobj_readinto(
            fsrc, fdst, length=min(file_size, fastcopy.COPY_BUFSIZE)
        )


def copyfile_range(src, dst):
    with open(src, "rb") as fsrc, open(dst, "wb+") as fdst:
        fastcopy._copy_file_range(fsrc, fdst)


def copyfile_reflink(src, dst):
    return system.reflink(src, dst)


def copyfile_sendfile(src, dst):
    with open(src, "rb") as fsrc, open(dst, "wb+") as fdst:
        fastcopy._sendfile(fsrc, fdst)


def copyfile_fcopyfile(src, dst):
    with open(src, "rb") as fsrc, open(dst, "wb+") as fdst:
        fastcopy._fcopyfile(fsrc, fdst, posix._COPYFILE_DATA)


COPY_FUNCTIONS = {
    "fastcopy": fastcopy.copyfile,  # platform-specific copy
    "shutil_copy": shutil.copyfile,
    "read": copyfile_read,  # read-write
    "readinto": copyfile_readinto,
    "64k": partial(copyfile_length, length=64 * 1024),
    # "128k": partial(copyfile_length, length=128 * 1024),
    # "256k": partial(copyfile_length, length=256 * 1024),
    # "512k": partial(copyfile_length, length=512 * 1024),
    # "1M": partial(copyfile_length, length=1024 * 1024),
    # "4M": partial(copyfile_length, length=4 * 1024 * 1024),
    # "10M": partial(copyfile_length, length=10 * 1024  * 1024),
    # "100M": partial(copyfile_length, length=100 * 1024 * 1024),
}

if posix and fastcopy._HAS_FCOPYFILE:
    COPY_FUNCTIONS["fcopyfile"] = copyfile_fcopyfile

if fastcopy._USE_CP_COPY_FILE_RANGE:
    COPY_FUNCTIONS["copy_file_range"] = copyfile_range

if fastcopy._USE_CP_SENDFILE:
    COPY_FUNCTIONS["sendfile"] = copyfile_sendfile

COPY_FUNCTIONS["reflink"] = pytest.param(
    copyfile_reflink, marks=pytest.mark.xfail(raises=OSError)
)


@pytest.mark.parametrize("hsize", FILE_SIZES)
@pytest.mark.parametrize(
    "copy_function", COPY_FUNCTIONS.values(), ids=COPY_FUNCTIONS.keys()
)
def test_sendfile(request, benchmark, copy_function, hsize):
    src = TEST_DIR / f"orig-{hsize}"
    dst = TEST_DIR / f"dup-{hsize}"
    if not src.exists():
        write_random_data(src, human_readable_to_bytes(hsize))
    request.addfinalizer(partial(remove, os.fspath(dst)))

    benchmark(copy_function, src, dst)
    assert dst.stat().st_size == src.stat().st_size


if __name__ == "__main__":
    for hsize in FILE_SIZES:
        size = human_readable_to_bytes(hsize)
        write_random_data(TEST_DIR / f"orig-{hsize}", size)
        print(hsize)
