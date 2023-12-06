from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from dvc_objects.fs.memory import MemoryFileSystem


@pytest.fixture(autouse=True)
def memfs():
    return MemoryFileSystem(global_store=False)


@pytest.fixture(scope="session")
def make_tmp_dir_pytest_cache(request):
    cache = request.config.cache
    path = cache.mkdir("dvc_objects_tests")

    def inner(name=None):
        tmp_dir = TemporaryDirectory(prefix=name, dir=path)
        request.addfinalizer(tmp_dir.cleanup)
        return Path(tmp_dir.name)

    return inner


@pytest.fixture
def tmp_dir_pytest_cache(make_tmp_dir_pytest_cache):
    """Create a test directory within cache directory.

    The cache directory by default, is in the root of the repo, where reflink
    may be supported.
    """
    return make_tmp_dir_pytest_cache("tmp")
