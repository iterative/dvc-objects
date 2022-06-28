import pytest

from dvc_objects.fs import MemoryFileSystem


@pytest.fixture(autouse=True)
def memfs():
    return MemoryFileSystem(global_store=False)
