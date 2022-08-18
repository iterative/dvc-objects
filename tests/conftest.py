import pytest

from dvc_objects.fs.memory import MemoryFileSystem


@pytest.fixture(autouse=True)
def memfs():
    return MemoryFileSystem(global_store=False)
