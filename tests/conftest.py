import pytest

from dvc_objects.fs import as_filesystem


@pytest.fixture
def memfs(memory_path):
    yield as_filesystem(memory_path.fs)
