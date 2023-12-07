import os

import pytest
from reflink import ReflinkImpossibleError
from reflink import reflink as pyreflink

from dvc_objects.fs.system import hardlink, reflink, symlink

NLINKS = 1_000


@pytest.fixture(scope="session")
def original(make_tmp_dir_pytest_cache):
    return make_tmp_dir_pytest_cache("original")


@pytest.fixture(scope="session")
def links(make_tmp_dir_pytest_cache):
    return make_tmp_dir_pytest_cache("links")


@pytest.fixture(
    params=[pytest.param(pyreflink, id="pyreflink"), reflink, hardlink, symlink]
)
def link(request, original, links):
    link = request.param
    (original / "test").write_text("test")
    try:
        link(os.fspath(original / "test"), os.fspath(links / "test"))
    except (OSError, NotImplementedError, ReflinkImpossibleError) as exc:
        pytest.skip(reason=f"{link.__module__}.{link.__name__} not supported: {exc}")
    return link


@pytest.fixture(scope="session")
def paths(original, links):
    paths = []
    for idx in range(NLINKS):
        path = original / str(idx)
        path.write_text(path.name)
        paths.append((os.fspath(path), os.fspath(links / path.name)))
    return paths


def test_link(benchmark, paths, links, link):
    def setup():
        for link in links.iterdir():
            if link.is_file():
                link.unlink()

    def _link(paths):
        for src, path in paths:
            link(src, path)

    benchmark.pedantic(_link, args=(paths,), setup=setup, rounds=10, warmup_rounds=3)
