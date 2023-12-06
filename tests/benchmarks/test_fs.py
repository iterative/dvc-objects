import os

import pytest
from reflink import reflink as pyreflink

from dvc_objects.fs.system import hardlink, reflink, symlink

NLINKS = 1000


@pytest.mark.parametrize(
    "link",
    [pytest.param(pyreflink, id="pyreflink"), reflink, hardlink, symlink],
)
def test_link(benchmark, tmp_dir_pytest_cache, link):
    original = tmp_dir_pytest_cache / "original"
    original.mkdir()

    links = tmp_dir_pytest_cache / "links"
    links.mkdir()

    (original / "test").write_text("test")
    try:
        reflink(original / "test", links / "test")
    except OSError as exc:
        pytest.skip(reason=f"reflink not supported: {exc}")

    paths = []
    for idx in range(NLINKS):
        path = original / str(idx)
        path.write_text(path.name)
        paths.append((os.fspath(path), os.fspath(links / path.name)))

    def setup():
        for link in links.iterdir():
            if link.is_file():
                link.unlink()

    def _link(paths):
        for src, path in paths:
            link(src, path)

    benchmark.pedantic(_link, args=(paths,), setup=setup, rounds=10, warmup_rounds=3)
