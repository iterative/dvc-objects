import errno
import shutil

import pytest
from reflink import reflink as pyreflink
from reflink.error import ReflinkImpossibleError

from dvc_objects.fs.system import hardlink, reflink, symlink

NLINKS = 10000


@pytest.mark.parametrize(
    "link",
    [pytest.param(pyreflink, id="pyreflink"), reflink, hardlink, symlink],
)
def test_link(benchmark, tmp_path, link):
    (tmp_path / "original").mkdir()

    for idx in range(NLINKS):
        (tmp_path / "original" / str(idx)).write_text(str(idx))

    def _setup():
        try:
            shutil.rmtree(tmp_path / "links")
        except FileNotFoundError:
            pass

        (tmp_path / "links").mkdir()

    original = str(tmp_path / "original")
    links = str(tmp_path / "links")

    def _link():
        for idx in range(NLINKS):
            try:
                link(f"{original}/{idx}", f"{links}/{idx}")
            except Exception as exc:
                if isinstance(exc, (ReflinkImpossibleError, NotImplementedError)) or (
                    isinstance(exc, OSError) and exc.errno == errno.ENOTSUP
                ):
                    pytest.skip(str(exc))
                raise

    benchmark.pedantic(_link, setup=_setup)
