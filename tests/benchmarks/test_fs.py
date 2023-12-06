import os
import errno
import platform
import pytest
import shutil
from reflink import reflink as _pyreflink
from reflink.error import ReflinkImpossibleError
from dvc_objects.fs.system import reflink, hardlink, symlink, umask

NLINKS = 10000


def pyreflink(src, dst):
    _pyreflink(src, dst)

    if platform.system() == "Darwin":
        # NOTE: pyreflink is not chmod-ing the link to restore normal permissions
        # on macos, so we need to do that ourselves to be fair.
        # See https://github.com/iterative/dql/pull/1007#issuecomment-1841892597

        os.chmod(dst, 0o666 & ~umask)


@pytest.mark.parametrize(
    "link", [pytest.param(pyreflink, id="pyreflink"), reflink, hardlink, symlink]
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
