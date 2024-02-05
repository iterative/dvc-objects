from fsspec.callbacks import Callback

from dvc_objects.fs.callbacks import wrap_file


def test_wrap_file(memfs):
    memfs.pipe_file("/file", b"foo\n")

    callback = Callback()

    callback.set_size(4)
    with memfs.open("/file", mode="rb") as f:
        wrapped = wrap_file(f, callback)
        assert wrapped.read() == b"foo\n"

    assert callback.value == 4
    assert callback.size == 4
