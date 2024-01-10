from typing import Optional

import fsspec
import pytest

from dvc_objects.fs.callbacks import (
    DEFAULT_CALLBACK,
    Callback,
    TqdmCallback,
    branch_callback,
    wrap_and_branch_callback,
    wrap_file,
    wrap_fn,
)


@pytest.mark.parametrize("api", ["set_size", "absolute_update"])
def test_callback_with_none(request, api, mocker):
    """
    Test that callback don't fail if they receive None.
    The callbacks should not receive None, but there may be some
    filesystems that are not compliant, we may want to maintain
    maximum compatibility, and not break UI in these edge-cases.
    See https://github.com/iterative/dvc/issues/7704.
    """
    callback = Callback.as_callback()
    request.addfinalizer(callback.close)

    call_mock = mocker.spy(callback, "call")
    method = getattr(callback, api)
    method(None)
    call_mock.assert_called_once_with()
    if callback is not DEFAULT_CALLBACK:
        assert callback.size is None
        assert callback.value == 0


def test_wrap_fsspec():
    def _branch_fn(*args, callback: Optional["Callback"] = None, **kwargs):
        pass

    callback = fsspec.callbacks.Callback()
    assert callback.value == 0
    with Callback.as_callback(callback) as cb:
        assert not isinstance(cb, TqdmCallback)
        assert cb.value == 0
        cb.relative_update()
        assert cb.value == 1
        assert callback.value == 1

        with cb.branch("foo", "bar", {}) as child:
            _branch_fn("foo", "bar", callback=child)
            cb.relative_update()

        assert cb.value == 2
        assert callback.value == 2


def ids_func(cb_type):
    return f"{cb_type.__module__}.{cb_type.__qualname__}"


class IsDVCCallback:
    def __eq__(self, value: object) -> bool:
        return isinstance(value, Callback)


@pytest.fixture(params=[Callback, fsspec.Callback], ids=ids_func)
def cb_class(request):
    return request.param


def test_wrap_fn_sync(mocker, cb_class):
    m = mocker.MagicMock(return_value=1)
    callback = cb_class()

    wrapped = wrap_fn(callback, m)

    assert wrapped("arg") == 1
    assert callback.value == 1
    m.assert_called_once_with("arg")


@pytest.mark.asyncio
async def test_wrap_fn_async(mocker, cb_class):
    m = mocker.AsyncMock(return_value=1)
    callback = cb_class()

    wrapped = wrap_fn(callback, m)

    assert await wrapped("arg") == 1
    assert callback.value == 1
    m.assert_called_once_with("arg")


def test_branch_fn_sync(mocker, cb_class):
    m = mocker.MagicMock(return_value=1)
    callback = cb_class()
    spy = mocker.spy(callback, "branch")
    wrapped = branch_callback(callback, m)

    assert wrapped("arg1", "arg2") == 1
    assert callback.value == 0
    assert spy.call_count == 1
    m.assert_called_once_with("arg1", "arg2", callback=IsDVCCallback())


@pytest.mark.asyncio
async def test_branch_fn_async(mocker, cb_class):
    m = mocker.AsyncMock(return_value=1)
    callback = cb_class()
    spy = mocker.spy(callback, "branch")
    wrapped = branch_callback(callback, m)

    assert await wrapped("arg1", "arg2") == 1
    assert callback.value == 0
    assert spy.call_count == 1
    m.assert_called_once_with("arg1", "arg2", callback=IsDVCCallback())


def test_wrap_and_branch_callback_sync(mocker, cb_class):
    m = mocker.MagicMock(return_value=1)
    callback = cb_class()
    spy = mocker.spy(callback, "branch")
    wrapped = wrap_and_branch_callback(callback, m)

    assert wrapped("arg1", "arg2", arg3="arg3") == 1
    assert wrapped("argA", "argB", arg3="argC") == 1

    m.assert_any_call("arg1", "arg2", arg3="arg3", callback=IsDVCCallback())
    m.assert_any_call("argA", "argB", arg3="argC", callback=IsDVCCallback())
    assert callback.value == 2
    assert spy.call_count == 2


@pytest.mark.asyncio
async def test_wrap_and_branch_callback_async(mocker, cb_class):
    m = mocker.AsyncMock(return_value=1)
    callback = cb_class()
    spy = mocker.spy(callback, "branch")
    wrapped = wrap_and_branch_callback(callback, m)

    assert await wrapped("arg1", "arg2", arg3="arg3") == 1
    assert await wrapped("argA", "argB", arg3="argC") == 1

    m.assert_any_call("arg1", "arg2", arg3="arg3", callback=IsDVCCallback())
    m.assert_any_call("argA", "argB", arg3="argC", callback=IsDVCCallback())
    assert callback.value == 2
    assert spy.call_count == 2


def test_wrap_file(memfs):
    memfs.pipe_file("/file", b"foo\n")

    callback = Callback()

    callback.set_size(4)
    with memfs.open("/file", mode="rb") as f:
        wrapped = wrap_file(f, callback)
        assert wrapped.read() == b"foo\n"

    assert callback.value == 4
    assert callback.size == 4
