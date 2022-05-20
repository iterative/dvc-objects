import pytest

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, Callback


@pytest.mark.parametrize(
    "api", ["set_size", "relative_update", "absolute_update"]
)
@pytest.mark.parametrize(
    "callback_factory, kwargs",
    [
        (Callback.as_callback, {}),
        (Callback.as_tqdm_callback, {"desc": "test"}),
    ],
)
def test_callback_with_none(request, api, callback_factory, kwargs, mocker):
    """
    Test that callback don't fail if they receive None.
    The callbacks should not receive None, but there may be some
    filesystems that are not compliant, we may want to maintain
    maximum compatibility, and not break UI in these edge-cases.
    See https://github.com/iterative/dvc/issues/7704.
    """
    callback = callback_factory(**kwargs)
    request.addfinalizer(callback.close)

    call_mock = mocker.spy(callback, "call")
    method = getattr(callback, api)
    method(None)
    call_mock.assert_called_once_with()
    if callback is not DEFAULT_CALLBACK:
        assert callback.size is None
        assert callback.value == 0
