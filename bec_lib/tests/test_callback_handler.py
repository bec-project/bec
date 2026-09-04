import functools
import gc
import threading
from contextlib import contextmanager
from unittest import mock

import pytest

from bec_lib.callback_handler import CallbackEntry, CallbackHandler, CallbackRegister, EventType


class _MethodRecorder:
    def __init__(self):
        self.calls = []

    def callback(self, *args):
        self.calls.append(args)

    @staticmethod
    def static_callback(*args):
        _static_calls.append(args)


_static_calls = []
_module_calls = []


def _dummy_callback(*args):
    pass


def _module_callback(*args):
    _module_calls.append(args)


class _CallableRecorder:
    def __call__(self):
        pass


class _UnweakrefableCallableRecorder:
    __slots__ = ("calls",)

    def __init__(self):
        self.calls = []

    def __call__(self, *args):
        self.calls.append(args)


@contextmanager
def _lock_held_by_other_thread(handler):
    acquired, release = threading.Event(), threading.Event()

    def hold():
        with handler._lock:
            acquired.set()
            release.wait()

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    if not acquired.wait(timeout=5):
        pytest.fail("Timed out waiting for helper thread to acquire handler._lock")
    try:
        yield
    finally:
        release.set()
        thread.join()


def _callback_entry_repr(func_name, sync, pending_events):
    return (
        "<CallbackEntry>: "
        f"(event_type: EventType.SCAN_SEGMENT, function: {func_name}, "
        f"sync: {sync}, pending events: {pending_events})"
    )


def test_register_callback():
    handler = CallbackHandler()
    handler.register("scan_segment", _dummy_callback)

    assert len(handler.callbacks) == 1


def test_register_callback_with_cm():
    handler = CallbackHandler()
    with CallbackRegister("scan_segment", _dummy_callback, callback_handler=handler):
        assert len(handler.callbacks) == 1

    assert len(handler.callbacks) == 0


def test_register_callback_with_cm_multiple():
    handler = CallbackHandler()
    scan_id = handler.register("scan_segment", _dummy_callback)
    with CallbackRegister("scan_segment", _dummy_callback, callback_handler=handler):
        assert len(handler.callbacks) == 2

    assert len(handler.callbacks) == 1
    assert scan_id in handler.callbacks


def test_remove_returns_id():
    handler = CallbackHandler()
    scan_id = handler.register("scan_segment", _dummy_callback)
    assert handler.remove(scan_id) == scan_id


def test_removal_of_non_existing_item_returns():
    handler = CallbackHandler()
    handler.register("scan_segment", _dummy_callback)
    assert handler.remove(2) == -1


def test_async_callback_is_called():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    with CallbackRegister("scan_segment", recorder.callback, callback_handler=handler):
        handler.run("scan_segment", {"data": 1}, {"metadata": 1})
        assert recorder.calls == [({"data": 1}, {"metadata": 1})]


def test_sync_callback_is_called():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    with CallbackRegister("scan_segment", recorder.callback, sync=True, callback_handler=handler):
        handler.run("scan_segment", {"data": 1}, {"metadata": 1})
        assert recorder.calls == []

        handler.poll()
        assert recorder.calls == [({"data": 1}, {"metadata": 1})]


def test_run_ignores_callbacks_for_other_event_types():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_status", recorder.callback)

    handler.run("scan_segment", {"data": 1})

    assert recorder.calls == []
    assert callback_id in handler.callbacks


def test_poll_skips_async_callbacks_without_removing_them():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_segment", recorder.callback, sync=False)

    handler.poll()

    assert recorder.calls == []
    assert callback_id in handler.callbacks


def test_module_level_function_is_accepted_and_kept_alive():
    _module_calls.clear()
    handler = CallbackHandler()
    callback_id = handler.register("scan_segment", _module_callback)
    gc.collect()

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert _module_calls == [({"data": 1}, {"metadata": 1})]
    assert callback_id in handler.callbacks


def test_static_method_is_accepted_and_kept_alive():
    _static_calls.clear()
    handler = CallbackHandler()
    callback_id = handler.register("scan_segment", _MethodRecorder().static_callback)
    gc.collect()

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert _static_calls == [({"data": 1}, {"metadata": 1})]
    assert callback_id in handler.callbacks


def _local_function():
    def callback(data, metadata):
        pass

    return callback


@pytest.mark.parametrize(
    "callback",
    [
        lambda data, metadata: None,
        _local_function(),
        functools.partial(_dummy_callback, "prefix"),
        _UnweakrefableCallableRecorder(),
        [].append,
        mock.MagicMock(),
    ],
    ids=["lambda", "local function", "partial", "callable object", "builtin method", "MagicMock"],
)
def test_unsupported_callback_is_rejected_with_error_log(callback):
    handler = CallbackHandler()

    with mock.patch("bec_lib.callback_handler.logger") as logger:
        callback_id = handler.register("scan_segment", callback)

    assert callback_id == -1
    assert handler.callbacks == {}
    logger.error.assert_called_once()


def test_register_many_skips_rejected_callbacks():
    handler = CallbackHandler()

    with mock.patch("bec_lib.callback_handler.logger"):
        ids = handler.register_many("scan_segment", [_dummy_callback, lambda d, m: None])

    assert ids == [1, -1]
    assert list(handler.callbacks) == [1]


def test_falsy_callable_object_is_rejected_with_error_log_not_skipped():
    class Collector:
        def __len__(self):
            return 0

        def __call__(self, data, metadata):
            pass

    handler = CallbackHandler()

    with mock.patch("bec_lib.callback_handler.logger") as logger:
        ids = handler.register_many("scan_segment", [Collector(), None])

    assert ids == [-1, -1]
    logger.error.assert_called_once()


def test_callback_register_skips_rejected_callbacks():
    handler = CallbackHandler()
    recorder = _MethodRecorder()

    with mock.patch("bec_lib.callback_handler.logger"):
        with CallbackRegister(
            "scan_segment", [recorder.callback, lambda d, m: None], callback_handler=handler
        ):
            assert len(handler.callbacks) == 1

    assert handler.callbacks == {}


def test_bound_method_of_unweakrefable_owner_is_kept_alive():
    class Owner:
        __slots__ = ("calls",)

        def __init__(self):
            self.calls = []

        def on_event(self, data, metadata):
            self.calls.append(data)

    handler = CallbackHandler()
    owner = Owner()
    callback_id = handler.register("scan_segment", owner.on_event)
    gc.collect()

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert owner.calls == [{"data": 1}]
    assert callback_id in handler.callbacks


def test_callback_entry_str_uses_callback_name_and_pending_count():
    entry = CallbackEntry(1, EventType.SCAN_SEGMENT, _dummy_callback, sync=True)
    entry.run({"data": 1})

    assert str(entry) == _callback_entry_repr("_dummy_callback", sync=True, pending_events=1)


def test_callback_entry_str_uses_callable_type_name_when_callback_has_no_name():
    recorder = _CallableRecorder()
    entry = CallbackEntry(1, EventType.SCAN_SEGMENT, recorder, sync=False)

    assert str(entry) == _callback_entry_repr("_CallableRecorder", sync=False, pending_events=0)


def test_callback_entry_keeps_unweakrefable_callable_alive():
    recorder = _UnweakrefableCallableRecorder()
    entry = CallbackEntry(1, EventType.SCAN_SEGMENT, recorder, sync=False)

    entry.run({"data": 1})

    assert recorder.calls == [({"data": 1},)]
    assert entry.is_alive()
    assert str(entry) == _callback_entry_repr(
        "_UnweakrefableCallableRecorder", sync=False, pending_events=0
    )


def test_callback_entry_str_marks_dead_callback():
    recorder = _MethodRecorder()
    entry = CallbackEntry(1, EventType.SCAN_SEGMENT, recorder.callback, sync=False)

    del recorder
    gc.collect()

    assert str(entry) == _callback_entry_repr("<dead>", sync=False, pending_events=0)


def test_dead_bound_method_callback_is_removed_immediately():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_segment", recorder.callback)

    del recorder
    gc.collect()

    assert callback_id not in handler.callbacks


def test_dead_bound_method_callback_is_removed_on_run_when_handler_was_busy():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_segment", recorder.callback)

    with _lock_held_by_other_thread(handler):
        del recorder
        gc.collect()
        assert callback_id in handler.callbacks  # deferred, the finalizer did not block

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert callback_id not in handler.callbacks


def test_dead_bound_method_callback_is_removed_on_poll_when_handler_was_busy():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_segment", recorder.callback, sync=True)

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})
    assert handler.callbacks[callback_id].num_pending_events == 1

    with _lock_held_by_other_thread(handler):
        del recorder
        gc.collect()
        assert callback_id in handler.callbacks

    handler.poll()

    assert callback_id not in handler.callbacks


def test_dead_bound_method_callback_is_logged_and_removed_on_unrelated_event():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_status", recorder.callback)

    with mock.patch("bec_lib.callback_handler.logger") as logger:
        del recorder
        gc.collect()
    logger.info.assert_called_once()

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert callback_id not in handler.callbacks


def test_removed_callback_is_not_logged_when_owner_is_collected():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    handler.remove(handler.register("scan_segment", recorder.callback))

    with mock.patch("bec_lib.callback_handler.logger") as logger:
        del recorder
        gc.collect()

    logger.info.assert_not_called()


def test_bound_method_registered_twice_is_removed_from_both_entries_immediately():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    handler.register("scan_segment", recorder.callback)
    handler.register("scan_status", recorder.callback)

    del recorder
    gc.collect()

    assert handler.callbacks == {}


def test_bound_method_registered_on_two_handlers_is_removed_from_both():
    first, second = CallbackHandler(), CallbackHandler()
    recorder = _MethodRecorder()
    first.register("scan_segment", recorder.callback)
    second.register("scan_segment", recorder.callback)

    del recorder
    gc.collect()
    first.run("scan_segment", {"data": 1}, {"metadata": 1})
    second.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert first.callbacks == {}
    assert second.callbacks == {}
