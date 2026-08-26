import gc
from functools import partial

import pytest

from bec_lib.callback_handler import CallbackEntry, CallbackHandler, CallbackRegister, EventType


class _MethodRecorder:
    def __init__(self):
        self.calls = []

    def callback(self, *args):
        self.calls.append(args)


class _CallableRecorder:
    def __init__(self):
        self.calls = []

    def callback(self, *args):
        self.calls.append(args)

    def __call__(self, *args):
        self.calls.append(args)


class _UnweakrefableCallableRecorder:
    __slots__ = ("calls",)

    def __init__(self):
        self.calls = []

    def __call__(self, *args):
        self.calls.append(args)


def _dummy_callback(*args):
    pass


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
    recorder = _CallableRecorder()
    with CallbackRegister("scan_segment", recorder, callback_handler=handler):
        handler.run("scan_segment", {"data": 1}, {"metadata": 1})
        assert recorder.calls == [({"data": 1}, {"metadata": 1})]


def test_sync_callback_is_called():
    handler = CallbackHandler()
    recorder = _CallableRecorder()
    with CallbackRegister("scan_segment", recorder, sync=True, callback_handler=handler):
        handler.run("scan_segment", {"data": 1}, {"metadata": 1})
        assert recorder.calls == []

        handler.poll()
        assert recorder.calls == [({"data": 1}, {"metadata": 1})]


def test_run_ignores_callbacks_for_other_event_types():
    handler = CallbackHandler()
    recorder = _CallableRecorder()
    callback_id = handler.register("scan_status", recorder)

    handler.run("scan_segment", {"data": 1})

    assert recorder.calls == []
    assert callback_id in handler.callbacks


def test_poll_skips_async_callbacks_without_removing_them():
    handler = CallbackHandler()
    recorder = _CallableRecorder()
    callback_id = handler.register("scan_segment", recorder, sync=False)

    handler.poll()

    assert recorder.calls == []
    assert callback_id in handler.callbacks


def test_local_lambda_callback_is_rejected():
    handler = CallbackHandler()

    with pytest.raises(ValueError, match="Local functions such as lambdas"):
        handler.register("scan_segment", lambda data, metadata: None)


def test_inline_function_callback_is_rejected():
    handler = CallbackHandler()

    def callback(data, metadata):
        pass

    with pytest.raises(ValueError, match="Local functions such as lambdas"):
        handler.register("scan_segment", callback)


def test_partial_callback_is_rejected():
    handler = CallbackHandler()

    with pytest.raises(ValueError, match="functools.partial objects cannot be used as callbacks"):
        handler.register("scan_segment", partial(_dummy_callback, "prefix"))


def test_callback_entry_str_uses_callback_name_and_pending_count():
    entry = CallbackEntry(1, EventType.SCAN_SEGMENT, _dummy_callback, sync=True)
    entry.run({"data": 1})

    assert str(entry) == _callback_entry_repr("_dummy_callback", sync=True, pending_events=1)


def test_callback_entry_str_uses_callable_type_name_when_callback_has_no_name():
    recorder = _CallableRecorder()
    entry = CallbackEntry(1, EventType.SCAN_SEGMENT, recorder, sync=False)

    assert str(entry) == _callback_entry_repr("_CallableRecorder", sync=False, pending_events=0)


def test_callback_entry_rejects_unweakrefable_callable():
    recorder = _UnweakrefableCallableRecorder()

    with pytest.raises(TypeError, match="cannot create weak reference"):
        CallbackEntry(1, EventType.SCAN_SEGMENT, recorder, sync=False)


def test_callback_entry_str_marks_dead_callback():
    recorder = _MethodRecorder()
    entry = CallbackEntry(1, EventType.SCAN_SEGMENT, recorder.callback, sync=False)

    del recorder
    gc.collect()

    assert str(entry) == _callback_entry_repr("<dead>", sync=False, pending_events=0)


def test_dead_bound_method_callback_is_removed_on_run():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_segment", recorder.callback)

    del recorder
    gc.collect()

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert callback_id not in handler.callbacks


def test_dead_bound_method_callback_is_removed_by_weakref_callback():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_segment", recorder.callback)

    del recorder
    gc.collect()

    assert callback_id not in handler.callbacks


def test_dead_bound_method_callback_is_removed_on_poll():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_segment", recorder.callback, sync=True)

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})
    assert handler.callbacks[callback_id].num_pending_events == 1

    del recorder
    gc.collect()

    handler.poll()

    assert callback_id not in handler.callbacks
