import functools
import gc
from unittest import mock

from bec_lib.callback_handler import CallbackEntry, CallbackHandler, CallbackRegister, EventType


class _MethodRecorder:
    def __init__(self):
        self.calls = []

    def callback(self, *args):
        self.calls.append(args)


class _CallableRecorder:
    def __call__(self):
        pass


class _UnweakrefableCallableRecorder:
    __slots__ = ("calls",)

    def __init__(self):
        self.calls = []

    def __call__(self, *args):
        self.calls.append(args)


def _callback_entry_repr(func_name, sync, pending_events):
    return (
        "<CallbackEntry>: "
        f"(event_type: EventType.SCAN_SEGMENT, function: {func_name}, "
        f"sync: {sync}, pending events: {pending_events})"
    )


def test_register_callback():
    def dummy():
        pass

    handler = CallbackHandler()
    handler.register("scan_segment", dummy)

    assert len(handler.callbacks) == 1


def test_register_callback_with_cm():
    def dummy():
        pass

    handler = CallbackHandler()
    with CallbackRegister("scan_segment", dummy, callback_handler=handler):
        assert len(handler.callbacks) == 1

    assert len(handler.callbacks) == 0


def test_register_callback_with_cm_multiple():
    def dummy():
        pass

    handler = CallbackHandler()
    scan_id = handler.register("scan_segment", dummy)
    with CallbackRegister("scan_segment", dummy, callback_handler=handler):
        assert len(handler.callbacks) == 2

    assert len(handler.callbacks) == 1
    assert scan_id in handler.callbacks


def test_remove_returns_id():
    def dummy():
        pass

    handler = CallbackHandler()
    scan_id = handler.register("scan_segment", dummy)
    assert handler.remove(scan_id) == scan_id


def test_removal_of_non_existing_item_returns():
    def dummy():
        pass

    handler = CallbackHandler()
    handler.register("scan_segment", dummy)
    assert handler.remove(2) == -1


def test_async_callback_is_called():
    handler = CallbackHandler()
    dummy = mock.MagicMock()
    with CallbackRegister("scan_segment", dummy, callback_handler=handler):
        handler.run("scan_segment", {"data": 1}, {"metadata": 1})
        dummy.assert_called_once_with({"data": 1}, {"metadata": 1})


def test_sync_callback_is_called():
    handler = CallbackHandler()
    dummy = mock.MagicMock()
    with CallbackRegister("scan_segment", dummy, sync=True, callback_handler=handler):
        handler.run("scan_segment", {"data": 1}, {"metadata": 1})
        dummy.assert_not_called()

        handler.poll()
        dummy.assert_called_once_with({"data": 1}, {"metadata": 1})


def test_run_ignores_callbacks_for_other_event_types():
    handler = CallbackHandler()
    dummy = mock.MagicMock()
    callback_id = handler.register("scan_status", dummy)

    handler.run("scan_segment", {"data": 1})

    dummy.assert_not_called()
    assert callback_id in handler.callbacks


def test_poll_skips_async_callbacks_without_removing_them():
    handler = CallbackHandler()
    dummy = mock.MagicMock()
    callback_id = handler.register("scan_segment", dummy, sync=False)

    handler.poll()

    dummy.assert_not_called()
    assert callback_id in handler.callbacks


def test_lambda_callback_is_kept_alive_by_the_handler():
    handler = CallbackHandler()
    calls = []

    callback_id = handler.register("scan_segment", lambda data, metadata: calls.append(data))
    gc.collect()

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert calls == [{"data": 1}]
    assert callback_id in handler.callbacks


def test_inline_function_callback_is_kept_alive_by_the_handler():
    handler = CallbackHandler()
    calls = []

    def register_inline_callback():
        def callback(data, metadata):
            calls.append((data, metadata))

        return handler.register("scan_segment", callback)

    callback_id = register_inline_callback()
    gc.collect()

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert calls == [({"data": 1}, {"metadata": 1})]
    assert callback_id in handler.callbacks


def test_partial_and_callable_object_callbacks_are_kept_alive_by_the_handler():
    class Recorder:
        calls = []

        def __call__(self, data, metadata):
            type(self).calls.append(data)

    def record(sink, data, metadata):
        sink.append(data)

    handler = CallbackHandler()
    partial_calls = []
    handler.register("scan_segment", functools.partial(record, partial_calls))
    handler.register("scan_segment", Recorder())
    gc.collect()

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert partial_calls == [{"data": 1}]
    assert Recorder.calls == [{"data": 1}]
    assert len(handler.callbacks) == 2


def test_builtin_method_callback_can_be_registered():
    handler = CallbackHandler()
    received = []
    handler.register("scan_segment", received.append)

    handler.run("scan_segment", {"data": 1})

    assert received == [{"data": 1}]


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
    def dummy():
        pass

    entry = CallbackEntry(1, EventType.SCAN_SEGMENT, dummy, sync=True)
    entry.run({"data": 1})

    assert str(entry) == _callback_entry_repr("dummy", sync=True, pending_events=1)


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


def test_dead_bound_method_callback_is_removed_on_run():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    callback_id = handler.register("scan_segment", recorder.callback)

    del recorder
    gc.collect()

    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

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


def test_bound_method_registered_twice_is_removed_from_both_entries():
    handler = CallbackHandler()
    recorder = _MethodRecorder()
    handler.register("scan_segment", recorder.callback)
    handler.register("scan_status", recorder.callback)

    del recorder
    gc.collect()
    handler.run("scan_segment", {"data": 1}, {"metadata": 1})

    assert handler.callbacks == {}
