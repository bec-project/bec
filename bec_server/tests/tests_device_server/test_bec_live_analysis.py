"""Live analysis retains dynamic payloads in bounded streams without scan indices."""

from types import SimpleNamespace
from unittest import mock

import pytest
from ophyd import Device
from ophyd_devices.utils.bec_signals import DynamicSignal

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.bec_message_handler import BECMessageHandler
from bec_server.device_server.devices.devicemanager import DeviceManagerDS


def make_handler(connector=None):
    manager = SimpleNamespace(
        scan_info=SimpleNamespace(msg=None), connector=connector or mock.Mock(), devices={}
    )
    signal = DynamicSignal(
        name="det_roi",
        parent=Device(name="det"),
        max_size=3,
        ndim=0,
        signals=["det_roi"],
        async_update={"type": "add", "max_shape": [None]},
    )
    return manager, BECMessageHandler(manager), signal


def emit(handler, signal, value=1, *, preview=False):
    signal.put(
        {"det_roi": {"value": value, "timestamp": 1.0}},
        metadata={"scan_id": "preview"} if preview else {},
    )
    message = signal.get()
    handler.emit(signal, message)
    return message


@pytest.mark.parametrize(
    "status", ["open", "paused", "closed", "aborted", "halted", "user_completed"]
)
def test_device_scan_context_clears_only_on_terminal_status(status):
    manager = SimpleNamespace(scan_info=SimpleNamespace(msg=None))
    message = messages.ScanStatusMessage(scan_id="scan-1", status=status, info={})
    DeviceManagerDS._update_scan_info(manager, MessageObject(topic="", value=message))
    assert manager.scan_info.msg is (message if status in {"open", "paused"} else None)
    assert message.scan_id == "scan-1"
    assert message.status == status


@pytest.mark.parametrize(
    "status", [None, "open", "paused", "closed", "aborted", "halted", "user_completed"]
)
@pytest.mark.parametrize("preview", [False, True])
def test_analysis_routing_preserves_dynamic_message(status, preview):
    manager, handler, signal = make_handler()
    if status is not None:
        manager.scan_info.msg = messages.ScanStatusMessage(scan_id="scan-1", status=status, info={})
    try:
        message = emit(handler, signal, preview=preview)
        if not preview and status not in {"open", "paused"}:
            manager.connector.xadd.assert_not_called()
            manager.connector.rpush.assert_not_called()
            assert not handler._async_signal_index_state
            return
        expected_scan = "preview" if preview else "scan-1"
        call = manager.connector.xadd.call_args
        assert call.args[0] == MessageEndpoints.device_async_signal(
            scan_id=expected_scan, device="det", signal="det_roi"
        )
        assert call.args[1]["data"] is message
        assert isinstance(message, messages.DeviceMessage)
        assert message.signals == {"det_roi": {"value": 1, "timestamp": 1.0}}
        if expected_scan == "preview":
            assert "async_indices" not in message.metadata
            manager.connector.rpush.assert_not_called()
            assert call.kwargs["max_size"] == 100
            assert not handler._async_signal_index_state
        else:
            assert message.metadata["async_indices"] == {"det_roi": 0}
            index = manager.connector.rpush.call_args.args[1]
            assert index.scan_id == expected_scan
            assert index.indices == {"det_roi": 0}
            assert index.shapes == {"det_roi": []}
    finally:
        signal.destroy()


def test_scan_indices_are_unaffected_by_preview_updates():
    manager, handler, signal = make_handler()
    try:
        emit(handler, signal, preview=True)
        assert not handler._async_signal_index_state
        manager.scan_info.msg = messages.ScanStatusMessage(scan_id="scan-1", status="open", info={})
        assert emit(handler, signal).metadata["async_indices"] == {"det_roi": 0}
        manager.scan_info.msg = None
        assert "async_indices" not in emit(handler, signal, preview=True).metadata
        manager.scan_info.msg = messages.ScanStatusMessage(scan_id="scan-2", status="open", info={})
        assert emit(handler, signal).metadata["async_indices"] == {"det_roi": 0}
        assert len(handler._async_signal_index_state) == 1
    finally:
        signal.destroy()


def test_live_data_is_bounded_without_index_history(connected_connector):
    _, handler, signal = make_handler(connected_connector)
    try:
        # Approximate trimming removes complete Redis stream nodes.
        updates = 500
        for value in range(updates):
            emit(handler, signal, value, preview=True)
        endpoint = MessageEndpoints.device_async_signal("preview", "det", "det_roi")
        entries = connected_connector.xrange(endpoint, "-", "+")
        assert 100 <= len(entries) < updates
        assert entries[0]["data"].signals["det_roi"]["value"] == updates - len(entries)
        assert entries[-1]["data"].signals["det_roi"]["value"] == updates - 1
        indices = MessageEndpoints.device_async_signal_index("preview", "det", "det_roi")
        assert connected_connector.lrange(indices, 0, -1) == []
    finally:
        signal.destroy()


@pytest.mark.parametrize("status", ["open", "paused"])
def test_unmarked_results_without_scan_id_are_rejected(status):
    manager, handler, signal = make_handler()
    manager.scan_info.msg = messages.ScanStatusMessage(scan_id=None, status=status, info={})
    try:
        emit(handler, signal)
        manager.connector.xadd.assert_not_called()
        manager.connector.rpush.assert_not_called()
        assert not handler._async_signal_index_state
    finally:
        signal.destroy()


def test_explicit_live_routing_does_not_consult_changing_scan_context():
    _, handler, signal = make_handler()
    try:
        with mock.patch.object(
            handler, "_get_scan_status_message", side_effect=AssertionError("Live routing is fixed")
        ):
            message = emit(handler, signal, preview=True)
        assert message.metadata["scan_id"] == "preview"
        assert "async_indices" not in message.metadata
    finally:
        signal.destroy()
