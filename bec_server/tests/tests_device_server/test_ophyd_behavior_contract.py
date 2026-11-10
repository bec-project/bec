"""Characterize the Ophyd publication behavior that the provider migration must preserve."""

from __future__ import annotations

from unittest import mock

import numpy as np
import pytest
from ophyd import Component as Cpt
from ophyd import Device, Kind, Signal
from ophyd_devices import DynamicSignal, FileEventSignal, PreviewSignal, ProgressSignal

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.bec_message_handler import BECMessageHandler
from bec_server.device_server.devices.devicemanager import DSDevice, DeviceManagerDS

# pylint: disable=protected-access

pytestmark = pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])


class _PublicationDevice(Device):
    """Soft signals for snapshot and BEC message publication contracts."""

    first = Cpt(Signal, value=1, kind=Kind.normal)
    second = Cpt(Signal, value=2, kind=Kind.hinted)
    configuration = Cpt(Signal, value=3, kind=Kind.config)
    low_limit_travel = Cpt(Signal, value=-10, kind=Kind.omitted)
    high_limit_travel = Cpt(Signal, value=10, kind=Kind.omitted)
    file_event = Cpt(FileEventSignal)
    progress = Cpt(ProgressSignal)
    preview = Cpt(PreviewSignal, ndim=1)
    data = Cpt(DynamicSignal, max_size=7, signals=["left", "right"])


@pytest.fixture
def publication_device(device_manager, connected_connector):
    """Reuse the manager fixture with deterministic soft devices and real Redis semantics."""
    manager = device_manager
    # Full-suite collection can supply an already populated manager. Stop its worker before
    # replacing devices so tests can control drain boundaries without sleeps or thread races.
    manager._shutdown_event.set()
    if manager._auto_monitor_update_thread.is_alive():
        manager._auto_monitor_update_thread.join(timeout=5)
    assert not manager._auto_monitor_update_thread.is_alive()
    for existing in manager.devices.values():
        manager.disconnect_device(existing.obj)
    manager.devices.flush()
    manager._auto_monitor_readback_updates.clear()
    manager._auto_monitor_configuration_updates.clear()
    manager._limit_change_updates.clear()

    original_connector = manager.connector
    manager.connector = connected_connector
    manager._bec_message_handler = BECMessageHandler(manager)
    obj = _PublicationDevice(name="publication_device")
    device = DSDevice(
        name=obj.name,
        obj=obj,
        config={"deviceClass": "ophyd.Device", "enabled": True, "readoutPriority": "monitored"},
        parent=manager,
    )
    device.metadata = {"phase": "notification"}
    manager.devices._add_device(device.name, device)
    manager.scan_info.msg = messages.ScanStatusMessage(scan_id="scan-1", status="open", info={})
    try:
        yield manager, device
    finally:
        manager.connector = original_connector


def test_monitor_burst_publishes_latest_root_snapshot_in_one_pipeline(publication_device):
    """Coalesce a burst per category, read the whole root, and sample metadata at drain time."""
    manager, device = publication_device
    obj = device.obj
    connector = manager.connector
    for _ in range(5):
        manager._obj_callback_auto_monitor_readback(obj=obj.first)
        manager._obj_callback_auto_monitor_readback(obj=obj.second)
        manager._obj_callback_auto_monitor_configuration(obj=obj.configuration)
        manager._obj_callback_auto_monitor_limits(obj=obj.low_limit_travel)
    obj.first.put(11)
    device.metadata = {"phase": "drain", "scan_id": "scan-1"}
    pipe = connector.pipeline()
    with (
        mock.patch.object(manager._shutdown_event, "wait", side_effect=[False, True]) as wait,
        mock.patch.object(connector, "pipeline", return_value=pipe),
        mock.patch.object(pipe, "execute", wraps=pipe.execute) as execute,
        mock.patch.object(connector, "set_and_publish", wraps=connector.set_and_publish) as publish,
        mock.patch.object(obj, "read", wraps=obj.read) as read,
        mock.patch.object(obj, "read_configuration", wraps=obj.read_configuration) as read_config,
    ):
        manager._auto_monitor_update_loop()

    assert wait.call_args_list == [mock.call(0.01), mock.call(0.01)]
    read.assert_called_once_with()
    read_config.assert_called_once_with()
    execute.assert_called_once_with()
    assert [call.args[0] for call in publish.call_args_list] == [
        MessageEndpoints.device_readback(device.name),
        MessageEndpoints.device_read_configuration(device.name),
        MessageEndpoints.device_limits(device.name),
    ]
    assert all(call.kwargs.get("pipe") is pipe for call in publish.call_args_list)
    readback = connector.get(MessageEndpoints.device_readback(device.name))
    assert {name: value["value"] for name, value in readback.signals.items()} == {
        obj.first.name: 11,
        obj.second.name: 2,
    }
    assert readback.metadata == device.metadata
    configuration = connector.get(MessageEndpoints.device_read_configuration(device.name))
    assert configuration.signals[obj.configuration.name]["value"] == 3
    assert configuration.metadata == device.metadata
    limits = connector.get(MessageEndpoints.device_limits(device.name))
    assert limits.signals == {"low": {"value": -10}, "high": {"value": 10}}
    assert limits.metadata == {}
    assert connector.get(MessageEndpoints.device_read(device.name)) is None


def test_monitor_notification_during_acquisition_is_published_next_drain(publication_device):
    """A notification arriving while a snapshot is read must survive for the next drain."""
    manager, device = publication_device
    obj = device.obj
    manager._obj_callback_auto_monitor_readback(obj=obj.first)
    original_read = obj.read
    reads = 0

    def read_and_notify():
        nonlocal reads
        reading = original_read()
        reads += 1
        if reads == 1:
            obj.first.put(22)
            manager._obj_callback_auto_monitor_readback(obj=obj.first)
        return reading

    with (
        mock.patch.object(manager._shutdown_event, "wait", side_effect=[False, False, True]),
        mock.patch.object(obj, "read", side_effect=read_and_notify),
        mock.patch.object(
            manager.connector, "set_and_publish", wraps=manager.connector.set_and_publish
        ) as publish,
    ):
        manager._auto_monitor_update_loop()

    assert reads == 2
    assert [call.args[1].signals[obj.first.name]["value"] for call in publish.call_args_list] == [
        1,
        22,
    ]
    assert not manager._auto_monitor_readback_updates


def test_monitor_acquisition_error_does_not_publish_partial_batch(publication_device):
    """Keep the existing batch failure boundary when a later snapshot section fails."""
    manager, device = publication_device
    manager._obj_callback_auto_monitor_readback(obj=device.obj.first)
    manager._obj_callback_auto_monitor_configuration(obj=device.obj.configuration)
    manager._obj_callback_auto_monitor_limits(obj=device.obj.low_limit_travel)
    pipe = manager.connector.pipeline()
    with (
        mock.patch.object(manager._shutdown_event, "wait", side_effect=[False, True]),
        mock.patch.object(manager.connector, "pipeline", return_value=pipe),
        mock.patch.object(pipe, "execute", wraps=pipe.execute) as execute,
        mock.patch.object(
            device.obj, "read_configuration", side_effect=RuntimeError("read failed")
        ),
    ):
        manager._auto_monitor_update_loop()
    execute.assert_not_called()
    for endpoint in (
        MessageEndpoints.device_readback,
        MessageEndpoints.device_read_configuration,
        MessageEndpoints.device_limits,
    ):
        assert manager.connector.get(endpoint(device.name)) is None
    assert not manager._auto_monitor_readback_updates
    assert not manager._auto_monitor_configuration_updates
    assert not manager._limit_change_updates
    pipe.reset()


@pytest.mark.parametrize(
    "kind, category",
    [
        (Kind.normal, "readback"),
        (Kind.hinted, "readback"),
        (Kind.config, "configuration"),
        (Kind.omitted, None),
        (Kind.normal | Kind.config, None),
        (Kind.hinted | Kind.config, None),
    ],
)
def test_monitor_subscription_kind_contract(publication_device, kind, category):
    """Capture native kind selection, including combinations that currently do not monitor."""
    manager, device = publication_device
    signal = device.obj.first
    signal.kind = kind
    signal._auto_monitor = True
    with mock.patch.object(manager, "_ensure_auto_monitor_update_thread"):
        manager._subscribe_to_auto_monitors(device.obj)
    # Subscription setup uses run=False; the first notification comes from a subsequent put.
    assert not manager._auto_monitor_readback_updates
    assert not manager._auto_monitor_configuration_updates
    signal.put(9)
    assert manager._auto_monitor_readback_updates == (
        {device.name} if category == "readback" else set()
    )
    assert manager._auto_monitor_configuration_updates == (
        {device.name} if category == "configuration" else set()
    )


def test_file_events_keep_each_payload_and_both_destinations(publication_device):
    """Each file event is published to both existing destinations in its own pipeline."""
    manager, device = publication_device
    manager._subscribe_to_bec_signals(device.obj)
    payloads = [
        messages.FileMessage(
            file_path=f"/tmp/file-{index}.h5", done=True, successful=True, metadata={"event": index}
        )
        for index in (1, 2)
    ]
    with mock.patch.object(
        manager.connector, "set_and_publish", wraps=manager.connector.set_and_publish
    ) as publish:
        for payload in payloads:
            device.obj.file_event.put(payload)
    assert [call.args[0] for call in publish.call_args_list] == [
        MessageEndpoints.file_event(device.name),
        MessageEndpoints.public_file(scan_id="scan-1", name=device.name),
    ] * 2
    assert [call.args[1] for call in publish.call_args_list] == [payloads[0]] * 2 + [
        payloads[1]
    ] * 2
    assert all(call.kwargs["expire"] == 3600 for call in publish.call_args_list)
    calls = publish.call_args_list
    assert calls[0].kwargs["pipe"] is calls[1].kwargs["pipe"]
    assert calls[2].kwargs["pipe"] is calls[3].kwargs["pipe"]
    assert calls[0].kwargs["pipe"] is not calls[2].kwargs["pipe"]
    assert (
        manager.connector.get(MessageEndpoints.file_event(device.name)).file_path
        == payloads[-1].file_path
    )


def test_progress_events_preserve_each_update_and_enrich_metadata(publication_device):
    """Progress carries the scan context and keeps every callback delivery."""
    manager, device = publication_device
    manager._subscribe_to_bec_signals(device.obj)
    with mock.patch.object(
        manager.connector, "set_and_publish", wraps=manager.connector.set_and_publish
    ) as publish:
        for value in (1, 2):
            device.obj.progress.put(
                value=value, max_value=2, done=value == 2, metadata={"user": "value"}
            )
    assert [call.args[1].value for call in publish.call_args_list] == [1, 2]
    for call in publish.call_args_list:
        assert call.args[0] == MessageEndpoints.device_progress(device.name)
        assert call.args[1].metadata == {"user": "value", "scan_id": "scan-1", "status": "open"}
        assert call.kwargs == {"expire": 3600}


@pytest.mark.parametrize("category", ["file_event", "progress"])
def test_bec_message_events_without_scan_context_are_not_published(publication_device, category):
    """Keep the existing scan-context requirements for file and progress events."""
    manager, device = publication_device
    manager.scan_info.msg = None
    manager._subscribe_to_bec_signals(device.obj)
    payload = (
        messages.FileMessage(file_path="/tmp/file.h5", done=True, successful=True)
        if category == "file_event"
        else messages.ProgressMessage(value=1, max_value=2, done=False)
    )
    with mock.patch.object(manager.connector, "set_and_publish") as publish:
        getattr(device.obj, category).put(payload)
    publish.assert_not_called()


@pytest.mark.parametrize("size, max_size", [(8, 100), (11_000_000, 90)])
def test_preview_event_routing_and_stream_policy(publication_device, size, max_size):
    """Preview routing uses the payload signal name and preserves size-based retention."""
    manager, device = publication_device
    manager._subscribe_to_bec_signals(device.obj)
    payload = messages.DevicePreviewMessage(
        device=device.name, signal="wire_preview", data=np.zeros(size, dtype=np.uint8)
    )
    with mock.patch.object(manager.connector, "xadd") as publish:
        device.obj.preview.put(payload)
    publish.assert_called_once_with(
        MessageEndpoints.device_preview(device.name, "wire_preview"),
        {"data": payload},
        max_size=max_size,
        expire=3600,
    )


@pytest.mark.parametrize(
    "state", ["open", "paused", "closed", "aborted", "halted", "user_completed", "missing"]
)
def test_dynamic_message_scan_state_contract(publication_device, state):
    """Dynamic messages publish only in the existing accepted scan states."""
    manager, device = publication_device
    manager.scan_info.msg = (
        None
        if state == "missing"
        else messages.ScanStatusMessage(scan_id="scan-1", status=state, info={})
    )
    manager._subscribe_to_bec_signals(device.obj)
    payload = messages.DeviceMessage(
        signals={"left": {"value": [1, 2], "timestamp": 1}},
        metadata={"async_update": {"type": "add", "max_shape": [None]}},
    )
    with mock.patch.object(manager.connector, "xadd") as publish:
        device.obj.data.put(payload)
    if state in ("open", "paused"):
        publish.assert_called_once()
        assert payload.metadata["async_indices"] == {f"{device.obj.data.name}_left": 0}
    else:
        publish.assert_not_called()
        assert "async_indices" not in payload.metadata


def test_dynamic_message_indices_shapes_and_retention(publication_device):
    """Keep payload/index pairs, sparse channel counters, and scan-local index reset."""
    manager, device = publication_device
    manager._subscribe_to_bec_signals(device.obj)
    left = f"{device.obj.data.name}_left"
    right = f"{device.obj.data.name}_right"
    device.obj.data.signal_info.signal_metadata["expire"] = 123
    payloads = [
        messages.DeviceMessage(
            signals={
                "left": {"value": [1, 2], "timestamp": 1},
                "right": {"value": 3, "timestamp": 1},
            },
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
        messages.DeviceMessage(
            signals={"left": {"value": [4, 5, 6], "timestamp": 2}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
        messages.DeviceMessage(
            signals={"right": {"value": 7, "timestamp": 3}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
    ]
    with (
        mock.patch.object(manager.connector, "xadd", wraps=manager.connector.xadd) as publish,
        mock.patch.object(
            manager.connector, "rpush", wraps=manager.connector.rpush
        ) as publish_index,
    ):
        device.obj.data.put(payloads[0])
        device.obj.data.put(payloads[1])
        manager.scan_info.msg = messages.ScanStatusMessage(scan_id="scan-2", status="open", info={})
        device.obj.data.put(payloads[2])
    assert [payload.metadata["async_indices"] for payload in payloads] == [
        {left: 0, right: 0},
        {left: 1},
        {right: 0},
    ]
    assert publish.call_count == publish_index.call_count == 3
    for scan_id, data_call, index_call in zip(
        ("scan-1", "scan-1", "scan-2"), publish.call_args_list, publish_index.call_args_list
    ):
        assert data_call.args[0] == MessageEndpoints.device_async_signal(
            scan_id, device.name, device.obj.data.name
        )
        assert index_call.args[0] == MessageEndpoints.device_async_signal_index(
            scan_id, device.name, device.obj.data.name
        )
        assert data_call.kwargs["max_size"] == 7
        assert data_call.kwargs["expire"] == 123
        assert index_call.kwargs["expire"] == 3600
        assert data_call.kwargs["pipe"] is index_call.kwargs["pipe"]
    indices = manager.connector.lrange(
        MessageEndpoints.device_async_signal_index("scan-1", device.name, device.obj.data.name),
        0,
        -1,
    )
    assert [index.indices for index in indices] == [{left: 0, right: 0}, {left: 1}]
    assert [index.shapes for index in indices] == [{left: [2], right: []}, {left: [3]}]
