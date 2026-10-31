"""Accepted file/data events retain scan scope and precede terminal acknowledgement."""

import threading
from types import MethodType, SimpleNamespace
from unittest import mock

import pytest
from test_async_backend_contract import make_server
from test_backend_redis_workflow import ExistingDeviceProvider

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.devicemanager import DeviceManagerDS


def emit(raw):
    raw.file.put(file_path="old-scan.h5", done=True, successful=True)
    raw.progress.put(value=0.5, max_value=1, done=False)
    raw.dynamic.put(
        {"proof_dynamic": {"value": 3, "timestamp": 0}},
        async_update={"type": "add", "max_shape": [None]},
    )


@pytest.mark.parametrize(
    "next_context",
    [
        None,
        SimpleNamespace(scan_id="old-scan", status="closed"),
        SimpleNamespace(scan_id="next-scan", status="running"),
    ],
)
def test_queued_data_preserves_receipt_scan_context(next_context, connected_connector):
    provider = ExistingDeviceProvider()
    connector = connected_connector
    server, handler = make_server(provider, connector, specification="proof.MessageTree")
    manager = server.device_manager
    manager.scan_info.msg = SimpleNamespace(scan_id="old-scan", status="running")
    entered, release = threading.Event(), threading.Event()
    provider.events.submit(lambda: (entered.set(), release.wait(3)))
    assert entered.wait(2)
    try:
        emit(manager.devices.proof.obj)
        manager.scan_info.msg = next_context
        release.set()
        handler.flush_events(2)
        assert connector.get(MessageEndpoints.public_file("old-scan", "proof")) is not None
        assert connector.xrange(
            MessageEndpoints.device_async_signal("old-scan", "proof", "proof_dynamic"), "-", "+"
        )
        progress = connector.get(MessageEndpoints.device_progress("proof"))
        assert progress.metadata["scan_id"] == "old-scan"
        assert progress.metadata["status"] == "running"
    finally:
        release.set()
        manager.shutdown()


def test_completion_waits_for_already_accepted_publication(connected_connector):
    provider = ExistingDeviceProvider()
    server, handler = make_server(provider, connected_connector, specification="proof.MessageTree")
    instruction = messages.DeviceInstructionMessage(
        device="proof",
        action="trigger",
        parameter={},
        metadata={"device_instr_id": "trigger", "RID": "request", "scan_id": "proof-scan"},
    )
    entered, release = threading.Event(), threading.Event()
    try:
        server.handle_device_instructions(instruction)
        context = server.requests_handler.get_request("trigger")["status_objects"][0]
        provider.events.submit(lambda: (entered.set(), release.wait(3)))
        assert entered.wait(2)
        raw = server.device_manager.devices.proof.obj
        raw.file.put(file_path="file.h5", done=True, successful=True)
        raw.pending.set_finished()
        assert not context.service_done.wait(0.05)
        release.set()
        assert context.service_done.wait(2)
        assert server.requests_handler.get_request("trigger") is None
        assert (
            connected_connector.get(MessageEndpoints.public_file("proof-scan", "proof")) is not None
        )
    finally:
        release.set()
        server.device_manager.shutdown()


def test_unregistered_initialization_failure_releases_owned_native_object():
    provider = ExistingDeviceProvider()
    parent = SimpleNamespace(
        connector=ConnectorMock("", store_data=False),
        device_provider=provider,
        _service_name="device_server",
    )
    manager = DeviceManagerDS(parent, device_provider=provider)
    progress = mock.Mock()
    config = {
        "name": "root",
        "deviceClass": "proof.MessageTree",
        "enabled": True,
        "readOnly": False,
        "readoutPriority": "baseline",
        "deviceConfig": {},
    }
    try:
        with pytest.raises(Exception, match="protected"):
            manager._init_device(config, False, progress)
        assert len(manager.devices) == 0
        assert provider._handlers == {}
    finally:
        manager.shutdown()


def test_failed_initialization_cleanup_does_not_prevent_next_config_flush():
    provider = ExistingDeviceProvider()
    parent = SimpleNamespace(
        connector=ConnectorMock("", store_data=False),
        device_provider=provider,
        _service_name="device_server",
    )
    manager = DeviceManagerDS(parent, device_provider=provider)
    config_handler = ConfigUpdateHandler(manager)
    manager.config_update_handler = config_handler
    config = {
        "name": "proof",
        "deviceClass": "proof.MessageTree",
        "enabled": True,
        "readOnly": False,
        "readoutPriority": "monitored",
        "deviceConfig": {},
    }
    try:
        raw, remaining = manager.construct_device_obj(config, manager)
        with mock.patch.object(
            manager, "connect_device", return_value=RuntimeError("connection failed")
        ):
            with pytest.raises(RuntimeError, match="connection failed"):
                manager.initialize_device(config, remaining, raw)
        device = manager.devices.proof
        config_handler._cleanup_failed_device_init(raw, device)
        config_handler._flush_config()
        assert len(manager.devices) == 0
        assert provider._handlers == {}
    finally:
        manager.shutdown()


def test_absent_scan_at_receipt_does_not_publish_after_next_scan_starts(connected_connector):
    provider = ExistingDeviceProvider()
    server, handler = make_server(provider, connected_connector, specification="proof.MessageTree")
    manager = server.device_manager
    entered, release = threading.Event(), threading.Event()
    manager.scan_info.msg = None
    provider.events.submit(lambda: (entered.set(), release.wait(3)))
    assert entered.wait(2)
    try:
        emit(manager.devices.proof.obj)
        manager.scan_info.msg = SimpleNamespace(scan_id="later", status="running")
        release.set()
        handler.flush_events(2)
        assert connected_connector.get(MessageEndpoints.device_progress("proof")) is None
        assert connected_connector.get(MessageEndpoints.public_file("later", "proof")) is None
        assert not connected_connector.xrange(
            MessageEndpoints.device_async_signal("later", "proof", "proof_dynamic"), "-", "+"
        )
    finally:
        release.set()
        manager.shutdown()


def test_failed_publication_disposes_temporary_requested_target_subscription():
    from ophyd.status import StatusBase

    provider = ExistingDeviceProvider()
    server, handler = make_server(provider, specification="proof.MessageTree")
    child = handler.resolve("value")
    raw = server.device_manager.devices.proof.obj
    pending = StatusBase()
    instruction = messages.DeviceInstructionMessage(
        device="proof.value",
        action="set",
        parameter={"value": 2},
        metadata={"device_instr_id": "move", "RID": "move-rid"},
    )
    try:
        with (
            mock.patch.object(raw.value, "set", return_value=pending),
            mock.patch.object(
                child, "flush_events", side_effect=ConnectionError("Redis publication failed")
            ),
        ):
            server.handle_device_instructions(instruction)
            context = server.requests_handler.get_request("move")["status_objects"][0]
            provider.events.submit(lambda: None).result(2)
            pending.set_finished()
            assert context.service_done.wait(2)
        assert server.requests_handler.get_request("move") is None
        assert context.service_error is not None
        assert child._subscriptions == []
        assert context.subscription is None
    finally:
        server.device_manager.shutdown()


@pytest.mark.parametrize("action", ["stage", "unstage", "kickoff", "pre_scan", "rpc", "read"])
def test_immediate_results_wait_for_accepted_file_publication(action, connected_connector):
    from ophyd.device import Staged

    provider = ExistingDeviceProvider()
    server, handler = make_server(provider, connected_connector, specification="proof.MessageTree")
    raw = server.device_manager.devices.proof.obj
    entered, release, emitted, finished = [threading.Event() for _ in range(4)]
    provider.events.submit(lambda: (entered.set(), release.wait(3)))
    assert entered.wait(2)
    original_read = raw.read
    method = "custom" if action == "rpc" else action
    if action == "unstage":
        raw._staged = Staged.yes

    def immediate(self, metadata=None, **kwargs):
        self.file.put(file_path="final.h5", done=True, successful=True)
        emitted.set()
        if action == "read":
            return original_read()
        return [self] if action in ("stage", "unstage") else None

    instruction = messages.DeviceInstructionMessage(
        device="proof",
        action=action,
        parameter={"func": "custom", "rpc_id": "rpc"} if action == "rpc" else {},
        metadata={"device_instr_id": "immediate", "RID": "immediate"},
    )
    worker = threading.Thread(
        target=lambda: (server.handle_device_instructions(instruction), finished.set())
    )
    try:
        with mock.patch.object(raw, method, MethodType(immediate, raw), create=True):
            worker.start()
            assert emitted.wait(2)
            assert not finished.wait(0.03)
            assert (
                connected_connector.get(MessageEndpoints.public_file("proof-scan", "proof")) is None
            )
            release.set()
            assert finished.wait(2)
            request = server.requests_handler.get_request("immediate")
            assert request is None or request["done"]
            assert (
                connected_connector.get(MessageEndpoints.public_file("proof-scan", "proof"))
                is not None
            )
    finally:
        release.set()
        if worker.ident is not None:
            worker.join(2)
        server.device_manager.shutdown()


@pytest.mark.parametrize("action", ["disable", "remove"])
@pytest.mark.parametrize("acknowledged", [False, True])
def test_historical_publication_failure_does_not_leave_destroyed_device_enabled(
    action, acknowledged
):
    provider = ExistingDeviceProvider()
    server, handler = make_server(provider, specification="proof.MessageTree")
    manager = server.device_manager
    config_handler = ConfigUpdateHandler(manager)
    manager.config_update_handler = config_handler
    device = manager.devices.proof
    raw = device.obj
    try:
        with mock.patch.object(
            manager._bec_message_handler,
            "emit_event",
            side_effect=ConnectionError("historic publication failure"),
        ):
            raw.file.put(file_path="failed.h5", done=True, successful=True)
            provider.events.submit(lambda: None).result(2)
            if acknowledged:
                with pytest.raises(RuntimeError, match="historic publication failure"):
                    handler.flush_events(2)
        message = messages.DeviceConfigMessage(
            action="update" if action == "disable" else "remove",
            config={"proof": {"enabled": False}} if action == "disable" else {"proof": {}},
        )
        if action == "disable":
            config_handler._update_config(message, threading.Event())
            assert not manager.devices.proof.enabled
            assert not manager.devices.proof.initialized
        else:
            config_handler._remove_config(message, threading.Event())
            assert "proof" not in manager.devices
        assert handler._closed
        assert raw._destroyed
        assert not provider._handlers
    finally:
        manager.shutdown()
