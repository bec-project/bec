"""The same device-server boundary supports a genuinely asynchronous test HAL."""

from __future__ import annotations

import asyncio
import importlib.util
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from async_backend_fixture import AsyncProvider, Interrupted

from bec_lib import messages
from bec_lib.device_backend import Completion, DeviceHandler, DeviceProvider
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.device_server import DeviceServer, RequestHandler
from bec_server.device_server.devices.devicemanager import DeviceManagerDS
from bec_server.device_server.rpc_handler import RPCHandler


@pytest.fixture
def backend():
    provider = AsyncProvider()
    raw, _ = provider.construct(provider.load_class("proof.AsyncDevice"), {"name": "async"})
    handler = provider.wrap(raw)
    handler.connect()
    try:
        yield provider, raw, handler
    finally:
        if not provider.closed:
            provider.run(
                lambda: (
                    handler.stop_ack.set(),
                    handler.admission.set(),
                    setattr(handler, "fail_stop", False),
                )
            )
            provider.close()


def test_test_backend_uses_the_public_contract_without_native_base(backend):
    provider, native, handler = backend
    assert isinstance(provider, DeviceProvider)
    assert isinstance(handler, DeviceHandler)
    assert not hasattr(native, "read")
    assert handler.constructor_thread == provider.thread.ident
    operation = handler.trigger()
    assert isinstance(operation, Completion)
    assert handler.started.wait(2)
    assert not operation.done
    provider.run(handler.transport.set)
    operation.result(2)
    assert handler.thread_ids == {provider.thread.ident}


def test_stop_and_push_updates_run_while_waiting_for_transport(backend):
    provider, _, handler = backend
    pushed = threading.Event()
    handler.subscribe(lambda event: pushed.set())
    operation = handler.trigger()
    assert handler.started.wait(2)
    handler.transport_update(3)
    assert pushed.wait(2)
    assert handler.read()["async"]["value"] == 3
    handler.stop().result(2)
    assert handler.stop_confirmed
    assert not handler.hardware_running
    with pytest.raises(Interrupted):
        operation.result(2)


def test_stop_acknowledgement_precedes_interrupted_result(backend):
    provider, _, handler = backend
    provider.run(handler.stop_ack.clear)
    operation = handler.trigger()
    assert handler.started.wait(2)
    stop = handler.stop()
    assert handler.stop_started.wait(2)
    assert not operation.done
    assert not stop.done
    provider.run(handler.stop_ack.set)
    stop.result(2)
    with pytest.raises(Interrupted):
        operation.result(2)
    assert handler.stop_confirmed


def test_observer_timeout_does_not_stop_acquisition(backend):
    provider, _, handler = backend
    operation = handler.trigger()
    assert handler.started.wait(2)
    with pytest.raises(TimeoutError):
        operation.result(timeout=0)
    assert not operation.done
    assert not handler.stop_started.is_set()
    provider.run(handler.transport.set)
    operation.result(2)


@pytest.mark.parametrize("failure", ["native", "timeout"])
def test_native_failure_and_timeout_are_terminal_and_cleaned(backend, failure):
    provider, _, handler = backend
    if failure == "native":
        provider.run(lambda: setattr(handler, "fail_acquisition", True))
        provider.run(handler.transport.set)
        exception = ValueError
    else:
        provider.run(lambda: setattr(handler, "trigger_timeout", 0.01))
        exception = TimeoutError
    with pytest.raises(exception):
        handler.trigger().result(2)
    assert not handler.hardware_running


def test_cancelled_before_admission_never_touches_hardware(backend):
    provider, _, handler = backend
    provider.run(handler.admission.clear)
    queued = handler.trigger()
    handler.stop().result(2)
    with pytest.raises(Interrupted, match="before admission"):
        queued.result(2)
    assert handler.native_calls == 0
    assert not handler.started.is_set()


def test_device_to_device_await_and_cross_owner_sync_wait_guard(backend):
    provider, _, handler = backend
    peer_provider = AsyncProvider()
    raw, _ = peer_provider.construct(
        peer_provider.load_class("proof.AsyncDevice"), {"name": "peer"}
    )
    peer = peer_provider.wrap(raw)
    peer.connect()
    try:
        moved = threading.Event()
        peer.subscribe(lambda event: moved.set())
        provider.run(lambda: setattr(handler, "peer", peer))
        operation = handler.trigger()
        assert handler.started.wait(2)
        # Joining the peer's callbacks is not necessary: this barrier executes on
        # the peer owner after its submitted move and proves peer-loop progress.
        assert moved.wait(2)
        assert peer.read()["peer"]["value"] == 7
        provider.run(handler.transport.set)
        operation.result(2)
        with pytest.raises(RuntimeError, match="synchronously"):
            provider.run(peer.read)
    finally:
        peer_provider.close()


def test_stop_does_not_cancel_borrowed_peer_operation(backend):
    provider, _, handler = backend
    peer_provider = AsyncProvider()
    raw, _ = peer_provider.construct(
        peer_provider.load_class("proof.AsyncDevice"), {"name": "peer"}
    )
    peer = peer_provider.wrap(raw)
    peer.connect()
    try:
        borrowed = peer.trigger()
        assert peer.started.wait(2)
        awaiting = threading.Event()

        async def depend_on_borrowed_work():
            awaiting.set()
            await handler._observe(borrowed)

        own = handler._submit(depend_on_borrowed_work)
        assert awaiting.wait(2)
        handler.stop().result(2)
        with pytest.raises(Interrupted):
            own.result(2)
        assert not borrowed.done
        peer_provider.run(peer.transport.set)
        borrowed.result(2)
    finally:
        peer_provider.close()


def test_slow_service_callback_does_not_stall_owner_or_stop(backend):
    provider, _, handler = backend
    entered, release = threading.Event(), threading.Event()
    handler.subscribe(lambda event: (entered.set(), release.wait(2)))
    operation = handler.trigger()
    assert handler.started.wait(2)
    handler.transport_update(1)
    assert entered.wait(2)
    try:
        heartbeat = threading.Event()
        provider.loop.call_soon_threadsafe(heartbeat.set)
        assert heartbeat.wait(2)
        handler.stop().result(2)
        assert handler.stop_confirmed
        with pytest.raises(Interrupted):
            operation.result(2)
    finally:
        release.set()


def test_failed_stop_is_observable_and_does_not_strand_operation(backend):
    provider, _, handler = backend
    provider.run(lambda: setattr(handler, "fail_stop", True))
    operation = handler.trigger()
    assert handler.started.wait(2)
    with pytest.raises(RuntimeError, match="stop failed"):
        handler.stop().result(2)
    assert not handler.stop_confirmed
    assert handler.hardware_running
    with pytest.raises(Interrupted):
        operation.result(2)
    provider.run(lambda: setattr(handler, "fail_stop", False))
    handler.stop().result(2)
    assert handler.stop_confirmed


def test_repeated_stop_close_release_all_owned_threads(backend):
    provider, _, handler = backend
    handler.stop().result(2)
    handler.stop().result(2)
    provider.close()
    provider.close()
    assert not provider.thread.is_alive()
    assert not any(thread.is_alive() for thread in provider.events._threads)
    assert not any(thread.is_alive() for thread in provider.callbacks._threads)


def make_server(provider, connector=None):
    """Use real manager/dispatch/request code with an injected neutral provider."""
    connector = connector or ConnectorMock("", store_data=False)
    parent = SimpleNamespace(
        connector=connector, device_provider=provider, _service_name="device_server"
    )
    manager = DeviceManagerDS(parent, device_provider=provider)
    manager.config_update_handler = mock.Mock()
    config = {
        "name": "proof",
        "deviceClass": "proof.AsyncDevice",
        "enabled": True,
        "readOnly": False,
        "readoutPriority": "monitored",
        "deviceConfig": {},
    }
    raw, remaining = manager.construct_device_obj(config, manager)
    manager.initialize_device(config, remaining, raw)
    manager.resolve_device_dependencies([config])
    manager.scan_info.msg = SimpleNamespace(scan_id="proof-scan", status="running")
    server = DeviceServer.__new__(DeviceServer)
    server.device_provider = provider
    server.connector = connector
    server.device_manager = manager
    server._send_service_status = mock.Mock()
    server.requests_handler = RequestHandler(server)
    server.rpc_handler = RPCHandler(server)
    return server, manager.devices.proof.handler


def test_real_server_dispatches_opaque_async_device_without_raw_access():
    provider = AsyncProvider()
    server, handler = make_server(provider)
    instruction = messages.DeviceInstructionMessage(
        device="proof",
        action="trigger",
        parameter={},
        metadata={"device_instr_id": "proof-trigger", "RID": "proof-rid"},
    )
    try:
        server.handle_device_instructions(instruction)
        context = server.requests_handler.get_request("proof-trigger")["status_objects"][0]
        assert handler.started.wait(2)
        assert not context.done
        provider.run(handler.transport.set)
        assert context.service_done.wait(2)
        responses = [
            entry["msg"]
            for entry in server.connector.message_sent
            if getattr(entry["msg"], "instruction_id", None) == "proof-trigger"
        ]
        assert responses[-1].status == "completed"
        assert not hasattr(server.device_manager.devices.proof.obj, "read")
    finally:
        server.device_manager.shutdown()


def test_importing_core_dispatch_does_not_import_a_hal():
    # Execute in a fresh interpreter; an already-imported HAL cannot mask a leak.
    script = """
import importlib.abc, sys
class RejectHAL(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in ('ophyd', 'ophyd_async', 'ophyd_devices', 'bluesky'):
            raise AssertionError('Core imported HAL: ' + fullname)
sys.meta_path.insert(0, RejectHAL())
from bec_lib.device_backend import DeviceHandler
from bec_server.device_server.device_server import DeviceServer
print('HAL-independent import passed')
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "HAL-independent import passed" in result.stdout


def test_async_terminal_completion_notifies_late_observer_after_close(backend):
    provider, _, handler = backend
    operation = handler.trigger()
    assert handler.started.wait(2)
    provider.close()
    observed = []
    operation.add_callback(lambda result: observed.append(result.exception()))
    assert len(observed) == 1
    assert isinstance(observed[0], Interrupted)


def test_async_close_is_bounded_when_completion_observer_is_blocked(backend, monkeypatch):
    provider, _, handler = backend
    entered, release = threading.Event(), threading.Event()
    operation = handler.trigger()
    assert handler.started.wait(2)
    operation.add_callback(lambda completed: (entered.set(), release.wait(2)))
    provider.run(handler.transport.set)
    assert entered.wait(2)
    try:
        module = sys.modules[AsyncProvider.__module__]
        original = module.wait
        with monkeypatch.context() as patch:
            patch.setattr(module, "wait", lambda pending, timeout: original(pending, timeout=0))
            with pytest.raises(TimeoutError, match="completion callbacks"):
                provider.close()
        assert not provider.closed
        assert provider.thread.is_alive()
    finally:
        release.set()
    provider.close()
    assert provider.closed
