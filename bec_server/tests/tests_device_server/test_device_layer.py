"""Contract tests for the hardware-independent service boundary."""

import ast
import subprocess
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server import device_server
from bec_server.device_server.device_server import DeviceServer


class FakeStatus:
    """An immediately complete status with no hardware-library dependency."""

    done = True
    success = True
    device_name = "motor"
    status_type = "FakeStatus"

    def exception(self):
        return None

    def add_callback(self, callback):
        callback(self)


@pytest.fixture
def neutral_server():
    layer = mock.Mock()
    layer.device_manager = SimpleNamespace(
        devices={"motor": SimpleNamespace(enabled=True, read_only=False, metadata={})},
        scan_info=None,
    )
    layer.get_device_from_exception.return_value = None

    def factory(server):
        assert server.requests_handler is not None

        def dispatch(instruction):
            server.requests_handler.add_request(instruction, 1)
            server.requests_handler.add_status_object(instruction, FakeStatus())

        layer.instructions.dispatch.side_effect = dispatch
        return layer

    with (
        mock.patch.object(DeviceServer, "_start_metrics_emitter"),
        mock.patch.object(DeviceServer, "_start_update_service_info"),
    ):
        server = DeviceServer(ServiceConfig(), ConnectorMock, layer_factory=factory)
    yield server, layer
    server.shutdown()


def test_fake_layer_completes_real_request(neutral_server):
    server, layer = neutral_server
    instruction = messages.DeviceInstructionMessage(
        device="motor", action="trigger", parameter={}, metadata={"device_instr_id": "fake"}
    )
    with mock.patch.object(server.connector, "send") as send:
        server.handle_device_instructions(instruction)
    layer.instructions.dispatch.assert_called_once_with(instruction)
    responses = [call.args[1] for call in send.call_args_list]
    terminal = [response for response in responses if response.status != "running"]
    assert len(terminal) == 1
    assert terminal[0].status == "completed"
    assert terminal[0].result_is_status is True
    assert server.requests_handler.get_request("fake") is None


@pytest.mark.parametrize("already_done", [False, True])
def test_running_publication_failure_does_not_duplicate_completion(neutral_server, already_done):
    server, layer = neutral_server
    instruction = messages.DeviceInstructionMessage(
        device="motor",
        action="trigger",
        parameter={},
        metadata={"device_instr_id": "fake", "RID": "rid", "response": True},
    )
    status = FakeStatus()
    status.done = already_done
    callbacks = []

    def dispatch(instruction):
        server.requests_handler.add_request(instruction, 1)
        server.requests_handler.add_status_object(instruction, status)

    def observe(callback):
        if status.done:
            callback(status)
        else:
            callbacks.append(callback)

    layer.instructions.dispatch.side_effect = dispatch
    with (
        mock.patch.object(status, "add_callback", side_effect=observe),
        mock.patch.object(
            server.connector,
            "send",
            side_effect=[None, ConnectionError("running reply failed"), None, None, None],
        ) as send,
        mock.patch.object(server.connector, "xadd") as publish_device,
    ):
        server.handle_device_instructions(instruction)
        if not already_done:
            status.done = True
            for callback in callbacks:
                callback(status)

    terminal = [call.args[1] for call in send.call_args_list if call.args[1].status != "running"]
    assert [response.status for response in terminal] == ["completed"]
    device_replies = [
        call
        for call in publish_device.call_args_list
        if (call.args[0] if call.args else call.kwargs.get("topic"))
        == MessageEndpoints.device_req_status("rid")
    ]
    assert len(device_replies) == 1
    assert device_replies[0].args[1]["data"].success is True
    assert not server.requests_handler.has_request("fake")


def test_fake_rpc_and_stop_routes(neutral_server):
    server, layer = neutral_server
    instruction = messages.DeviceInstructionMessage(
        device="motor", action="rpc", parameter={}, metadata={"device_instr_id": "fake"}
    )
    server.handle_device_instructions(instruction)
    layer.rpc.run_rpc.assert_called_once_with(instruction)
    server.stop_devices(["motor"])
    layer.instructions.stop_devices.assert_called_once_with(["motor"])


def test_shutdown_closes_layer_once_before_transport_and_rejects_late_messages(neutral_server):
    server, layer = neutral_server
    order = []
    layer.shutdown.side_effect = lambda: order.append("layer")
    with mock.patch.object(
        server.connector, "shutdown", side_effect=lambda **_: order.append("transport")
    ):
        server.shutdown()
        server.shutdown()
    assert order == ["layer", "transport"]
    with mock.patch.object(server.executor, "submit") as submit:
        server.instructions_callback(SimpleNamespace(value=None))
        submit.assert_not_called()
    server.on_stop_devices(
        MessageObject(topic="unused", value=messages.VariableMessage(value=None))
    )
    layer.instructions.stop_devices.assert_not_called()


def test_service_has_no_native_imports_or_device_object_access():
    source = Path(device_server.__file__).read_text()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(
                alias.name.split(".")[0] not in {"ophyd", "ophyd_devices"} for alias in node.names
            )
        if isinstance(node, ast.ImportFrom):
            assert (node.module or "").split(".")[0] not in {"ophyd", "ophyd_devices"}
        if isinstance(node, ast.Attribute):
            assert node.attr != "obj"


def test_neutral_import_and_fake_layer_construction_without_ophyd():
    script = textwrap.dedent("""
        import importlib.abc
        import sys
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        class BlockNative(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split('.')[0] in {'ophyd', 'ophyd_devices'}:
                    raise AssertionError(f'Unexpected native import: {fullname}')

        sys.meta_path.insert(0, BlockNative())
        from bec_lib.service_config import ServiceConfig
        from bec_lib.tests.utils import ConnectorMock
        from bec_server.device_server.device_server import DeviceServer
        from bec_server.device_server.request_handler import RequestHandler
        from bec_server.device_server.device_status import DeviceStatus

        layer = Mock()
        layer.device_manager = SimpleNamespace(devices={}, scan_info=None)
        with patch.object(DeviceServer, '_start_metrics_emitter'), \\
             patch.object(DeviceServer, '_start_update_service_info'):
            server = DeviceServer(ServiceConfig(), ConnectorMock, layer_factory=lambda _: layer)
        server.shutdown()
        assert not any(name.split('.')[0] in {'ophyd', 'ophyd_devices'} for name in sys.modules)
        """)
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_shutdown_survives_transport_publication_failure(neutral_server):
    server, layer = neutral_server
    with (
        mock.patch.object(
            server.connector, "set_and_publish", side_effect=ConnectionError("offline")
        ),
        mock.patch.object(server.connector, "shutdown") as transport,
    ):
        server.shutdown()
        server.shutdown()
    layer.shutdown.assert_called_once()
    transport.assert_called_once()
    assert server.executor._shutdown


def test_backend_shutdown_error_still_closes_transport_and_allows_retry(neutral_server):
    server, layer = neutral_server
    layer.shutdown.side_effect = [RuntimeError("backend cleanup failed"), None]
    with mock.patch.object(server.connector, "shutdown") as transport:
        with pytest.raises(RuntimeError, match="backend cleanup failed"):
            server.shutdown()
        server.shutdown()
    assert layer.shutdown.call_count == 2
    assert transport.call_count == 2
    assert server._shutdown_complete
