import asyncio
import threading
from concurrent.futures import Future
from contextlib import contextmanager, nullcontext
from types import MethodType, SimpleNamespace
from unittest import mock
from unittest.mock import ANY, patch

import numpy as np
import pytest
from ophyd import Component as Cpt
from ophyd import Device, DeviceStatus, Kind, Signal, Staged
from ophyd.utils import errors as ophyd_errors
from ophyd_devices import PreviewSignal, StatusBase

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.device import OnFailure, RPCError
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import BECStatus
from bec_lib.redis_connector import MessageObject
from bec_lib.serialization import MsgpackSerialization
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock, DMClientMock
from bec_server.device_server.device_providers.device_interfaces import DeviceKind, DeviceTarget
from bec_server.device_server.device_providers.device_provider_manager import DeviceProviderManager
from bec_server.device_server.device_providers.ophyd.callback_monitor import OphydCallbackMonitor
from bec_server.device_server.device_providers.ophyd.ophyd_completion import (
    to_instruction_execution,
)
from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
    OphydDeviceProvider,
)
from bec_server.device_server.device_providers.ophyd.ophyd_rpc import OphydRPC
from bec_server.device_server.device_server import DeviceServer, InvalidDeviceError
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.devicemanager import DeviceManagerDS
from bec_server.device_server.devices.ds_device import DSDevice
from bec_server.device_server.instruction_execution import InstructionExecution

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


class DeviceManagerProxyMock(DMClientMock):
    is_shutting_down = False

    def begin_shutdown(self) -> None:
        self.is_shutting_down = True


@pytest.fixture
def device_manager_class():
    return DeviceManagerProxyMock


class DeviceServerMock(DeviceServer):
    def __init__(self, device_manager, connector_cls) -> None:
        config = ServiceConfig(redis={"host": "dummy", "port": 6379})
        self.__test_device_manager = device_manager
        with patch("bec_server.device_server.device_server.DeviceManagerDS"):
            super().__init__(config, connector_cls=ConnectorMock)

    def _start_device_manager(self):
        self.device_manager = self.__test_device_manager
        if not hasattr(self.device_manager, "device_provider_manager"):
            self.device_manager.device_provider_manager = DeviceProviderManager(self.device_manager)

    def _start_metrics_emitter(self):
        pass

    def _start_update_service_info(self):
        pass


@pytest.fixture
def device_server_mock(dm_with_devices):
    device_manager = dm_with_devices
    device_server = DeviceServerMock(device_manager, device_manager.connector)
    yield device_server
    device_server.shutdown()


@pytest.fixture
def ophyd_device_mock():
    dev = Device(name="dev", kind=Kind.normal)
    yield dev


@pytest.fixture
def device_instruction_message_mock(ophyd_device_mock):
    instr = messages.DeviceInstructionMessage(
        device=ophyd_device_mock.name,
        action="set",
        parameter={},
        metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
    )
    yield instr


@pytest.mark.parametrize("value, expected", [(0.0, 0), (1.0, 1), (np.float64(2.0), 2)])
def test_convert_value_if_needed_converts_integral_float_for_enum(value, expected):
    obj = SimpleNamespace(name="enum_signal", enum_strs=("zero", "one", "two"))

    converted = OphydDeviceProvider.convert_value_if_needed(obj, value)

    assert converted == expected
    assert isinstance(converted, int)


def test_convert_value_if_needed_rejects_non_integral_float_for_enum():
    obj = SimpleNamespace(name="enum_signal", enum_strs=("zero", "one", "two"))

    with pytest.raises(ValueError, match="Cannot convert float 1.5 to enum index"):
        OphydDeviceProvider.convert_value_if_needed(obj, 1.5)


@pytest.mark.parametrize("value", [1, 1.0, np.float64(1.0)])
def test_convert_value_if_needed_leaves_non_enum_values_unchanged(value):
    obj = SimpleNamespace(name="plain_signal")

    converted = OphydDeviceProvider.convert_value_if_needed(obj, value)

    assert converted is value


def test_convert_value_if_needed_leaves_enum_int_unchanged():
    obj = SimpleNamespace(name="enum_signal", enum_strs=("zero", "one"))

    converted = OphydDeviceProvider.convert_value_if_needed(obj, 1)

    assert converted == 1
    assert isinstance(converted, int)


def test_start(device_server_mock):
    device_server = device_server_mock
    startup = mock.Mock()
    with (
        patch.object(device_server.connector, "register") as register,
        patch.object(device_server.connector, "send") as send,
        patch.object(device_server, "_send_service_status") as status,
    ):
        startup.attach_mock(register, "register")
        startup.attach_mock(send, "send")
        startup.attach_mock(status, "status")
        device_server.start()

    assert device_server.status == BECStatus.RUNNING
    assert startup.mock_calls == [
        mock.call.register(
            MessageEndpoints.device_instructions(),
            event=ANY,
            cb=device_server.instructions_callback,
        ),
        mock.call.send(
            MessageEndpoints.device_config_update(),
            messages.DeviceConfigMessage(action="reload", config={}),
        ),
        mock.call.status(),
    ]
    assert device_server.provider_manager.providers["ophyd"].callback_monitor._thread.is_alive()
    assert (
        device_server.provider_manager.providers["ophyd"].callback_monitor.queue_threshold == 1000
    )


@pytest.mark.parametrize("remove_device", [False, True])
def test_start_refreshes_existing_device_manager(
    device_server_mock, connected_connector, remove_device
):
    device = device_server_mock.device_manager.devices.samx
    config = {**device._config, "name": "samx", "enabled": True}
    connected_connector.set(
        MessageEndpoints.device_config(), messages.AvailableResourceMessage(resource=[config])
    )
    connected_connector.set_and_publish(
        MessageEndpoints.device_info("samx"),
        messages.DeviceInfoMessage(device="samx", info=device._info),
    )
    service = mock.Mock(connector=connected_connector)
    manager = DeviceManagerBase(service)
    reloaded = threading.Event()
    service.callbacks.run.side_effect = lambda *_: reloaded.set()
    try:
        manager.initialize("localhost:1")
        assert manager.devices.samx.enabled

        # A restart can change the stored configuration without a config request.
        updated_config = [] if remove_device else [{**config, "enabled": False}]
        connected_connector.set(
            MessageEndpoints.device_config(),
            messages.AvailableResourceMessage(resource=updated_config),
        )
        with patch.object(device_server_mock, "connector", connected_connector):
            device_server_mock.start()

        assert reloaded.wait(timeout=2), "Device-server startup did not refresh the consumer"
        if remove_device:
            assert not manager.devices
        else:
            assert not manager.devices.samx.enabled
    finally:
        manager.shutdown()


def test_start_does_not_duplicate_ophyd_callback_monitor(device_server_mock):
    device_server_mock.start()
    thread = device_server_mock.provider_manager.providers["ophyd"].callback_monitor._thread

    device_server_mock.start()

    assert device_server_mock.provider_manager.providers["ophyd"].callback_monitor._thread is thread
    assert thread.is_alive()


@pytest.mark.parametrize("shutdown_fails", [False, True])
def test_shutdown_joins_ophyd_callback_monitor_first(device_server_mock, shutdown_fails):
    device_server_mock.start()
    monitor = device_server_mock.provider_manager.providers["ophyd"].callback_monitor
    thread = monitor._thread

    def shutdown_service():
        assert monitor._stop_event.is_set()
        assert not thread.is_alive()
        if shutdown_fails:
            raise RuntimeError("Service shutdown failed")

    with mock.patch(
        "bec_server.device_server.device_server.BECService.shutdown", side_effect=shutdown_service
    ):
        if shutdown_fails:
            with pytest.raises(RuntimeError, match="Service shutdown failed"):
                device_server_mock.shutdown()
        else:
            device_server_mock.shutdown()

    assert monitor._thread is None


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_shutdown_keeps_provider_runtime_alive_for_device_cleanup(
    device_server_mock, cleanup_fails
):
    server = device_server_mock
    calls = mock.Mock()

    @contextmanager
    def stop_runtime():
        calls.prepare_runtime_shutdown()
        try:
            yield
        finally:
            calls.finalize_runtime()

    if cleanup_fails:
        calls.shutdown_device_manager.side_effect = RuntimeError("Device cleanup failed")

    with (
        mock.patch.object(server.provider_manager, "stop_runtime", stop_runtime),
        mock.patch.object(server, "stop", calls.stop_accepting_instructions),
        mock.patch.object(server.device_manager, "shutdown", calls.shutdown_device_manager),
        mock.patch(
            "bec_server.device_server.device_server.BECService.shutdown", calls.shutdown_service
        ),
    ):
        if cleanup_fails:
            with pytest.raises(RuntimeError, match="Device cleanup failed"):
                server.shutdown()
        else:
            server.shutdown()

    assert calls.mock_calls == [
        mock.call.prepare_runtime_shutdown(),
        mock.call.stop_accepting_instructions(),
        mock.call.shutdown_device_manager(),
        mock.call.shutdown_service(),
        mock.call.finalize_runtime(),
    ]


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_shutdown_ignores_new_and_queued_instructions(device_server_mock):
    server = device_server_mock
    handler = ConfigUpdateHandler(server.device_manager)
    instruction = mock.MagicMock()
    message = SimpleNamespace(value=instruction)
    config_message = SimpleNamespace(value=messages.DeviceConfigMessage(action="reload", config={}))

    with (
        mock.patch.object(server.executor, "submit") as submit,
        mock.patch.object(handler.executor, "submit") as submit_config,
    ):
        server.instructions_callback(message)
        submit.assert_called_once_with(server.handle_device_instructions, instruction)
        run, queued_instruction = submit.call_args.args
        submit.reset_mock()
        original_stop = server.stop

        def stop():
            assert server.is_shutting_down
            assert server.device_manager.is_shutting_down
            server.instructions_callback(message)
            run(queued_instruction)
            handler._device_config_callback(config_message)
            original_stop()

        with mock.patch.object(server, "stop", side_effect=stop):
            server.shutdown()

    submit.assert_not_called()
    submit_config.assert_not_called()
    instruction.content.__getitem__.assert_not_called()


def test_shutdown_without_starting_ophyd_callback_monitor(device_server_mock):
    device_server_mock.shutdown()
    device_server_mock.shutdown()

    assert device_server_mock.provider_manager.providers[
        "ophyd"
    ].callback_monitor._stop_event.is_set()
    assert device_server_mock.provider_manager.providers["ophyd"].callback_monitor._thread is None


def test_shutdown_closes_connector_before_joining_blocked_warning(device_server_mock):
    device_server_mock.provider_manager.providers["ophyd"].callback_monitor = OphydCallbackMonitor(
        sample_interval=0.01
    )
    callback_monitor = device_server_mock.provider_manager.providers["ophyd"].callback_monitor
    warning_started = threading.Event()
    warning_released = threading.Event()
    warning_wait_results = []
    monitor = SimpleNamespace(
        queue=mock.Mock(qsize=mock.Mock(return_value=1001)),
        ident=None,
        current_callback="slow_callback",
    )

    def blocked_warning(_message):
        warning_started.set()
        warning_wait_results.append(warning_released.wait(5))

    def close_connector(**_kwargs):
        assert callback_monitor._stop_event.is_set()
        warning_released.set()

    with (
        mock.patch(
            "bec_server.device_server.device_providers.ophyd.callback_monitor.ophyd.get_cl"
        ) as get_cl,
        mock.patch(
            "bec_server.device_server.device_providers.ophyd.callback_monitor.logger.warning",
            side_effect=blocked_warning,
        ),
        mock.patch.object(
            device_server_mock.connector, "shutdown", side_effect=close_connector
        ) as close,
    ):
        get_cl.return_value.get_dispatcher.return_value = SimpleNamespace(
            threads={"monitor": monitor}
        )
        device_server_mock.start()
        thread = callback_monitor._thread
        try:
            assert warning_started.wait(2)

            device_server_mock.shutdown()

            close.assert_called_once()
            assert warning_wait_results == [True]
            assert not thread.is_alive()
            assert callback_monitor._thread is None
        finally:
            warning_released.set()
            callback_monitor.request_stop()
            thread.join(timeout=3)


@pytest.mark.parametrize("status", [BECStatus.ERROR, BECStatus.RUNNING, BECStatus.IDLE])
def test_update_status(device_server_mock, status):
    device_server = device_server_mock
    assert device_server.status == BECStatus.BUSY

    device_server.update_status(status)

    assert device_server.status == status


def test_stop(device_server_mock):
    device_server = device_server_mock
    device_server.stop()
    assert device_server.status == BECStatus.IDLE


@pytest.mark.parametrize("has_native_owner", [False, True])
def test_device_server_status_callback(
    device_server_mock, ophyd_device_mock, device_instruction_message_mock, has_native_owner
):
    """Refresh the bound target even when the native status has no owner."""
    device = ophyd_device_mock
    device._kind = Kind.normal
    instr = device_instruction_message_mock
    native = StatusBase(obj=device if has_native_owner else None)
    execution = OphydDeviceProvider.adapt_status(
        native, device, DeviceTarget(SimpleNamespace(name=device.name))
    ).bind(instr)
    with mock.patch.object(device_server_mock, "_read_device") as read:
        device_server_mock.status_callback(execution)
    read.assert_called_once_with(instr, new_status=False)
    assert not {"instruction", "obj_ref", "sub_id"}.intersection(native.__dict__)


@pytest.mark.parametrize("nested_target", [False, True])
@pytest.mark.parametrize("success", [False, True])
@pytest.mark.parametrize("action", ["set", "rpc"])
def test_status_callback_uses_requested_target_for_delegated_status(
    device_server_mock, nested_target, success, action
):
    """A child-owned status must complete and refresh the requested device."""

    class DelegatingDevice(Device):
        readback = Cpt(Signal, kind=Kind.hinted)
        confirm_energy = Cpt(Signal, kind=Kind.config)

    device = DelegatingDevice(name="cff")
    device._kind = Kind.normal
    target = device.readback if nested_target else device
    target_name = "cff.readback" if nested_target else "cff"
    instr = messages.DeviceInstructionMessage(
        device=target_name,
        action=action,
        parameter={"value": 2.25} if action == "set" else {"func": "set"},
        metadata={"device_instr_id": "diid", "RID": "request-id", "response": True},
    )
    status = DeviceStatus(device.confirm_energy)
    device_server = device_server_mock
    subscription = mock.Mock()
    subscription.close.return_value = Future()
    subscription.close.return_value.set_result(None)
    execution = OphydDeviceProvider.adapt_status(
        status, target, DeviceTarget(SimpleNamespace(name=device.name), target.dotted_name)
    ).bind(instr)
    if nested_target:
        execution.subscription = subscription
    if success:
        status.set_finished()
    else:
        status.set_exception(RuntimeError("Energy confirmation failed"))

    with (
        mock.patch.object(device_server, "_read_device") as read_device,
        mock.patch.object(device_server.connector, "xadd") as xadd,
        mock.patch.object(target, "unsubscribe") as unsubscribe_target,
        mock.patch.object(device.confirm_energy, "unsubscribe") as unsubscribe_confirmation,
    ):
        device_server.status_callback(execution)

    read_device.assert_called_once_with(instr, new_status=False)
    if nested_target:
        subscription.close.assert_called_once_with()
    else:
        subscription.close.assert_not_called()
    unsubscribe_target.assert_not_called()
    unsubscribe_confirmation.assert_not_called()
    xadd.assert_called_once()
    response = xadd.call_args.args[1]["data"]
    assert response.device == target_name
    assert response.success is success
    if success:
        assert response.metadata["error_info"] is None
    else:
        assert response.metadata["error_info"].exception_type == "RuntimeError"
        assert "Energy confirmation failed" in response.metadata["error_info"].error_message
    assert status.device is device.confirm_energy


@pytest.mark.parametrize("status_success", [True, False])
def test_device_server_status_callback_response_includes_error_info(
    device_server_mock, ophyd_device_mock, status_success
):
    device_server = device_server_mock
    dev = ophyd_device_mock
    dev._kind = Kind.normal
    instr = messages.DeviceInstructionMessage(
        device=dev.name,
        action="set",
        parameter={},
        metadata={
            "stream": "primary",
            "device_instr_id": "diid",
            "RID": "request-id",
            "response": True,
        },
    )

    status = DeviceStatus(dev)
    execution = OphydDeviceProvider.adapt_status(
        status, dev, DeviceTarget(SimpleNamespace(name=dev.name))
    ).bind(instr)
    if status_success:
        status.set_finished()
    else:
        status.set_exception(RuntimeError("motor failed"))

    with mock.patch.object(device_server, "_read_device") as mock_read_device:
        with mock.patch.object(device_server.connector, "xadd") as xadd_mock:
            device_server.status_callback(execution)

    mock_read_device.assert_called_once_with(instr, new_status=False)
    xadd_mock.assert_called_once()
    dev_msg = xadd_mock.call_args.args[1]["data"]
    assert isinstance(dev_msg, messages.DeviceReqStatusMessage)
    assert dev_msg.success is status_success
    if status_success:
        assert dev_msg.metadata["error_info"] is None
    else:
        assert dev_msg.metadata["error_info"] is not None
        assert dev_msg.metadata["error_info"].exception_type == "RuntimeError"
        assert "motor failed" in dev_msg.metadata["error_info"].error_message


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("kind", [Kind.normal, Kind.hinted])
@pytest.mark.parametrize("success", [True, False])
@pytest.mark.parametrize(
    "action, parameter",
    [("set", {"value": 5}), ("rpc", {"func": "set"}), ("rpc", {"func": "readback.set"})],
)
def test_motion_readback_preserves_instruction_result(
    device_server_mock, kind, success, action, parameter
):
    device_server = device_server_mock
    device = device_server.device_manager.devices.samx.obj
    device._kind = kind
    instr = messages.DeviceInstructionMessage(
        device=device.name,
        action=action,
        parameter=parameter,
        metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
    )
    status = DeviceStatus(device)
    execution = OphydDeviceProvider.adapt_status(
        status, device, DeviceTarget(device_server.device_manager.devices[device.name])
    ).bind(instr)
    if success:
        status.set_finished()
    else:
        status.set_exception(RuntimeError("motor failed"))

    handler = device_server.requests_handler
    handler.add_request(instr, num_status_objects=1)
    signals = {device.name: {"value": 5, "timestamp": 1234}}
    with mock.patch.object(device, "read", return_value=signals):
        handler.add_status_object("diid", execution)

    responses = [
        sent["msg"]
        for sent in device_server.connector.message_sent
        if sent["queue"] == MessageEndpoints.device_instructions_response()
    ]
    final_status = "completed" if success else "error"
    assert responses[-1].status == final_status
    assert [response.status for response in responses] == ["running", "running", final_status]
    assert responses[-1].result_is_status is True
    assert responses[-1].instruction == instr
    assert responses[-1].metadata == instr.metadata
    if success:
        assert responses[-1].error_info is None
    else:
        assert responses[-1].error_info.exception_type == "RuntimeError"
        assert "motor failed" in responses[-1].error_info.error_message
    assert handler.get_request("diid") is None

    for endpoint in (
        MessageEndpoints.device_read(device.name),
        MessageEndpoints.device_readback(device.name),
    ):
        readings = [
            sent["msg"]
            for sent in device_server.connector.message_sent
            if sent["queue"] == endpoint.endpoint
        ]
        assert readings[-1].signals == signals
        assert readings[-1].metadata == instr.metadata


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize(
    "first_success, second_success", [(True, True), (False, True), (True, False)]
)
def test_motion_readback_keeps_other_set_instruction_pending(
    device_server_mock, first_success, second_success
):
    device_server = device_server_mock
    handler = device_server.requests_handler
    devices = [device_server.device_manager.devices[name].obj for name in ("samx", "samy")]
    statuses = [DeviceStatus(device) for device in devices]
    callbacks_finished = [threading.Event(), threading.Event()]
    instructions = [
        messages.DeviceInstructionMessage(
            device=device.name,
            action="set",
            parameter={"value": 5},
            metadata={"stream": "primary", "device_instr_id": device.name, "RID": "test"},
        )
        for device in devices
    ]
    for device in devices:
        device._kind = Kind.normal

    with (
        mock.patch.object(devices[0], "set", return_value=statuses[0]) as first_set,
        mock.patch.object(devices[1], "set", return_value=statuses[1]) as second_set,
        mock.patch.object(
            devices[0], "read", return_value={"samx": {"value": 5, "timestamp": 1234}}
        ) as first_read,
        mock.patch.object(
            devices[1], "read", return_value={"samy": {"value": 5, "timestamp": 1234}}
        ) as second_read,
    ):
        try:
            for instr, status, event in zip(instructions, statuses, callbacks_finished):
                device_server.handle_device_instructions(instr)
                status.add_callback(lambda _, event=event: event.set())
            first_set.assert_called_once_with(5)
            second_set.assert_called_once_with(5)
            second_request = handler.get_request("samy")
            tracked = second_request["status_objects"]
            assert len(tracked) == 1
            assert isinstance(tracked[0], InstructionExecution)
            assert tracked[0].instruction is instructions[1]
            assert not tracked[0].done
            assert "instruction" not in statuses[1].__dict__
            assert "obj_ref" not in statuses[1].__dict__
            assert second_request["num_status_objects"] == 1

            if first_success:
                statuses[0].set_finished()
            else:
                statuses[0].set_exception(RuntimeError("first motor failed"))
            assert callbacks_finished[0].wait(timeout=5)

            assert handler.get_request("samx") is None
            assert handler.get_request("samy") is second_request
            assert not statuses[1].done
            first_read.assert_called_once_with()
            second_read.assert_not_called()
            responses = [
                sent["msg"]
                for sent in device_server.connector.message_sent
                if sent["queue"] == MessageEndpoints.device_instructions_response()
            ]
            assert [
                response.status for response in responses if response.instruction == instructions[1]
            ] == ["running", "running"]
        finally:
            for status, success, event, message in zip(
                statuses,
                (first_success, second_success),
                callbacks_finished,
                ("first motor failed", "second motor failed"),
            ):
                if not status.done:
                    if success:
                        status.set_finished()
                    else:
                        status.set_exception(RuntimeError(message))
                assert event.wait(timeout=5)
        second_read.assert_called_once_with()

    responses = [
        sent["msg"]
        for sent in device_server.connector.message_sent
        if sent["queue"] == MessageEndpoints.device_instructions_response()
    ]
    for instr, success, error in zip(
        instructions, (first_success, second_success), ("first motor failed", "second motor failed")
    ):
        instruction_responses = [
            response for response in responses if response.instruction == instr
        ]
        assert [response.status for response in instruction_responses] == [
            "running",
            "running",
            "completed" if success else "error",
        ]
        final = instruction_responses[-1]
        assert final.result_is_status is True
        if success:
            assert final.error_info is None
        else:
            assert final.error_info.device == instr.device
            assert final.error_info.exception_type == "RuntimeError"
            assert error in final.error_info.error_message
        assert handler.get_request(instr.metadata["device_instr_id"]) is None


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("kind", [Kind.normal, Kind.hinted])
@pytest.mark.parametrize("motion_fails", [False, True])
@pytest.mark.parametrize("action", ["set", "rpc"])
def test_pending_motion_readback_error_finishes_instruction(
    device_server_mock, kind, motion_fails, action
):
    device_server = device_server_mock
    device = device_server.device_manager.devices.samx
    device.obj._kind = kind
    device._config["onFailure"] = OnFailure.RAISE
    status = DeviceStatus(device.obj)
    instr = messages.DeviceInstructionMessage(
        device=device.name,
        action=action,
        parameter=(
            {"value": 5} if action == "set" else {"func": "set", "args": [5], "rpc_id": "rpc-id"}
        ),
        metadata={"device_instr_id": "diid", "RID": "test"},
    )
    callbacks_finished = threading.Event()

    def read():
        if status.done:
            raise RuntimeError("readback lost")
        return {}

    with (
        mock.patch.object(device.obj, "set", return_value=status),
        mock.patch.object(device.obj, "read", side_effect=read),
    ):
        device_server.handle_device_instructions(instr)
        status.add_callback(lambda _: callbacks_finished.set())
        if motion_fails:
            status.set_exception(ValueError("motor failed"))
        else:
            status.set_finished()
        assert callbacks_finished.wait(timeout=5), "Status callbacks did not finish"

    responses = [
        sent["msg"]
        for sent in device_server.connector.message_sent
        if sent["queue"] == MessageEndpoints.device_instructions_response()
    ]
    assert [response.status for response in responses] == ["running", "running", "error"]
    assert responses[-1].result_is_status is True
    assert responses[-1].instruction == instr
    assert responses[-1].metadata == instr.metadata
    expected_error = "motor failed" if motion_fails else "readback lost"
    expected_type = "ValueError" if motion_fails else "RuntimeError"
    assert responses[-1].error_info.exception_type == expected_type
    assert expected_error in responses[-1].error_info.error_message
    assert device_server.requests_handler.get_request("diid") is None


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("kind", [Kind.normal, Kind.hinted])
@pytest.mark.parametrize("already_finished", [False, True])
@pytest.mark.parametrize("motion_fails", [False, True])
def test_rpc_readback_error_finishes_client_status(
    device_server_mock, connected_connector, kind, already_finished, motion_fails
):
    server = device_server_mock
    device = server.device_manager.devices.samx
    device.obj._kind = kind
    device._config["onFailure"] = OnFailure.RAISE
    status = DeviceStatus(device.obj)
    instr = messages.DeviceInstructionMessage(
        device=device.name,
        action="rpc",
        parameter={"func": "set", "args": [5], "rpc_id": "rpc-id"},
        metadata={"device_instr_id": "diid", "RID": "test", "response": True},
    )

    def finish_motion():
        if motion_fails:
            status.set_exception(ValueError("motor failed"))
        else:
            status.set_finished()

    if already_finished:
        finish_motion()

    with (
        mock.patch.object(device.obj, "set", return_value=status),
        mock.patch.object(device.obj, "read", side_effect=[{}, RuntimeError("readback lost")]),
        mock.patch.object(
            server.connector, "xadd", side_effect=connected_connector.xadd
        ) as publish,
        mock.patch.object(server.device_manager, "connector", connected_connector),
    ):
        try:
            server.rpc_handler.run_rpc(instr)
            rpc_response = next(
                sent["msg"]
                for sent in server.connector.message_sent
                if sent["queue"] == MessageEndpoints.device_rpc("rpc-id")
            )
            client_status = device._handle_rpc_response(rpc_response)
            if not already_finished:
                finish_motion()
            with pytest.raises(RPCError):
                client_status.wait(timeout=5)
        finally:
            if not status.done:
                finish_motion()

    client_calls = [
        call
        for call in publish.call_args_list
        if call.args and call.args[0] == MessageEndpoints.device_req_status("test")
    ]
    assert len(client_calls) == 1
    assert client_calls[0].kwargs["expire"] == 3600
    client_response = client_calls[0].args[1]["data"]
    assert client_status.done
    assert client_response.success is False
    assert client_response.device == device.name
    assert client_response.request_id == "test"
    expected_error = "motor failed" if motion_fails else "readback lost"
    expected_type = "ValueError" if motion_fails else "RuntimeError"
    error_info = client_response.metadata["error_info"]
    assert error_info.exception_type == expected_type
    assert expected_error in error_info.error_message
    assert (
        messages.ErrorInfo.model_validate(client_status._request_status.metadata["error_info"])
        == error_info
    )
    instruction_response = [
        sent["msg"]
        for sent in server.connector.message_sent
        if sent["queue"] == MessageEndpoints.device_instructions_response()
    ][-1]
    assert instruction_response.status == "error"
    assert instruction_response.error_info == error_info
    assert server.requests_handler.get_request("diid") is None


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("kind", [Kind.normal, Kind.hinted])
def test_finished_rpc_status_readback_error_finishes_instruction(device_server_mock, kind):
    device_server = device_server_mock
    device = device_server.device_manager.devices.samx
    device.obj._kind = kind
    device._config["onFailure"] = OnFailure.RAISE
    status = DeviceStatus(device.obj)
    status.set_finished()
    instr = messages.DeviceInstructionMessage(
        device=device.name,
        action="rpc",
        parameter={"func": "set", "args": [5], "rpc_id": "rpc-id"},
        metadata={"device_instr_id": "diid", "RID": "test"},
    )

    with (
        mock.patch.object(device.obj, "set", return_value=status),
        mock.patch.object(device.obj, "read", side_effect=[{}, RuntimeError("readback lost")]),
    ):
        device_server.rpc_handler.run_rpc(instr)

    responses = [
        sent["msg"]
        for sent in device_server.connector.message_sent
        if sent["queue"] == MessageEndpoints.device_instructions_response()
    ]
    assert responses[-1].status == "error"
    assert responses[-1].error_info.exception_type == "RuntimeError"
    assert "readback lost" in responses[-1].error_info.error_message
    assert device_server.requests_handler.get_request("diid") is None


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="read",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
        messages.DeviceInstructionMessage(
            device=["samx", "samy"],
            action="read",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test2"},
        ),
    ],
)
def test_update_device_metadata(device_server_mock, instr):
    device_server = device_server_mock

    devices = instr.content["device"]
    if not isinstance(devices, list):
        devices = [devices]

    device_server._update_device_metadata(instr)

    for dev in devices:
        assert device_server.device_manager.devices.get(dev).metadata == instr.metadata


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_stop_devices(device_server_mock):
    device_server = device_server_mock
    dev = device_server.device_manager.devices
    assert len(dev) > len(dev.enabled_devices)
    with mock.patch.object(dev.samx.obj, "stop") as stop:
        device_server.stop_devices()
        stop.assert_called_once()

    with mock.patch.object(dev.samy.obj, "stop", side_effect=Exception) as stop:
        with mock.patch.object(device_server.connector, "raise_alarm") as raise_alarm:
            device_server.stop_devices()
            stop.assert_called_once()
            assert raise_alarm.call_count == 1
            assert raise_alarm.call_args == mock.call(
                severity=Alarms.WARNING, info=mock.ANY, metadata=mock.ANY
            )
            # If stop raises an exception, the device server must get back to running state
            assert device_server.status == BECStatus.RUNNING

    with mock.patch.object(dev.motor1_disabled.obj, "stop") as stop:
        device_server.stop_devices()
        stop.assert_not_called()

    with mock.patch.object(dev.motor1_disabled_set.obj, "stop") as stop:
        device_server.stop_devices()
        stop.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_on_stop_devices(device_server_mock):
    msg = messages.VariableMessage(value=None, metadata={})
    msg_obj = MessageObject(topic="internal/queue/stop_devices", value=msg)
    device_server = device_server_mock
    with mock.patch.object(device_server, "stop_devices") as stop:
        device_server.on_stop_devices(msg_obj, parent=device_server)
        stop.assert_called_once_with()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_on_stop_devices_with_empty_list(device_server_mock):
    msg = messages.VariableMessage(value=[], metadata={})
    msg_obj = MessageObject(topic="internal/queue/stop_devices", value=msg)
    device_server = device_server_mock
    with mock.patch.object(device_server, "stop_devices") as stop:
        device_server.on_stop_devices(msg_obj, parent=device_server)
        stop.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_on_stop_devices_with_list(device_server_mock):
    msg = messages.VariableMessage(value=["samx"], metadata={})
    msg_obj = MessageObject(topic="internal/queue/stop_devices", value=msg)
    device_server = device_server_mock
    with mock.patch.object(device_server, "stop_devices") as stop:
        device_server.on_stop_devices(msg_obj, parent=device_server)
        stop.assert_called_once_with(["samx"])


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="eiger",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
        messages.DeviceInstructionMessage(
            device=["samx", "samy"],
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
        messages.DeviceInstructionMessage(
            device="motor2_disabled",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
        messages.DeviceInstructionMessage(
            device="motor1_disabled",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
    ],
)
def test_assert_device_is_enabled(device_server_mock, instr):
    device_server = device_server_mock
    devices = instr.content["device"]

    if not isinstance(devices, list):
        devices = [devices]

    for dev in devices:
        if not device_server.device_manager.devices[dev].enabled:
            with pytest.raises(Exception) as exc_info:
                device_server.assert_device_is_enabled(instr)
            assert exc_info.value.args[0] == f"Cannot access disabled device {dev}."
        else:
            device_server.assert_device_is_enabled(instr)


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
        messages.DeviceInstructionMessage(
            device="not_a_valid_device",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
        messages.DeviceInstructionMessage(
            device=None,
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
    ],
)
def test_assert_device_is_valid(device_server_mock, instr):
    device_server = device_server_mock
    devices = instr.content["device"]

    if not devices:
        with pytest.raises(InvalidDeviceError):
            device_server.assert_device_is_valid(instr)
        return

    if not isinstance(devices, list):
        devices = [devices]

    for dev in devices:
        if dev not in device_server.device_manager.devices:
            with pytest.raises(InvalidDeviceError) as exc_info:
                device_server.assert_device_is_valid(instr)
            assert exc_info.value.args[0] == f"There is no device with the name {dev}."
        else:
            device_server.assert_device_is_enabled(instr)


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="set",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_handle_device_instructions_set(device_server_mock, instructions):
    device_server = device_server_mock

    with mock.patch.object(device_server, "assert_device_is_valid") as assert_device_is_valid_mock:
        with mock.patch.object(
            device_server, "assert_device_is_enabled"
        ) as assert_device_is_enabled_mock:
            with mock.patch.object(
                device_server, "_update_device_metadata"
            ) as update_device_metadata_mock:
                with mock.patch.object(device_server, "_set_device") as set_mock:
                    device_server.handle_device_instructions(instructions)

                    assert_device_is_valid_mock.assert_called_once_with(instructions)
                    assert_device_is_enabled_mock.assert_called_once_with(instructions)
                    update_device_metadata_mock.assert_called_once_with(instructions)

                    set_mock.assert_called_once_with(instructions)


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="set",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
def test_handle_device_instruction_disabled_device(device_server_mock, instructions):
    """
    Test that handling device instructions for a disabled device resolves the status as failed.
    """
    device_server = device_server_mock
    with mock.patch.object(device_server, "assert_device_is_enabled", side_effect=RuntimeError):
        with mock.patch.object(device_server.connector, "send") as send_mock:
            device_server.handle_device_instructions(instructions)
            assert send_mock.call_count == 2
            pending_msg = send_mock.call_args_list[0].args[1]
            error_msg = send_mock.call_args_list[1].args[1]

            assert pending_msg.instruction_id == instructions.metadata["device_instr_id"]
            assert pending_msg.status == "running"
            assert error_msg.instruction_id == instructions.metadata["device_instr_id"]
            assert error_msg.status == "error"
            assert error_msg.error_info is not None
            assert (
                device_server.requests_handler.get_request(instructions.metadata["device_instr_id"])
                is None
            )


@pytest.mark.parametrize("action", ["set", "rpc"])
@pytest.mark.parametrize("error_type", [ophyd_errors.LimitError, RuntimeError])
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_handle_device_instructions_emits_errors_consistently(
    device_server_mock, action, error_type
):
    """Report native limits and other exceptions through the same path."""
    server = device_server_mock
    instructions = messages.DeviceInstructionMessage(
        device="samx",
        action=action,
        parameter={},
        metadata={"device_instr_id": "operation-error", "RID": "test"},
    )
    operation = server if action == "set" else server.rpc_handler
    method = "_set_device" if action == "set" else "run_rpc"
    error = error_type("Operation failed")
    calls = mock.Mock()
    with (
        mock.patch.object(operation, method, side_effect=error),
        mock.patch.object(server.requests_handler, "set_finished", calls.finish),
        mock.patch.object(server.rpc_handler, "send_rpc_exception", calls.rpc),
        mock.patch("bec_server.device_server.device_server.logger.error", calls.log),
    ):
        server.handle_device_instructions(instructions)

    info = calls.finish.call_args.kwargs["error_info"]
    assert type(info) is messages.ErrorInfo
    assert info.exception_type == error_type.__name__
    assert info.compact_error_message.endswith("Operation failed\n")
    expected_calls = [mock.call.log(info.error_message)]
    if action == "rpc":
        expected_calls.append(mock.call.rpc(error, instructions))
    expected_calls.append(mock.call.finish("operation-error", success=False, error_info=info))
    assert calls.mock_calls == expected_calls


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="read",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
def test_handle_device_instructions_read(device_server_mock, instructions):
    device_server = device_server_mock

    with mock.patch.object(device_server, "_read_device") as read_mock:
        device_server.handle_device_instructions(instructions)
        read_mock.assert_called_once_with(instructions)


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="rpc",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_handle_device_instructions_rpc(device_server_mock, instructions):
    device_server = device_server_mock
    with mock.patch.object(device_server, "assert_device_is_valid") as assert_device_is_valid_mock:
        with mock.patch.object(
            device_server, "assert_device_is_enabled"
        ) as assert_device_is_enabled_mock:
            with mock.patch.object(
                device_server, "_update_device_metadata"
            ) as update_device_metadata_mock:
                with mock.patch.object(device_server.rpc_handler, "run_rpc") as rpc_mock:
                    device_server.handle_device_instructions(instructions)
                    rpc_mock.assert_called_once_with(instructions)

                    assert_device_is_valid_mock.assert_called_once_with(instructions)
                    assert_device_is_enabled_mock.assert_not_called()
                    update_device_metadata_mock.assert_called_once_with(instructions)


@pytest.mark.parametrize("device", ["samx", []])
def test_handle_device_instructions_broadcast_scan_device_info(device_server_mock, device):
    instructions = messages.DeviceInstructionMessage(
        device=device,
        action="broadcast_scan_device_info",
        parameter={"scan_id": "scan-id-1"},
        metadata={"RID": "rid-1", "device_instr_id": "broadcast-diid"},
    )

    with mock.patch.object(
        device_server_mock,
        "assert_device_is_valid",
        wraps=device_server_mock.assert_device_is_valid,
    ) as valid_mock:
        with mock.patch.object(
            device_server_mock,
            "assert_device_is_enabled",
            wraps=device_server_mock.assert_device_is_enabled,
        ) as enabled_mock:
            with mock.patch.object(
                device_server_mock, "_broadcast_scan_device_info"
            ) as broadcast_mock:
                device_server_mock.handle_device_instructions(instructions)

    broadcast_mock.assert_called_once_with(instructions)
    if device:
        valid_mock.assert_called_once_with(instructions)
        enabled_mock.assert_called_once_with(instructions)
    else:
        valid_mock.assert_not_called()
        enabled_mock.assert_not_called()


def test_broadcast_scan_device_info_publishes_device_records(device_server_mock):
    class Detector(Device):
        preview = Cpt(PreviewSignal, ndim=2)
        plain = Cpt(Signal, value=0)

    detector = Detector(name="eiger")
    detector.preview.signal_info.acquisition_group = "fly-scan"
    detector.preview.signal_info.use_alias = True
    detector.preview.signal_info.signal_metadata["processing"] = {"rotation": 90}
    expected_info = detector.preview.signal_info

    device_server_mock.device_manager.devices = {
        "eiger": SimpleNamespace(obj=detector),
        "samx": SimpleNamespace(obj=Signal(name="samx", value=0)),
    }
    device_server_mock.connector.xadd = mock.MagicMock()
    instructions = messages.DeviceInstructionMessage(
        device=["eiger", "samx"],
        action="broadcast_scan_device_info",
        parameter={"scan_id": "scan-id-1"},
        metadata={"RID": "rid-1", "device_instr_id": "broadcast-diid"},
    )

    with mock.patch.object(device_server_mock.connector, "send") as send_mock:
        device_server_mock._broadcast_scan_device_info(instructions)

    device_server_mock.connector.xadd.assert_called_once()
    endpoint, payload = device_server_mock.connector.xadd.call_args.args
    msg = payload["data"]
    assert endpoint == MessageEndpoints.scan_device_info()
    assert device_server_mock.connector.xadd.call_args.kwargs == {"max_size": 10}
    assert msg == messages.ScanDeviceInfoMessage(
        scan_id="scan-id-1",
        devices={"eiger": messages.DeviceRuntimeInfo(signal_info={"preview": expected_info})},
        metadata={"RID": "rid-1", "device_instr_id": "broadcast-diid"},
    )
    responses = [call.args[1] for call in send_mock.call_args_list]
    assert [response.status for response in responses] == ["running", "completed"]
    assert all(response.instruction_id == "broadcast-diid" for response in responses)
    assert all(response.instruction == instructions for response in responses)
    assert device_server_mock.requests_handler.get_request("broadcast-diid") is None


def test_broadcast_scan_device_info_uses_runtime_metadata(device_server_mock):
    class SubDetector(Device):
        preview = Cpt(PreviewSignal, ndim=2)

    class Detector(Device):
        sub = Cpt(SubDetector)
        plain = Cpt(Signal, value=0)

    detector = Detector(name="eiger")
    signal = detector.sub.preview
    runtime_info = messages.SignalInfo(
        saved=False,
        ndim=2,
        role="preview",
        enabled=False,
        use_alias=True,
        signals=[("image", Kind.hinted.value)],
        signal_metadata={"units": "counts", "processing": {"rotation": 90}},
        correlation_group="fly-scan",
    )
    signal.signal_info = runtime_info
    signal.describe = mock.Mock(side_effect=AssertionError("Use the live signal_info model"))

    device_server_mock.device_manager.devices = {"eiger": SimpleNamespace(obj=detector)}
    device_server_mock.connector.xadd = mock.MagicMock()
    instructions = messages.DeviceInstructionMessage(
        device="eiger",
        action="broadcast_scan_device_info",
        parameter={"scan_id": "scan-id-1"},
        metadata={"device_instr_id": "broadcast-diid"},
    )

    device_server_mock._broadcast_scan_device_info(instructions)

    msg = device_server_mock.connector.xadd.call_args.args[1]["data"]
    snapshot = msg.devices["eiger"].signal_info["sub.preview"]
    assert snapshot == runtime_info
    assert snapshot is runtime_info


@pytest.mark.parametrize("devices", [[], ["samx"], ["samx", "empty"]])
def test_broadcast_scan_device_info_omits_empty_devices(device_server_mock, devices):
    class EmptyDetector(Device):
        plain = Cpt(Signal, value=0)

    device_server_mock.device_manager.devices = {
        "samx": SimpleNamespace(obj=Signal(name="samx", value=0), enabled=True),
        "empty": SimpleNamespace(obj=EmptyDetector(name="empty"), enabled=True),
    }
    device_server_mock.connector.xadd = mock.MagicMock()
    instructions = messages.DeviceInstructionMessage(
        device=devices,
        action="broadcast_scan_device_info",
        parameter={"scan_id": "scan-id-1"},
        metadata={"RID": "rid-1", "device_instr_id": "broadcast-diid"},
    )

    with mock.patch.object(device_server_mock.connector, "send") as send_mock:
        device_server_mock.handle_device_instructions(instructions)

    device_server_mock.connector.xadd.assert_called_once()
    endpoint, payload = device_server_mock.connector.xadd.call_args.args
    assert endpoint == MessageEndpoints.scan_device_info()
    assert payload["data"] == messages.ScanDeviceInfoMessage(
        scan_id="scan-id-1", devices={}, metadata=instructions.metadata
    )
    assert [call.args[1].status for call in send_mock.call_args_list] == ["running", "completed"]
    assert device_server_mock.requests_handler.get_request("broadcast-diid") is None


def test_broadcast_scan_device_info_resolves_failure_when_publish_raises(device_server_mock):
    instructions = messages.DeviceInstructionMessage(
        device=[],
        action="broadcast_scan_device_info",
        parameter={"scan_id": "scan-id-1"},
        metadata={"RID": "rid-1", "device_instr_id": "broadcast-diid"},
    )
    device_server_mock.connector.xadd = mock.MagicMock(side_effect=RuntimeError("publish failed"))

    with mock.patch.object(device_server_mock.connector, "send") as send_mock:
        device_server_mock.handle_device_instructions(instructions)

    responses = [call.args[1] for call in send_mock.call_args_list]
    assert [response.status for response in responses] == ["running", "error"]
    assert responses[-1].error_info.exception_type == "RuntimeError"
    assert device_server_mock.requests_handler.get_request("broadcast-diid") is None


def test_broadcast_scan_device_info_skips_without_scan_id(device_server_mock):
    device_server_mock.connector.xadd = mock.MagicMock()
    instructions = messages.DeviceInstructionMessage(
        device="samx",
        action="broadcast_scan_device_info",
        parameter={},
        metadata={"RID": "rid-1", "device_instr_id": "broadcast-diid"},
    )

    with mock.patch.object(device_server_mock.connector, "send") as send_mock:
        device_server_mock._broadcast_scan_device_info(instructions)

    device_server_mock.connector.xadd.assert_not_called()
    responses = [call.args[1] for call in send_mock.call_args_list]
    assert [response.status for response in responses] == ["running", "completed"]
    assert all(response.instruction_id == "broadcast-diid" for response in responses)
    assert all(response.instruction == instructions for response in responses)
    assert device_server_mock.requests_handler.get_request("broadcast-diid") is None


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="kickoff",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_handle_device_instructions_kickoff(device_server_mock, instructions):
    device_server = device_server_mock

    with mock.patch.object(device_server, "_kickoff_device") as kickoff_mock:
        device_server.handle_device_instructions(instructions)
        kickoff_mock.assert_called_once_with(instructions)


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="complete",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_handle_device_instructions_complete(device_server_mock, instructions):
    device_server = device_server_mock

    with mock.patch.object(device_server, "_run_operations") as complete_mock:
        device_server.handle_device_instructions(instructions)
        complete_mock.assert_called_once_with(instructions)


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="flyer_sim",
            action="complete",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
        messages.DeviceInstructionMessage(
            device="bpm4i",
            action="complete",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
        messages.DeviceInstructionMessage(
            device=None,
            action="complete",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_complete_device(device_server_mock, instr):
    device_server = device_server_mock
    complete_mock = mock.MagicMock()
    device = instr.content["device"]
    oph_device = device_server.device_manager.devices.get(device)
    status = DeviceStatus(oph_device)
    status.set_finished()
    complete_mock.return_value = status
    if device is not None:
        oph_device.obj.complete = complete_mock
    device_server._run_operations(instr)
    if instr.content["device"] is not None:
        oph_device.obj.complete.assert_called_once()


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="eiger",
            action="pre_scan",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_handle_device_instructions_pre_scan(device_server_mock, instructions):
    device_server = device_server_mock

    finished_thread_event = threading.Event()

    def finished_callback():
        finished_thread_event.set()

    status = DeviceStatus(device=device_server.device_manager.devices.eiger.obj)
    status.add_callback(finished_callback)

    with mock.patch.object(
        device_server.device_manager.devices.eiger.obj, "pre_scan", return_value=status
    ):
        with mock.patch.object(
            device_server.requests_handler, "send_device_instruction_response"
        ) as send_response_mock:
            device_server.handle_device_instructions(instructions)
            request_info = device_server.requests_handler.get_request(instr_id="diid")
            assert len(request_info["status_objects"]) == 1
            tracked = request_info["status_objects"][0]
            assert isinstance(tracked, InstructionExecution)
            assert tracked.instruction is instructions
            assert not tracked.done
            assert "instruction" not in status.__dict__
            assert "obj_ref" not in status.__dict__
            assert status.done is False
            assert send_response_mock.call_count == 2
            assert send_response_mock.call_args_list[0] == mock.call("diid", None, False)
            assert send_response_mock.call_args_list[1] == mock.call(
                "diid", success=False, done=False, is_status_obj=True, result=None
            )
            status.set_finished()
            finished_thread_event.wait()
            assert status.done is True
            assert send_response_mock.call_count == 3
            assert send_response_mock.call_args == mock.call(
                "diid", True, done=True, error_info=None, result=None, is_status_obj=True
            )


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="trigger",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
def test_handle_device_instructions_trigger(device_server_mock, instructions):
    device_server = device_server_mock

    with mock.patch.object(device_server, "_run_operations") as trigger_mock:
        device_server.handle_device_instructions(instructions)
        trigger_mock.assert_called_once_with(instructions)


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
def test_handle_device_instructions_stage(device_server_mock, instructions):
    device_server = device_server_mock

    with mock.patch.object(device_server, "_run_operations") as stage_mock:
        device_server.handle_device_instructions(instructions)
        stage_mock.assert_called_once_with(instructions)


@pytest.mark.parametrize(
    "instructions",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="unstage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
def test_handle_device_instructions_unstage(device_server_mock, instructions):
    device_server = device_server_mock

    with mock.patch.object(device_server, "_run_operations") as unstage_mock:
        device_server.handle_device_instructions(instructions)
        unstage_mock.assert_called_once_with(instructions)


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="eiger",
            action="trigger",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "12345"},
        ),
        messages.DeviceInstructionMessage(
            device=["samx", "samy"],
            action="trigger",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "12345"},
        ),
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_trigger_device(device_server_mock, instr):
    device_server = device_server_mock
    devices = instr.content["device"]
    if not isinstance(devices, list):
        devices = [devices]
    for dev in devices:
        metadata = device_server.device_manager.devices.get(dev).metadata
        with mock.patch.object(
            device_server.device_manager.devices.get(dev).obj, "trigger"
        ) as trigger:
            trigger.return_value = mock.MagicMock(spec=DeviceStatus)
            device_server._run_operations(instr)
            trigger.assert_called_once()
        assert device_server.device_manager.devices.get(dev).metadata is metadata


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="flyer_sim",
            action="kickoff",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_kickoff_device(device_server_mock, instr):
    device_server = device_server_mock
    with mock.patch.object(
        device_server.device_manager.devices.flyer_sim.obj, "kickoff"
    ) as kickoff:
        kickoff.return_value = mock.MagicMock(spec=DeviceStatus)
        device_server._kickoff_device(instr)
        kickoff.assert_called_once()


@pytest.mark.timeout(30)
@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="set",
            parameter={"value": 5},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_set_device(device_server_mock, instr):
    device_server = device_server_mock
    device_server._set_device(instr)
    while True:
        res = [
            msg
            for msg in device_server.connector.message_sent
            if msg["queue"] == MessageEndpoints.device_instructions_response()
        ]
        if res:
            break
    msg = res[0]["msg"]
    assert msg.metadata["RID"] == "test"

    # Test that if _set raises an exception, the status is set as failed
    with mock.patch.object(
        device_server.device_manager.devices.samx.obj, "set", side_effect=Exception("Set failed")
    ):
        with mock.patch.object(
            device_server.requests_handler, "send_device_instruction_response"
        ) as mock_send_response:
            device_server._set_device(instr)
            # Call arg list should not contain any duplicate calls, error response should only be sent once
            assert (
                mock_send_response.call_count == 3
            )  # Make sure that there are no duplicate calls for error response.
            mock_send_response.assert_called_with(
                instr.metadata["device_instr_id"],
                False,
                done=True,
                error_info=ANY,
                result=None,
                is_status_obj=True,
            )


@pytest.mark.timeout(30)
@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="set",
            parameter={"value": 5},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        )
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_set_device_error_formatted_nicely(device_server_mock, instr):
    """Test that if set() raises an exception, and the exception is known,
    a better-formated response is returned."""
    with (
        mock.patch.object(
            device_server_mock.device_manager.devices.samx.obj,
            "set",
            side_effect=TypeError("tuple indices must be integers or slices, not float"),
        ),
        mock.patch.object(
            device_server_mock.requests_handler, "send_device_instruction_response"
        ) as mock_send_response,
    ):
        device_server_mock._set_device(instr)
        assert (
            """DeviceInstructionError: An incorrect value was provided to a .set() command. This could be, for example, providing a float rather than an int to an enum PV. Device: samx, value: 5."""
            in mock_send_response.call_args.kwargs["error_info"].compact_error_message
        )


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="read",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
        ),
        messages.DeviceInstructionMessage(
            device=["samx", "samy"],
            action="read",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test2"},
        ),
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_read_device(device_server_mock, instr):
    device_server = device_server_mock
    device_server._read_device(instr)
    devices = instr.content["device"]
    if not isinstance(devices, list):
        devices = [devices]
    for device in devices:
        res = [
            msg
            for msg in device_server.connector.message_sent
            if msg["queue"] == MessageEndpoints.device_read(device).endpoint
        ]
        assert res[-1]["msg"].metadata["RID"] == instr.metadata["RID"]
        assert res[-1]["msg"].metadata["stream"] == "primary"


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_read_device_can_return_result(device_server_mock):
    device_server = device_server_mock
    instr = messages.DeviceInstructionMessage(
        device=["samx", "samy"],
        action="read",
        parameter={"return_result": True},
        metadata={"stream": "primary", "device_instr_id": "diid", "RID": "test"},
    )

    device_server._read_device(instr)

    responses = [
        msg["msg"]
        for msg in device_server.connector.message_sent
        if msg["queue"] == MessageEndpoints.device_instructions_response()
    ]
    response = responses[-1]
    assert response.result is not None
    assert len(response.result) == 2
    assert response.result[0].keys() == device_server.device_manager.devices.samx.obj.read().keys()
    assert response.result[1].keys() == device_server.device_manager.devices.samy.obj.read().keys()


@pytest.mark.parametrize("devices", [["samx", "samy"], ["samx"]])
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_read_config_and_update_devices(device_server_mock, devices):
    device_server = device_server_mock
    device_server._read_config_and_update_devices(devices, metadata={"RID": "test"})
    for device in devices:
        res = [
            msg
            for msg in device_server.connector.message_sent
            if msg["queue"] == MessageEndpoints.device_read_configuration(device).endpoint
        ]
        config = device_server.device_manager.devices[device].obj.read_configuration()
        msg = res[-1]["msg"]
        assert msg.content["signals"].keys() == config.keys()
        assert res[-1]["queue"] == MessageEndpoints.device_read_configuration(device).endpoint


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_rpc_reads_root_configuration_signal(device_server_mock):
    server = device_server_mock
    signal = Signal(name="root_config", value=42, kind=Kind.config)
    device = DSDevice(
        name=signal.name,
        obj=signal,
        config={"deviceClass": "ophyd.Signal", "enabled": True, "readoutPriority": "monitored"},
        parent=server.device_manager,
    )
    server.device_manager.devices._add_device(device.name, device)
    instruction = messages.DeviceInstructionMessage(
        device=device.name, action="rpc", parameter={"func": "read_configuration"}, metadata={}
    )

    result = server.rpc_handler.process_rpc_instruction(instruction)

    assert result[device.name]["value"] == 42
    endpoint = MessageEndpoints.device_read_configuration(device.name).endpoint
    assert any(
        message["queue"] == endpoint and message["msg"].signals == result
        for message in server.connector.message_sent
    )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_rpc_read_preserves_values_supported_by_the_wire_encoder(device_server_mock):
    server = device_server_mock
    signal = Signal(name="set_value", value={1, 2}, kind=Kind.omitted)
    device = DSDevice(
        name=signal.name,
        obj=signal,
        config={"deviceClass": "ophyd.Signal", "enabled": True, "readoutPriority": "monitored"},
        parent=server.device_manager,
    )
    server.device_manager.devices._add_device(device.name, device)
    instruction = messages.DeviceInstructionMessage(
        device=device.name,
        action="rpc",
        parameter={"func": "read", "rpc_id": "read-set"},
        metadata={"device_instr_id": "read-set"},
    )
    with (
        mock.patch.object(server.connector, "set") as write,
        mock.patch.object(server.connector, "raise_alarm") as alarm,
    ):
        server.rpc_handler.run_rpc(instruction)
    response = next(
        call.args[1]
        for call in write.call_args_list
        if call.args[0] == MessageEndpoints.device_rpc("read-set")
    )
    decoded = MsgpackSerialization.loads(MsgpackSerialization.dumps(response))
    assert decoded.success
    assert decoded.return_val[device.name]["value"] == {1, 2}
    alarm.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("action", ["trigger", "stage", "unstage"])
def test_classic_operation_publishes_native_name(device_server_mock, action):
    server = device_server_mock
    obj = Device(name="native_detector")
    device = DSDevice(
        name="configured_detector",
        obj=obj,
        config={"deviceClass": "ophyd.Device", "enabled": True, "readoutPriority": "monitored"},
        parent=server.device_manager,
    )
    server.device_manager.devices._add_device(device.name, device)
    status = DeviceStatus(obj)
    obj._staged = Staged.yes if action == "unstage" else Staged.no
    instruction = messages.DeviceInstructionMessage(
        device=device.name,
        action=action,
        parameter={},
        metadata={"device_instr_id": "native-name", "RID": "native-name", "response": True},
    )
    with (
        mock.patch.object(obj, action, return_value=status),
        mock.patch.object(server.connector, "xadd") as publish,
        mock.patch.object(server.connector, "set") as write,
    ):
        server.handle_device_instructions(instruction)
        obj._staged = Staged.yes if action == "stage" else Staged.no
        status.set_finished()

    assert publish.call_args.args[1]["data"].device == obj.name
    if action != "trigger":
        assert write.call_args.args[0] == MessageEndpoints.device_staged(obj.name)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_delayed_classic_callback_publishes_after_aggregate_completion(device_server_mock):
    server = device_server_mock
    devices = [server.device_manager.devices[name] for name in ("samx", "samy")]
    instruction = messages.DeviceInstructionMessage(
        device=[device.name for device in devices],
        action="trigger",
        parameter={},
        metadata={"device_instr_id": "multi", "RID": "multi", "response": True},
    )
    statuses = [DeviceStatus(device.obj) for device in devices]
    entered = threading.Event()
    release = threading.Event()

    def delay(_status):
        entered.set()
        assert release.wait(5)

    statuses[0].add_callback(delay)
    server.requests_handler.add_request(instruction, num_status_objects=2)
    for device, status in zip(devices, statuses):
        execution = to_instruction_execution(status, DeviceTarget(device)).bind(instruction)
        server.requests_handler.add_status_object("multi", execution)

    first = threading.Thread(target=statuses[0].set_finished, daemon=True)
    with mock.patch.object(server.connector, "xadd") as publish:
        first.start()
        try:
            assert entered.wait(2)
            statuses[1].set_finished()
            assert server.requests_handler.get_request("multi") is None
        finally:
            release.set()
            first.join(2)
    assert not first.is_alive()
    responses = [call.args[1]["data"] for call in publish.call_args_list]
    assert [response.device for response in responses] == ["samy", "samx"]


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_read_and_update_devices_exception(device_server_mock):
    device_server = device_server_mock
    samx_obj = device_server.device_manager.devices.samx.obj
    with pytest.raises(Exception):
        with mock.patch.object(device_server, "_retry_obj_method") as mock_retry:
            with mock.patch.object(samx_obj, "read") as read_mock:
                read_mock.side_effect = Exception
                mock_retry.side_effect = Exception
                device_server._read_and_update_devices(["samx"], metadata={"RID": "test"})
                mock_retry.assert_called_once_with("samx", samx_obj, "read", Exception())


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_read_config_and_update_devices_exception(device_server_mock):
    device_server = device_server_mock
    samx_obj = device_server.device_manager.devices.samx.obj
    with pytest.raises(Exception):
        with mock.patch.object(device_server, "_retry_obj_method") as mock_retry:
            with mock.patch.object(samx_obj, "read_configuration") as read_config:
                read_config.side_effect = Exception
                mock_retry.side_effect = Exception
                device_server._read_config_and_update_devices(["samx"], metadata={"RID": "test"})
                mock_retry.assert_called_once_with(
                    "samx", samx_obj, "read_configuration", Exception()
                )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_retry_obj_method_raise(device_server_mock):
    device_server = device_server_mock
    samx = device_server.device_manager.devices.samx
    with mock.patch.object(samx.obj, "read_configuration") as read_config:
        read_config.side_effect = TimeoutError
        samx._config["onFailure"] = "raise"
        with pytest.raises(TimeoutError):
            device_server._retry_obj_method("samx", "read_configuration", TimeoutError())


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_retry_obj_method_retry(device_server_mock):
    device_server = device_server_mock
    samx = device_server.device_manager.devices.samx
    signals_before = samx.obj.read_configuration()
    samx._config["onFailure"] = "retry"
    signals = device_server._retry_obj_method("samx", "read_configuration", Exception())
    assert signals.keys() == signals_before.keys()


@pytest.mark.parametrize("instr", ["read", "read_configuration", "unknown_method"])
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_retry_obj_method_buffer(device_server_mock, instr):
    device_server = device_server_mock
    samx = device_server.device_manager.devices.samx
    samx._config["onFailure"] = "buffer"
    if instr not in ["read", "read_configuration"]:
        with pytest.raises(ValueError):
            device_server._retry_obj_method("samx", instr, Exception())
        return

    signals_before = getattr(samx.obj, instr)()
    device_server.connector = mock.MagicMock()
    device_server.connector.get.return_value = messages.DeviceMessage(
        signals=signals_before, metadata={"RID": "test", "stream": "primary"}
    )

    signals = device_server._retry_obj_method("samx", instr, Exception())
    assert signals.keys() == signals_before.keys()


@pytest.mark.parametrize("native_state", [Staged.yes, Staged.no, None])
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_stage_callback_publishes_before_validating_native_state(device_server_mock, native_state):
    """Only an explicit non-staged state fails the completed stage check."""
    server = device_server_mock
    obj = SimpleNamespace()
    if native_state is not None:
        obj._staged = native_state
    target = DeviceTarget(DSDevice("stage_target", obj, config=None))
    instruction = messages.DeviceInstructionMessage(
        device=target.name, action="stage", parameter={}, metadata={"device_instr_id": "stage"}
    )
    execution = InstructionExecution(Future(), target).bind(instruction)

    with mock.patch.object(server.connector, "set") as publish:
        if native_state is Staged.no:
            with pytest.raises(ValueError, match="Failed to stage device stage_target"):
                server._operation_staged_callback(execution)
        else:
            server._operation_staged_callback(execution)

    publish.assert_called_once_with(
        MessageEndpoints.device_staged(target.name),
        messages.DeviceStatusMessage(device=target.name, status=1, metadata=instruction.metadata),
    )


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid"},
        ),
        messages.DeviceInstructionMessage(
            device=["samx", "samy"],
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid"},
        ),
        messages.DeviceInstructionMessage(
            device="ring_current_sim",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid"},
        ),
        messages.DeviceInstructionMessage(
            device="device_with_not_resolving_status",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid"},
        ),
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_stage_device(device_server_mock, instr):
    device_server = device_server_mock
    if instr.content["device"] != "device_with_not_resolving_status":
        device_server._run_operations(instr)
        devices = instr.content["device"]
        devices = devices if isinstance(devices, list) else [devices]
        dev_man = device_server.device_manager.devices
        for dev in devices:
            if not hasattr(dev_man[dev].obj, "_staged"):
                continue
            assert device_server.device_manager.devices[dev].obj._staged == Staged.yes
        device_server._run_operations(instr.model_copy(update={"action": "unstage"}))
        for dev in devices:
            if not hasattr(dev_man[dev].obj, "_staged"):
                continue
            assert device_server.device_manager.devices[dev].obj._staged == Staged.no
    else:
        device_server._run_operations(instr)
        status = device_server.requests_handler._storage["diid"]["status_objects"][0]
        assert status.done is False
        dev = "device_with_not_resolving_status"
        obj = device_server.device_manager.devices[dev].obj
        obj.stage_thread_event.set()
        while not status.done:
            pass
        assert status.done is True
        assert device_server.device_manager.devices[dev].obj._staged == Staged.yes


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="stage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid"},
        )
    ],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_stage_timeout_unstage_device(device_server_mock, instr):
    """First test staging of samx, than test logic that raises if device is staged, needs to be unstaged and unstage fails"""

    def callback():
        device_server.device_manager.devices["samx"].obj._staged = Staged.no
        status.set_finished()
        return status

    device_server = device_server_mock
    device_server._run_operations(instr)
    device_server.device_manager.devices["samx"].obj.unstage()
    with mock.patch.object(
        device_server.device_manager.devices["samx"].obj, "unstage"
    ) as mock_unstage:
        assert device_server.device_manager.devices["samx"].obj._staged == Staged.no
        device_server._run_operations(instr)
        assert device_server.device_manager.devices["samx"].obj._staged == Staged.yes
        status = DeviceStatus(device=device_server.device_manager.devices["samx"].obj)
        mock_unstage.return_value = status
        with mock.patch.object(
            status, "wait", side_effect=ophyd_errors.WaitTimeoutError("still unstaging")
        ) as wait:
            with pytest.raises(ValueError, match="failed to finish in 30 seconds"):
                device_server._run_operations(instr)
            assert wait.call_args_list == [mock.call(timeout=10)] * 3
        # Change the mock to return the resolved unstage status + unstage the device
        mock_unstage.side_effect = callback
        device_server._run_operations(instr)


@pytest.mark.parametrize(
    "instr",
    [
        messages.DeviceInstructionMessage(
            device="samx",
            action="unstage",
            parameter={},
            metadata={"stream": "primary", "device_instr_id": "diid"},
        ),
        messages.DeviceInstructionMessage(
            device="test_device", action="kickoff", parameter={}, metadata={}
        ),
    ],
)
def test_get_metadata_for_alarm(device_server_mock, instr):
    device_server = device_server_mock
    metadata = device_server._get_metadata_for_alarm(instr)
    assert metadata == instr.metadata


def test_get_metadata_for_alarm_no_device_manager(device_server_mock):
    device_server = device_server_mock
    instr = messages.DeviceInstructionMessage(
        device="test_device", action="kickoff", parameter={}, metadata={}
    )
    device_server.device_manager = None
    metadata = device_server._get_metadata_for_alarm(instr)
    assert metadata == instr.metadata


def test_get_metadata_for_alarm_no_scan_info(device_server_mock):
    device_server = device_server_mock
    instr = messages.DeviceInstructionMessage(
        device="test_device", action="kickoff", parameter={}, metadata={}
    )
    device_server.device_manager.scan_info = None
    metadata = device_server._get_metadata_for_alarm(instr)
    assert metadata == instr.metadata


def test_get_metadata_for_alarm_no_scan_info_msg(device_server_mock):
    device_server = device_server_mock
    instr = messages.DeviceInstructionMessage(
        device="test_device", action="kickoff", parameter={}, metadata={}
    )
    device_server.device_manager.scan_info.msg = None
    metadata = device_server._get_metadata_for_alarm(instr)
    assert metadata == instr.metadata


@pytest.mark.parametrize(
    "msg",
    [
        messages.ScanStatusMessage(
            scan_id="12345", scan_number=1, status="open", info={}, metadata={}
        ),
        messages.ScanStatusMessage(
            scan_id="12345", scan_number=1, status="open", info={}, metadata={}
        ),
    ],
)
def test_get_metadata_for_alarm_with_scan_info_msg(device_server_mock, msg):
    device_server = device_server_mock
    instr = messages.DeviceInstructionMessage(
        device="test_device", action="kickoff", parameter={}, metadata={"scan_id": "12345"}
    )
    device_server.device_manager.scan_info.msg = msg
    metadata = device_server._get_metadata_for_alarm(instr)
    assert metadata["scan_id"] == msg.scan_id
    assert metadata["scan_number"] == msg.scan_number


@pytest.mark.parametrize("stop_id", ["scan_id-12345", ["scan_id-12345", "other_id"]])
def test_request_handler_ignores_response_if_stop_id(device_server_mock, stop_id):
    """
    Test that if a device instruction message's metadata contains a field that
    matches a stopped request's field, no response is sent when the status
    object is updated with an exception.
    """
    device_server = device_server_mock

    request = messages.DeviceInstructionMessage(
        device="test_device",
        action="complete",
        parameter={},
        metadata={"scan_id": "scan_id-12345", "device_instr_id": "diid"},
    )

    status = StatusBase()
    device_server.requests_handler.add_request(request, num_status_objects=1)
    execution = to_instruction_execution(
        status, DeviceTarget(device_server.device_manager.devices.samx)
    ).bind(request)
    device_server.requests_handler.add_status_object("diid", execution)

    with mock.patch.object(device_server.connector, "send") as send_mock:
        with mock.patch.object(device_server, "stop_devices") as stop_mock:
            device_server.on_stop_devices(
                MessageObject(
                    topic=MessageEndpoints.stop_devices().endpoint,
                    value=messages.VariableMessage(value=None, metadata={"stop_id": stop_id}),
                )
            )
            stop_mock.assert_called()
        status.set_exception(RuntimeError("Test exception"))
        send_mock.assert_not_called()


def test_request_handler_update_instruction_uses_snapshot_when_request_removed(device_server_mock):
    device_server = device_server_mock
    request = messages.DeviceInstructionMessage(
        device="test_device", action="complete", parameter={}, metadata={"device_instr_id": "diid"}
    )

    status = StatusBase()
    status.set_finished()
    request_info = {
        "instr": request,
        "status_objects": [
            to_instruction_execution(
                status, DeviceTarget(device_server.device_manager.devices.samx)
            ).bind(request)
        ],
        "num_status_objects": 1,
        "done": False,
        "is_status_obj": True,
    }

    with mock.patch.object(
        device_server.requests_handler, "get_request", return_value=request_info
    ):
        with mock.patch.object(device_server.requests_handler, "set_finished") as set_finished_mock:
            device_server.requests_handler._storage.clear()
            device_server.requests_handler._update_instruction("diid", is_status_obj=True)

    set_finished_mock.assert_called_once_with("diid", success=True)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("already_done", [False, True])
@pytest.mark.parametrize("success", [False, True])
@pytest.mark.parametrize("kind", [Kind.normal, Kind.config, Kind.normal | Kind.config])
def test_set_completion_publication_contract(
    device_server_mock, connected_connector, already_done, success, kind
):
    """Preserve native-kind cache routing and publication before terminal responses."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    status = DeviceStatus(device.obj)
    instruction = messages.DeviceInstructionMessage(
        device=device.name,
        action="set",
        parameter={"value": 5},
        metadata={
            "device_instr_id": "completion-contract",
            "RID": "completion-rid",
            "response": True,
        },
    )
    signals = {device.name: {"value": 5, "timestamp": 1234}}
    endpoints = []
    if kind == Kind.normal:
        endpoints = [
            MessageEndpoints.device_read(device.name),
            MessageEndpoints.device_readback(device.name),
        ]
    elif kind == Kind.config:
        endpoints = [MessageEndpoints.device_read_configuration(device.name)]
    terminal_snapshots = []
    original_send = connected_connector.send

    def send_and_observe(topic, message, *args, **kwargs):
        if isinstance(message, messages.DeviceInstructionResponse) and message.status in (
            "completed",
            "error",
        ):
            terminal_snapshots.append([connected_connector.get(endpoint) for endpoint in endpoints])
        return original_send(topic, message, *args, **kwargs)

    def finish():
        if success:
            status.set_finished()
        else:
            status.set_exception(RuntimeError("motion failed"))

    if already_done:
        finish()
    try:
        with (
            mock.patch.object(server, "connector", connected_connector),
            mock.patch.object(server.requests_handler, "connector", connected_connector),
            mock.patch.object(connected_connector, "send", side_effect=send_and_observe) as send,
            mock.patch.object(
                connected_connector, "xadd", wraps=connected_connector.xadd
            ) as client_status,
            mock.patch.object(device.obj, "_kind", kind),
            mock.patch.object(device.obj, "set", return_value=status),
            mock.patch.object(device.obj, "read", return_value=signals),
            mock.patch.object(device.obj, "read_configuration", return_value=signals),
        ):
            server.handle_device_instructions(instruction)
            if not already_done:
                assert server.requests_handler.get_request("completion-contract") is not None
                assert terminal_snapshots == []
                finish()

        responses = [call.args[1] for call in send.call_args_list]
        assert [response.status for response in responses] == [
            "running",
            "running",
            "completed" if success else "error",
        ]
        assert responses[-1].result_is_status is True
        assert server.requests_handler.get_request("completion-contract") is None
        assert len(terminal_snapshots) == 1
        for endpoint in (
            MessageEndpoints.device_read(device.name),
            MessageEndpoints.device_readback(device.name),
            MessageEndpoints.device_read_configuration(device.name),
        ):
            if endpoint not in endpoints:
                assert connected_connector.get(endpoint) is None
        for snapshot in terminal_snapshots[0]:
            assert snapshot.signals == signals
            assert snapshot.metadata == instruction.metadata
        client_status.assert_called_once()
        assert client_status.call_args.args[0] == MessageEndpoints.device_req_status(
            "completion-rid"
        )
        assert client_status.call_args.args[1]["data"].success is success
        assert client_status.call_args.kwargs["expire"] == 3600
    finally:
        if not status.done:
            status.set_finished()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("action", ["stage", "unstage"])
@pytest.mark.parametrize("result", [None, []])
def test_synchronous_stage_results_complete_without_status_objects(
    device_server_mock, action, result
):
    """None and list staging results remain immediate successes without status registration."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    instruction = messages.DeviceInstructionMessage(
        device=device.name,
        action=action,
        parameter={},
        metadata={"device_instr_id": "stage-contract", "RID": "stage-rid"},
    )
    with (
        mock.patch.object(device.obj, "_staged", Staged.no if action == "stage" else Staged.yes),
        mock.patch.object(device.obj, action, return_value=result),
        mock.patch.object(server.connector, "send") as send,
        mock.patch.object(server.connector, "set") as set_message,
    ):
        server.handle_device_instructions(instruction)

    responses = [call.args[1] for call in send.call_args_list]
    assert [response.status for response in responses] == ["running", "completed"]
    assert responses[-1].result_is_status is None
    assert server.requests_handler.get_request("stage-contract") is None
    assert not any(
        call.args[0] == MessageEndpoints.device_staged(device.name)
        for call in set_message.call_args_list
    )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize(
    "action", ["set", "trigger", "kickoff", "complete", "pre_scan", "stage", "unstage"]
)
@pytest.mark.parametrize("already_done", [False, True])
def test_instruction_operations_keep_context_off_native_status(
    device_server_mock, action, already_done
):
    """All migrated operation paths keep BEC state on their own completion handle."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    native = DeviceStatus(device.obj)
    instruction = messages.DeviceInstructionMessage(
        device=device.name,
        action=action,
        parameter={"value": 5},
        metadata={"device_instr_id": "neutral-status", "RID": "neutral-rid"},
    )
    if already_done:
        native.set_finished()

    def invoke(*_args, **_kwargs):
        if action == "stage":
            device.obj._staged = Staged.yes
        elif action == "unstage":
            device.obj._staged = Staged.no
        return native

    with (
        mock.patch.object(device.obj, "_staged", Staged.yes if action == "unstage" else Staged.no),
        mock.patch.object(device.obj, action, side_effect=invoke, create=True) as operation,
        mock.patch.object(device.obj, "configure", create=True),
        mock.patch.object(server.connector, "send") as send,
    ):
        server.handle_device_instructions(instruction)
        operation.assert_called_once()
        if not already_done:
            request = server.requests_handler.get_request("neutral-status")
            assert len(request["status_objects"]) == 1
            tracked = request["status_objects"][0]
            assert isinstance(tracked, InstructionExecution)
            assert tracked.target.device is device
            assert tracked.instruction is instruction
            native.set_finished()
        assert server.requests_handler.get_request("neutral-status") is None
        responses = [call.args[1] for call in send.call_args_list]
        assert [response.status for response in responses] == ["running", "running", "completed"]
        assert responses[-1].result_is_status is True
    assert not {"instruction", "obj_ref", "sub_id", "status"}.intersection(native.__dict__)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("failure", [False, True])
def test_metadata_kickoff_preserves_response_sequence(device_server_mock, failure):
    """Publish only the terminal response on a successful metadata-style kickoff."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    native = DeviceStatus(device.obj)
    calls = []

    def kickoff(self, metadata, **configure):
        assert server.requests_handler.get_request("metadata-kickoff") is None
        send.assert_not_called()
        calls.append((metadata, configure))
        if failure:
            raise RuntimeError("metadata kickoff failed")
        return native

    instruction = messages.DeviceInstructionMessage(
        device=device.name,
        action="kickoff",
        parameter={"configure": {"points": 3}},
        metadata={"device_instr_id": "metadata-kickoff", "RID": "metadata-rid"},
    )
    with (
        mock.patch.object(device.obj, "kickoff", MethodType(kickoff, device.obj), create=True),
        mock.patch.object(server.connector, "send") as send,
        mock.patch.object(device.obj, "configure") as configure,
    ):
        server.handle_device_instructions(instruction)
    assert calls == [(instruction.metadata, {"points": 3})]
    configure.assert_not_called()
    responses = [call.args[1] for call in send.call_args_list]
    assert [response.status for response in responses] == (
        ["running", "error"] if failure else ["completed"]
    )
    assert responses[-1].result_is_status is None
    if failure:
        assert responses[-1].error_info.exception_type == "RuntimeError"
    assert server.requests_handler.get_request("metadata-kickoff") is None
    assert "instruction" not in native.__dict__
    native.set_finished()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize(
    "terminal", ["success", "failure", "clear", "invalid_status", "tracking_error", "cleanup_error"]
)
def test_nested_set_owns_zero_token_subscription(device_server_mock, terminal):
    """Temporary subscriptions close once on every terminal path, including token zero."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    signal = device.obj.setpoint
    native = DeviceStatus(device.obj)
    instruction = messages.DeviceInstructionMessage(
        device="samx.setpoint",
        action="set",
        parameter={"value": 4},
        metadata={"device_instr_id": "nested-owned", "RID": "nested-rid", "response": True},
    )
    with (
        mock.patch.object(signal, "subscribe", return_value=0),
        mock.patch.object(
            signal,
            "unsubscribe",
            side_effect=RuntimeError("cleanup failed") if terminal == "cleanup_error" else None,
        ) as unsubscribe,
        mock.patch.object(
            signal, "set", return_value=None if terminal == "invalid_status" else native
        ),
        mock.patch.object(server.connector, "send") as send,
        mock.patch.object(server.connector, "xadd") as client_status,
    ):
        if terminal == "tracking_error":
            with mock.patch.object(
                server.requests_handler,
                "add_status_object",
                side_effect=RuntimeError("tracking failed"),
            ):
                server.handle_device_instructions(instruction)
        elif terminal == "invalid_status":
            server.handle_device_instructions(instruction)
        else:
            server.handle_device_instructions(instruction)
            assert not native.done
            if terminal == "clear":
                server.requests_handler.clear()
                native.set_finished()
            elif terminal == "failure":
                native.set_exception(RuntimeError("nested set failed"))
            else:
                native.set_finished()
        unsubscribe.assert_called_once_with(0)
        assert server.requests_handler.get_request("nested-owned") is None
        assert not {"instruction", "obj_ref", "sub_id"}.intersection(native.__dict__)
        if terminal != "clear":
            responses = [call.args[1] for call in send.call_args_list]
            assert responses[-1].status == ("completed" if terminal == "success" else "error")
        if terminal in ("success", "failure", "cleanup_error"):
            response = client_status.call_args.args[1]["data"]
            assert response.device == "samx.setpoint"
            assert response.success is (terminal == "success")
    if not native.done:
        native.set_finished()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("already_done", [False, True])
def test_foreign_loop_completion_refreshes_before_final_response(
    device_server_mock, connected_connector, already_done
):
    """Async-loop completion callbacks must neither block that loop nor finish before refresh."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    provider = server.device_manager.device_provider_manager
    operation, reading = Future(), Future()
    if already_done:
        operation.set_result(None)
    loop = asyncio.new_event_loop()
    ready, read_started, terminal = threading.Event(), threading.Event(), threading.Event()
    loop_thread_id, read_threads = [], []
    signals = {"samx": {"value": 9, "timestamp": 1}}

    def run_loop():
        asyncio.set_event_loop(loop)
        loop_thread_id.append(threading.get_ident())
        ready.set()
        loop.run_forever()

    owner = threading.Thread(target=run_loop, name="TestBackendLoop")
    owner.start()
    assert ready.wait(3)
    instruction = messages.DeviceInstructionMessage(
        device=device.name,
        action="set",
        parameter={"value": 9},
        metadata={"device_instr_id": "foreign-loop", "RID": "foreign-rid", "response": True},
    )
    original_send = connected_connector.send
    original_remove = server.requests_handler.remove_request

    def send(topic, message, *args, **kwargs):
        return original_send(topic, message, *args, **kwargs)

    def remove_request(instr_id):
        original_remove(instr_id)
        if instr_id == "foreign-loop":
            terminal.set()

    def read(*_args, **_kwargs):
        read_threads.append(threading.get_ident())
        read_started.set()
        return reading

    try:
        with (
            mock.patch.object(server, "connector", connected_connector),
            mock.patch.object(server.requests_handler, "connector", connected_connector),
            mock.patch.object(connected_connector, "send", side_effect=send),
            mock.patch.object(
                server.requests_handler, "remove_request", side_effect=remove_request
            ),
            mock.patch.object(
                provider, "set", return_value=InstructionExecution(operation, DeviceTarget(device))
            ),
            mock.patch.object(provider, "get_kind", return_value=DeviceKind.READOUT),
            mock.patch.object(provider, "read", side_effect=read),
        ):
            server.handle_device_instructions(instruction)
            if not already_done:
                loop.call_soon_threadsafe(operation.set_result, None)
            assert read_started.wait(3)
            tracked = server.requests_handler.get_request("foreign-loop")["status_objects"][0]
            assert operation.done()
            assert not tracked.done
            assert not terminal.is_set()
            loop.call_soon_threadsafe(reading.set_result, signals)
            assert terminal.wait(3)
            assert server.requests_handler.get_request("foreign-loop") is None
            assert (
                connected_connector.get(MessageEndpoints.device_readback(device.name)).signals
                == signals
            )
            assert connected_connector.xrange(
                MessageEndpoints.device_req_status("foreign-rid"), "-", "+"
            )[-1]["data"].success
            assert read_threads and all(thread != loop_thread_id[0] for thread in read_threads)
    finally:
        if not operation.done():
            operation.set_result(None)
        if not reading.done():
            reading.set_result(signals)
        loop.call_soon_threadsafe(loop.stop)
        owner.join(timeout=3)
        assert not owner.is_alive()
        loop.close()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cancelled_backend_operation_finishes_instruction(device_server_mock):
    """Execution cancellation produces a terminal error without requesting hardware stop."""
    server = device_server_mock
    completion = Future()
    completion.cancel()
    terminal = threading.Event()
    provider = server.device_manager.device_provider_manager
    instruction = messages.DeviceInstructionMessage(
        device="samx",
        action="trigger",
        parameter={},
        metadata={
            "device_instr_id": "cancelled-operation",
            "RID": "cancelled-rid",
            "response": True,
        },
    )
    original_remove = server.requests_handler.remove_request

    def remove_request(instr_id):
        original_remove(instr_id)
        terminal.set()

    with (
        mock.patch.object(
            provider,
            "trigger",
            return_value=InstructionExecution(
                completion, DeviceTarget(server.device_manager.devices.samx)
            ),
        ),
        mock.patch.object(provider, "stop_device") as stop,
        mock.patch.object(server.requests_handler, "remove_request", side_effect=remove_request),
        mock.patch.object(server.connector, "send") as send,
    ):
        server.handle_device_instructions(instruction)
        assert terminal.wait(3)
        responses = [call.args[1] for call in send.call_args_list]
        assert responses[-1].status == "error"
        assert responses[-1].error_info.exception_type == "CancelledError"
        assert server.requests_handler.get_request("cancelled-operation") is None
        stop.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_set_failure_registration_keeps_original_client_error(device_server_mock):
    """A status terminal during failure cleanup must publish its original hardware error."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    entered, released = threading.Event(), threading.Event()
    failure = RuntimeError("original hardware failure")

    class PausingFailure(DeviceStatus):
        def _handle_failure(self):
            entered.set()
            assert released.wait(5)

    native = PausingFailure(device.obj)
    provider = server.device_manager.device_provider_manager
    original_set = provider.set
    worker = threading.Thread(target=native.set_exception, args=(failure,))

    def set_operation(*args, **kwargs):
        result = original_set(*args, **kwargs)
        worker.start()
        assert entered.wait(2)
        return result

    instruction = messages.DeviceInstructionMessage(
        device=device.name,
        action="set",
        parameter={"value": 3},
        metadata={"device_instr_id": "failure-window", "RID": "failure-rid", "response": True},
    )
    try:
        with (
            mock.patch.object(device.obj, "set", return_value=native),
            mock.patch.object(provider, "set", side_effect=set_operation),
            mock.patch.object(provider, "get_kind", return_value=DeviceKind.OMITTED),
            mock.patch.object(server.connector, "send") as send,
            mock.patch.object(server.connector, "xadd") as publish_status,
        ):
            server.handle_device_instructions(instruction)
        responses = [call.args[1] for call in send.call_args_list]
        assert [response.status for response in responses] == ["running", "running", "error"]
        assert responses[-1].error_info.exception_type == "RuntimeError"
        assert "original hardware failure" in responses[-1].error_info.error_message
        publish_status.assert_called_once()
        client = publish_status.call_args.args[1]["data"]
        assert client.success is False
        assert client.metadata["error_info"].model_dump(exclude={"id"}) == responses[
            -1
        ].error_info.model_dump(exclude={"id"})
        assert server.requests_handler.get_request("failure-window") is None
    finally:
        released.set()
        if worker.ident is not None:
            worker.join(timeout=2)
            assert not worker.is_alive()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_temporary_ophyd_subscription_keeps_initial_refresh_inline(device_server_mock):
    """Initial readback and updates inside set retain the callback-owned lock."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    signal = device.obj.setpoint
    # run=True replays a cached native notification; prime it before observing reads.
    signal.put(signal.get())
    native = DeviceStatus(device.obj)
    device_lock = threading.RLock()
    caller = threading.get_ident()
    reads = []
    original_read = device.obj.read
    original_subscribe = signal.subscribe
    callbacks = []

    def read():
        if not device_lock.acquire(timeout=0.2):
            raise RuntimeError("Readback crossed a callback-owned device lock")
        try:
            reads.append(threading.get_ident())
            return original_read()
        finally:
            device_lock.release()

    def subscribe(callback, *, run):
        callbacks.append(callback)
        return original_subscribe(callback, run=run)

    def set_value(_value):
        assert reads == [caller]
        callbacks[0]()
        assert reads == [caller, caller]
        return native

    instruction = messages.DeviceInstructionMessage(
        device="samx.setpoint",
        action="set",
        parameter={"value": 4},
        metadata={"device_instr_id": "inline-subscription", "RID": "inline-rid"},
    )
    try:
        with (
            mock.patch.object(device.obj, "read", side_effect=read),
            mock.patch.object(signal, "set", side_effect=set_value),
            mock.patch.object(signal, "subscribe", side_effect=subscribe),
            mock.patch.object(server.connector, "send") as send,
        ):
            with device_lock:
                server.handle_device_instructions(instruction)
                assert reads == [caller, caller]
                native.set_finished()
        assert all(thread == caller for thread in reads)
        assert [call.args[1].status for call in send.call_args_list][
            -1
        ] == "completed", send.call_args.args[1].error_info
        assert server.requests_handler.get_request("inline-subscription") is None
    finally:
        if not native.done:
            native.set_finished()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_foreign_subscription_notification_queues_without_blocking_its_loop(
    device_server_mock, connected_connector
):
    """Actual temporary-subscription ingress leaves its owning loop free to acquire snapshots."""
    server = device_server_mock
    manager = server.device_manager

    class AsyncRoot:
        """An opaque backend object with no Ophyd connected/root/signal attributes."""

    device = DSDevice(
        name="async_subscription",
        obj=AsyncRoot(),
        config={**manager.devices.samx._config, "name": "async_subscription"},
        parent=manager,
    )
    manager.devices._add_device(device.name, device)
    provider = manager.device_provider_manager
    loop = asyncio.new_event_loop()
    ready, heartbeat, published, terminal = (threading.Event() for _ in range(4))
    setup, operation = Future(), Future()
    pending_reads = []
    caller_threads = []
    loop_threads = []
    signals = {device.name: {"value": 17, "timestamp": 1}}
    instruction = messages.DeviceInstructionMessage(
        device=f"{device.name}.setpoint",
        action="set",
        parameter={"value": 17},
        metadata={"device_instr_id": "subscription-loop", "RID": "subscription-rid"},
    )

    def run_loop():
        asyncio.set_event_loop(loop)
        loop_threads.append(threading.get_ident())
        ready.set()
        loop.run_forever()

    owner = threading.Thread(target=run_loop, name="TestSubscriptionLoop")
    owner.start()
    assert ready.wait(3)

    def set_on_backend(target, _value, *, refresh):
        def install():
            refresh(target)
            loop.call_soon(heartbeat.set)
            setup.set_result(None)

        loop.call_soon_threadsafe(install)
        setup.result(timeout=3)
        return InstructionExecution(operation, DeviceTarget(device))

    def read(target):
        assert target == DeviceTarget(device)
        caller_threads.append(threading.get_ident())
        future = Future()
        pending_reads.append(future)
        loop.call_soon_threadsafe(lambda: None if future.done() else future.set_result(signals))
        return future

    pipe = connected_connector.pipeline()
    original_execute = pipe.execute
    original_remove = server.requests_handler.remove_request

    def execute():
        result = original_execute()
        if connected_connector.get(MessageEndpoints.device_readback(device.name)) is not None:
            published.set()
        return result

    def remove_request(instr_id):
        original_remove(instr_id)
        if instr_id == "subscription-loop":
            terminal.set()

    backend = SimpleNamespace(
        supports_device_class=lambda cls: cls is AsyncRoot,
        is_connected=lambda _device: True,
        read=read,
        disconnect_device=lambda _device: None,
        stop_runtime=nullcontext,
    )
    provider.providers["async_test"] = backend
    invocation = None
    try:
        with (
            mock.patch.object(manager, "connector", connected_connector),
            mock.patch.object(connected_connector, "pipeline", return_value=pipe),
            mock.patch.object(pipe, "execute", side_effect=execute),
            mock.patch.object(provider, "set", side_effect=set_on_backend),
            mock.patch.object(provider, "get_kind", return_value=DeviceKind.OMITTED),
            mock.patch.object(
                server.requests_handler, "remove_request", side_effect=remove_request
            ),
        ):
            invocation = server.executor.submit(server.handle_device_instructions, instruction)
            try:
                assert heartbeat.wait(
                    3
                ), "Subscription notification must leave its backend loop free"
                invocation.result(timeout=3)
                assert published.wait(3)
                assert (
                    connected_connector.get(MessageEndpoints.device_readback(device.name)).signals
                    == signals
                )
                assert caller_threads and all(
                    thread != loop_threads[0] for thread in caller_threads
                )
            finally:
                # Also release the old deadlocking implementation when this regression fails.
                for future in pending_reads:
                    if not future.done():
                        future.set_result(signals)
                if not setup.done():
                    setup.set_result(None)
                if not operation.done():
                    operation.set_result(None)
                if invocation is not None:
                    invocation.result(timeout=3)
                assert terminal.wait(3)
                manager._shutdown_event.set()
                manager._auto_monitor_update_thread.join(timeout=3)
                assert not manager._auto_monitor_update_thread.is_alive()
    finally:
        loop.call_soon_threadsafe(loop.stop)
        owner.join(timeout=3)
        assert not owner.is_alive()
        loop.close()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("kind", [Kind.normal, Kind.hinted, Kind.config, Kind.omitted])
def test_set_property_alias_preserves_success_and_cache_routing(device_server_mock, kind):
    """A property alias must complete successfully after the underlying signal is set."""

    class ImmediateSignal(Signal):
        def set(self, value, **kwargs):
            self.put(value)
            status = DeviceStatus(self)
            status.set_finished()
            return status

    class AliasedDevice(Device):
        setpoint = Cpt(ImmediateSignal, value=0)

        @property
        def alias(self):
            return self.setpoint

    server = device_server_mock
    obj = AliasedDevice(name="aliased")
    obj.setpoint.kind = kind
    wrapper = DSDevice(
        name=obj.name,
        obj=obj,
        config={"enabled": True, "deviceClass": "AliasedDevice", "readoutPriority": "monitored"},
        parent=server.device_manager,
    )
    server.device_manager.devices._add_device(obj.name, wrapper)
    instr = messages.DeviceInstructionMessage(
        device="aliased.alias",
        action="set",
        parameter={"value": 5},
        metadata={"device_instr_id": "alias-set", "RID": "alias-request", "response": True},
    )
    with (
        mock.patch.object(server.requests_handler, "send_device_instruction_response") as respond,
        mock.patch.object(server.connector, "set_and_publish") as publish,
        mock.patch.object(server.connector, "xadd") as xadd,
    ):
        server.handle_device_instructions(instr)

    assert obj.setpoint.get() == 5
    assert respond.call_args.args == ("alias-set", True)
    assert respond.call_args.kwargs["done"] is True
    assert respond.call_args.kwargs["error_info"] is None
    responses = [call.args[1]["data"] for call in xadd.call_args_list]
    assert len(responses) == 1
    assert responses[0].success is True
    assert responses[0].device == "aliased.setpoint"
    endpoints = [call.args[0] for call in publish.call_args_list]
    if kind == Kind.config:
        assert MessageEndpoints.device_read_configuration(obj.name) in endpoints
    elif kind in (Kind.normal, Kind.hinted):
        assert MessageEndpoints.device_read(obj.name) in endpoints
    else:
        assert MessageEndpoints.device_read(obj.name) not in endpoints


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("retarget", [False, True])
@pytest.mark.parametrize("change_kind", [False, True])
def test_pending_alias_set_keeps_original_target_and_uses_completion_kind(
    device_server_mock, retarget, change_kind
):
    """Retargeting aliases must not replace the signal whose operation completed."""

    class PendingSignal(Signal):
        def set(self, value, **kwargs):
            self.put(value)
            self.operation = DeviceStatus(self)
            return self.operation

    class AliasedDevice(Device):
        reading = Cpt(PendingSignal, value=0, kind=Kind.normal)
        configuration = Cpt(PendingSignal, value=0, kind=Kind.config)
        use_configuration = False

        @property
        def alias(self):
            return self.configuration if self.use_configuration else self.reading

    server = device_server_mock
    obj = AliasedDevice(name="pending_alias")
    wrapper = DSDevice(
        obj.name,
        obj,
        config={"enabled": True, "deviceClass": "AliasedDevice", "readoutPriority": "monitored"},
        parent=server.device_manager,
    )
    server.device_manager.devices._add_device(obj.name, wrapper)
    instr = messages.DeviceInstructionMessage(
        device="pending_alias.alias",
        action="set",
        parameter={"value": 5},
        metadata={"device_instr_id": "pending-alias", "RID": "pending-alias-request"},
    )
    finished = threading.Event()

    def respond(*args, **kwargs):
        if kwargs.get("done"):
            finished.set()

    with (
        mock.patch.object(
            server.requests_handler, "send_device_instruction_response", side_effect=respond
        ) as response,
        mock.patch.object(server.connector, "set_and_publish") as publish,
    ):
        server.handle_device_instructions(instr)
        assert obj.reading.get() == 5
        assert obj.configuration.get() == 0
        if retarget:
            obj.use_configuration = True
        if change_kind:
            obj.reading.kind = Kind.config
        publish.reset_mock()
        obj.reading.operation.set_finished()
        assert finished.wait(2), "The set instruction must resolve after its native operation"

    assert response.call_args.args == ("pending-alias", True)
    assert response.call_args.kwargs["error_info"] is None
    endpoints = [call.args[0] for call in publish.call_args_list]
    expected = (
        [MessageEndpoints.device_read_configuration(obj.name)]
        if change_kind
        else [MessageEndpoints.device_read(obj.name), MessageEndpoints.device_readback(obj.name)]
    )
    assert endpoints == expected


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("has_events", [False, True])
def test_alias_set_uses_same_target_for_subscription_and_invocation(device_server_mock, has_events):
    """Resolve a dynamic alias once, even if it has no readback subscription to install."""

    class ImmediateSignal(Signal):
        @property
        def event_types(self):
            return super().event_types if has_events else ()

        def set(self, value, **kwargs):
            self.put(value)
            status = DeviceStatus(self)
            status.set_finished()
            return status

    class AlternatingDevice(Device):
        first = Cpt(ImmediateSignal, value=0, kind=Kind.normal)
        second = Cpt(ImmediateSignal, value=0, kind=Kind.config)
        accesses = 0

        @property
        def alias(self):
            self.accesses += 1
            return self.first if self.accesses == 1 else self.second

    server = device_server_mock
    obj = AlternatingDevice(name="alternating_alias")
    wrapper = DSDevice(
        obj.name,
        obj,
        config={
            "enabled": True,
            "deviceClass": "AlternatingDevice",
            "readoutPriority": "monitored",
        },
        parent=server.device_manager,
    )
    server.device_manager.devices._add_device(obj.name, wrapper)
    instr = messages.DeviceInstructionMessage(
        device="alternating_alias.alias",
        action="set",
        parameter={"value": 5},
        metadata={"device_instr_id": "alternating-alias", "RID": "alternating-alias-request"},
    )
    with mock.patch.object(server.requests_handler, "send_device_instruction_response") as response:
        server.handle_device_instructions(instr)
    assert response.call_args.args == ("alternating-alias", True)
    assert response.call_args.kwargs["error_info"] is None
    assert obj.accesses == 1
    assert obj.first.get() == 5
    assert obj.second.get() == 0


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("failure", [None, "configure", "kickoff"])
def test_unified_kickoff_preserves_registration_and_native_call_order(device_server_mock, failure):
    """One backend invocation registers pending work before native setup, including failures."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    native = DeviceStatus(device.obj)
    instruction = messages.DeviceInstructionMessage(
        device=device.name,
        action="kickoff",
        parameter={"configure": {"points": 3}},
        metadata={"device_instr_id": "unified-kickoff", "RID": "unified-rid"},
    )
    events = []

    def send(endpoint, response, **kwargs):
        if endpoint == MessageEndpoints.device_instructions_response():
            events.append(response.status)

    def configure(config):
        assert config == {"points": 3}
        assert server.requests_handler.get_request("unified-kickoff")["num_status_objects"] == 1
        events.append("configure")
        if failure == "configure":
            raise RuntimeError("configure failed")

    def kickoff(self):
        events.append("kickoff")
        if failure == "kickoff":
            raise RuntimeError("kickoff failed")
        return native

    with (
        mock.patch.object(server.connector, "send", side_effect=send),
        mock.patch.object(device.obj, "configure", side_effect=configure),
        mock.patch.object(device.obj, "kickoff", MethodType(kickoff, device.obj), create=True),
    ):
        server.handle_device_instructions(instruction)
        if failure is None:
            assert events == ["running", "configure", "kickoff", "running"]
            native.set_finished()
            assert events[-1] == "completed"
        else:
            expected = ["running", "configure"]
            if failure == "kickoff":
                expected.append("kickoff")
            assert events == [*expected, "error"]
        assert server.requests_handler.get_request("unified-kickoff") is None
    if not native.done:
        native.set_finished()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("metadata_signature", [False, True])
@pytest.mark.parametrize("configure", ["omitted", None])
def test_provider_kickoff_configuration_is_optional(
    device_server_mock, metadata_signature, configure
):
    """Omitted and explicit None configuration use the same native empty-config convention."""
    server = device_server_mock
    device = server.device_manager.devices.samx
    native = DeviceStatus(device.obj)
    native.set_finished()
    metadata = {"scan_id": "scan", "RID": "request"}
    calls = []

    def legacy_kickoff(self):
        calls.append("legacy")
        return native

    def metadata_kickoff(self, metadata, **configure):
        calls.append((metadata, configure))
        return native

    method = metadata_kickoff if metadata_signature else legacy_kickoff
    options = {} if configure == "omitted" else {"configure": configure}
    with (
        mock.patch.object(device.obj, "kickoff", MethodType(method, device.obj), create=True),
        mock.patch.object(device.obj, "configure") as native_configure,
    ):
        result = server.device_manager.device_provider_manager.kickoff(
            server._target(device.name), metadata=metadata, **options
        )
    if metadata_signature:
        assert result is None
        assert calls == [(metadata, {})]
        native_configure.assert_not_called()
    else:
        assert result.completion.done()
        assert calls == ["legacy"]
        native_configure.assert_called_once_with({})


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("already_done", [False, True])
@pytest.mark.parametrize("success", [False, True])
@pytest.mark.parametrize("func", ["set", "readback.set", "trigger"])
@pytest.mark.parametrize("kind", [Kind.normal, Kind.hinted, Kind.config, Kind.omitted])
def test_rpc_status_uses_execution_and_preserves_callback_order(
    device_server_mock, already_done, success, func, kind
):
    """RPC retains native callback/cache ordering and wire results without mutating statuses."""
    server = device_server_mock
    wrapper = server.device_manager.devices.samx
    instruction = messages.DeviceInstructionMessage(
        device=wrapper.name,
        action="rpc",
        parameter={"func": func, "args": [5] if func.endswith("set") else [], "rpc_id": "rpc"},
        metadata={"device_instr_id": "rpc-status", "RID": "request", "response": True},
    )
    provider = server.provider_manager.get_provider(type(wrapper.obj))
    target_obj, method = provider.rpc._get_rpc_components(wrapper, instruction)
    native = DeviceStatus(Signal(name="delegated", kind=Kind.config))
    events = []
    executions = []
    original_register = server.requests_handler.add_status_object
    original_callback = server.status_callback
    original_cache = OphydRPC._update_cache

    def register(instr_id, execution):
        assert isinstance(execution, InstructionExecution)
        assert execution.target.device is wrapper
        assert execution.target.path == target_obj.dotted_name
        executions.append(execution)
        events.append("register")
        original_register(instr_id, execution)

    def callback(execution):
        events.append("completion")
        original_callback(execution)

    def cache(*args):
        events.append("rpc-cache")
        return original_cache(*args)

    def finish():
        if success:
            native.set_finished()
        else:
            native.set_exception(RuntimeError("RPC hardware failure"))

    if already_done:
        finish()
    with (
        mock.patch.object(target_obj, method.__name__, return_value=native),
        mock.patch.object(target_obj, "_kind", kind),
        mock.patch.object(provider, "adapt_rpc_status", wraps=provider.adapt_rpc_status) as adapt,
        mock.patch.object(server.requests_handler, "add_status_object", side_effect=register),
        mock.patch.object(server, "status_callback", side_effect=callback),
        mock.patch.object(OphydRPC, "_update_cache", autospec=True, side_effect=cache),
        mock.patch.object(server.connector, "set") as write,
        mock.patch.object(server.connector, "send") as send,
        mock.patch.object(server.connector, "xadd") as client_response,
    ):
        server.rpc_handler.run_rpc(instruction)
        adapt.assert_called_once_with(target_obj, native)
        assert len(executions) == 1
        execution = executions[0]
        assert execution.callback_inline is True
        rpc_message = next(
            call.args[1]
            for call in write.call_args_list
            if call.args[0] == MessageEndpoints.device_rpc("rpc")
        )
        assert rpc_message.success is True
        assert rpc_message.return_val == {
            "type": "status",
            "RID": "request",
            "success": already_done and success,
            "timeout": native.timeout,
            "done": already_done,
            "settle_time": native.settle_time,
        }
        if not already_done:
            assert events == ["register"]
            assert server.requests_handler.get_request("rpc-status") is not None
            client_response.assert_not_called()
            finish()

        if func.endswith("set"):
            expected = (
                ["rpc-cache", "register", "completion"]
                if already_done
                else ["register", "rpc-cache", "completion"]
            )
        else:
            expected = ["register", "completion"]
        assert events == expected
        assert server.requests_handler.get_request("rpc-status") is None
        responses = [call.args[1] for call in send.call_args_list]
        assert [response.status for response in responses] == [
            "running",
            "running",
            "completed" if success else "error",
        ]
        client_response.assert_called_once()
        response = client_response.call_args.args[1]["data"]
        assert response.device == execution.target.name
        assert response.success is success
        if not success:
            assert response.metadata["error_info"].device == "delegated"
            assert "RPC hardware failure" in response.metadata["error_info"].error_message
    assert not {"instruction", "obj_ref", "sub_id"}.intersection(native.__dict__)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("root_registration", ["registered", "unregistered", "different-object"])
@pytest.mark.parametrize("already_done", [False, True])
def test_rpc_configuration_alias_refreshes_actual_native_root(
    device_server_mock, root_registration, already_done
):
    """An RPC alias refreshes its original native root, even without a registered wrapper."""
    server = device_server_mock

    class Hardware(Device):
        configuration = Cpt(Signal, value=5, kind=Kind.config)

    hardware = Hardware(name="hardware")
    replacement = Hardware(name="hardware")

    class Aliased(Device):
        @property
        def foreign(self):
            return hardware.configuration

    alias = Aliased(name="alias")
    objects = [alias]
    if root_registration == "registered":
        objects.append(hardware)
    elif root_registration == "different-object":
        objects.append(replacement)
    for obj in objects:
        server.device_manager.devices._add_device(
            obj.name,
            DSDevice(
                obj.name,
                obj,
                config={"enabled": True, "deviceClass": "Device", "readoutPriority": "monitored"},
                parent=server.device_manager,
            ),
        )
    native = DeviceStatus(hardware.configuration)
    if already_done:
        native.set_finished()
    instruction = messages.DeviceInstructionMessage(
        device=alias.name,
        action="rpc",
        parameter={"func": "foreign.set", "args": [5], "rpc_id": "foreign-rpc"},
        metadata={"device_instr_id": "foreign-status", "RID": "request", "response": True},
    )
    executions = []
    register = server.requests_handler.add_status_object

    def track(instr_id, execution):
        executions.append(execution)
        register(instr_id, execution)

    with (
        mock.patch.object(hardware.configuration, "set", return_value=native),
        mock.patch.object(
            hardware, "read_configuration", wraps=hardware.read_configuration
        ) as read,
        mock.patch.object(replacement, "read_configuration") as read_replacement,
        mock.patch.object(server.requests_handler, "add_status_object", side_effect=track),
        mock.patch.object(server.connector, "set_and_publish") as publish,
        mock.patch.object(server.connector, "xadd") as response,
    ):
        server.rpc_handler.run_rpc(instruction)
        if not already_done:
            read.assert_not_called()
            native.set_finished()
        read.assert_called_once_with()
        read_replacement.assert_not_called()
        configurations = [
            call.args[1]
            for call in publish.call_args_list
            if call.args[0] == MessageEndpoints.device_read_configuration(hardware.name)
        ]
        assert len(configurations) == 1
        assert configurations[0].signals == hardware.read_configuration()
        assert configurations[0].metadata == instruction.metadata
        response.assert_called_once()
        assert response.call_args.args[1]["data"].device == "hardware.configuration"
        assert response.call_args.args[1]["data"].success is True
    assert executions[0].target.device.obj is hardware
    assert executions[0].target.path == "configuration"
    assert executions[0].instruction is instruction
    if root_registration == "registered":
        assert executions[0].target.device is server.device_manager.devices.hardware
    elif root_registration == "unregistered":
        assert "hardware" not in server.device_manager.devices
    else:
        assert server.device_manager.devices.hardware.obj is replacement
    assert not {"instruction", "obj_ref", "sub_id"}.intersection(native.__dict__)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("kind", [Kind.normal, Kind.hinted, Kind.config, Kind.omitted])
@pytest.mark.parametrize("has_events", [False, True])
@pytest.mark.parametrize("already_done", [False, True])
@pytest.mark.parametrize("retarget", [False, True])
def test_set_cross_root_alias_retains_acquisition_target(
    device_server_mock, kind, has_events, already_done, retarget
):
    """Invocation, initial refresh and completion retain one resolved native root."""
    server = device_server_mock
    manager = server.device_manager

    class BoundSignal(Signal):
        @property
        def event_types(self):
            return super().event_types if has_events else ()

        def set(self, value, **kwargs):
            self.put(value)
            self.operation = DeviceStatus(self)
            if already_done:
                self.operation.set_finished()
            return self.operation

    class Hardware(Device):
        value = Cpt(BoundSignal, value=0)

    hardware = Hardware(name="hardware")
    other = Hardware(name="other")
    hardware.value.kind = kind
    hardware.value.put(0)  # Seed the cached notification for subscribe(run=True).
    lookups = []

    class Aliased(Device):
        selected = hardware.value

        @property
        def foreign(self):
            lookups.append(self.selected)
            return self.selected

    alias = Aliased(name="alias")
    for obj in (alias, hardware, other):
        manager.devices._add_device(
            obj.name,
            DSDevice(
                obj.name,
                obj,
                config={"enabled": True, "deviceClass": "Device", "readoutPriority": "monitored"},
                parent=manager,
            ),
        )
    instruction = messages.DeviceInstructionMessage(
        device="alias.foreign",
        action="set",
        parameter={"value": 9},
        metadata={"device_instr_id": "cross-root-set", "RID": "request", "response": True},
    )
    with (
        mock.patch.object(manager, "_publish_refresh", wraps=manager._publish_refresh) as refresh,
        mock.patch.object(server, "_read_device") as read_instruction,
        mock.patch.object(
            server.requests_handler,
            "add_status_object",
            wraps=server.requests_handler.add_status_object,
        ) as register,
        mock.patch.object(server.connector, "set_and_publish") as publish,
        mock.patch.object(server.connector, "xadd") as response,
        mock.patch.object(
            hardware.value, "unsubscribe", wraps=hardware.value.unsubscribe
        ) as unsubscribe,
    ):
        server.handle_device_instructions(instruction)
        execution = register.call_args.args[1]
        assert execution.target.device is manager.devices.hardware
        assert execution.target.path == "value"
        assert execution.instruction is instruction
        assert hardware.value.get() == 9
        assert other.value.get() == 0
        assert lookups == [hardware.value]
        alias_refreshes = [
            call
            for call in refresh.call_args_list
            if call.args[0].name in {"hardware", "alias", "other"}
        ]
        if has_events:
            # Both the initial notification and the value change use the actual root.
            assert len(alias_refreshes) == 2
            assert all(call.args[0] is manager.devices.hardware for call in alias_refreshes)
        else:
            assert alias_refreshes == []
        if retarget:
            alias.selected = other.value
        if not already_done:
            assert server.requests_handler.get_request("cross-root-set") is not None
            response.assert_not_called()
            hardware.value.operation.set_finished()
        assert lookups == [hardware.value]
        assert server.requests_handler.get_request("cross-root-set") is None
        response.assert_called_once()
        assert response.call_args.args[1]["data"].device == "hardware.value"
        assert response.call_args.args[1]["data"].success is True
        configuration_calls = [
            call
            for call in publish.call_args_list
            if call.args[0]
            in {
                MessageEndpoints.device_read_configuration("hardware"),
                MessageEndpoints.device_read_configuration("alias"),
                MessageEndpoints.device_read_configuration("other"),
            }
        ]
        if kind == Kind.config:
            assert len(configuration_calls) == 1
            call = configuration_calls[0]
            assert call.args[0] == MessageEndpoints.device_read_configuration("hardware")
            assert call.args[1].signals == hardware.read_configuration()
            assert call.args[1].metadata == instruction.metadata
            read_instruction.assert_not_called()
        else:
            assert configuration_calls == []
            if kind in (Kind.normal, Kind.hinted):
                read_instruction.assert_called_once_with(instruction, new_status=False)
            else:
                read_instruction.assert_not_called()
        assert unsubscribe.call_count == int(has_events)
        refresh.reset_mock()
        hardware.value.put(10)
        assert not any(
            call.args[0].name in {"hardware", "alias", "other"} for call in refresh.call_args_list
        )
