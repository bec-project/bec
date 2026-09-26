import threading
from types import SimpleNamespace
from unittest import mock
from unittest.mock import ANY

import numpy as np
import pytest
from ophyd import Device, DeviceStatus, Kind, Staged
from ophyd.utils import errors as ophyd_errors
from ophyd_devices import StatusBase

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.device import RPCError, Status
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import BECStatus
from bec_lib.redis_connector import MessageObject
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.device_server import DeviceServer, InvalidDeviceError
from bec_server.device_server.devices.devicemanager import DeviceManagerDS
from bec_server.device_server.ophyd.backend import OphydDeviceLayer
from bec_server.device_server.ophyd.instructions import OphydInstructions
from bec_server.device_server.ophyd.status import OphydStatus

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


class DeviceServerMock(DeviceServer):
    def __init__(self, device_manager, connector_cls) -> None:
        config = ServiceConfig(redis={"host": "dummy", "port": 6379})
        super().__init__(
            config,
            connector_cls=ConnectorMock,
            layer_factory=lambda server: OphydDeviceLayer(server, device_manager=device_manager),
        )

    def _start_device_manager(self):
        pass

    def _start_metrics_emitter(self):
        pass

    def _start_update_service_info(self):
        pass


@pytest.fixture
def device_manager_class():
    return DeviceManagerDS


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

    converted = OphydInstructions.convert_value_if_needed(obj, value)

    assert converted == expected
    assert isinstance(converted, int)


def test_convert_value_if_needed_rejects_non_integral_float_for_enum():
    obj = SimpleNamespace(name="enum_signal", enum_strs=("zero", "one", "two"))

    with pytest.raises(ValueError, match="Cannot convert float 1.5 to enum index"):
        OphydInstructions.convert_value_if_needed(obj, 1.5)


@pytest.mark.parametrize("value", [1, 1.0, np.float64(1.0)])
def test_convert_value_if_needed_leaves_non_enum_values_unchanged(value):
    obj = SimpleNamespace(name="plain_signal")

    converted = OphydInstructions.convert_value_if_needed(obj, value)

    assert converted is value


def test_convert_value_if_needed_leaves_enum_int_unchanged():
    obj = SimpleNamespace(name="enum_signal", enum_strs=("zero", "one"))

    converted = OphydInstructions.convert_value_if_needed(obj, 1)

    assert converted == 1
    assert isinstance(converted, int)


def test_start(device_server_mock):
    device_server = device_server_mock

    device_server.start()

    assert device_server.status == BECStatus.RUNNING


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


@pytest.mark.parametrize("native_with_obj", [False, True])
def test_device_server_status_callback(
    device_server_mock, ophyd_device_mock, device_instruction_message_mock, native_with_obj
):
    device_server = device_server_mock
    dev = ophyd_device_mock
    instr = device_instruction_message_mock
    dev._kind = Kind.normal
    native = StatusBase(obj=dev if native_with_obj else None)
    route = device_server.device_layer.instructions
    with mock.patch.object(route, "read_and_update_devices") as read:
        status = route.register_status(native, instr, dev)
        native.set_finished()
        assert status.done
        assert status.success
        read.assert_called_once_with([dev.name], instr.metadata)


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
    if status_success:
        status.set_finished()
    else:
        status.set_exception(RuntimeError("motor failed"))

    route = device_server.device_layer.instructions
    with mock.patch.object(route, "read_and_update_devices") as mock_read_device:
        with mock.patch.object(device_server.connector, "xadd") as xadd_mock:
            adapted = route.register_status(status, instr, dev)
            assert adapted.done

    mock_read_device.assert_called_once_with([dev.name], instr.metadata)
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
                with mock.patch.object(
                    device_server.device_layer.instructions, "_set_device"
                ) as set_mock:
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
def test_handle_device_instructions_limit_error(device_server_mock, instructions):
    """
    Test that handling device instructions that raise LimitError resolves the status as failed.
    """
    device_server = device_server_mock

    with mock.patch.object(device_server.requests_handler, "set_finished") as set_finished_mock:
        with mock.patch.object(device_server.device_layer.instructions, "_set_device") as set_mock:
            set_mock.side_effect = ophyd_errors.LimitError("Wrong limits")
            device_server.handle_device_instructions(instructions)

            set_finished_mock.assert_called_once_with(
                instructions.metadata["device_instr_id"], success=False, error_info=ANY
            )


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

    with mock.patch.object(device_server.device_layer.instructions, "_read_device") as read_mock:
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

    with mock.patch.object(
        device_server.device_layer.instructions, "_kickoff_device"
    ) as kickoff_mock:
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

    with mock.patch.object(
        device_server.device_layer.instructions, "_complete_device"
    ) as complete_mock:
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
    device_server.device_layer.instructions._complete_device(instr)
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
        with mock.patch.object(device_server.connector, "send") as send_response_mock:
            device_server.handle_device_instructions(instructions)
            request_info = device_server.requests_handler.get_request(instr_id="diid")
            assert len(request_info.status_objects) == 1
            assert request_info.status_objects[0].native_status is status
            assert status.done is False
            responses = [call.args[1] for call in send_response_mock.call_args_list]
            assert [response.status for response in responses] == ["running", "running"]
            assert responses[-1].result_is_status is True
            status.set_finished()
            assert finished_thread_event.wait(2)
            responses = [call.args[1] for call in send_response_mock.call_args_list]
            assert [response.status for response in responses] == [
                "running",
                "running",
                "completed",
            ]
            assert responses[-1].instruction_id == "diid"
            assert responses[-1].result_is_status is True


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

    with mock.patch.object(
        device_server.device_layer.instructions, "_trigger_device"
    ) as trigger_mock:
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

    with mock.patch.object(device_server.device_layer.instructions, "_stage_device") as stage_mock:
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

    with mock.patch.object(
        device_server.device_layer.instructions, "_unstage_device"
    ) as unstage_mock:
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
        with mock.patch.object(
            device_server.device_manager.devices.get(dev).obj, "trigger"
        ) as trigger:
            trigger.return_value = mock.MagicMock(spec=DeviceStatus)
            device_server.device_layer.instructions._trigger_device(instr)
            trigger.assert_called_once()
        assert device_server.device_manager.devices.get(dev).metadata == instr.metadata


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
        device_server.device_layer.instructions._kickoff_device(instr)
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
    device_server.device_layer.instructions._set_device(instr)
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
        with mock.patch.object(device_server.connector, "send") as mock_send_response:
            device_server.device_layer.instructions._set_device(instr)
            responses = [call.args[1] for call in mock_send_response.call_args_list]
            assert [response.status for response in responses] == ["running", "running", "error"]
            assert responses[-1].instruction_id == instr.metadata["device_instr_id"]
            assert responses[-1].error_info is not None
            assert responses[-1].result_is_status is True


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
        mock.patch.object(device_server_mock.connector, "send") as mock_send_response,
    ):
        device_server_mock.device_layer.instructions._set_device(instr)
        assert (
            """DeviceInstructionError: An incorrect value was provided to a .set() command. This could be, for example, providing a float rather than an int to an enum PV. Device: samx, value: 5."""
            in mock_send_response.call_args.args[1].error_info.compact_error_message
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
    device_server.device_layer.instructions._read_device(instr)
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

    device_server.device_layer.instructions._read_device(instr)

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
    device_server.device_layer.instructions.read_config_and_update_devices(
        devices, metadata={"RID": "test"}
    )
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
def test_read_and_update_devices_exception(device_server_mock):
    device_server = device_server_mock
    samx_obj = device_server.device_manager.devices.samx.obj
    with pytest.raises(Exception):
        with mock.patch.object(
            device_server.device_layer.instructions, "_retry_obj_method"
        ) as mock_retry:
            with mock.patch.object(samx_obj, "read") as read_mock:
                read_mock.side_effect = Exception
                mock_retry.side_effect = Exception
                device_server.device_layer.instructions.read_and_update_devices(
                    ["samx"], metadata={"RID": "test"}
                )
                mock_retry.assert_called_once_with("samx", samx_obj, "read", Exception())


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_read_config_and_update_devices_exception(device_server_mock):
    device_server = device_server_mock
    samx_obj = device_server.device_manager.devices.samx.obj
    with pytest.raises(Exception):
        with mock.patch.object(
            device_server.device_layer.instructions, "_retry_obj_method"
        ) as mock_retry:
            with mock.patch.object(samx_obj, "read_configuration") as read_config:
                read_config.side_effect = Exception
                mock_retry.side_effect = Exception
                device_server.device_layer.instructions.read_config_and_update_devices(
                    ["samx"], metadata={"RID": "test"}
                )
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
            device_server.device_layer.instructions._retry_obj_method(
                "samx", samx.obj, "read_configuration", TimeoutError()
            )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_retry_obj_method_retry(device_server_mock):
    device_server = device_server_mock
    samx = device_server.device_manager.devices.samx
    signals_before = samx.obj.read_configuration()
    samx._config["onFailure"] = "retry"
    signals = device_server.device_layer.instructions._retry_obj_method(
        "samx", samx.obj, "read_configuration", Exception()
    )
    assert signals.keys() == signals_before.keys()


@pytest.mark.parametrize("instr", ["read", "read_configuration", "unknown_method"])
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_retry_obj_method_buffer(device_server_mock, instr):
    device_server = device_server_mock
    samx = device_server.device_manager.devices.samx
    samx._config["onFailure"] = "buffer"
    if instr not in ["read", "read_configuration"]:
        with pytest.raises(ValueError):
            device_server.device_layer.instructions._retry_obj_method(
                "samx", samx.obj, instr, Exception()
            )
        return

    signals_before = getattr(samx.obj, instr)()
    device_server.connector = mock.MagicMock()
    device_server.device_layer.instructions.connector = device_server.connector
    device_server.connector.get.return_value = messages.DeviceMessage(
        signals=signals_before, metadata={"RID": "test", "stream": "primary"}
    )

    signals = device_server.device_layer.instructions._retry_obj_method(
        "samx", samx.obj, instr, Exception()
    )
    assert signals.keys() == signals_before.keys()


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
        device_server.device_layer.instructions._stage_device(instr)
        devices = instr.content["device"]
        devices = devices if isinstance(devices, list) else [devices]
        dev_man = device_server.device_manager.devices
        for dev in devices:
            if not hasattr(dev_man[dev].obj, "_staged"):
                continue
            assert device_server.device_manager.devices[dev].obj._staged == Staged.yes
        device_server.device_layer.instructions._unstage_device(instr)
        for dev in devices:
            if not hasattr(dev_man[dev].obj, "_staged"):
                continue
            assert device_server.device_manager.devices[dev].obj._staged == Staged.no
    else:
        device_server.device_layer.instructions._stage_device(instr)
        status = device_server.requests_handler.get_request("diid").status_objects[0]
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
    device_server.device_layer.instructions._stage_device(instr)
    device_server.device_manager.devices["samx"].obj.unstage()
    with mock.patch.object(
        device_server.device_manager.devices["samx"].obj, "unstage"
    ) as mock_unstage:
        assert device_server.device_manager.devices["samx"].obj._staged == Staged.no
        device_server.device_layer.instructions._stage_device(instr, timeout_on_unstage=0.1)
        assert device_server.device_manager.devices["samx"].obj._staged == Staged.yes
        status = DeviceStatus(device=device_server.device_manager.devices["samx"].obj)
        mock_unstage.return_value = status
        with pytest.raises(ValueError):
            device_server.device_layer.instructions._stage_device(instr, timeout_on_unstage=0.1)
        # Change the mock to return the resolved unstage status + unstage the device
        mock_unstage.side_effect = callback
        device_server.device_layer.instructions._stage_device(instr, timeout_on_unstage=0.1)


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
    metadata = device_server.get_metadata_for_alarm(instr)
    assert metadata == instr.metadata


def test_get_metadata_for_alarm_no_device_manager(device_server_mock):
    device_server = device_server_mock
    instr = messages.DeviceInstructionMessage(
        device="test_device", action="kickoff", parameter={}, metadata={}
    )
    device_server.device_manager = None
    metadata = device_server.get_metadata_for_alarm(instr)
    assert metadata == instr.metadata


def test_get_metadata_for_alarm_no_scan_info(device_server_mock):
    device_server = device_server_mock
    instr = messages.DeviceInstructionMessage(
        device="test_device", action="kickoff", parameter={}, metadata={}
    )
    device_server.device_manager.scan_info = None
    metadata = device_server.get_metadata_for_alarm(instr)
    assert metadata == instr.metadata


def test_get_metadata_for_alarm_no_scan_info_msg(device_server_mock):
    device_server = device_server_mock
    instr = messages.DeviceInstructionMessage(
        device="test_device", action="kickoff", parameter={}, metadata={}
    )
    device_server.device_manager.scan_info.msg = None
    metadata = device_server.get_metadata_for_alarm(instr)
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
    metadata = device_server.get_metadata_for_alarm(instr)
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
    device_server.requests_handler.add_status_object(
        request, OphydStatus(status, request, Device(name="test_device"))
    )

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


def test_removed_request_does_not_publish_aggregate_on_completion(device_server_mock):
    handler = device_server_mock.requests_handler
    request = messages.DeviceInstructionMessage(
        device="test_device", action="complete", parameter={}, metadata={"device_instr_id": "diid"}
    )
    native = StatusBase()
    obj = Device(name="test_device")
    try:
        handler.add_request(request, 1)
        handler.add_status_object(request, OphydStatus(native, request, obj))
        handler.remove_request("diid")
        with mock.patch.object(device_server_mock.connector, "send") as send:
            native.set_finished()
        send.assert_not_called()
        assert not handler.has_request("diid")
    finally:
        obj.destroy()


def test_set_completion_keeps_original_request(device_server_mock):
    server = device_server_mock
    instruction = messages.DeviceInstructionMessage(
        device="samx", action="set", parameter={"value": 1}, metadata={"device_instr_id": "set"}
    )
    native = DeviceStatus(server.device_manager.devices.samx.obj)
    with mock.patch.object(server.device_manager.devices.samx.obj, "set", return_value=native):
        server.handle_device_instructions(instruction)
    request = server.requests_handler.get_request("set")
    with mock.patch.object(server.requests_handler, "add_request") as add_request:
        with mock.patch.object(server.connector, "send") as send:
            native.set_finished()
    add_request.assert_not_called()
    assert len(request.status_objects) == 1
    assert request.status_objects[0].done
    assert server.requests_handler.get_request("set") is None
    assert [call.args[1].status for call in send.call_args_list] == ["completed"]


@pytest.mark.parametrize("remove_request", [False, True])
def test_native_completion_releases_subscription_zero(device_server_mock, remove_request):
    server = device_server_mock
    obj = server.device_manager.devices.samx.obj
    native = DeviceStatus(obj)
    instruction = messages.DeviceInstructionMessage(
        device="samx", action="trigger", parameter={}, metadata={"device_instr_id": "cleanup"}
    )
    server.requests_handler.add_request(instruction, 1)
    status = server.device_layer.instructions.register_status(native, instruction, obj, sub_id=0)
    if remove_request:
        server.requests_handler.remove_request("cleanup")
    with mock.patch.object(obj, "unsubscribe") as unsubscribe:
        native.set_finished()
    unsubscribe.assert_called_once_with(0)
    assert status.done


def test_completion_cache_failure_resolves_request(device_server_mock):
    server = device_server_mock
    obj = server.device_manager.devices.samx.obj
    obj._kind = Kind.normal
    native = DeviceStatus(obj)
    instruction = messages.DeviceInstructionMessage(
        device="samx", action="set", parameter={"value": 1}, metadata={"device_instr_id": "failed"}
    )
    with mock.patch.object(obj, "set", return_value=native):
        server.handle_device_instructions(instruction)
    with (
        mock.patch.object(
            server.device_layer.instructions,
            "read_and_update_devices",
            side_effect=RuntimeError("cache refresh failed"),
        ),
        mock.patch.object(server.connector, "send") as send,
    ):
        native.set_finished()
    terminal = [call.args[1] for call in send.call_args_list]
    assert len(terminal) == 1
    assert terminal[0].status == "error"
    assert "cache refresh failed" in terminal[0].error_info.error_message
    assert server.requests_handler.get_request("failed") is None


def test_late_completion_after_shutdown_only_cleans_subscription(device_server_mock):
    server = device_server_mock
    obj = server.device_manager.devices.samx.obj
    native = DeviceStatus(obj)
    instruction = messages.DeviceInstructionMessage(
        device="samx", action="set", parameter={"value": 1}, metadata={"device_instr_id": "late"}
    )
    server.requests_handler.add_request(instruction, 1)
    status = server.device_layer.instructions.register_status(native, instruction, obj, sub_id=0)
    server.shutdown()
    with (
        mock.patch.object(obj, "unsubscribe") as unsubscribe,
        mock.patch.object(server.connector, "send") as send,
        mock.patch.object(server.connector, "pipeline") as pipeline,
    ):
        native.set_finished()
    assert status.done
    unsubscribe.assert_called_once_with(0)
    send.assert_not_called()
    pipeline.assert_not_called()


@pytest.mark.parametrize("already_done", [False, True])
def test_rpc_finalization_failure_resolves_client_status(device_server_mock, already_done):
    server = device_server_mock
    obj = server.device_manager.devices.samx.obj
    obj._kind = Kind.normal
    native = DeviceStatus(obj)
    if already_done:
        native.set_finished()
    instruction = messages.DeviceInstructionMessage(
        device="samx",
        action="rpc",
        parameter={"func": "set", "args": [1], "rpc_id": "rpc"},
        metadata={"device_instr_id": "rpc-failed", "RID": "client-status", "response": True},
    )
    with (
        mock.patch.object(obj, "set", return_value=native),
        mock.patch.object(
            server.device_layer.instructions,
            "read_and_update_devices",
            side_effect=RuntimeError("cache refresh failed"),
        ),
        mock.patch.object(server.connector, "xadd") as publish,
        mock.patch.object(server.connector, "set") as rpc_reply,
        mock.patch.object(server.connector, "send") as instruction_reply,
    ):
        server.handle_device_instructions(instruction)
        if not already_done:
            native.set_finished()
    status_calls = [
        call
        for call in publish.call_args_list
        if call.args and call.args[0] == MessageEndpoints.device_req_status("client-status")
    ]
    assert len(status_calls) == 1
    response = status_calls[0].args[1]["data"]
    assert response.success is False
    assert "cache refresh failed" in response.metadata["error_info"].error_message
    terminal = [
        call.args[1]
        for call in instruction_reply.call_args_list
        if call.args[1].status != "running"
    ]
    assert len(terminal) == 1
    assert terminal[0].status == "error"
    result = rpc_reply.call_args.args[1].return_val
    assert result["type"] == "status"
    if already_done:
        assert result["done"] is True
        assert result["success"] is False
    client_status = Status(mock.Mock(), "client-status")
    client_status._on_status_update({"data": response})
    with pytest.raises(RPCError):
        client_status.wait(timeout=0)


def test_rpc_config_set_refreshes_configuration_cache(device_server_mock):
    server = device_server_mock
    obj = server.device_manager.devices.samx.obj
    signal = obj.velocity
    native = DeviceStatus(signal)
    native.set_finished()
    instruction = messages.DeviceInstructionMessage(
        device="samx",
        action="rpc",
        parameter={"func": "velocity.set", "args": [1], "rpc_id": "rpc"},
        metadata={"device_instr_id": "config-set"},
    )
    with (
        mock.patch.object(signal, "set", return_value=native),
        mock.patch.object(
            server.device_layer.instructions, "_update_read_configuration"
        ) as refresh,
    ):
        server.handle_device_instructions(instruction)
    refresh.assert_called_once_with(signal, instruction.metadata, mock.ANY)


def test_shutdown_unregister_failure_still_disconnects_devices(device_server_mock):
    server = device_server_mock
    manager = server.device_manager
    with (
        mock.patch.object(server.connector, "unregister", side_effect=ConnectionError("offline")),
        mock.patch.object(manager, "disconnect_device") as disconnect,
        mock.patch.object(server.connector, "shutdown") as transport,
    ):
        num_devices = len(manager.devices)
        server.shutdown()
        server.shutdown()
    assert disconnect.call_count == num_devices
    assert manager._shutdown_complete
    assert server.executor._shutdown
    transport.assert_called_once()
