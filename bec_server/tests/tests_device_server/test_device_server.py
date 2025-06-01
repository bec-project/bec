import threading
from unittest import mock
from unittest.mock import ANY

import pytest
from ophyd import DeviceStatus, Staged
from ophyd.utils import errors as ophyd_errors

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import BECStatus
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.device_server import DeviceServer, InvalidDeviceError
from bec_server.device_server.devices.devicemanager import DeviceManagerDS

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


class DeviceServerMock(DeviceServer):
    def __init__(self, device_manager, connector_cls) -> None:
        config = ServiceConfig(redis={"host": "dummy", "port": 6379})
        super().__init__(config, connector_cls=ConnectorMock)
        self.device_manager = device_manager

    def _start_device_manager(self):
        pass

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

    with mock.patch.object(dev.motor1_disabled.obj, "stop") as stop:
        device_server.stop_devices()
        stop.assert_not_called()

    with mock.patch.object(dev.motor1_disabled_set.obj, "stop") as stop:
        device_server.stop_devices()
        stop.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_on_stop_all_devices(device_server_mock):
    msg = messages.VariableMessage(value=1, metadata={})
    device_server = device_server_mock
    with mock.patch.object(device_server, "stop_devices") as stop:
        device_server.on_stop_all_devices(msg, parent=device_server)
        stop.assert_called_once()


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
                device_server._assert_device_is_enabled(instr)
            assert exc_info.value.args[0] == f"Cannot access disabled device {dev}."
        else:
            device_server._assert_device_is_enabled(instr)


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
            device_server._assert_device_is_valid(instr)
        return

    if not isinstance(devices, list):
        devices = [devices]

    for dev in devices:
        if dev not in device_server.device_manager.devices:
            with pytest.raises(InvalidDeviceError) as exc_info:
                device_server._assert_device_is_valid(instr)
            assert exc_info.value.args[0] == f"There is no device with the name {dev}."
        else:
            device_server._assert_device_is_enabled(instr)


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

    with mock.patch.object(device_server, "_assert_device_is_valid") as assert_device_is_valid_mock:
        with mock.patch.object(
            device_server, "_assert_device_is_enabled"
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


def test_handle_device_instruction_raises_alarm(device_server_mock):
    device_server = device_server_mock
    with mock.patch.object(device_server, "_assert_device_is_enabled", side_effect=RuntimeError):
        with mock.patch.object(device_server.connector, "raise_alarm") as raise_alarm:
            device_server.handle_device_instructions(mock.MagicMock())
            raise_alarm.assert_called_once_with(
                severity=Alarms.MAJOR, source=ANY, msg=ANY, alarm_type=ANY, metadata=ANY
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
    device_server = device_server_mock

    with mock.patch.object(device_server.connector, "raise_alarm") as alarm_mock:
        with mock.patch.object(device_server, "_set_device") as set_mock:
            set_mock.side_effect = ophyd_errors.LimitError("Wrong limits")
            device_server.handle_device_instructions(instructions)

            alarm_mock.assert_called_once_with(
                severity=Alarms.MAJOR,
                source=instructions.content,
                msg=ANY,
                alarm_type="LimitError",
                metadata=instructions.metadata,
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
    with mock.patch.object(device_server, "_assert_device_is_valid") as assert_device_is_valid_mock:
        with mock.patch.object(
            device_server, "_assert_device_is_enabled"
        ) as assert_device_is_enabled_mock:
            with mock.patch.object(
                device_server, "_update_device_metadata"
            ) as update_device_metadata_mock:
                with mock.patch.object(device_server, "run_rpc") as rpc_mock:
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

    with mock.patch.object(device_server, "_complete_device") as complete_mock:
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
    if device is not None:
        oph_device.obj.complete = complete_mock
    device_server._complete_device(instr)
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
            assert id(status) == id(request_info["status_objects"][0])
            assert status.done is False
            assert send_response_mock.call_count == 1
            assert send_response_mock.call_args == mock.call("diid", None, False)
            status.set_finished()
            finished_thread_event.wait()
            assert status.done is True
            assert send_response_mock.call_count == 2
            assert send_response_mock.call_args == mock.call(
                "diid", True, done=True, error_message=None, result=None
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

    with mock.patch.object(device_server, "_trigger_device") as trigger_mock:
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

    with mock.patch.object(device_server, "_stage_device") as stage_mock:
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

    with mock.patch.object(device_server, "_unstage_device") as unstage_mock:
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
            device_server._trigger_device(instr)
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
        device_server._kickoff_device(instr)
        kickoff.assert_called_once()


@pytest.mark.timeout(5)
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
            device_server._retry_obj_method("samx", samx.obj, "read_configuration", TimeoutError())


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_retry_obj_method_retry(device_server_mock):
    device_server = device_server_mock
    samx = device_server.device_manager.devices.samx
    signals_before = samx.obj.read_configuration()
    samx._config["onFailure"] = "retry"
    signals = device_server._retry_obj_method("samx", samx.obj, "read_configuration", Exception())
    assert signals.keys() == signals_before.keys()


@pytest.mark.parametrize("instr", ["read", "read_configuration", "unknown_method"])
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_retry_obj_method_buffer(device_server_mock, instr):
    device_server = device_server_mock
    samx = device_server.device_manager.devices.samx
    samx._config["onFailure"] = "buffer"
    if instr not in ["read", "read_configuration"]:
        with pytest.raises(ValueError):
            device_server._retry_obj_method("samx", samx.obj, instr, Exception())
        return

    signals_before = getattr(samx.obj, instr)()
    device_server.connector = mock.MagicMock()
    device_server.connector.get.return_value = messages.DeviceMessage(
        signals=signals_before, metadata={"RID": "test", "stream": "primary"}
    )

    signals = device_server._retry_obj_method("samx", samx.obj, instr, Exception())
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
        device_server._stage_device(instr)
        devices = instr.content["device"]
        devices = devices if isinstance(devices, list) else [devices]
        dev_man = device_server.device_manager.devices
        for dev in devices:
            if not hasattr(dev_man[dev].obj, "_staged"):
                continue
            assert device_server.device_manager.devices[dev].obj._staged == Staged.yes
        device_server._unstage_device(instr)
        for dev in devices:
            if not hasattr(dev_man[dev].obj, "_staged"):
                continue
            assert device_server.device_manager.devices[dev].obj._staged == Staged.no
    else:
        device_server._stage_device(instr)
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
    device_server._stage_device(instr)
    device_server.device_manager.devices["samx"].obj.unstage()
    with mock.patch.object(
        device_server.device_manager.devices["samx"].obj, "unstage"
    ) as mock_unstage:
        assert device_server.device_manager.devices["samx"].obj._staged == Staged.no
        device_server._stage_device(instr, timeout_on_unstage=0.1)
        assert device_server.device_manager.devices["samx"].obj._staged == Staged.yes
        status = DeviceStatus(device=device_server.device_manager.devices["samx"].obj)
        mock_unstage.return_value = status
        with pytest.raises(ValueError):
            device_server._stage_device(instr, timeout_on_unstage=0.1)
        # Change the mock to return the resolved unstage status + unstage the device
        mock_unstage.side_effect = callback
        device_server._stage_device(instr, timeout_on_unstage=0.1)
