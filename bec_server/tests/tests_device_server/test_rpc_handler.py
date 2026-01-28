# pylint: skip-file
from collections import namedtuple
from unittest import mock

import pytest
from ophyd import Device, DeviceStatus, Kind, Signal, Staged, StatusBase

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.device_server import DeviceServer, RequestHandler
from bec_server.device_server.rpc_handler import RPCHandler


@pytest.fixture
def rpc_cls() -> RPCHandler:  # type: ignore
    device_server_mock = mock.MagicMock(spec=DeviceServer)
    device_server_mock.device_manager = mock.MagicMock()
    device_server_mock.connector = mock.MagicMock()
    device_server_mock.requests_handler = mock.MagicMock(spec=RequestHandler)
    rpc_handler = RPCHandler(device_server=device_server_mock)

    return rpc_handler


@pytest.fixture()
def mock_rpc_methods(rpc_cls):
    """Fixture to mock the common RPC read methods"""
    with (
        mock.patch.object(rpc_cls, "_rpc_read_configuration_and_return") as mock_read_config,
        mock.patch.object(rpc_cls, "_rpc_read_and_return") as mock_read,
    ):
        yield {"read_config": mock_read_config, "read": mock_read}


@pytest.fixture
def instr():
    yield messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter={"rpc_id": "rpc_id", "func": "trigger"},
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )


@pytest.mark.parametrize(
    "instr_params",
    [
        ({"args": (1, 2, 3), "kwargs": {"a": 1, "b": 2}}),
        ({"args": (1, 2, 3)}),
        ({"kwargs": {"a": 1, "b": 2}}),
        ({}),
    ],
)
def test_execute_rpc_call(rpc_cls: RPCHandler, instr_params):
    rpc_var = mock.MagicMock()
    rpc_var.return_value = 1
    msg = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter=instr_params,
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    out = rpc_cls._execute_rpc_call(rpc_var=rpc_var, instr=msg)
    if instr_params:
        if instr_params.get("args") and instr_params.get("kwargs"):
            rpc_var.assert_called_once_with(*instr_params["args"], **instr_params["kwargs"])
        elif instr_params.get("args"):
            rpc_var.assert_called_once_with(*instr_params["args"])
        elif instr_params.get("kwargs"):
            rpc_var.assert_called_once_with(**instr_params["kwargs"])
        else:
            rpc_var.assert_called_once_with()
    assert out == 1


@pytest.mark.parametrize("instr_params", [({"args": (1, 2, 3), "kwargs": {"a": 1, "b": 2}}), ({})])
def test_execute_rpc_call_var(rpc_cls: RPCHandler, instr_params: dict):
    rpc_var = 5
    msg = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter=instr_params,
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    out = rpc_cls._execute_rpc_call(rpc_var=rpc_var, instr=msg)
    assert out == 5


def test_execute_rpc_call_not_serializable(rpc_cls: RPCHandler):
    rpc_var = mock.MagicMock()
    rpc_var.return_value = mock.MagicMock()
    rpc_var.return_value.__str__.side_effect = Exception
    msg = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter={"func": "trigger"},
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    with mock.patch("bec_lib.messages.uuid.uuid4", return_value="uuid"):
        out = rpc_cls._execute_rpc_call(rpc_var=rpc_var, instr=msg)
        assert out is None
        error_info = messages.ErrorInfo(
            id="uuid",
            error_message=f"Return value of rpc call {msg.parameter} is not serializable.",
            compact_error_message=f"Return value of rpc call {msg.parameter} is not serializable.",
            exception_type="TypeError",
            device="device",
        )
        rpc_cls.connector.raise_alarm.assert_called_once_with(
            severity=Alarms.WARNING, info=error_info
        )


def test_execute_rpc_call_ophyd_status(rpc_cls: RPCHandler):
    rpc_var = mock.MagicMock()
    status = StatusBase()
    rpc_var.return_value = status
    msg = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter={},
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    out = rpc_cls._execute_rpc_call(rpc_var=rpc_var, instr=msg)
    assert out is rpc_var.return_value
    status.set_finished()


def test_execute_rpc_call_list_from_stage(rpc_cls: RPCHandler):
    rpc_var = mock.MagicMock()
    rpc_var.return_value = [mock.MagicMock(), mock.MagicMock()]
    rpc_var.return_value[0]._staged = True
    rpc_var.return_value[1]._staged = False
    msg = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter={"func": "stage"},
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    out = rpc_cls._execute_rpc_call(rpc_var=rpc_var, instr=msg)
    assert out == [True, False]


def test_send_rpc_exception(rpc_cls: RPCHandler, instr: messages.DeviceInstructionMessage):
    with mock.patch.object(
        rpc_cls.device_server, "get_device_from_exception", return_value="device"
    ):
        rpc_cls.send_rpc_exception(Exception(), instr)
    error_info = rpc_cls.connector.set.call_args[0][1].out
    rpc_cls.connector.set.assert_called_once_with(
        MessageEndpoints.device_rpc("rpc_id"),
        messages.DeviceRPCMessage(
            device="device",
            return_val=None,
            out=messages.ErrorInfo(
                id=error_info.id,
                error_message=error_info.error_message,
                compact_error_message=error_info.compact_error_message,
                exception_type="Exception",
                device="device",
            ),
            success=False,
        ),
    )


def test_send_rpc_result_to_client(rpc_cls: RPCHandler):
    result = mock.MagicMock()
    result.getvalue.return_value = "result"
    rpc_cls.send_rpc_result_to_client(mock.MagicMock(), "device", {"rpc_id": "rpc_id"}, 1, result)
    rpc_cls.connector.set.assert_called_once_with(
        MessageEndpoints.device_rpc("rpc_id"),
        messages.DeviceRPCMessage(device="device", return_val=1, out="result", success=True),
        expire=1800,
    )


def test_run_rpc(rpc_cls: RPCHandler, instr: messages.DeviceInstructionMessage):
    with (
        mock.patch.object(rpc_cls, "process_rpc_instruction") as _process_rpc_instruction,
        mock.patch.object(rpc_cls, "send_rpc_result_to_client") as _send_rpc_result_to_client,
    ):
        _process_rpc_instruction.return_value = 1
        rpc_cls.run_rpc(instr)
        rpc_cls.device_server.assert_device_is_enabled.assert_called_once_with(instr)
        _process_rpc_instruction.assert_called_once_with(instr)
        _send_rpc_result_to_client.assert_called_once_with(
            instr, "device", {"rpc_id": "rpc_id", "func": "trigger"}, 1, mock.ANY
        )


def test_run_rpc_sends_rpc_exception(rpc_cls, instr):
    with (
        mock.patch.object(rpc_cls, "process_rpc_instruction") as _process_rpc_instruction,
        mock.patch.object(rpc_cls, "send_rpc_exception") as _send_rpc_exception,
    ):
        _process_rpc_instruction.side_effect = Exception
        rpc_cls.run_rpc(instr)
        rpc_cls.device_server.assert_device_is_enabled.assert_called_once_with(instr)
        _process_rpc_instruction.assert_called_once_with(instr)
        _send_rpc_exception.assert_called_once_with(mock.ANY, instr)


@pytest.fixture()
def dev_mock():
    dev_mock = mock.MagicMock()
    dev_mock.obj = mock.MagicMock(spec=Device)
    dev_mock.obj.readback = mock.MagicMock(spec=Signal)
    dev_mock.obj.readback.kind = Kind.hinted
    dev_mock.obj.user_setpoint = mock.MagicMock(spec=Signal)
    dev_mock.obj.user_setpoint.kind = Kind.normal
    dev_mock.obj.velocity = mock.MagicMock(spec=Signal)
    dev_mock.obj.velocity.kind = Kind.config
    dev_mock.obj.notused = mock.MagicMock(spec=Signal)
    dev_mock.obj.notused.kind = Kind.omitted
    return dev_mock


@pytest.mark.parametrize(
    "func, read_called",
    [
        ("read", True),
        ("read_configuration", False),
        ("readback.read_configuration", False),
        ("readback.read", True),
        ("user_setpoint.read", True),
        ("user_setpoint.read_configuration", False),
        ("velocity.read", False),
        ("velocity.read_configuration", False),
        ("notused.read", False),
        ("notused.read_configuration", False),
    ],
)
def test_process_rpc_instruction_read(rpc_cls, dev_mock, instr, func, read_called):
    instr.content["parameter"]["func"] = func
    rpc_cls.device_manager.devices = {"device": dev_mock}
    rpc_cls.device_server._read_and_update_devices = mock.MagicMock()
    rpc_cls.device_server._read_config_and_update_devices = mock.MagicMock()
    rpc_cls.process_rpc_instruction(instr)
    if read_called:
        rpc_cls.device_server._read_and_update_devices.assert_called_once_with(
            ["device"], instr.metadata
        )
        rpc_cls.device_server._read_config_and_update_devices.assert_not_called()
    else:
        rpc_cls.device_server._read_and_update_devices.assert_not_called()
        if "notused" not in func:
            rpc_cls.device_server._read_config_and_update_devices.assert_called_once_with(
                ["device"], instr.metadata
            )


def test_process_rpc_instruction_with_status_return(rpc_cls, dev_mock, instr):
    rpc_cls.device_manager.devices = {"device": dev_mock}
    rpc_cls._status_callback = mock.MagicMock()
    with mock.patch.object(rpc_cls, "_execute_rpc_call") as rpc_result:
        status = StatusBase()
        rpc_result.return_value = status
        res = rpc_cls.process_rpc_instruction(instr)
        assert res == {
            "type": "status",
            "RID": "RID",
            "success": status.success,
            "timeout": status.timeout,
            "done": status.done,
            "settle_time": status.settle_time,
        }
        status.set_finished()


def test_process_rpc_instruction_with_namedtuple_return(rpc_cls, dev_mock, instr):
    rpc_cls.device_manager.devices = {"device": dev_mock}
    with mock.patch.object(rpc_cls, "_execute_rpc_call") as rpc_result:
        point_type = namedtuple("Point", ["x", "y"])
        point_tuple = point_type(5, 6)
        rpc_result.return_value = point_tuple
        res = rpc_cls.process_rpc_instruction(instr)
        assert res == {
            "type": "namedtuple",
            "RID": instr.metadata.get("RID"),
            "fields": point_tuple._fields,
            "values": point_tuple._asdict(),
        }


@pytest.mark.parametrize(
    "return_val,result",
    [([], []), ([1, 2, 3], [1, 2, 3]), ([Staged.no, Staged.yes], ["Staged.no", "Staged.yes"])],
)
def test_process_rpc_instruction_with_list_return(rpc_cls, dev_mock, instr, return_val, result):
    rpc_cls.device_manager.devices = {"device": dev_mock}
    with mock.patch.object(rpc_cls, "_execute_rpc_call") as rpc_result:
        rpc_result.return_value = return_val
        res = rpc_cls.process_rpc_instruction(instr)
        assert res == result


def test_process_rpc_instruction_set_attribute(rpc_cls, dev_mock, instr):
    instr.content["parameter"]["kwargs"] = {"_set_property": True}
    instr.content["parameter"]["args"] = [5]
    instr.content["parameter"]["func"] = "attr_value"
    rpc_cls.device_manager.devices = {"device": dev_mock}
    rpc_cls.process_rpc_instruction(instr)
    rpc_cls.device_manager.devices["device"].obj.attr_value == 5


def test_process_rpc_instruction_set_attribute_on_sub_device(rpc_cls, dev_mock, instr):
    instr.content["parameter"]["kwargs"] = {"_set_property": True}
    instr.content["parameter"]["args"] = [5]
    instr.content["parameter"]["func"] = "user_setpoint.attr_value"
    rpc_cls.device_manager.devices = {"device": dev_mock}
    rpc_cls.process_rpc_instruction(instr)
    rpc_cls.device_manager.devices["device"].obj.user_setpoint.attr_value == 5


def test_set_config_signal_updates_cache(rpc_cls, dev_mock, instr):
    instr_params = {"func": "velocity.set", "args": [10]}
    rpc_cls.device_manager.devices = {"device": dev_mock}
    rpc_cls._update_cache = mock.MagicMock()
    rpc_cls._execute_rpc_call = mock.MagicMock()
    rpc_cls._execute_rpc_call.return_value = DeviceStatus(
        device=dev_mock.obj, done=True, success=True
    )
    instr = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter=instr_params,
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    rpc_cls.process_rpc_instruction(instr=instr)
    rpc_cls._update_cache.assert_called_once_with(dev_mock.obj.velocity, instr)


def test_update_cache_config_kind(rpc_cls, instr, mock_rpc_methods):
    """Test that _update_cache calls read_configuration for config kind signals"""
    signal_mock = mock.MagicMock(spec=Signal)
    signal_mock.kind = Kind.config
    mock_rpc_methods["read_config"].return_value = {"config": "data"}

    result = rpc_cls._update_cache(signal_mock, instr)

    mock_rpc_methods["read_config"].assert_called_once_with(instr)
    mock_rpc_methods["read"].assert_not_called()
    assert result == {"config": "data"}


def test_update_cache_hinted_kind(rpc_cls, instr, mock_rpc_methods):
    """Test that _update_cache calls read for hinted kind signals"""
    signal_mock = mock.MagicMock(spec=Signal)
    signal_mock.kind = Kind.hinted
    mock_rpc_methods["read"].return_value = {"read": "data"}

    result = rpc_cls._update_cache(signal_mock, instr)

    mock_rpc_methods["read"].assert_called_once_with(instr)
    mock_rpc_methods["read_config"].assert_not_called()
    assert result == {"read": "data"}


def test_update_cache_normal_kind(rpc_cls, instr, mock_rpc_methods):
    """Test that _update_cache calls read for normal kind signals"""
    signal_mock = mock.MagicMock(spec=Signal)
    signal_mock.kind = Kind.normal
    mock_rpc_methods["read"].return_value = {"read": "data"}

    result = rpc_cls._update_cache(signal_mock, instr)

    mock_rpc_methods["read"].assert_called_once_with(instr)
    mock_rpc_methods["read_config"].assert_not_called()
    assert result == {"read": "data"}


def test_update_cache_omitted_kind(rpc_cls, instr, mock_rpc_methods):
    """Test that _update_cache calls both read methods for omitted kind signals"""
    signal_mock = mock.MagicMock(spec=Signal)
    signal_mock.kind = Kind.omitted
    mock_rpc_methods["read_config"].return_value = {"config": "data"}

    result = rpc_cls._update_cache(signal_mock, instr)

    mock_rpc_methods["read"].assert_called_once_with(instr)
    mock_rpc_methods["read_config"].assert_called_once_with(instr)
    assert result == {"config": "data"}
