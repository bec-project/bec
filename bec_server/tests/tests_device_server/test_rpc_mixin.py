# pylint: skip-file
from collections import namedtuple
from unittest import mock

import pytest
from ophyd import Device, Kind, Signal, Staged, StatusBase

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.device_server import RequestHandler
from bec_server.device_server.rpc_mixin import RPCMixin


@pytest.fixture
def rpc_cls():
    rpc_mixin = RPCMixin()
    rpc_mixin.connector = mock.MagicMock()
    rpc_mixin.connector = mock.MagicMock()
    rpc_mixin.device_manager = mock.MagicMock()
    rpc_mixin.requests_handler = mock.MagicMock(spec=RequestHandler)
    yield rpc_mixin


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
def test_get_result_from_rpc(rpc_cls, instr_params):
    rpc_var = mock.MagicMock()
    rpc_var.return_value = 1
    out = rpc_cls._get_result_from_rpc(rpc_var=rpc_var, instr_params=instr_params)
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
def test_get_result_from_rpc_var(rpc_cls, instr_params):
    rpc_var = 5
    out = rpc_cls._get_result_from_rpc(rpc_var=rpc_var, instr_params=instr_params)
    assert out == 5


def test_get_result_from_rpc_not_serializable(rpc_cls):
    rpc_var = mock.MagicMock()
    rpc_var.return_value = mock.MagicMock()
    rpc_var.return_value.__str__.side_effect = Exception
    out = rpc_cls._get_result_from_rpc(rpc_var=rpc_var, instr_params={})
    assert out is None
    rpc_cls.connector.raise_alarm.assert_called_once_with(
        severity=Alarms.WARNING,
        alarm_type="TypeError",
        source={},
        msg="Return value of rpc call {} is not serializable.",
        metadata={},
    )


def test_get_result_from_rpc_ophyd_status(rpc_cls):
    rpc_var = mock.MagicMock()
    status = StatusBase()
    rpc_var.return_value = status
    out = rpc_cls._get_result_from_rpc(rpc_var=rpc_var, instr_params={})
    assert out is rpc_var.return_value
    status.set_finished()


def test_get_result_from_rpc_list_from_stage(rpc_cls):
    rpc_var = mock.MagicMock()
    rpc_var.return_value = [mock.MagicMock(), mock.MagicMock()]
    rpc_var.return_value[0]._staged = True
    rpc_var.return_value[1]._staged = False
    out = rpc_cls._get_result_from_rpc(rpc_var=rpc_var, instr_params={"func": "stage"})
    assert out == [True, False]


def test_send_rpc_exception(rpc_cls, instr):
    rpc_cls._send_rpc_exception(Exception(), instr)
    rpc_cls.connector.set.assert_called_once_with(
        MessageEndpoints.device_rpc("rpc_id"),
        messages.DeviceRPCMessage(
            device="device",
            return_val=None,
            out={"error": "Exception", "msg": (), "traceback": "NoneType: None\n"},
            success=False,
        ),
    )


def test_send_rpc_result_to_client(rpc_cls):
    result = mock.MagicMock()
    result.getvalue.return_value = "result"
    rpc_cls._send_rpc_result_to_client(mock.MagicMock(), "device", {"rpc_id": "rpc_id"}, 1, result)
    rpc_cls.connector.set.assert_called_once_with(
        MessageEndpoints.device_rpc("rpc_id"),
        messages.DeviceRPCMessage(device="device", return_val=1, out="result", success=True),
        expire=1800,
    )


def test_run_rpc(rpc_cls, instr):
    rpc_cls._assert_device_is_enabled = mock.MagicMock()
    with (
        mock.patch.object(rpc_cls, "process_rpc_instruction") as _process_rpc_instruction,
        mock.patch.object(rpc_cls, "_send_rpc_result_to_client") as _send_rpc_result_to_client,
    ):
        _process_rpc_instruction.return_value = 1
        rpc_cls.run_rpc(instr)
        rpc_cls._assert_device_is_enabled.assert_called_once_with(instr)
        _process_rpc_instruction.assert_called_once_with(instr)
        _send_rpc_result_to_client.assert_called_once_with(
            instr, "device", {"rpc_id": "rpc_id", "func": "trigger"}, 1, mock.ANY
        )


def test_run_rpc_sends_rpc_exception(rpc_cls, instr):
    rpc_cls._assert_device_is_enabled = mock.MagicMock()
    with (
        mock.patch.object(rpc_cls, "process_rpc_instruction") as _process_rpc_instruction,
        mock.patch.object(rpc_cls, "_send_rpc_exception") as _send_rpc_exception,
    ):
        _process_rpc_instruction.side_effect = Exception
        rpc_cls.run_rpc(instr)
        rpc_cls._assert_device_is_enabled.assert_called_once_with(instr)
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
    rpc_cls._read_and_update_devices = mock.MagicMock()
    rpc_cls._read_config_and_update_devices = mock.MagicMock()
    rpc_cls.process_rpc_instruction(instr)
    if read_called:
        rpc_cls._read_and_update_devices.assert_called_once_with(["device"], instr.metadata)
        rpc_cls._read_config_and_update_devices.assert_not_called()
    else:
        rpc_cls._read_and_update_devices.assert_not_called()
        if "notused" not in func:
            rpc_cls._read_config_and_update_devices.assert_called_once_with(
                ["device"], instr.metadata
            )


def test_process_rpc_instruction_with_status_return(rpc_cls, dev_mock, instr):
    rpc_cls.device_manager.devices = {"device": dev_mock}
    rpc_cls._status_callback = mock.MagicMock()
    with mock.patch.object(rpc_cls, "_get_result_from_rpc") as rpc_result:
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
    with mock.patch.object(rpc_cls, "_get_result_from_rpc") as rpc_result:
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
    with mock.patch.object(rpc_cls, "_get_result_from_rpc") as rpc_result:
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
