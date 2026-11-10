# pylint: skip-file
from collections import namedtuple
from unittest import mock

import pytest
from ophyd import Device, DeviceStatus, Kind, Signal, Staged, StatusBase

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.device_providers.device_provider_manager import DeviceProviderManager
from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
    OphydDeviceProvider,
)
from bec_server.device_server.device_providers.ophyd.ophyd_rpc import OphydRPC
from bec_server.device_server.device_server import DeviceServer, DisabledDeviceError, RequestHandler
from bec_server.device_server.rpc_handler import RPCHandler


@pytest.fixture
def rpc_cls() -> RPCHandler:  # type: ignore
    device_server_mock = mock.MagicMock(spec=DeviceServer)
    device_server_mock.device_manager = mock.MagicMock()
    device_server_mock.connector = mock.MagicMock()
    device_server_mock.requests_handler = mock.MagicMock(spec=RequestHandler)
    rpc_handler = RPCHandler(device_server=device_server_mock)

    return rpc_handler


@pytest.fixture
def ophyd_rpc(rpc_cls, dev_mock):
    rpc_cls.device_manager.devices = {"device": dev_mock}
    provider = OphydDeviceProvider(rpc_cls.device_manager)
    return provider.rpc


@pytest.fixture()
def mock_rpc_methods(ophyd_rpc):
    """Fixture to mock the common RPC read methods"""
    with (
        mock.patch.object(ophyd_rpc, "_rpc_read_configuration_and_return") as mock_read_config,
        mock.patch.object(ophyd_rpc, "_rpc_read_and_return") as mock_read,
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
def test_execute_rpc_call(ophyd_rpc: OphydRPC, instr_params):
    rpc_var = mock.MagicMock()
    rpc_var.return_value = 1
    msg = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter=instr_params,
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    out = ophyd_rpc._execute_rpc_call(rpc_var=rpc_var, instr=msg)
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
def test_execute_rpc_call_var(ophyd_rpc: OphydRPC, instr_params: dict):
    rpc_var = 5
    msg = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter=instr_params,
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    out = ophyd_rpc._execute_rpc_call(rpc_var=rpc_var, instr=msg)
    assert out == 5


@pytest.mark.parametrize("supported", [True, False])
def test_provider_manager_validates_rpc_result(rpc_cls, instr, supported):
    manager = DeviceProviderManager(rpc_cls.device_manager)
    device = mock.Mock()
    value = {"value": 42} if supported else object()
    provider = mock.Mock()
    provider.run_rpc.return_value = value

    with (
        mock.patch.object(manager, "get_provider", return_value=provider),
        mock.patch("bec_lib.messages.uuid.uuid4", return_value="uuid"),
    ):
        out = manager.run_rpc(device, instr, rpc_cls)

    provider.run_rpc.assert_called_once_with(device, instr, rpc_cls)
    if supported:
        assert out is value
        rpc_cls.connector.raise_alarm.assert_not_called()
    else:
        assert out is None
        message = f"Return value of rpc call {instr.parameter} is not serializable."
        rpc_cls.connector.raise_alarm.assert_called_once_with(
            severity=Alarms.WARNING,
            info=messages.ErrorInfo(
                id="uuid",
                error_message=message,
                compact_error_message=message,
                exception_type="TypeError",
                device=instr.device,
            ),
        )


def test_execute_rpc_call_ophyd_status(ophyd_rpc: OphydRPC):
    rpc_var = mock.MagicMock()
    status = StatusBase()
    rpc_var.return_value = status
    msg = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter={},
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    out = ophyd_rpc._execute_rpc_call(rpc_var=rpc_var, instr=msg)
    assert out is rpc_var.return_value
    status.set_finished()


def test_rpc_adapts_native_stage_results(ophyd_rpc: OphydRPC, rpc_cls, dev_mock):
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
    with mock.patch.object(dev_mock.obj, "stage", rpc_var):
        out = ophyd_rpc.run(dev_mock, msg, rpc_cls)
    assert out == [True, False]


def test_rpc_rejects_malformed_stage_result(ophyd_rpc, rpc_cls, dev_mock, instr):
    instr.parameter["func"] = "stage"
    with (
        mock.patch.object(dev_mock.obj, "stage", return_value=[object()]),
        pytest.raises(AttributeError, match="_staged"),
    ):
        ophyd_rpc.run(dev_mock, instr, rpc_cls)


@pytest.mark.parametrize(
    "statuses_done, finishes_instruction",
    [([], True), ([True], True), ([False], False), ([True, False], False)],
)
def test_send_rpc_exception(
    rpc_cls: RPCHandler,
    instr: messages.DeviceInstructionMessage,
    statuses_done,
    finishes_instruction,
):
    rpc_cls.requests_handler.get_request.return_value = {
        "status_objects": [mock.Mock(done=done) for done in statuses_done]
    }
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
        expire=1800,
    )
    if finishes_instruction:
        rpc_cls.requests_handler.set_finished.assert_called_once_with(
            "diid", success=False, error_info=error_info
        )
    else:
        rpc_cls.requests_handler.set_finished.assert_not_called()


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


def test_rpc_dispatches_to_the_registered_provider(rpc_cls, instr):
    device = mock.Mock()
    rpc_cls.device_manager.devices = {"device": device}
    providers = rpc_cls.device_manager.device_provider_manager
    providers.run_rpc.return_value = {"value": 42}
    instr.device = "device.component"

    assert rpc_cls.process_rpc_instruction(instr) == {"value": 42}
    providers.run_rpc.assert_called_once_with(device, instr, rpc_cls)


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
    dev_mock.name = "device"
    dev_mock.read_only = False
    dev_mock.obj = mock.MagicMock(spec=Device)
    dev_mock.obj.name = "device"
    dev_mock.obj.root = dev_mock.obj
    dev_mock.obj.dotted_name = ""
    dev_mock.obj.readback = mock.MagicMock(spec=Signal)
    dev_mock.obj.readback.kind = Kind.hinted
    dev_mock.obj.user_setpoint = mock.MagicMock(spec=Signal)
    dev_mock.obj.user_setpoint.kind = Kind.normal
    dev_mock.obj.velocity = mock.MagicMock(spec=Signal)
    dev_mock.obj.velocity.kind = Kind.config
    dev_mock.obj.notused = mock.MagicMock(spec=Signal)
    dev_mock.obj.notused.kind = Kind.omitted
    for path in ("readback", "user_setpoint", "velocity", "notused"):
        component = getattr(dev_mock.obj, path)
        component.root = dev_mock.obj
        component.dotted_name = path
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
def test_process_rpc_instruction_read(ophyd_rpc, dev_mock, instr, func, read_called, rpc_cls):
    instr.content["parameter"]["func"] = func
    rpc_cls.device_server._read_and_update_devices = mock.MagicMock()
    rpc_cls.device_server._read_config_and_update_devices = mock.MagicMock()
    ophyd_rpc.run(dev_mock, instr, rpc_cls)
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


def test_process_rpc_instruction_with_status_return(ophyd_rpc, dev_mock, instr, rpc_cls):
    with mock.patch.object(ophyd_rpc, "_execute_rpc_call") as rpc_result:
        status = StatusBase()
        rpc_result.return_value = status
        res = ophyd_rpc.run(dev_mock, instr, rpc_cls)
        assert res == {
            "type": "status",
            "RID": "RID",
            "success": status.success,
            "timeout": status.timeout,
            "done": status.done,
            "settle_time": status.settle_time,
        }
        status.set_finished()


def test_process_rpc_instruction_with_namedtuple_return(ophyd_rpc, dev_mock, instr, rpc_cls):
    with mock.patch.object(ophyd_rpc, "_execute_rpc_call") as rpc_result:
        point_type = namedtuple("Point", ["x", "y"])
        point_tuple = point_type(5, 6)
        rpc_result.return_value = point_tuple
        res = ophyd_rpc.run(dev_mock, instr, rpc_cls)
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
def test_process_rpc_instruction_with_list_return(
    ophyd_rpc, dev_mock, instr, return_val, result, rpc_cls
):
    with mock.patch.object(ophyd_rpc, "_execute_rpc_call") as rpc_result:
        rpc_result.return_value = return_val
        res = ophyd_rpc.run(dev_mock, instr, rpc_cls)
        assert res == result


def test_process_rpc_instruction_set_attribute(ophyd_rpc, dev_mock, instr, rpc_cls):
    instr.content["parameter"]["kwargs"] = {"_set_property": True}
    instr.content["parameter"]["args"] = [5]
    instr.content["parameter"]["func"] = "attr_value"
    ophyd_rpc.run(dev_mock, instr, rpc_cls)
    assert dev_mock.obj.attr_value == 5


def test_process_rpc_instruction_set_attribute_on_sub_device(ophyd_rpc, dev_mock, instr, rpc_cls):
    instr.content["parameter"]["kwargs"] = {"_set_property": True}
    instr.content["parameter"]["args"] = [5]
    instr.content["parameter"]["func"] = "user_setpoint.attr_value"
    ophyd_rpc.run(dev_mock, instr, rpc_cls)
    assert dev_mock.obj.user_setpoint.attr_value == 5


@pytest.mark.parametrize(
    "parameter",
    [
        {"func": "velocity.set", "args": [5]},
        {"func": "user_setpoint.put", "args": [5]},
        {"func": "attr_value", "args": [5], "kwargs": {"_set_property": True}},
    ],
)
def test_process_rpc_instruction_rejects_write_calls_for_read_only_device(
    ophyd_rpc, dev_mock, parameter, rpc_cls
):
    dev_mock.name = "device"
    dev_mock.read_only = True
    instr = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter=parameter,
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )

    with mock.patch.object(ophyd_rpc, "_execute_rpc_call") as mock_execute_rpc:
        with pytest.raises(
            DisabledDeviceError, match="Setting the device device is currently disabled."
        ):
            ophyd_rpc.run(dev_mock, instr, rpc_cls)

    mock_execute_rpc.assert_not_called()


def test_set_config_signal_updates_cache(ophyd_rpc, dev_mock, instr, rpc_cls):
    instr_params = {"func": "velocity.set", "args": [10]}
    ophyd_rpc._update_cache = mock.MagicMock()
    ophyd_rpc._execute_rpc_call = mock.MagicMock()
    ophyd_rpc._execute_rpc_call.return_value = DeviceStatus(
        device=dev_mock.obj, done=True, success=True
    )
    instr = messages.DeviceInstructionMessage(
        device="device",
        action="rpc",
        parameter=instr_params,
        metadata={"RID": "RID", "device_instr_id": "diid"},
    )
    ophyd_rpc.run(dev_mock, instr, rpc_cls)
    ophyd_rpc._update_cache.assert_called_once_with(dev_mock.obj.velocity, instr, rpc_cls)


def test_pending_set_refresh_keeps_its_rpc_handler(ophyd_rpc, dev_mock, instr):
    provider = ophyd_rpc.provider
    first_handler = mock.Mock(spec=RPCHandler)
    second_handler = mock.Mock(spec=RPCHandler)
    set_instruction = instr.model_copy(update={"parameter": {"func": "velocity.set", "args": [5]}})
    status = DeviceStatus(dev_mock.obj.velocity)
    dev_mock.obj.velocity.set.return_value = status
    dev_mock.obj.trigger.return_value = 17

    provider.run_rpc(dev_mock, set_instruction, first_handler)
    first_handler.read_rpc.assert_not_called()
    assert provider.run_rpc(dev_mock, instr, second_handler) == 17
    status.set_finished()

    first_handler.read_rpc.assert_called_once_with(set_instruction, configuration=True)
    second_handler.read_rpc.assert_not_called()


def test_update_cache_config_kind(ophyd_rpc, instr, mock_rpc_methods, rpc_cls):
    """Test that _update_cache calls read_configuration for config kind signals"""
    signal_mock = mock.MagicMock(spec=Signal)
    signal_mock.kind = Kind.config
    mock_rpc_methods["read_config"].return_value = {"config": "data"}

    result = ophyd_rpc._update_cache(signal_mock, instr, rpc_cls)

    mock_rpc_methods["read_config"].assert_called_once_with(instr, rpc_cls)
    mock_rpc_methods["read"].assert_not_called()
    assert result == {"config": "data"}


def test_update_cache_hinted_kind(ophyd_rpc, instr, mock_rpc_methods, rpc_cls):
    """Test that _update_cache calls read for hinted kind signals"""
    signal_mock = mock.MagicMock(spec=Signal)
    signal_mock.kind = Kind.hinted
    mock_rpc_methods["read"].return_value = {"read": "data"}

    result = ophyd_rpc._update_cache(signal_mock, instr, rpc_cls)

    mock_rpc_methods["read"].assert_called_once_with(instr, rpc_cls)
    mock_rpc_methods["read_config"].assert_not_called()
    assert result == {"read": "data"}


def test_update_cache_normal_kind(ophyd_rpc, instr, mock_rpc_methods, rpc_cls):
    """Test that _update_cache calls read for normal kind signals"""
    signal_mock = mock.MagicMock(spec=Signal)
    signal_mock.kind = Kind.normal
    mock_rpc_methods["read"].return_value = {"read": "data"}

    result = ophyd_rpc._update_cache(signal_mock, instr, rpc_cls)

    mock_rpc_methods["read"].assert_called_once_with(instr, rpc_cls)
    mock_rpc_methods["read_config"].assert_not_called()
    assert result == {"read": "data"}


def test_update_cache_omitted_kind(ophyd_rpc, instr, mock_rpc_methods, rpc_cls):
    """Test that _update_cache calls both read methods for omitted kind signals"""
    signal_mock = mock.MagicMock(spec=Signal)
    signal_mock.kind = Kind.omitted
    mock_rpc_methods["read_config"].return_value = {"config": "data"}

    result = ophyd_rpc._update_cache(signal_mock, instr, rpc_cls)

    mock_rpc_methods["read"].assert_called_once_with(instr, rpc_cls)
    mock_rpc_methods["read_config"].assert_called_once_with(instr, rpc_cls)
    assert result == {"config": "data"}


@pytest.mark.parametrize("kind", [Kind.normal | Kind.config, Kind.hinted | Kind.config])
def test_rpc_mixed_kind_cache_refresh_order(ophyd_rpc, instr, kind, rpc_cls):
    """Mixed native kinds refresh readings before configuration, unlike monitor selection."""
    target = mock.Mock(kind=kind)
    order = []
    with (
        mock.patch.object(
            ophyd_rpc, "_rpc_read_and_return", side_effect=lambda *_: order.append("read")
        ),
        mock.patch.object(
            ophyd_rpc,
            "_rpc_read_configuration_and_return",
            side_effect=lambda *_: order.append("configuration"),
        ),
    ):
        ophyd_rpc._update_cache(target, instr, rpc_cls)
    assert order == ["read", "configuration"]
