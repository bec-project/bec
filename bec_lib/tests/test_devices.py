from typing import Any, Callable, Literal
from unittest import mock

import pytest
from typeguard import TypeCheckError

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.config_helper import ConfigHelper
from bec_lib.device import (
    AdjustableMixin,
    ComputedSignal,
    Device,
    DeviceBaseWithConfig,
    Positioner,
    ReadoutPriority,
    RPCError,
    Signal,
    Status,
    set_device_config,
)
from bec_lib.devicemanager import DeviceContainer, DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.fixtures import device_manager_class
from bec_lib.tests.utils import ClientMock, ConnectorMock, get_device_info_mock

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


@pytest.fixture(name="dev")
def fixture_dev(bec_client_mock: ClientMock | BECClient):
    yield bec_client_mock.device_manager.devices


def test_nested_device_root(dev: Any):
    assert dev.dyn_signals.name == "dyn_signals"
    assert dev.dyn_signals.messages.name == "messages"
    assert dev.dyn_signals.root == dev.dyn_signals
    assert dev.dyn_signals.messages.root == dev.dyn_signals


def test_read(dev: Any):
    with mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get:
        mock_get.return_value = messages.DeviceMessage(
            signals={
                "samx": {"value": 0, "timestamp": 1701105880.1711318},
                "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
                "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
            },
            metadata={"scan_id": "scan_id", "scan_type": "scan_type"},
        )
        res = dev.samx.read(cached=True)
        mock_get.assert_called_once_with(MessageEndpoints.device_readback("samx"))
        assert res == {
            "samx": {"value": 0, "timestamp": 1701105880.1711318},
            "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
            "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
        }


def test_read_filtered_hints(dev: Any):
    with mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get:
        mock_get.return_value = messages.DeviceMessage(
            signals={
                "samx": {"value": 0, "timestamp": 1701105880.1711318},
                "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
                "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
            },
            metadata={"scan_id": "scan_id", "scan_type": "scan_type"},
        )
        res = dev.samx.read(cached=True, filter_to_hints=True)
        mock_get.assert_called_once_with(MessageEndpoints.device_readback("samx"))
        assert res == {"samx": {"value": 0, "timestamp": 1701105880.1711318}}


def test_read_use_read(dev: Any):
    with mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get:
        data = {
            "samx": {"value": 0, "timestamp": 1701105880.1711318},
            "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
            "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
        }
        mock_get.return_value = messages.DeviceMessage(
            signals=data, metadata={"scan_id": "scan_id", "scan_type": "scan_type"}
        )
        res = dev.samx.read(cached=True, use_readback=False)
        mock_get.assert_called_once_with(MessageEndpoints.device_read("samx"))
        assert res == data


def test_read_nested_device(dev: Any):
    with mock.patch.object(dev.dyn_signals.root.parent.connector, "get") as mock_get:
        data = {
            "dyn_signals_messages_message1": {"value": 0, "timestamp": 1701105880.0716832},
            "dyn_signals_messages_message2": {"value": 0, "timestamp": 1701105880.071722},
            "dyn_signals_messages_message3": {"value": 0, "timestamp": 1701105880.071739},
            "dyn_signals_messages_message4": {"value": 0, "timestamp": 1701105880.071753},
            "dyn_signals_messages_message5": {"value": 0, "timestamp": 1701105880.071766},
        }
        mock_get.return_value = messages.DeviceMessage(
            signals=data, metadata={"scan_id": "scan_id", "scan_type": "scan_type"}
        )
        res = dev.dyn_signals.messages.read(cached=True)
        mock_get.assert_called_once_with(MessageEndpoints.device_readback("dyn_signals"))
        assert res == data


@pytest.mark.parametrize(
    "kind,cached", [("normal", True), ("hinted", True), ("config", False), ("omitted", False)]
)
def test_read_kind_hinted(
    dev: Any,
    kind: Literal["normal"] | Literal["hinted"] | Literal["config"] | Literal["omitted"],
    cached: bool,
):
    with (
        mock.patch.object(dev.samx.readback, "_run") as mock_run,
        mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get,
    ):
        data = {
            "samx": {"value": 0, "timestamp": 1701105880.1711318},
            "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
            "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
        }
        mock_get.return_value = messages.DeviceMessage(
            signals=data, metadata={"scan_id": "scan_id", "scan_type": "scan_type"}
        )
        dev.samx.readback._signal_info["kind_str"] = f"Kind.{kind}"
        res = dev.samx.readback.read(cached=cached)
        if cached:
            mock_get.assert_called_once_with(MessageEndpoints.device_readback("samx"))
            mock_run.assert_not_called()
            assert res == {"samx": {"value": 0, "timestamp": 1701105880.1711318}}
        else:
            mock_run.assert_called_once_with(cached=False, fcn=dev.samx.readback.read)
            mock_get.assert_not_called()


@pytest.mark.parametrize(
    "is_signal,is_config_signal,method",
    [
        (True, False, "read"),
        (False, True, "read_configuration"),
        (False, False, "read_configuration"),
    ],
)
def test_read_configuration_not_cached(
    dev: Any,
    is_signal: bool,
    is_config_signal: bool,
    method: Literal["read"] | Literal["read_configuration"],
):
    with (
        mock.patch.object(
            dev.samx.readback,
            "_get_rpc_signal_info",
            return_value=(is_signal, is_config_signal, False),
        ),
        mock.patch.object(dev.samx.readback, "_run") as mock_run,
    ):
        dev.samx.readback.read_configuration(cached=False)
        mock_run.assert_called_once_with(cached=False, fcn=getattr(dev.samx.readback, method))


@pytest.mark.parametrize(
    "is_signal,is_config_signal,method",
    [(True, False, "read"), (False, True, "redis"), (False, False, "redis")],
)
def test_read_configuration_cached(
    dev: Any, is_signal: bool, is_config_signal: bool, method: Literal["read"] | Literal["redis"]
):
    with (
        mock.patch.object(
            dev.samx.readback,
            "_get_rpc_signal_info",
            return_value=(is_signal, is_config_signal, True),
        ),
        mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get,
        mock.patch.object(dev.samx.readback, "read") as mock_read,
    ):
        mock_get.return_value = messages.DeviceMessage(
            signals={
                "samx": {"value": 0, "timestamp": 1701105880.1711318},
                "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
                "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
            },
            metadata={"scan_id": "scan_id", "scan_type": "scan_type"},
        )
        dev.samx.readback.read_configuration(cached=True)
        if method == "redis":
            mock_get.assert_called_once_with(MessageEndpoints.device_read_configuration("samx"))
            mock_read.assert_not_called()
        else:
            mock_read.assert_called_once_with(cached=True)
            mock_get.assert_not_called()


@pytest.mark.parametrize(
    ["mock_rpc", "method", "args", "kwargs", "expected_call"],
    [
        ("_get_rpc_response", "set", (1,), {}, (mock.ANY, mock.ANY)),
        ("_run_rpc_call", "set", (1,), {}, ("samx", "setpoint.set", 1)),
        ("_run_rpc_call", "put", (1,), {"wait": True}, ("samx", "setpoint.set", 1)),
        ("_run_rpc_call", "put", (1,), {}, ("samx", "setpoint.put", 1)),
    ],
)
def test_run_rpc_call(dev: Any, mock_rpc, method, args, kwargs, expected_call):
    dev.samx.root.parent.parent.queue.get_default_scan_queue.return_value = "primary"
    with mock.patch.object(dev.samx.setpoint, "_validate_rpc_client"):
        with mock.patch.object(dev.samx.setpoint, mock_rpc) as mock_rpc:
            getattr(dev.samx.setpoint, method)(*args, **kwargs)
            mock_rpc.assert_called_once_with(*expected_call)


def test_get_rpc_func_name_read(dev: Any):
    with mock.patch.object(dev.samx, "_run_rpc_call") as mock_rpc:
        dev.samx.read(cached=False)
        mock_rpc.assert_called_once_with("samx", "read")


@pytest.mark.parametrize(
    "kind,cached", [("normal", True), ("hinted", True), ("config", False), ("omitted", False)]
)
def test_get_rpc_func_name_readback_get(
    dev: Any,
    kind: Literal["normal"] | Literal["hinted"] | Literal["config"] | Literal["omitted"],
    cached: bool,
):
    with (
        mock.patch.object(dev.samx.readback, "_run") as mock_rpc,
        mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get,
    ):
        mock_get.return_value = messages.DeviceMessage(
            signals={
                "samx": {"value": 0, "timestamp": 1701105880.1711318},
                "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
                "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
            },
            metadata={"scan_id": "scan_id", "scan_type": "scan_type"},
        )
        dev.samx.readback._signal_info["kind_str"] = f"Kind.{kind}"
        dev.samx.readback.get(cached=cached)
        if cached:
            mock_get.assert_called_once_with(MessageEndpoints.device_readback("samx"))
            mock_rpc.assert_not_called()
        else:
            mock_rpc.assert_called_once_with(cached=False, fcn=dev.samx.readback.get)
            mock_get.assert_not_called()


def test_get_rpc_func_name_nested(dev: Any):
    with mock.patch.object(
        dev.rt_controller._custom_rpc_methods["dummy_controller"]._custom_rpc_methods[
            "_func_with_args"
        ],
        "_run_rpc_call",
    ) as mock_rpc:
        dev.rt_controller.dummy_controller._func_with_args(1, 2)
        mock_rpc.assert_called_once_with("rt_controller", "dummy_controller._func_with_args", 1, 2)


def test_handle_rpc_response(dev: Any):
    msg = messages.DeviceRPCMessage(device="samx", return_val=1, out="done", success=True)
    assert dev.samx._handle_rpc_response(msg) == 1


def test_handle_rpc_response_returns_status(dev: Any, bec_client_mock: ClientMock | BECClient):
    msg = messages.DeviceRPCMessage(
        device="samx", return_val={"type": "status", "RID": "request_id"}, out="done", success=True
    )
    assert dev.samx._handle_rpc_response(msg) == Status(
        bec_client_mock.device_manager.connector, "request_id"
    )


def test_rpc_status_raises_error(dev: Any):
    msg = messages.DeviceReqStatusMessage(device="samx", success=False, request_id="request_id")
    connector = mock.MagicMock()
    status = Status(connector, "request_id")
    status._on_status_update({"data": msg}, parent=status)
    with pytest.raises(RPCError):
        status.wait(raise_on_failure=True)

    status.wait(raise_on_failure=False)


def test_handle_rpc_response_raises(dev: Any):
    msg = messages.DeviceRPCMessage(
        device="samx",
        return_val={"type": "status", "RID": "request_id"},
        out=messages.ErrorInfo(
            exception_type="RPCError", error_message="An error occurred", compact_error_message=None
        ),
        success=False,
    )
    with pytest.raises(RPCError):
        dev.samx._handle_rpc_response(msg)


def test_handle_rpc_response_returns_dict(dev: Any):
    msg = messages.DeviceRPCMessage(device="samx", return_val={"a": "b"}, out="done", success=True)
    assert dev.samx._handle_rpc_response(msg) == {"a": "b"}


def test_run_rpc_call_calls_stop_on_keyboardinterrupt(dev: Any):
    with mock.patch.object(dev.samx.setpoint, "_prepare_rpc_msg") as mock_rpc:
        with mock.patch.object(dev.samx.setpoint, "_validate_rpc_client"):
            mock_rpc.side_effect = [KeyboardInterrupt]
            with pytest.raises(RPCError, match="User interruption during RPC call."):
                with mock.patch.object(dev.samx, "stop") as mock_stop:
                    dev.samx.setpoint.set(1)
            mock_rpc.assert_called_once()
            mock_stop.assert_called_once()


@pytest.fixture
def device_config():
    return {
        "id": "1c6518b2-b779-4b28-b8b1-31295f8fbf26",
        "accessGroups": "customer",
        "name": "eiger",
        "sessionId": "569ea788-09d7-44fc-a140-b0b34a2b7f6f",
        "enabled": True,
        "readOnly": False,
        "readoutPriority": "monitored",
        "deviceClass": "SimCamera",
        "deviceConfig": {"device_access": True, "labels": "eiger", "name": "eiger"},
        "deviceTags": {"detector"},
    }


BASIC_CONFIG = {
    "enabled": True,
    "deviceClass": "TestDevice",
    "readoutPriority": ReadoutPriority.MONITORED.value,
}


@pytest.fixture
def dev_w_config():
    def _func(config: dict = {}):
        dm_base = DeviceManagerBase(mock.MagicMock())
        dm_base.config_helper = mock.MagicMock(spec=ConfigHelper)
        return DeviceBaseWithConfig(name="test", config=BASIC_CONFIG | config, parent=dm_base)

    return _func


@pytest.fixture
def device_obj(device_config: dict[str, Any]):
    service_mock = mock.MagicMock()
    service_mock.connector = ConnectorMock("")
    dm = DeviceManagerBase(service_mock)
    info = get_device_info_mock(device_config["name"], device_config["deviceClass"])
    dm._add_device(device_config, info)
    yield dm.devices[device_config["name"]]


def test_create_device_saves_config(
    device_obj: DeviceBaseWithConfig, device_config: dict[str, Any]
):
    assert {k: v for k, v in device_obj._config.items() if k in device_config} == device_config


def test_device_enabled(device_obj: DeviceBaseWithConfig, device_config: dict[str, Any]):
    assert device_obj.enabled == device_config["enabled"]
    device_config["enabled"] = False
    set_device_config(device_obj, device_config)
    assert device_obj.enabled == device_config["enabled"]


def test_device_enable(device_obj: DeviceBaseWithConfig):
    with mock.patch.object(device_obj.parent.config_helper, "send_config_request") as config_req:
        device_obj.enabled = True
        config_req.assert_called_once_with(
            action="update", config={device_obj.name: {"enabled": True}}
        )


def test_device_enable_set(device_obj: DeviceBaseWithConfig):
    with mock.patch.object(device_obj.parent.config_helper, "send_config_request") as config_req:
        device_obj.read_only = False
        config_req.assert_called_once_with(
            action="update", config={device_obj.name: {"readOnly": False}}
        )


@pytest.mark.parametrize(
    "val,raised_error",
    [({"in": 5}, None), ({"in": 5, "out": 10}, None), ({"5", "4"}, TypeCheckError)],
)
def test_device_set_user_parameter(
    device_obj: DeviceBaseWithConfig,
    val: dict[str, int] | set[str],
    raised_error: None | TypeCheckError,
):
    with mock.patch.object(device_obj.parent.config_helper, "send_config_request") as config_req:
        if raised_error is None:
            device_obj.set_user_parameter(val)
            config_req.assert_called_once_with(
                action="update", config={device_obj.name: {"userParameter": val}}
            )
        else:
            with pytest.raises(raised_error):
                device_obj.set_user_parameter(val)


@pytest.mark.parametrize(
    "user_param,val,out,raised_error",
    [
        ({"in": 2, "out": 5}, {"in": 5}, {"in": 5, "out": 5}, None),
        ({"in": 2, "out": 5}, {"in": 5, "out": 10}, {"in": 5, "out": 10}, None),
        ({"in": 2, "out": 5}, {"5", "4"}, None, TypeCheckError),
        (None, {"in": 5}, {"in": 5}, None),
    ],
)
def test_device_update_user_parameter(
    device_obj: DeviceBaseWithConfig,
    user_param: dict[str, int] | None,
    val: dict[str, int] | set[str],
    out: dict[str, int] | None,
    raised_error: None | TypeCheckError,
):
    device_obj._config["userParameter"] = user_param
    with mock.patch.object(device_obj.parent.config_helper, "send_config_request") as config_req:
        if raised_error is None:
            device_obj.update_user_parameter(val)
            config_req.assert_called_once_with(
                action="update", config={device_obj.name: {"userParameter": out}}
            )
        else:
            with pytest.raises(raised_error):
                device_obj.update_user_parameter(val)


def test_status_wait():
    connector = mock.MagicMock()

    status = Status(connector, "test_rid")

    with pytest.raises(TimeoutError):
        status.wait(0.1)
    status._on_status_update(
        {
            "data": messages.DeviceReqStatusMessage(
                device="test_device", success=True, request_id="test_rid"
            )
        },
        parent=status,
    )
    status.wait()


@pytest.fixture
def device_w_tags(dev_w_config: Callable[..., DeviceBaseWithConfig]):
    yield dev_w_config({"deviceTags": {"tag1", "tag2"}})


@pytest.mark.parametrize(
    ["method", "args", "result"],
    [
        ("set_device_tags", {"tag3", "tag4"}, {"tag3", "tag4"}),
        ("set_device_tags", ["tag3", "tag3", "tag3", "tag4"], {"tag3", "tag4"}),
        ("add_device_tag", "tag3", {"tag1", "tag2", "tag3"}),
        ("add_device_tag", "tag1", {"tag1", "tag2"}),
        ("remove_device_tag", "tag1", {"tag2"}),
    ],
)
def test_set_device_tags(device_w_tags, method, args, result):
    getattr(device_w_tags, method)(args)
    assert device_w_tags.get_device_tags() == result


def test_device_wm(device_w_tags):
    with mock.patch.object(DeviceContainer, "wm") as mock_wm:
        _ = device_w_tags.wm
        mock_wm.assert_called_once_with(device_w_tags.name)


@pytest.mark.parametrize(
    ["config", "attr", "value"],
    [
        ({"onFailure": "buffer"}, "on_failure", "buffer"),
        ({"readoutPriority": "baseline"}, "readout_priority", "baseline"),
        ({"read_only": False}, "read_only", False),
    ],
)
def test_properties(dev_w_config: Callable[..., DeviceBaseWithConfig], config, attr, value):
    assert getattr(dev_w_config(config), attr) == value


@pytest.mark.parametrize(
    ["config", "method", "value"],
    [({"deviceTags": {"tag1", "tag2"}}, "get_device_tags", {"tag1", "tag2"})],
)
def test_methods(dev_w_config: Callable[..., DeviceBaseWithConfig], config, method, value):
    assert getattr(dev_w_config(config), method)() == value


@pytest.mark.parametrize(
    ["config", "attr", "value", "result"],
    [
        ({"readoutPriority": "baseline"}, "readout_priority", "monitored", "monitored"),
        ({"onFailure": "buffer"}, "on_failure", "retry", "retry"),
        ({"read_only": False}, "read_only", True, True),
    ],
)
def test_properties_assign(
    dev_w_config: Callable[..., DeviceBaseWithConfig], config, attr, value, result
):
    device = dev_w_config(config)
    setattr(device, attr, value)
    assert getattr(device, attr) == result
    device.parent.config_helper.send_config_request.assert_called_once()


@pytest.fixture
def dev_container():
    (devs := DeviceContainer())["test"] = Device(
        name="test", config=BASIC_CONFIG, parent=mock.MagicMock(spec=DeviceManagerBase)
    )
    return devs


def test_device_container_wm(dev_container, capsys):
    with mock.patch.object(dev_container.test, "read", return_value={"test": {"value": 1}}) as read:
        dev_container.wm("test")
        dev_container.wm("tes*")
        captured = capsys.readouterr()
        assert "test" in captured.out
        assert "1.0000" in captured.out


def test_device_container_wm_read_contains_None(dev_container, capsys):
    with mock.patch.object(
        dev_container.test, "read", return_value={"test": {"value": None}}
    ) as read:
        dev_container.wm("test")
        captured = capsys.readouterr()
        assert "test" in captured.out
        assert "N/A" in captured.out


@pytest.mark.parametrize(
    "reading",
    [
        {"test": {"value": 1}, "test_setpoint": {"value": 1}},
        {"test": {"value": 1}, "test_user_setpoint": {"value": 1}},
    ],
)
def test_device_container_wm_with_setpoint_names(dev_container, reading):
    with mock.patch.object(dev_container.test, "read", return_value=reading) as read:
        dev_container.wm("test")


@pytest.mark.parametrize("device_cls", [Device, Signal, Positioner])
def test_device_has_describe_method(device_cls: Device | Signal | Positioner, dev_container):
    parent = mock.MagicMock(spec=DeviceManagerBase)
    dev_container["test"] = device_cls(name="test", config=BASIC_CONFIG, parent=parent)
    assert hasattr(dev_container.test, "describe")
    with mock.patch.object(dev_container.test, "_run_rpc_call") as mock_rpc:
        dev_container.test.describe()
        mock_rpc.assert_not_called()


@pytest.mark.parametrize("device_cls", [Device, Signal, Positioner])
def test_device_has_describe_configuration_method(device_cls: Device | Signal | Positioner):
    devs = DeviceContainer()
    parent = mock.MagicMock(spec=DeviceManagerBase)
    devs["test"] = device_cls(name="test", config=BASIC_CONFIG, parent=parent)
    assert hasattr(devs.test, "describe_configuration")
    with mock.patch.object(devs.test, "_run_rpc_call") as mock_rpc:
        devs.test.describe_configuration()
        mock_rpc.assert_not_called()


def test_show_all():
    # Create a mock Console object
    console = mock.MagicMock()
    parent = mock.MagicMock()
    parent.parent = mock.MagicMock(spec=DeviceManagerBase)

    # Create a DeviceContainer with some mock Devices
    devs = DeviceContainer()
    devs["dev1"] = DeviceBaseWithConfig(
        name="dev1",
        config={
            "description": "Device 1",
            "enabled": True,
            "readOnly": False,
            "deviceClass": "Class1",
            "readoutPriority": "monitored",
            "deviceTags": {"tag1", "tag2"},
        },
        parent=parent,
    )
    devs["dev2"] = DeviceBaseWithConfig(
        name="dev2",
        config={
            "description": "Device 2",
            "enabled": False,
            "readOnly": True,
            "deviceClass": "Class2",
            "readoutPriority": "baseline",
            "deviceTags": {"tag3", "tag4"},
        },
        parent=parent,
    )

    # Call show_all with the mock Console
    devs.show_all(console)

    # check that the device names were printed
    table = console.print.call_args[0][0]
    assert len(table.rows) == 2
    assert list(table.columns[0].cells) == ["dev1", "dev2"]
    # Check that Console.print was called with a Table containing the correct data
    console.print.assert_called_once()


@pytest.fixture()
def adj():
    (adj := AdjustableMixin()).root = mock.MagicMock()
    adj._update_config = mock.MagicMock()
    return adj


def test_adjustable_mixin_limits(adj):
    adj.root.parent.connector.get.return_value = messages.DeviceMessage(
        signals={"low": {"value": -12}, "high": {"value": 12}}, metadata={}
    )
    assert adj.limits == [-12, 12]


def test_adjustable_mixin_limits_missing(adj):
    adj.root.parent.connector.get.return_value = None
    assert adj.limits == [0, 0]


def test_adjustable_mixin_set_limits(adj):
    adj.limits = [-12, 12]
    adj._update_config.assert_called_once_with({"deviceConfig": {"limits": [-12, 12]}})


def test_adjustable_mixin_set_low_limit(adj):
    adj.root.parent.connector.get.return_value = messages.DeviceMessage(
        signals={"low": {"value": -12}, "high": {"value": 12}}, metadata={}
    )
    adj.low_limit = -20
    adj._update_config.assert_called_once_with({"deviceConfig": {"limits": [-20, 12]}})


def test_adjustable_mixin_set_high_limit(adj):
    adj.root.parent.connector.get.return_value = messages.DeviceMessage(
        signals={"low": {"value": -12}, "high": {"value": 12}}, metadata={}
    )
    adj.high_limit = 20
    adj._update_config.assert_called_once_with({"deviceConfig": {"limits": [-12, 20]}})


def test_computed_signal_set_compute_method():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())

    def my_compute_method():
        return "a + b"

    with mock.patch.object(comp_signal, "_update_config") as _update_config:
        comp_signal.set_compute_method(my_compute_method)
        _update_config.assert_called_once_with(
            {
                "deviceConfig": {
                    "compute_method": comp_signal._header
                    + '    def my_compute_method():\n        return "a + b"\n'
                }
            }
        )


def test_computed_signal_set_bound_method_raises_error():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())

    class MyClass:
        def my_method(self):
            return "a + b"

    my_instance = MyClass()

    with pytest.raises(ValueError) as excinfo:
        comp_signal.set_compute_method(my_instance.my_method)
    assert "bound method" in str(excinfo.value)


def test_computed_signal_set_lambda_raises_error():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())

    with pytest.raises(ValueError) as excinfo:
        comp_signal.set_compute_method(lambda: "a + b")
    assert "lambda function" in str(excinfo.value)


def test_computed_signal_show_all():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())
    comp_signal._config = {
        "deviceConfig": {
            "input_signals": ["a", "b"],
            "compute_method": "def compute():\n    return a + b\n",
        }
    }
    with mock.patch("rich.console.Console.print") as mock_print:
        comp_signal.show_all()
        table = mock_print.call_args[0][0]
        assert table.title == "comp_signal - ComputedSignal Information"
        assert [col.header for col in table.columns] == ["Property", "Value"]

        cells = []
        for column in table.columns:
            for cell in column.cells:
                cells.append(cell)

        assert cells == [
            "Compute Method",
            "Input Signals",
            "def compute():\n    return a + b",
            "a, b",
        ]


def test_computed_signal_set_signals():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())
    with mock.patch.object(comp_signal, "_update_config") as _update_config:
        comp_signal.set_input_signals(
            Signal(name="a", parent=mock.MagicMock(spec=DeviceManagerBase)),
            Signal(name="b", parent=mock.MagicMock(spec=DeviceManagerBase)),
        )
        _update_config.assert_called_once_with({"deviceConfig": {"input_signals": ["a", "b"]}})


def test_computed_signal_set_signals_raises_error():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())
    with pytest.raises(ValueError):
        comp_signal.set_input_signals("a", "b")


def test_computed_signal_set_signals_empty():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())
    with mock.patch.object(comp_signal, "_update_config") as _update_config:
        comp_signal.set_input_signals()
        _update_config.assert_called_once_with({"deviceConfig": {"input_signals": []}})


def test_computed_signal_raises_error_on_set_compute_method():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())
    with pytest.raises(ValueError):
        comp_signal.set_compute_method("a + b")


def test_device_summary(dev: Any):
    """Test that the device summary method creates a table with the correct structure."""
    with mock.patch("rich.console.Console.print") as mock_print:
        dev.samx.summary()
        # verify that print was called with a Table object
        table = mock_print.call_args[0][0]
        assert table.title == "samx - Summary of Available Signals"
        assert [col.header for col in table.columns] == [
            "Name",
            "Data Name",
            "Kind",
            "Source",
            "Type",
            "Description",
        ]


def test_device_summary_signal_grouping(dev: Any):
    """Test that signals are correctly grouped by kind in the summary table."""

    with mock.patch("rich.console.Console.print"):
        with mock.patch("rich.table.Table.add_row") as mock_add_row:
            dev.samx.summary()

            num_rows = mock_add_row.call_count
            assert num_rows == len(dev.samx._info["signals"]) + 3  # 3 extra rows for headers

            assert mock_add_row.call_args_list[0][0] == (
                "readback",
                "samx",
                "hinted",
                "SIM:samx",
                "integer",
                "readback doc string",
            )
            assert mock_add_row.call_args_list[1][0] == tuple()
            assert mock_add_row.call_args_list[2][0] == (
                "setpoint",
                "samx_setpoint",
                "normal",
                "SIM:samx_setpoint",
                "integer",
                "setpoint doc string",
            )
            devs = [row_call[0][0] for row_call in mock_add_row.call_args_list if row_call[0]]
            assert devs == [
                "readback",
                "setpoint",
                "motor_is_moving",
                "velocity",
                "acceleration",
                "high_limit_travel",
                "low_limit_travel",
                "unused",
            ]


def test_device_summary_empty_signals(dev: Any):
    """Test that summary handles devices with no signals."""
    # Create a device with no signals
    device = Device(name="empty_device", info={"signals": {}})

    with mock.patch("rich.console.Console.print") as mock_print:
        device.summary()
        table = mock_print.call_args[0][0]

        # Verify table is created but empty
        assert table.title == "empty_device - Summary of Available Signals"
        assert len([row for row in table.rows if row]) == 0


def test_device_summary_bec_signals(dm_with_devices):
    """Test that BEC signals are correctly included in the summary."""
    dev = dm_with_devices.devices
    with mock.patch("rich.console.Console.print") as mock_print:
        with mock.patch("rich.table.Table.add_row") as mock_add_row:
            dev.eiger.summary()
            mock_add_row.assert_has_calls(
                [
                    mock.call(
                        "preview",
                        "eiger_preview",
                        "hinted",
                        "BECMessageSignal:eiger_preview",
                        "DevicePreviewMessage",
                        "",
                    )
                ]
            )
            assert mock_print.call_count == 1


def test_device_str(dm_with_devices):
    """Test that the device string representation includes the name and class."""
    dev = dm_with_devices.devices.eiger
    out = str(dev)
    assert "SimCamera(name=eiger, enabled=True)" == out


def test_device_repr_pretty_cycle(dm_with_devices):
    """Test that _repr_pretty_ uses simple representation when cycling."""
    dev = dm_with_devices.devices.eiger

    # Mock the pretty printer object
    class MockPrinter:
        def __init__(self):
            self.text_output = None

        def text(self, value):
            self.text_output = value

    p = MockPrinter()
    dev._repr_pretty_(p, cycle=True)

    assert p.text_output == "SimCamera(...)"


def test_device_repr_pretty_in_container(dm_with_devices):
    """Test that _repr_pretty_ uses simple representation when indented (in container)."""
    dev = dm_with_devices.devices.eiger

    # Mock the pretty printer object with indentation
    class MockPrinter:
        def __init__(self):
            self.text_output = None
            self.indentation = 1  # Simulate being in a container

        def text(self, value):
            self.text_output = value

    p = MockPrinter()
    dev._repr_pretty_(p, cycle=False)

    assert p.text_output == str(dev)


def test_device_repr_pretty_rich_format(dm_with_devices):
    """Test that _repr_pretty_ uses rich formatting when not cycling and not in container."""
    dev = dm_with_devices.devices.eiger

    # Mock the pretty printer object
    class MockPrinter:
        def __init__(self):
            self.text_output = None
            self.indentation = 0  # Not in a container

        def text(self, value):
            self.text_output = value

    with mock.patch.object(
        dev, "read", return_value={"eiger": {"value": 1, "timestamp": 1701105880.1711318}}
    ):
        p = MockPrinter()
        dev._repr_pretty_(p, cycle=False)

        # Check that we got rich formatted output (contains ANSI codes or table structure)
        assert p.text_output is not None
        assert (
            "SimCamera: eiger" in p.text_output or "[3m" in p.text_output
        )  # Rich formatting present


def test_device_compile_rich_str_with_values(dm_with_devices):
    """Test that _compile_rich_str creates output with current values."""
    import numpy as np

    dev = dm_with_devices.devices.eiger

    with mock.patch.object(
        dev,
        "read",
        return_value={
            "eiger": {"value": 5.0, "timestamp": 1701105880.1711318},
            "eiger_array": {"value": np.array([1, 2, 3, 4, 5]), "timestamp": 1701105880.1711318},
        },
    ):
        result = dev._compile_rich_str(dev)

        assert result is not None
        assert "Current Values" in result
        assert "eiger" in result
        assert "5.0" in result or "5" in result
        # Check numpy array formatting
        assert "shape=" in result


def test_device_compile_rich_str_with_config_signals(dev):
    """Test that _compile_rich_str includes config signals section."""
    # Mock device with config signals
    # Note: Only signals defined in _info with kind_str == "config" will be shown
    with mock.patch.object(dev.samx.parent, "get_device_config_cached") as mock_config:
        mock_config.return_value = {
            "samx": {"deviceConfig": {"velocity": 10.5, "acceleration": 5.0}}
        }

        with mock.patch.object(
            dev.samx, "read", return_value={"samx": {"value": 5.0, "timestamp": 1701105880.1711318}}
        ):
            result = dev.samx._compile_rich_str(dev.samx)

            assert result is not None
            assert "Config Signals" in result
            # velocity and acceleration are config signals in the samx device info
            assert "velocity" in result
            assert "10.5" in result


def test_rpc_call_without_client_raises(dm_with_devices):
    """Test that calling an RPC method without a client raises an error."""
    dev = dm_with_devices.devices.eiger
    dev.parent.parent = None  # Remove reference to DeviceManagerBase

    with pytest.raises(RPCError, match="RPC calls can only be made from a BECClient instance"):
        dev.read(cached=False)


def test_rpc_call_without_alarm_handler_raises(dev):
    """Test that calling an RPC method without an alarm handler raises an error."""
    import builtins

    original_isinstance = builtins.isinstance

    def isinstance_side_effect(obj, classinfo):
        """Side effect function to mock isinstance checks and identify BECClient instances."""
        if classinfo is BECClient:
            return True
        return original_isinstance(obj, classinfo)

    with mock.patch.object(dev.samx.root.parent.parent, "alarm_handler", None):
        with mock.patch("bec_lib.device.isinstance", side_effect=isinstance_side_effect):
            with pytest.raises(
                RPCError, match="RPC calls require an alarm handler to be set in the BECClient"
            ):
                dev.samx.read(cached=False)
