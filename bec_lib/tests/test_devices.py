from unittest import mock

import pytest
from typeguard import TypeCheckError

from bec_lib import messages
from bec_lib.device import (
    AdjustableMixin,
    ComputedSignal,
    Device,
    DeviceBase,
    Positioner,
    RPCError,
    Signal,
    Status,
)
from bec_lib.devicemanager import DeviceContainer, DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.utils import ConnectorMock, get_device_info_mock

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


@pytest.fixture(name="dev")
def fixture_dev(bec_client_mock):
    yield bec_client_mock.device_manager.devices


def test_nested_device_root(dev):
    assert dev.dyn_signals.name == "dyn_signals"
    assert dev.dyn_signals.messages.name == "messages"
    assert dev.dyn_signals.root == dev.dyn_signals
    assert dev.dyn_signals.messages.root == dev.dyn_signals


def test_read(dev):
    with mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get:
        mock_get.return_value = messages.DeviceMessage(
            signals={
                "samx": {"value": 0, "timestamp": 1701105880.1711318},
                "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
                "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
            },
            metadata={"scan_id": "scan_id", "scan_type": "scan_type"},
        )
        res = dev.samx.read()
        mock_get.assert_called_once_with(MessageEndpoints.device_readback("samx"))
        assert res == {
            "samx": {"value": 0, "timestamp": 1701105880.1711318},
            "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
            "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
        }


def test_read_filtered_hints(dev):
    with mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get:
        mock_get.return_value = messages.DeviceMessage(
            signals={
                "samx": {"value": 0, "timestamp": 1701105880.1711318},
                "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
                "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
            },
            metadata={"scan_id": "scan_id", "scan_type": "scan_type"},
        )
        res = dev.samx.read(filter_to_hints=True)
        mock_get.assert_called_once_with(MessageEndpoints.device_readback("samx"))
        assert res == {"samx": {"value": 0, "timestamp": 1701105880.1711318}}


def test_read_use_read(dev):
    with mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get:
        data = {
            "samx": {"value": 0, "timestamp": 1701105880.1711318},
            "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
            "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
        }
        mock_get.return_value = messages.DeviceMessage(
            signals=data, metadata={"scan_id": "scan_id", "scan_type": "scan_type"}
        )
        res = dev.samx.read(use_readback=False)
        mock_get.assert_called_once_with(MessageEndpoints.device_read("samx"))
        assert res == data


def test_read_nested_device(dev):
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
        res = dev.dyn_signals.messages.read()
        mock_get.assert_called_once_with(MessageEndpoints.device_readback("dyn_signals"))
        assert res == data


@pytest.mark.parametrize(
    "kind,cached", [("normal", True), ("hinted", True), ("config", False), ("omitted", False)]
)
def test_read_kind_hinted(dev, kind, cached):
    with mock.patch.object(dev.samx.readback, "_run") as mock_run:
        with mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get:
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
def test_read_configuration_not_cached(dev, is_signal, is_config_signal, method):
    with mock.patch.object(
        dev.samx.readback, "_get_rpc_signal_info", return_value=(is_signal, is_config_signal, False)
    ):
        with mock.patch.object(dev.samx.readback, "_run") as mock_run:
            dev.samx.readback.read_configuration(cached=False)
            mock_run.assert_called_once_with(cached=False, fcn=getattr(dev.samx.readback, method))


@pytest.mark.parametrize(
    "is_signal,is_config_signal,method",
    [(True, False, "read"), (False, True, "redis"), (False, False, "redis")],
)
def test_read_configuration_cached(dev, is_signal, is_config_signal, method):
    with mock.patch.object(
        dev.samx.readback, "_get_rpc_signal_info", return_value=(is_signal, is_config_signal, True)
    ):
        with mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get:
            mock_get.return_value = messages.DeviceMessage(
                signals={
                    "samx": {"value": 0, "timestamp": 1701105880.1711318},
                    "samx_setpoint": {"value": 0, "timestamp": 1701105880.1693492},
                    "samx_motor_is_moving": {"value": 0, "timestamp": 1701105880.16935},
                },
                metadata={"scan_id": "scan_id", "scan_type": "scan_type"},
            )
            with mock.patch.object(dev.samx.readback, "read") as mock_read:
                dev.samx.readback.read_configuration(cached=True)
                if method == "redis":
                    mock_get.assert_called_once_with(
                        MessageEndpoints.device_read_configuration("samx")
                    )
                    mock_read.assert_not_called()
                else:
                    mock_read.assert_called_once_with(cached=True)
                    mock_get.assert_not_called()


def test_run_rpc_call(dev):
    with mock.patch.object(dev.samx.setpoint, "_get_rpc_response") as mock_rpc:
        dev.samx.setpoint.set(1)
        mock_rpc.assert_called_once()


def test_set_calls_rpc(dev):
    with mock.patch.object(dev.samx.setpoint, "_run_rpc_call") as mock_rpc:
        dev.samx.setpoint.set(1)
        mock_rpc.assert_called_once_with("samx", "setpoint.set", 1)


def test_put_calls_set_if_wait(dev):
    with mock.patch.object(dev.samx.setpoint, "_run_rpc_call") as mock_rpc:
        dev.samx.setpoint.put(1, wait=True)
        mock_rpc.assert_called_once_with("samx", "setpoint.set", 1)


def test_put_calls_rpc(dev):
    with mock.patch.object(dev.samx.setpoint, "_run_rpc_call") as mock_rpc:
        dev.samx.setpoint.put(1)
        mock_rpc.assert_called_once_with("samx", "setpoint.put", 1)


def test_get_rpc_func_name_decorator(dev):
    with mock.patch.object(dev.samx.setpoint, "_run_rpc_call") as mock_rpc:
        dev.samx.setpoint.set(1)
        mock_rpc.assert_called_once_with("samx", "setpoint.set", 1)


def test_get_rpc_func_name_read(dev):
    with mock.patch.object(dev.samx, "_run_rpc_call") as mock_rpc:
        dev.samx.read(cached=False)
        mock_rpc.assert_called_once_with("samx", "read")


@pytest.mark.parametrize(
    "kind,cached", [("normal", True), ("hinted", True), ("config", False), ("omitted", False)]
)
def test_get_rpc_func_name_readback_get(dev, kind, cached):
    with mock.patch.object(dev.samx.readback, "_run") as mock_rpc:
        with mock.patch.object(dev.samx.root.parent.connector, "get") as mock_get:
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


def test_get_rpc_func_name_nested(dev):
    with mock.patch.object(
        dev.rt_controller._custom_rpc_methods["dummy_controller"]._custom_rpc_methods[
            "_func_with_args"
        ],
        "_run_rpc_call",
    ) as mock_rpc:
        dev.rt_controller.dummy_controller._func_with_args(1, 2)
        mock_rpc.assert_called_once_with("rt_controller", "dummy_controller._func_with_args", 1, 2)


def test_handle_rpc_response(dev):
    msg = messages.DeviceRPCMessage(device="samx", return_val=1, out="done", success=True)
    assert dev.samx._handle_rpc_response(msg) == 1


def test_handle_rpc_response_returns_status(dev, bec_client_mock):
    msg = messages.DeviceRPCMessage(
        device="samx", return_val={"type": "status", "RID": "request_id"}, out="done", success=True
    )
    assert dev.samx._handle_rpc_response(msg) == Status(
        bec_client_mock.device_manager, "request_id"
    )


def test_rpc_status_raises_error(dev):
    msg = messages.DeviceReqStatusMessage(device="samx", success=False, metadata={})
    connector = mock.MagicMock()
    status = Status(connector, "request_id")
    connector.lrange.return_value = [msg]
    with pytest.raises(RPCError):
        status.wait(raise_on_failure=True)

    status.wait(raise_on_failure=False)


def test_handle_rpc_response_raises(dev):
    msg = messages.DeviceRPCMessage(
        device="samx",
        return_val={"type": "status", "RID": "request_id"},
        out={
            "msg": "Didn't work...",
            "traceback": "Traceback (most recent call last):",
            "error": "error",
        },
        success=False,
    )
    with pytest.raises(RPCError):
        dev.samx._handle_rpc_response(msg)


def test_handle_rpc_response_returns_dict(dev):
    msg = messages.DeviceRPCMessage(device="samx", return_val={"a": "b"}, out="done", success=True)
    assert dev.samx._handle_rpc_response(msg) == {"a": "b"}


def test_run_rpc_call_calls_stop_on_keyboardinterrupt(dev):
    with mock.patch.object(dev.samx.setpoint, "_prepare_rpc_msg") as mock_rpc:
        mock_rpc.side_effect = [KeyboardInterrupt]
        with pytest.raises(RPCError):
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
        "deviceTags": ["detector"],
    }


@pytest.fixture
def device_obj(device_config):
    service_mock = mock.MagicMock()
    service_mock.connector = ConnectorMock("")
    dm = DeviceManagerBase(service_mock)
    info = get_device_info_mock(device_config["name"], device_config["deviceClass"])
    dm._add_device(device_config, info)
    obj = dm.devices[device_config["name"]]
    yield obj


def test_create_device_saves_config(device_obj, device_config):
    obj = device_obj
    assert obj._config == device_config


def test_device_enabled(device_obj, device_config):
    obj = device_obj
    assert obj.enabled == device_config["enabled"]
    device_config["enabled"] = False
    assert obj.enabled == device_config["enabled"]


def test_device_enable(device_obj):
    obj = device_obj
    with mock.patch.object(obj.parent.config_helper, "send_config_request") as config_req:
        obj.enabled = True
        config_req.assert_called_once_with(action="update", config={obj.name: {"enabled": True}})


def test_device_enable_set(device_obj):
    obj = device_obj
    with mock.patch.object(obj.parent.config_helper, "send_config_request") as config_req:
        obj.read_only = False
        config_req.assert_called_once_with(action="update", config={obj.name: {"readOnly": False}})


@pytest.mark.parametrize(
    "val,raised_error",
    [({"in": 5}, None), ({"in": 5, "out": 10}, None), ({"5", "4"}, TypeCheckError)],
)
def test_device_set_user_parameter(device_obj, val, raised_error):
    obj = device_obj
    with mock.patch.object(obj.parent.config_helper, "send_config_request") as config_req:
        if raised_error is None:
            obj.set_user_parameter(val)
            config_req.assert_called_once_with(
                action="update", config={obj.name: {"userParameter": val}}
            )
        else:
            with pytest.raises(raised_error):
                obj.set_user_parameter(val)


@pytest.mark.parametrize(
    "user_param,val,out,raised_error",
    [
        ({"in": 2, "out": 5}, {"in": 5}, {"in": 5, "out": 5}, None),
        ({"in": 2, "out": 5}, {"in": 5, "out": 10}, {"in": 5, "out": 10}, None),
        ({"in": 2, "out": 5}, {"5", "4"}, None, TypeCheckError),
        (None, {"in": 5}, {"in": 5}, None),
    ],
)
def test_device_update_user_parameter(device_obj, user_param, val, out, raised_error):
    obj = device_obj
    obj._config["userParameter"] = user_param
    with mock.patch.object(obj.parent.config_helper, "send_config_request") as config_req:
        if raised_error is None:
            obj.update_user_parameter(val)
            config_req.assert_called_once_with(
                action="update", config={obj.name: {"userParameter": out}}
            )
        else:
            with pytest.raises(raised_error):
                obj.update_user_parameter(val)


def test_status_wait():
    connector = mock.MagicMock()

    connector.lrange.side_effect = [
        [],
        [messages.DeviceReqStatusMessage(device="test", success=True, metadata={})],
    ]
    status = Status(connector, "test")
    status.wait()


def test_status_wait_raises_timeout():
    connector = mock.MagicMock()
    connector.lrange.return_value = False
    status = Status(connector, "test")
    with pytest.raises(TimeoutError):
        status.wait(timeout=0.1)


def test_device_get_device_config():
    device = DeviceBase(name="test", config={"deviceConfig": {"tolerance": 1}})
    assert device.get_device_config() == {"tolerance": 1}


def test_device_set_device_config():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"deviceConfig": {"tolerance": 1}}, parent=parent)
    device.set_device_config({"tolerance": 2})
    assert device.get_device_config() == {"tolerance": 2}
    parent.config_helper.send_config_request.assert_called_once()


def test_get_device_tags():
    device = DeviceBase(name="test", config={"deviceTags": ["tag1", "tag2"]})
    assert device.get_device_tags() == ["tag1", "tag2"]

    device = DeviceBase(name="test", config={})
    assert device.get_device_tags() == []


def test_set_device_tags():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"deviceTags": ["tag1", "tag2"]}, parent=parent)
    device.set_device_tags(["tag3", "tag4"])
    assert device.get_device_tags() == ["tag3", "tag4"]
    parent.config_helper.send_config_request.assert_called_once()


def test_add_device_tag():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"deviceTags": ["tag1", "tag2"]}, parent=parent)
    device.add_device_tag("tag3")
    assert device.get_device_tags() == ["tag1", "tag2", "tag3"]
    parent.config_helper.send_config_request.assert_called_once()


def test_add_device_tags_duplicate():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"deviceTags": ["tag1", "tag2"]}, parent=parent)
    device.add_device_tag("tag1")
    assert device.get_device_tags() == ["tag1", "tag2"]
    parent.config_helper.send_config_request.assert_not_called()


def test_remove_device_tag():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"deviceTags": ["tag1", "tag2"]}, parent=parent)
    device.remove_device_tag("tag1")
    assert device.get_device_tags() == ["tag2"]
    parent.config_helper.send_config_request.assert_called_once()


def test_device_wm():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"deviceTags": ["tag1", "tag2"]}, parent=parent)
    with mock.patch.object(parent.devices, "wm", new_callable=mock.PropertyMock) as wm:
        res = device.wm
        parent.devices.wm.assert_called_once()


def test_readout_priority():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"readoutPriority": "baseline"}, parent=parent)
    assert device.readout_priority == "baseline"


def test_set_readout_priority():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"readoutPriority": "baseline"}, parent=parent)
    device.readout_priority = "monitored"
    assert device.readout_priority == "monitored"
    parent.config_helper.send_config_request.assert_called_once()


def test_on_failure():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"onFailure": "buffer"}, parent=parent)
    assert device.on_failure == "buffer"


def test_set_on_failure():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"onFailure": "buffer"}, parent=parent)
    device.on_failure = "retry"
    assert device.on_failure == "retry"
    parent.config_helper.send_config_request.assert_called_once()


def test_read_only():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"read_only": False}, parent=parent)
    assert device.read_only is False


def test_set_read_only():
    parent = mock.MagicMock(spec=DeviceManagerBase)
    device = DeviceBase(name="test", config={"read_only": False}, parent=parent)
    device.read_only = True
    assert device.read_only is True
    parent.config_helper.send_config_request.assert_called_once()


def test_device_container_wm():
    devs = DeviceContainer()
    devs["test"] = Device(name="test", config={}, parent=mock.MagicMock(spec=DeviceManagerBase))
    with mock.patch.object(devs.test, "read", return_value={"test": {"value": 1}}) as read:
        devs.wm("test")
        devs.wm("tes*")


def test_device_container_wm_with_setpoint():
    devs = DeviceContainer()
    devs["test"] = Device(name="test", config={}, parent=mock.MagicMock(spec=DeviceManagerBase))
    with mock.patch.object(
        devs.test, "read", return_value={"test": {"value": 1}, "test_setpoint": {"value": 1}}
    ) as read:
        devs.wm("test")


def test_device_container_wm_with_user_setpoint():
    devs = DeviceContainer()
    devs["test"] = Device(name="test", config={}, parent=mock.MagicMock(spec=DeviceManagerBase))
    with mock.patch.object(
        devs.test, "read", return_value={"test": {"value": 1}, "test_user_setpoint": {"value": 1}}
    ) as read:
        devs.wm("test")


@pytest.mark.parametrize("device_cls", [Device, Signal, Positioner])
def test_device_has_describe_method(device_cls):
    devs = DeviceContainer()
    parent = mock.MagicMock(spec=DeviceManagerBase)
    devs["test"] = device_cls(name="test", config={}, parent=parent)
    assert hasattr(devs.test, "describe")
    with mock.patch.object(devs.test, "_run_rpc_call") as mock_rpc:
        devs.test.describe()
        mock_rpc.assert_not_called()


@pytest.mark.parametrize("device_cls", [Device, Signal, Positioner])
def test_device_has_describe_configuration_method(device_cls):
    devs = DeviceContainer()
    parent = mock.MagicMock(spec=DeviceManagerBase)
    devs["test"] = device_cls(name="test", config={}, parent=parent)
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
    devs["dev1"] = DeviceBase(
        name="dev1",
        config={
            "description": "Device 1",
            "enabled": True,
            "readOnly": False,
            "deviceClass": "Class1",
            "readoutPriority": "high",
            "deviceTags": ["tag1", "tag2"],
        },
        parent=parent,
    )
    devs["dev2"] = DeviceBase(
        name="dev2",
        config={
            "description": "Device 2",
            "enabled": False,
            "readOnly": True,
            "deviceClass": "Class2",
            "readoutPriority": "low",
            "deviceTags": ["tag3", "tag4"],
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


def test_adjustable_mixin_limits():
    adj = AdjustableMixin()
    adj.root = mock.MagicMock()
    adj.root.parent.connector.get.return_value = messages.DeviceMessage(
        signals={"low": {"value": -12}, "high": {"value": 12}}, metadata={}
    )
    assert adj.limits == [-12, 12]


def test_adjustable_mixin_limits_missing():
    adj = AdjustableMixin()
    adj.root = mock.MagicMock()
    adj.root.parent.connector.get.return_value = None
    assert adj.limits == [0, 0]


def test_adjustable_mixin_set_limits():
    adj = AdjustableMixin()
    adj.update_config = mock.MagicMock()
    adj.limits = [-12, 12]
    adj.update_config.assert_called_once_with({"deviceConfig": {"limits": [-12, 12]}})


def test_adjustable_mixin_set_low_limit():
    adj = AdjustableMixin()
    adj.update_config = mock.MagicMock()
    adj.root = mock.MagicMock()
    adj.root.parent.connector.get.return_value = messages.DeviceMessage(
        signals={"low": {"value": -12}, "high": {"value": 12}}, metadata={}
    )
    adj.low_limit = -20
    adj.update_config.assert_called_once_with({"deviceConfig": {"limits": [-20, 12]}})


def test_adjustable_mixin_set_high_limit():
    adj = AdjustableMixin()
    adj.update_config = mock.MagicMock()
    adj.root = mock.MagicMock()
    adj.root.parent.connector.get.return_value = messages.DeviceMessage(
        signals={"low": {"value": -12}, "high": {"value": 12}}, metadata={}
    )
    adj.high_limit = 20
    adj.update_config.assert_called_once_with({"deviceConfig": {"limits": [-12, 20]}})


def test_computed_signal_set_compute_method():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())

    def my_compute_method():
        return "a + b"

    with mock.patch.object(comp_signal, "update_config") as update_config:
        comp_signal.set_compute_method(my_compute_method)
        update_config.assert_called_once_with(
            {
                "deviceConfig": {
                    "compute_method": '    def my_compute_method():\n        return "a + b"\n'
                }
            }
        )


def test_computed_signal_set_signals():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())
    with mock.patch.object(comp_signal, "update_config") as update_config:
        comp_signal.set_input_signals(
            Signal(name="a", parent=mock.MagicMock(spec=DeviceManagerBase)),
            Signal(name="b", parent=mock.MagicMock(spec=DeviceManagerBase)),
        )
        update_config.assert_called_once_with({"deviceConfig": {"input_signals": ["a", "b"]}})


def test_computed_signal_set_signals_raises_error():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())
    with pytest.raises(ValueError):
        comp_signal.set_input_signals("a", "b")


def test_computed_signal_set_signals_empty():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())
    with mock.patch.object(comp_signal, "update_config") as update_config:
        comp_signal.set_input_signals()
        update_config.assert_called_once_with({"deviceConfig": {"input_signals": []}})


def test_computed_signal_raises_error_on_set_compute_method():
    comp_signal = ComputedSignal(name="comp_signal", parent=mock.MagicMock())
    with pytest.raises(ValueError):
        comp_signal.set_compute_method("a + b")


def test_device_summary(dev):
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


def test_device_summary_signal_grouping(dev):
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


def test_device_summary_empty_signals(dev):
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
