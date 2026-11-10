import copy
import os
import threading
from types import SimpleNamespace
from unittest import mock

import ophyd
import pytest

import bec_lib
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.device_info import DeviceInfo
from bec_server.device_server.devices.devicemanager import DeviceConfigError, DeviceManagerDS
from bec_server.device_server.devices.ds_device import DSDevice

dir_path = os.path.dirname(bec_lib.__file__)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_request_response(session_from_test_config, device_manager):
    def get_config_from_mock():
        device_manager._session = copy.deepcopy(session_from_test_config)
        device_manager._load_session()

    def mocked_failed_connection(device, **_kwargs):
        if device.name == "samx":
            return ConnectionError("PV is unreachable")
        return None

    config_reply = messages.RequestResponseMessage(accepted=True, message="")
    with mock.patch.object(
        device_manager.device_provider_manager.providers["ophyd"],
        "connect_device",
        wraps=mocked_failed_connection,
    ):
        with mock.patch.object(device_manager, "_get_config", get_config_from_mock):
            with mock.patch.object(
                device_manager.config_helper, "wait_for_config_reply", return_value=config_reply
            ):
                with mock.patch.object(device_manager.config_helper, "wait_for_service_response"):
                    device_manager.initialize("")
                    with mock.patch.object(
                        device_manager.config_update_handler, "send_config_request_reply"
                    ) as request_reply:
                        device_manager.config_update_handler.parse_config_request(
                            msg=messages.DeviceConfigMessage(
                                action="update", config={"something": "something"}
                            ),
                            cancel_event=threading.Event(),
                        )
                        request_reply.assert_called_once()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_handler_update_config(dm_with_devices):
    device_manager = dm_with_devices
    handler = ConfigUpdateHandler(device_manager)

    metadata = {
        key: device_manager.devices.bpm4i._config[key]
        for key in ("id", "sessionId", "accessGroups")
    }
    # bpm4i doesn't have a controller, so it should be destroyed
    msg = messages.DeviceConfigMessage(action="update", config={"bpm4i": {"enabled": False}})
    handler._update_config(msg, cancel_event=threading.Event())
    assert device_manager.devices.bpm4i.enabled is False
    assert device_manager.devices.bpm4i.initialized is False
    assert device_manager.devices.bpm4i.obj._destroyed is True

    msg = messages.DeviceConfigMessage(action="update", config={"bpm4i": {"enabled": True}})
    handler._update_config(msg, cancel_event=threading.Event())
    assert device_manager.devices.bpm4i.enabled is True
    assert device_manager.devices.bpm4i.initialized is True
    assert device_manager.devices.bpm4i.obj._destroyed is False
    for key, value in metadata.items():
        assert device_manager.devices.bpm4i._config[key] == value


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_handler_failed_disable_preserves_state_and_can_retry(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    device = dm_with_devices.devices.bpm4i
    session_config = next(
        config
        for config in dm_with_devices.current_session["devices"]
        if config["name"] == device.name
    )
    msg = messages.DeviceConfigMessage(action="update", config={device.name: {"enabled": False}})

    with (
        mock.patch.object(device.obj, "destroy", side_effect=RuntimeError("destroy failed")),
        mock.patch.object(handler, "send_config_request_reply") as send_reply,
    ):
        handler.parse_config_request(msg, cancel_event=threading.Event())

    assert device.enabled is True
    assert device.initialized is True
    assert device.obj._destroyed is False
    assert session_config["enabled"] is True
    assert send_reply.call_args.kwargs["accepted"] is False
    assert "destroy failed" in send_reply.call_args.kwargs["error_msg"]

    with mock.patch.object(handler, "send_config_request_reply") as send_reply:
        handler.parse_config_request(msg, cancel_event=threading.Event())

    assert device.enabled is False
    assert device.initialized is False
    assert device.obj._destroyed is True
    assert session_config["enabled"] is False
    send_reply.assert_called_once_with(accepted=True, error_msg="", metadata=msg.metadata)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_failed_disable_preserves_live_readback(dm_with_devices, connected_connector):
    manager = dm_with_devices
    manager.connector = connected_connector
    signal = ophyd.Signal(name="disable_readback", value=1)
    device = DSDevice(
        name=signal.name,
        obj=signal,
        config={"deviceClass": "ophyd.Signal", "enabled": True, "readoutPriority": "monitored"},
        parent=manager,
    )
    manager.device_provider_manager.initialize_device(device, {})
    msg = messages.DeviceConfigMessage(action="update", config={device.name: {"enabled": False}})
    handler = ConfigUpdateHandler(manager)

    with (
        mock.patch.object(signal, "destroy", side_effect=RuntimeError("destroy failed")),
        pytest.raises(RuntimeError, match="destroy failed"),
    ):
        handler._update_config(msg, cancel_event=threading.Event())

    signal.put(42)
    endpoint = MessageEndpoints.device_readback(device.name)
    assert connected_connector.get(endpoint).signals[device.name]["value"] == 42


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_handler_disable_destroys_disconnected_device(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    device = dm_with_devices.devices.bpm4i
    msg = messages.DeviceConfigMessage(action="update", config={device.name: {"enabled": False}})

    with (
        mock.patch.object(
            type(device.obj), "connected", new_callable=mock.PropertyMock
        ) as connected,
        mock.patch.object(device.obj, "destroy", wraps=device.obj.destroy) as destroy,
    ):
        connected.return_value = False
        handler._update_config(msg, cancel_event=threading.Event())

    destroy.assert_called_once_with()
    assert device.obj._destroyed is True
    assert device.initialized is False
    assert device.enabled is False


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cleanup_failed_device_init_destroys_disconnected_object(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    obj = ophyd.Signal(name="failed_device")
    device = DSDevice(name=obj.name, obj=obj, config=None)
    device.initialized = True
    with (
        mock.patch.object(
            type(obj), "connected", new_callable=mock.PropertyMock, return_value=False
        ),
        mock.patch.object(obj, "destroy", wraps=obj.destroy) as destroy,
    ):
        handler._cleanup_failed_device_init(device)

    destroy.assert_called_once_with()
    assert obj._destroyed is True


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cleanup_failed_device_init_does_not_raise_on_cleanup_errors(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    device = dm_with_devices.devices.samx
    with (
        mock.patch.object(
            device.obj, "destroy", side_effect=RuntimeError("destroy failed")
        ) as destroy,
        mock.patch.object(
            type(device),
            "initialized",
            create=True,
            new_callable=mock.PropertyMock,
            side_effect=RuntimeError("reset failed"),
        ) as reset_device,
    ):
        handler._cleanup_failed_device_init(device)

    destroy.assert_called_once_with()
    reset_device.assert_called_once_with(False)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_handler_failed_enable_remains_disabled(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    device_name = "motor1_disabled"
    constructed_objects = []
    session_config = next(
        config
        for config in dm_with_devices.current_session["devices"]
        if config["name"] == device_name
    )

    construct_device = dm_with_devices.device_provider_manager.construct_device

    def capture_constructed_object(*args, **kwargs):
        device, config = construct_device(*args, **kwargs)
        constructed_objects.append(device.obj)
        return device, config

    with (
        mock.patch.object(
            dm_with_devices.device_provider_manager,
            "construct_device",
            side_effect=capture_constructed_object,
        ),
        mock.patch.object(
            dm_with_devices.device_provider_manager.providers["ophyd"],
            "connect_device",
            return_value=ConnectionError("PV is unreachable"),
        ) as connect_device,
        mock.patch.object(handler, "send_config_request_reply") as send_reply,
    ):
        for _ in range(2):
            msg = messages.DeviceConfigMessage(
                action="update", config={device_name: {"enabled": True}}
            )
            handler.parse_config_request(msg, cancel_event=threading.Event())

            assert dm_with_devices.devices[device_name].enabled is False
            assert session_config["enabled"] is False

    assert connect_device.call_count == 2
    assert len(constructed_objects) == 2
    assert all(obj._destroyed for obj in constructed_objects)
    assert [call.kwargs["accepted"] for call in send_reply.call_args_list] == [False, False]


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_handler_failed_enable_cleans_and_restores_old_device(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    device_name = "motor1_disabled"
    old_device = dm_with_devices.devices[device_name]
    replacement_devices = []

    def fail_after_initialization(*_args, **_kwargs):
        replacement_devices.append(dm_with_devices.devices[device_name])
        raise RuntimeError("post-connect initialization failure")

    with (
        mock.patch.object(
            dm_with_devices.device_provider_manager.providers["ophyd"],
            "update_device_config",
            side_effect=fail_after_initialization,
        ),
        mock.patch.object(
            dm_with_devices.device_provider_manager.providers["ophyd"],
            "connect_device",
            wraps=dm_with_devices.device_provider_manager.providers["ophyd"].connect_device,
        ) as connect_device,
    ):
        for _ in range(2):
            msg = messages.DeviceConfigMessage(
                action="update", config={device_name: {"enabled": True}}
            )
            with pytest.raises(RuntimeError, match="post-connect initialization failure"):
                handler._update_config(msg, cancel_event=threading.Event())

            replacement = replacement_devices[-1]
            assert replacement is not old_device
            assert replacement.enabled is False
            assert replacement.initialized is False
            assert replacement.obj.connected is False
            assert dm_with_devices.devices[device_name] is old_device
            assert old_device.enabled is False

    assert connect_device.call_count == 2


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_handler_update_config_raises(dm_with_devices):
    device_manager = dm_with_devices
    handler = ConfigUpdateHandler(device_manager)

    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"deviceConfig": {"doesntexist": True}}}
    )
    old_config = device_manager.devices.samx._config["deviceConfig"].copy()
    with pytest.raises(DeviceConfigError):
        handler._update_config(msg, cancel_event=threading.Event())
    assert device_manager.devices.samx._config["deviceConfig"] == old_config


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_reload_action(dm_with_devices):
    device_manager = dm_with_devices
    handler = ConfigUpdateHandler(device_manager)
    dm = handler.device_manager
    with mock.patch.object(dm.devices.samx.obj, "destroy") as obj_destroy:
        with mock.patch.object(dm, "_get_config") as get_config:
            handler._reload_config(cancel_event=threading.Event())
            obj_destroy.assert_called_once()
            get_config.assert_called_once()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_update(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"deviceConfig": {"doesntexist": True}}}
    )
    cancel_event = threading.Event()
    with mock.patch.object(handler, "_update_config") as update_config:
        handler.parse_config_request(msg, cancel_event=cancel_event)
        update_config.assert_called_once_with(msg, cancel_event)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_reload(device_manager):
    handler = ConfigUpdateHandler(device_manager)
    dm = handler.device_manager
    dm.failed_devices = ["samx"]
    msg = messages.DeviceConfigMessage(action="reload", config={})
    with mock.patch.object(handler, "_reload_config") as reload_config:
        handler.parse_config_request(msg, cancel_event=threading.Event())
        reload_config.assert_called_once()
        assert msg.metadata["failed_devices"] == ["samx"]


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_add_remove(dm_with_devices):
    """
    Test adding and removing a device from the device manager
    """
    handler = ConfigUpdateHandler(dm_with_devices)
    config = {
        "new_device": {
            "readoutPriority": "baseline",
            "deviceClass": "ophyd_devices.SimPositioner",
            "deviceConfig": {
                "delay": 1,
                "limits": [-50, 50],
                "tolerance": 0.01,
                "update_frequency": 400,
            },
            "deviceTags": {"user motors"},
            "enabled": True,
            "readOnly": False,
            "name": "new_device",
            "id": "added-device-id",
            "sessionId": "added-session-id",
            "accessGroups": "customer",
        }
    }
    msg = messages.DeviceConfigMessage(action="add", config=config)
    handler.parse_config_request(msg, cancel_event=threading.Event())
    assert "new_device" in dm_with_devices.devices
    for key in ("id", "sessionId", "accessGroups"):
        assert dm_with_devices.devices.new_device._config[key] == config["new_device"][key]

    config = {"new_device": {}}
    msg = messages.DeviceConfigMessage(action="remove", config=config)
    handler.parse_config_request(msg, cancel_event=threading.Event())
    assert "new_device" not in dm_with_devices.devices


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_failed_add_is_disabled(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    config = {
        "failed_device": {
            "readoutPriority": "baseline",
            "deviceClass": "ophyd_devices.SimPositioner",
            "deviceConfig": {},
            "deviceTags": {"user motors"},
            "enabled": True,
            "readOnly": False,
            "name": "failed_device",
        }
    }
    msg = messages.DeviceConfigMessage(action="add", config=config)

    with mock.patch.object(
        dm_with_devices.device_provider_manager.providers["ophyd"],
        "connect_device",
        return_value=ConnectionError("PV is unreachable"),
    ):
        handler.parse_config_request(msg, cancel_event=threading.Event())

    assert dm_with_devices.devices.failed_device._config["enabled"] is False
    session_config = next(
        config
        for config in dm_with_devices.current_session["devices"]
        if config["name"] == "failed_device"
    )
    assert session_config["enabled"] is False


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_failed_add_cleans_initialized_device(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    device_name = "failed_device"
    config = {
        device_name: {
            "readoutPriority": "baseline",
            "deviceClass": "ophyd_devices.SimPositioner",
            "deviceConfig": {},
            "deviceTags": {"user motors"},
            "enabled": True,
            "readOnly": False,
            "name": device_name,
        }
    }
    msg = messages.DeviceConfigMessage(action="add", config=config)
    initialized_devices = []

    def fail_after_initialization(*_args, **_kwargs):
        initialized_devices.append(dm_with_devices.devices[device_name])
        raise RuntimeError("post-connect initialization failure")

    with (
        mock.patch.object(
            dm_with_devices.device_provider_manager.providers["ophyd"],
            "update_device_config",
            side_effect=fail_after_initialization,
        ),
        mock.patch.object(handler, "send_config_request_reply") as send_reply,
    ):
        handler.parse_config_request(msg, cancel_event=threading.Event())

    failed_device = dm_with_devices.devices[device_name]
    assert failed_device is initialized_devices[0]
    assert failed_device.enabled is False
    assert failed_device.initialized is False
    assert failed_device.obj.connected is False
    session_config = next(
        config
        for config in dm_with_devices.current_session["devices"]
        if config["name"] == device_name
    )
    assert session_config["enabled"] is False
    assert "post-connect initialization failure" in msg.metadata["failed_devices"][device_name]
    send_reply.assert_called_once_with(accepted=True, error_msg="", metadata=msg.metadata)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("registered", [False, True])
def test_parse_config_request_add_preserves_existing_device_data(
    dm_with_devices, connected_connector, registered
):
    dm = dm_with_devices
    dm.connector = connected_connector
    handler = ConfigUpdateHandler(dm)
    device = dm.devices.samx
    if not registered:
        device.obj.destroy()
        del dm.devices[device.name]
    original_devices = dict(dm.devices)
    original_session = copy.deepcopy(dm.current_session)
    status = messages.DeviceStatusMessage(device=device.name, status=0)
    readback = messages.DeviceMessage(signals={"value": {"value": 12}})
    connected_connector.set(MessageEndpoints.device_status(device.name), status)
    connected_connector.set_and_publish(MessageEndpoints.device_readback(device.name), readback)
    msg = messages.DeviceConfigMessage(
        action="add",
        config={
            name: copy.deepcopy(device._config) | {"name": name}
            for name in ("new_device", device.name)
        },
    )

    with (
        mock.patch.object(dm.device_provider_manager, "construct_device") as construct,
        mock.patch.object(handler, "send_config_request_reply") as send_reply,
    ):
        handler.parse_config_request(msg, cancel_event=threading.Event())

    assert send_reply.call_args.kwargs["accepted"] is False
    assert "already exists" in send_reply.call_args.kwargs["error_msg"]
    construct.assert_not_called()
    assert dict(dm.devices) == original_devices
    assert dm.current_session == original_session
    assert connected_connector.get(MessageEndpoints.device_status(device.name)) == status
    assert connected_connector.get(MessageEndpoints.device_readback(device.name)) == readback


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("failure", ["device_info", "construction", "dependency"])
@pytest.mark.parametrize("first_init_fails", [False, True])
def test_parse_config_request_rejected_add_rolls_back_batch(
    dm_with_devices, connected_connector, failure, first_init_fails
):
    dm = dm_with_devices
    dm.connector = connected_connector
    handler = ConfigUpdateHandler(dm)
    original_devices = dict(dm.devices)
    original_session = copy.deepcopy(dm.current_session)
    config = {
        name: {
            "name": name,
            "deviceClass": "ophyd_devices.SimPositioner",
            "deviceConfig": {},
            "enabled": True,
            "readOnly": False,
            "readoutPriority": "baseline",
            "deviceTags": [],
        }
        for name in ("first_added", "second_added")
    }
    if failure == "dependency":
        config["second_added"]["needs"] = ["missing_device"]
    msg = messages.DeviceConfigMessage(action="add", config=config)
    created_objects = []
    construct_device = dm.device_provider_manager.construct_device
    provider = dm.device_provider_manager.providers["ophyd"]
    get_device_info = provider.get_device_info
    connect_device = provider.connect_device

    def construct(dev_config, **kwargs):
        if failure == "construction" and dev_config.name == "second_added":
            raise RuntimeError("constructor failed")
        device, device_config = construct_device(dev_config, **kwargs)
        created_objects.append(device.obj)
        device.obj.destroy = mock.Mock(wraps=device.obj.destroy)
        return device, device_config

    def inspect_device(device, **kwargs):
        if failure == "device_info" and device.name == "second_added":
            raise RuntimeError("device info failed")
        return get_device_info(device, **kwargs)

    def connect(device, **kwargs):
        if first_init_fails and device.name == "first_added":
            return RuntimeError("first device initialization failed")
        return connect_device(device, **kwargs)

    with (
        mock.patch.object(dm.device_provider_manager, "construct_device", side_effect=construct),
        mock.patch.object(provider, "get_device_info", side_effect=inspect_device),
        mock.patch.object(provider, "connect_device", side_effect=connect),
        mock.patch.object(handler, "send_config_request_reply") as send_reply,
    ):
        handler.parse_config_request(msg, cancel_event=threading.Event())

    assert send_reply.call_args.kwargs["accepted"] is False
    expected_error = {
        "device_info": "device info failed",
        "construction": "constructor failed",
        "dependency": "needs unknown device",
    }[failure]
    assert expected_error in send_reply.call_args.kwargs["error_msg"]
    assert dict(dm.devices) == original_devices
    assert dm.current_session == original_session
    assert dm.failed_devices == {}
    assert "failed_devices" not in msg.metadata
    assert created_objects
    assert all(obj._destroyed for obj in created_objects)
    for obj in created_objects:
        obj.destroy.assert_called_once_with()
    for name in config:
        assert name not in dm.devices.__dict__
        for endpoint in (
            MessageEndpoints.device_status,
            MessageEndpoints.device_read,
            MessageEndpoints.device_readback,
            MessageEndpoints.device_read_configuration,
            MessageEndpoints.device_info,
            MessageEndpoints.device_limits,
        ):
            assert connected_connector.get(endpoint(name)) is None

    msg.config["second_added"].pop("needs", None)
    msg.config["first_added"]["enabled"] = True
    with mock.patch.object(handler, "send_config_request_reply") as send_reply:
        handler.parse_config_request(msg, cancel_event=threading.Event())

    send_reply.assert_called_once_with(accepted=True, error_msg="", metadata=msg.metadata)
    assert all(name in dm.devices for name in config)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_rejected_add_survives_redis_cleanup_failure(dm_with_devices):
    dm = dm_with_devices
    handler = ConfigUpdateHandler(dm)
    original_devices = dict(dm.devices)
    original_session = copy.deepcopy(dm.current_session)
    config = {
        name: copy.deepcopy(dm.devices.samx._config.copy()) | {"name": name}
        for name in ("first_added", "second_added")
    }
    msg = messages.DeviceConfigMessage(action="add", config=config)
    created_objects = []
    construct_device = dm.device_provider_manager.construct_device
    failed_pipeline = mock.MagicMock()
    failed_pipeline.execute.side_effect = RuntimeError("Redis cleanup failed")

    def construct(dev_config, **kwargs):
        if dev_config.name == "second_added":
            raise RuntimeError("constructor failed")
        device, device_config = construct_device(dev_config, **kwargs)
        created_objects.append(device.obj)
        return device, device_config

    with (
        mock.patch.object(handler, "connector", wraps=dm.connector) as cleanup_connector,
        mock.patch.object(dm.device_provider_manager, "construct_device", side_effect=construct),
        mock.patch.object(handler, "send_config_request_reply") as send_reply,
    ):
        # Keep the failure injection separate from the background monitor's connector.
        cleanup_connector.pipeline.return_value = failed_pipeline
        handler.parse_config_request(msg, cancel_event=threading.Event())

    assert send_reply.call_args.kwargs["accepted"] is False
    assert "constructor failed" in send_reply.call_args.kwargs["error_msg"]
    assert "Redis cleanup failed" not in send_reply.call_args.kwargs["error_msg"]
    assert failed_pipeline.execute.call_count == len(config)
    assert dict(dm.devices) == original_devices
    assert dm.current_session == original_session
    assert all(name not in dm.devices.__dict__ for name in config)
    assert len(created_objects) == 1
    assert created_objects[0]._destroyed is True


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_remove_device_not_in_config(dm_with_devices):
    """
    Test that removing a device that is not in the config does not raise an error
    """
    handler = ConfigUpdateHandler(dm_with_devices)
    config = {"new_device": {}}
    msg = messages.DeviceConfigMessage(action="remove", config=config)
    handler.parse_config_request(msg, cancel_event=threading.Event())
    assert "new_device" not in dm_with_devices.devices


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_config_callback_normal_request(dm_with_devices):
    """Test _device_config_callback with a normal (non-cancel) request."""
    handler = ConfigUpdateHandler(dm_with_devices)

    msg_mock = mock.MagicMock()
    msg_mock.value = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={"RID": "12345"}
    )

    with mock.patch.object(handler.executor, "submit") as submit:
        mock_future = mock.MagicMock()
        submit.return_value = mock_future

        handler._device_config_callback(msg_mock)

        # Verify executor.submit was called with parse_config_request
        submit.assert_called_once()
        call_args = submit.call_args
        assert call_args[0][0] == handler.parse_config_request
        assert call_args[0][1] == msg_mock.value
        # Check that a cancel_event was passed
        assert isinstance(call_args[0][2], threading.Event)

        # Verify active request was set
        assert handler._active_request is not None
        assert handler._active_request["future"] == mock_future
        assert handler._active_request["request_id"] == "12345"
        assert isinstance(handler._active_request["cancel_event"], threading.Event)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_config_callback_cancel_request(dm_with_devices):
    """Test _device_config_callback with a cancel request."""
    handler = ConfigUpdateHandler(dm_with_devices)

    msg_mock = mock.MagicMock()
    msg_mock.value = messages.DeviceConfigMessage(
        action="cancel", config={}, metadata={"RID": "12345"}
    )

    with mock.patch.object(handler, "_cancel_config_request") as cancel_request:
        handler._device_config_callback(msg_mock)

        # Verify _cancel_config_request was called
        cancel_request.assert_called_once_with(msg_mock.value)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_remove_active_request(dm_with_devices):
    """Test _remove_active_request clears the active request."""
    handler = ConfigUpdateHandler(dm_with_devices)

    # Set up an active request
    handler._active_request = {
        "future": mock.MagicMock(),
        "cancel_event": threading.Event(),
        "request_id": "test_id",
    }

    # Call _remove_active_request
    handler._remove_active_request()

    # Verify it was cleared
    assert handler._active_request is None


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cancel_config_request_with_active_request(dm_with_devices):
    """Test _cancel_config_request when there is an active request."""
    handler = ConfigUpdateHandler(dm_with_devices)
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "12345"})

    # Set up an active request
    cancel_event = threading.Event()
    mock_future = mock.MagicMock()
    handler._active_request = {
        "future": mock_future,
        "cancel_event": cancel_event,
        "request_id": "active_request_id",
    }

    with mock.patch.object(handler, "send_config_request_reply") as req_reply:
        with mock.patch("concurrent.futures.wait") as cf_wait:
            handler._cancel_config_request(msg)

            # Verify cancel_event was set
            assert cancel_event.is_set()

            # Verify we waited for the future
            cf_wait.assert_called_once_with([mock_future], timeout=30)

            # Verify success reply was sent
            req_reply.assert_called_once_with(
                accepted=True, error_msg="", metadata={"RID": "12345"}
            )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cancel_config_request_without_active_request(dm_with_devices):
    """Test _cancel_config_request when there is no active request."""
    handler = ConfigUpdateHandler(dm_with_devices)
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "12345"})

    # No active request
    handler._active_request = None

    with mock.patch.object(handler, "send_config_request_reply") as req_reply:
        handler._cancel_config_request(msg)

        # Verify error reply was sent
        req_reply.assert_called_once_with(
            accepted=False, error_msg="No active request found to cancel", metadata={"RID": "12345"}
        )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cancel_config_request_with_exception(dm_with_devices):
    """Test _cancel_config_request when waiting for future raises exception."""
    handler = ConfigUpdateHandler(dm_with_devices)
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "12345"})

    # Set up an active request
    cancel_event = threading.Event()
    mock_future = mock.MagicMock()
    handler._active_request = {
        "future": mock_future,
        "cancel_event": cancel_event,
        "request_id": "active_request_id",
    }

    with mock.patch.object(handler, "send_config_request_reply") as req_reply:
        with mock.patch("concurrent.futures.wait", side_effect=RuntimeError("Test error")):
            handler._cancel_config_request(msg)

            # Verify cancel_event was set
            assert cancel_event.is_set()

            # Verify error reply was sent
            req_reply.assert_called_once_with(
                accepted=False,
                error_msg="Error during cancellation: Test error",
                metadata={"RID": "12345"},
            )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_flushes_on_cancelled_error(dm_with_devices):
    """Test parse_config_request flushes the config when CancelledError is raised."""
    handler = ConfigUpdateHandler(dm_with_devices)
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={"RID": "12345"}
    )
    cancel_event = threading.Event()
    # Set the cancel event to trigger CancelledError
    cancel_event.set()

    with mock.patch.object(handler, "_flush_config") as flush_config:
        with mock.patch.object(handler, "send_config_request_reply") as req_reply:
            handler.parse_config_request(msg, cancel_event)

            # Verify _flush_config was called
            flush_config.assert_called_once()

            # Verify error reply was sent with accepted=False
            req_reply.assert_called_once()
            call_args = req_reply.call_args
            assert call_args[1]["accepted"] is False
            assert call_args[1]["error_msg"] == "Request was cancelled"
            assert call_args[1]["metadata"] == {"RID": "12345"}


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cancel_config_request_timeout_sends_alarm_and_flushes(dm_with_devices):
    """Test _cancel_config_request sends alarm and flushes config when future doesn't resolve within timeout."""
    handler = ConfigUpdateHandler(dm_with_devices)
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "12345"})

    # Set up an active request
    cancel_event = threading.Event()
    mock_future = mock.MagicMock()
    handler._active_request = {
        "future": mock_future,
        "cancel_event": cancel_event,
        "request_id": "active_request_id",
    }

    # Create a mock WaitResult with future in not_done set
    class WaitResult:
        def __init__(self, done=None, not_done=None):
            self.done = done or set()
            self.not_done = not_done or set()

    wait_result = WaitResult(done=set(), not_done={mock_future})

    with mock.patch.object(handler, "send_config_request_reply") as req_reply:
        with mock.patch.object(handler.connector, "raise_alarm") as raise_alarm:
            with mock.patch.object(handler, "_flush_config") as flush_config:
                with mock.patch("concurrent.futures.wait", return_value=wait_result):
                    handler._cancel_config_request(msg, timeout=30.0)

                    # Verify cancel_event was set
                    assert cancel_event.is_set()

                    # Verify alarm was raised
                    raise_alarm.assert_called_once()
                    alarm_call = raise_alarm.call_args
                    assert alarm_call[1]["severity"] == bec_lib.alarm_handler.Alarms.WARNING
                    assert "ConfigCancellationTimeout" in str(alarm_call)

                    # Verify _flush_config was called
                    flush_config.assert_called_once()

                    # Verify success reply was still sent after completion
                    req_reply.assert_called_once_with(
                        accepted=True, error_msg="", metadata={"RID": "12345"}
                    )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize(
    "operation", ["disable", "remove", "flush", "failed_registered", "failed_unregistered"]
)
def test_config_handler_uses_selected_provider_for_teardown(dm_with_devices, operation):
    """Configuration teardown must support objects without an Ophyd destroy method."""
    dm = dm_with_devices
    handler = ConfigUpdateHandler(dm)
    obj = SimpleNamespace(name="custom_device")
    device = DSDevice(
        name=obj.name,
        obj=obj,
        config={"enabled": True, "deviceClass": "custom", "readoutPriority": "monitored"},
        parent=dm,
    )
    device.initialized = True
    provider = mock.Mock()
    provider.supports_device_class.side_effect = lambda cls: cls is type(obj)
    dm.device_provider_manager.providers["custom"] = provider
    if operation != "failed_unregistered":
        dm.devices._add_device(device.name, device)

    if operation in ("disable", "remove"):
        msg = messages.DeviceConfigMessage(
            action="update" if operation == "disable" else "remove",
            config={device.name: {"enabled": False} if operation == "disable" else {}},
        )
        if operation == "disable":
            handler._update_config(msg, cancel_event=threading.Event())
        else:
            handler._remove_config(msg, cancel_event=threading.Event())
    elif operation == "flush":
        handler._flush_config()
    else:
        handler._cleanup_failed_device_init(device)

    provider.disconnect_device.assert_called_once()
    wrapper = provider.disconnect_device.call_args.args[0]
    assert wrapper.obj is obj
    assert wrapper.name == device.name
    assert wrapper is device
    if operation in ("remove", "flush", "failed_unregistered"):
        assert device.name not in dm.devices
    if operation in ("disable", "remove", "failed_registered", "failed_unregistered"):
        assert device.initialized is False
    if operation == "disable":
        assert device.enabled is False


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("fails", [False, True])
@pytest.mark.parametrize("has_limits", [False, True])
def test_config_handler_uses_selected_provider_for_update(dm_with_devices, fails, has_limits):
    """Apply and roll back configuration without requiring Ophyd attributes."""
    dm = dm_with_devices
    handler = ConfigUpdateHandler(dm)
    obj = SimpleNamespace(name="custom_device")
    old_config = {"setting": 1}
    new_config = {"setting": 2}
    if has_limits:
        new_config["limits"] = [-2, 2]
    device = DSDevice(
        name=obj.name,
        obj=obj,
        parent=dm,
        config={
            "enabled": True,
            "deviceClass": "custom",
            "readoutPriority": "monitored",
            "deviceConfig": old_config,
        },
    )
    dm.devices._add_device(device.name, device)
    provider = mock.Mock()
    provider.supports_device_class.side_effect = lambda cls: cls is type(obj)
    limits = {"low": {"value": -2}, "high": {"value": 2}} if has_limits else None
    provider.update_device_config.side_effect = (
        [RuntimeError("update failed"), None] if fails else [None]
    )
    provider.get_device_limits.return_value = limits
    provider.get_device_info.return_value = DeviceInfo(
        device_base_class="device", device_class="CustomDevice"
    )
    dm.device_provider_manager.providers["custom"] = provider
    msg = messages.DeviceConfigMessage(
        action="update", config={device.name: {"deviceConfig": new_config}}
    )

    with mock.patch.object(dm.connector, "set_and_publish") as publish:
        if fails:
            with pytest.raises(DeviceConfigError, match="update failed"):
                handler._update_config(msg, cancel_event=threading.Event())
        else:
            handler._update_config(msg, cancel_event=threading.Event())

    expected = [mock.call(device, new_config)]
    if fails:
        expected.append(mock.call(device, old_config))
    assert provider.update_device_config.call_args_list == expected
    assert device._config["deviceConfig"] == old_config
    if has_limits and not fails:
        publish.assert_called_once_with(
            MessageEndpoints.device_limits(device.name), messages.DeviceMessage(signals=limits)
        )
    else:
        publish.assert_not_called()
        provider.get_device_limits.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_handler_update_publishes_ophyd_limits(dm_with_devices):
    """Keep publishing actual limit readbacks after an Ophyd configuration update."""
    dm = dm_with_devices
    device = dm.devices.samx
    handler = ConfigUpdateHandler(dm)
    msg = messages.DeviceConfigMessage(
        action="update", config={device.name: {"deviceConfig": {"limits": [-20, 20]}}}
    )
    with mock.patch.object(dm.connector, "set_and_publish") as publish:
        handler._update_config(msg, cancel_event=threading.Event())

    assert device.obj.low_limit_travel.get() == -20
    assert device.obj.high_limit_travel.get() == 20
    assert (
        mock.call(
            MessageEndpoints.device_limits(device.name),
            messages.DeviceMessage(signals={"low": {"value": -20}, "high": {"value": 20}}),
        )
        in publish.call_args_list
    )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("failure", ["readback", "publication"])
def test_limit_publication_failure_does_not_roll_back_hardware(dm_with_devices, failure):
    """Only config application failures roll back, matching the previous update behavior."""
    dm = dm_with_devices
    device = dm.devices.samx
    handler = ConfigUpdateHandler(dm)
    providers = dm.device_provider_manager
    provider = providers.providers["ophyd"]
    error = RuntimeError(
        "limit readback failed" if failure == "readback" else "limit publication failed"
    )
    msg = messages.DeviceConfigMessage(
        action="update", config={device.name: {"deviceConfig": {"limits": [-20, 20]}}}
    )
    with (
        mock.patch.object(
            provider, "update_device_config", wraps=provider.update_device_config
        ) as apply,
        mock.patch.object(provider, "get_device_limits", wraps=provider.get_device_limits) as read,
        mock.patch.object(dm.connector, "set_and_publish") as publish,
    ):
        if failure == "readback":
            read.side_effect = error
        else:
            publish.side_effect = error
        with pytest.raises(RuntimeError) as exc:
            handler._update_config(msg, cancel_event=threading.Event())

    assert exc.value is error
    apply.assert_called_once_with(device, {"limits": [-20, 20]})
    assert device.obj.low_limit_travel.get() == -20
    assert device.obj.high_limit_travel.get() == 20


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("failure", [None, "extraction", "publication"])
def test_config_update_refreshes_metadata_and_rolls_back_before_reply(dm_with_devices, failure):
    """Match main's metadata refresh and rollback boundary through the provider facade."""
    dm = dm_with_devices
    obj = SimpleNamespace(name="metadata_device", save_roi_data=True)
    device = DSDevice(
        obj.name,
        obj,
        config={
            "enabled": True,
            "deviceClass": "metadata",
            "readoutPriority": "monitored",
            "deviceConfig": {"save_roi_data": True},
        },
        parent=dm,
    )
    dm.devices._add_device(device.name, device)
    provider = mock.Mock()
    provider.supports_device_class.side_effect = lambda cls: cls is type(obj)
    provider.update_device_config.side_effect = lambda dev, cfg: setattr(
        dev.obj, "save_roi_data", cfg["save_roi_data"]
    )
    calls = []
    failed = False

    def get_info(dev, connect=True):
        nonlocal failed
        calls.append(("extract", dev.obj.save_roi_data, connect))
        if failure == "extraction" and not failed:
            failed = True
            raise RuntimeError("Metadata unavailable")
        return DeviceInfo.model_validate(
            {
                "device_base_class": "device",
                "device_class": "MetadataDevice",
                "signals": {
                    "data": {
                        "component_name": "data",
                        "signal_class": "Signal",
                        "obj_name": "data",
                        "save_roi_data": dev.obj.save_roi_data,
                    }
                },
            }
        )

    def publish(dev, info, pipe=None):
        nonlocal failed
        calls.append(("publish", info["signals"]["data"]["save_roi_data"]))
        if failure == "publication" and not failed:
            failed = True
            raise RuntimeError("Metadata unavailable")

    provider.get_device_info.side_effect = get_info
    dm.device_provider_manager.providers["metadata"] = provider
    handler = ConfigUpdateHandler(dm)
    msg = messages.DeviceConfigMessage(
        action="update", config={obj.name: {"deviceConfig": {"save_roi_data": False}}}
    )
    with mock.patch.object(dm, "publish_device_info", side_effect=publish):
        if failure:
            with pytest.raises(DeviceConfigError, match="Metadata unavailable"):
                handler._update_config(msg, threading.Event())
        else:
            handler._update_config(msg, threading.Event())
    assert obj.save_roi_data is bool(failure)
    assert calls[-1] == ("publish", bool(failure))
    assert calls[0] == ("extract", False, True)
    expected = [mock.call(device, {"save_roi_data": False})]
    if failure:
        expected.append(mock.call(device, {"save_roi_data": True}))
        assert ("extract", False, False) in calls
    assert provider.update_device_config.call_args_list == expected
