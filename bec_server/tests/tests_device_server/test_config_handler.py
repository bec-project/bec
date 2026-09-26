import copy
import os
import threading
from concurrent.futures import Future
from types import SimpleNamespace
from unittest import mock

import pytest

import bec_lib
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.devices.devicemanager import DeviceConfigError, DeviceManagerDS
from bec_server.device_server.ophyd.configuration import ConfigUpdateHandler

dir_path = os.path.dirname(bec_lib.__file__)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_request_response(session_from_test_config, device_manager):
    def get_config_from_mock():
        device_manager._session = copy.deepcopy(session_from_test_config)
        device_manager._load_session()

    def mocked_failed_connection(obj):
        if obj.name == "samx":
            raise ConnectionError

    config_reply = messages.RequestResponseMessage(accepted=True, message="")
    with mock.patch.object(device_manager, "connect_device", wraps=mocked_failed_connection):
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
    configuration = device_manager.configuration

    # bpm4i doesn't have a controller, so it should be destroyed
    msg = messages.DeviceConfigMessage(action="update", config={"bpm4i": {"enabled": False}})
    configuration.parse_config_request(msg, cancel_event=threading.Event())
    assert device_manager.devices.bpm4i.enabled is False
    assert device_manager.devices.bpm4i.initialized is False
    assert device_manager.devices.bpm4i.obj._destroyed is True

    msg = messages.DeviceConfigMessage(action="update", config={"bpm4i": {"enabled": True}})
    configuration.parse_config_request(msg, cancel_event=threading.Event())
    assert device_manager.devices.bpm4i.enabled is True
    assert device_manager.devices.bpm4i.initialized is True
    assert device_manager.devices.bpm4i.obj._destroyed is False


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
        mock.patch.object(dm_with_devices, "reset_device") as reset_device,
        mock.patch.object(handler, "send_config_request_reply") as send_reply,
    ):
        handler.parse_config_request(msg, cancel_event=threading.Event())

    assert device.enabled is True
    assert device.initialized is True
    assert device.obj._destroyed is False
    assert session_config["enabled"] is True
    reset_device.assert_not_called()
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
def test_config_handler_disable_destroys_disconnected_device(dm_with_devices):
    configuration = dm_with_devices.configuration
    device = dm_with_devices.devices.bpm4i
    msg = messages.DeviceConfigMessage(action="update", config={device.name: {"enabled": False}})

    with (
        mock.patch.object(
            type(device.obj), "connected", new_callable=mock.PropertyMock
        ) as connected,
        mock.patch.object(device.obj, "destroy", wraps=device.obj.destroy) as destroy,
    ):
        connected.return_value = False
        configuration.parse_config_request(msg, cancel_event=threading.Event())

    destroy.assert_called_once_with()
    assert device.obj._destroyed is True
    assert device.initialized is False
    assert device.enabled is False


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cleanup_failed_device_init_destroys_disconnected_object(dm_with_devices):
    configuration = dm_with_devices.configuration
    obj = mock.MagicMock(name="failed_device")
    obj.name = "failed_device"
    obj.connected = False

    configuration._cleanup_failed_device_init(obj)

    obj.destroy.assert_called_once_with()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cleanup_failed_device_init_does_not_raise_on_cleanup_errors(dm_with_devices):
    configuration = dm_with_devices.configuration
    obj = mock.MagicMock(name="failed_device")
    obj.name = "failed_device"
    obj.destroy.side_effect = RuntimeError("destroy failed")
    device = mock.MagicMock(name="failed_device_wrapper")
    device.name = "failed_device"

    with mock.patch.object(
        dm_with_devices, "reset_device", side_effect=RuntimeError("reset failed")
    ) as reset_device:
        configuration._cleanup_failed_device_init(obj, device)

    obj.destroy.assert_called_once_with()
    reset_device.assert_called_once_with(device)


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

    construct_device_obj = dm_with_devices.construct_device_obj

    def capture_constructed_object(*args, **kwargs):
        obj, config = construct_device_obj(*args, **kwargs)
        constructed_objects.append(obj)
        return obj, config

    with (
        mock.patch.object(
            dm_with_devices, "construct_device_obj", side_effect=capture_constructed_object
        ),
        mock.patch.object(
            dm_with_devices, "connect_device", return_value=ConnectionError("PV is unreachable")
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
    configuration = dm_with_devices.configuration
    device_name = "motor1_disabled"
    old_device = dm_with_devices.devices[device_name]
    replacement_devices = []

    def fail_after_initialization(*_args, **_kwargs):
        replacement_devices.append(dm_with_devices.devices[device_name])
        raise RuntimeError("post-connect initialization failure")

    with (
        mock.patch.object(dm_with_devices, "update_config", side_effect=fail_after_initialization),
        mock.patch.object(
            dm_with_devices, "connect_device", wraps=dm_with_devices.connect_device
        ) as connect_device,
    ):
        for _ in range(2):
            msg = messages.DeviceConfigMessage(
                action="update", config={device_name: {"enabled": True}}
            )
            with pytest.raises(RuntimeError, match="post-connect initialization failure"):
                configuration.parse_config_request(msg, cancel_event=threading.Event())

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
    configuration = device_manager.configuration

    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"deviceConfig": {"doesntexist": True}}}
    )
    old_config = device_manager.devices.samx._config["deviceConfig"].copy()
    with pytest.raises(DeviceConfigError):
        configuration.parse_config_request(msg, cancel_event=threading.Event())
    assert device_manager.devices.samx._config["deviceConfig"] == old_config


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_reload_action(dm_with_devices):
    device_manager = dm_with_devices
    configuration = device_manager.configuration
    dm = device_manager
    with mock.patch.object(dm.devices.samx.obj, "destroy") as obj_destroy:
        with mock.patch.object(dm, "_get_config") as get_config:
            configuration.parse_config_request(
                messages.DeviceConfigMessage(action="reload", config={}), threading.Event()
            )
            obj_destroy.assert_called_once()
            get_config.assert_called_once()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_update(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"deviceConfig": {"labels": ["updated"]}}}
    )
    with mock.patch.object(handler, "send_config_request_reply") as reply:
        handler.parse_config_request(msg, cancel_event=threading.Event())
    assert dm_with_devices.devices.samx.obj._ophyd_labels_ == {"updated"}
    reply.assert_called_once_with(accepted=True, error_msg="", metadata=msg.metadata)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_reload(device_manager):
    handler = ConfigUpdateHandler(device_manager)
    device_manager.failed_devices = {"samx": "connection failed"}
    device_manager.current_session["devices"] = []
    msg = messages.DeviceConfigMessage(action="reload", config={})
    with (
        mock.patch.object(device_manager, "_get_config") as get_config,
        mock.patch("bec_server.device_server.ophyd.configuration.reload_plugin_modules"),
        mock.patch.object(handler, "send_config_request_reply") as reply,
    ):
        handler.parse_config_request(msg, cancel_event=threading.Event())
    get_config.assert_called_once()
    assert msg.metadata["failed_devices"] == {"samx": "connection failed"}
    reply.assert_called_once_with(accepted=True, error_msg="", metadata=msg.metadata)


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
        }
    }
    msg = messages.DeviceConfigMessage(action="add", config=config)
    handler.parse_config_request(msg, cancel_event=threading.Event())
    assert "new_device" in dm_with_devices.devices

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
        dm_with_devices, "connect_device", return_value=ConnectionError("PV is unreachable")
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
        mock.patch.object(dm_with_devices, "update_config", side_effect=fail_after_initialization),
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
        mock.patch.object(dm, "construct_device_obj") as construct,
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
    construct_device_obj = dm.construct_device_obj
    publish_device_info = dm.publish_device_info

    def construct(dev_config, **kwargs):
        if failure == "construction" and dev_config["name"] == "second_added":
            raise RuntimeError("constructor failed")
        obj, device_config = construct_device_obj(dev_config, **kwargs)
        created_objects.append(obj)
        obj.destroy = mock.Mock(wraps=obj.destroy)
        return obj, device_config

    def publish(obj, **kwargs):
        if failure == "device_info" and obj.name == "second_added":
            raise RuntimeError("device info failed")
        result = publish_device_info(obj, **kwargs)
        if first_init_fails and obj.name == "first_added":
            return RuntimeError("first device initialization failed")
        return result

    with (
        mock.patch.object(dm, "construct_device_obj", side_effect=construct),
        mock.patch.object(dm, "publish_device_info", side_effect=publish),
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
    construct_device_obj = dm.construct_device_obj
    failed_pipeline = mock.MagicMock()
    failed_pipeline.execute.side_effect = RuntimeError("Redis cleanup failed")

    def construct(dev_config, **kwargs):
        if dev_config["name"] == "second_added":
            # Simulate Redis becoming unavailable when the rejected batch is rolled back.
            cleanup_pipeline.return_value = failed_pipeline
            raise RuntimeError("constructor failed")
        obj, device_config = construct_device_obj(dev_config, **kwargs)
        created_objects.append(obj)
        return obj, device_config

    with (
        mock.patch.object(
            dm.connector, "pipeline", wraps=dm.connector.pipeline
        ) as cleanup_pipeline,
        mock.patch.object(dm, "construct_device_obj", side_effect=construct),
        mock.patch.object(handler, "send_config_request_reply") as send_reply,
    ):
        handler.parse_config_request(msg, cancel_event=threading.Event())

    assert send_reply.call_args.kwargs["accepted"] is False
    assert "constructor failed" in send_reply.call_args.kwargs["error_msg"]
    assert "Redis cleanup failed" not in send_reply.call_args.kwargs["error_msg"]
    assert failed_pipeline.execute.call_count >= len(config)
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


@pytest.fixture
def config_handler(dm_with_devices):
    """Close the handler after exercising configuration scheduling."""
    handler = ConfigUpdateHandler(dm_with_devices)
    try:
        yield handler
    finally:
        handler.shutdown()


def schedule_request(handler, future):
    """Submit a request with a controlled executor result and capture its cancellation signal."""
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={"RID": "active"}
    )
    with mock.patch.object(handler.executor, "submit", return_value=future) as submit:
        handler._device_config_callback(SimpleNamespace(value=msg))
    return submit.call_args.args[2]


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_config_callback_normal_request(dm_with_devices, connected_connector):
    """A scheduled request changes the device and publishes a matching reply."""
    dm_with_devices.connector = connected_connector
    handler = ConfigUpdateHandler(dm_with_devices)
    completed = threading.Event()
    original_send = handler.send_config_request_reply

    def send_reply(**kwargs):
        original_send(**kwargs)
        completed.set()

    msg = messages.DeviceConfigMessage(
        action="update", config={"bpm4i": {"enabled": False}}, metadata={"RID": "update"}
    )
    try:
        with mock.patch.object(handler, "send_config_request_reply", side_effect=send_reply):
            handler._device_config_callback(SimpleNamespace(value=msg))
            assert completed.wait(timeout=5)
        reply = connected_connector.get(MessageEndpoints.device_config_request_response("update"))
        assert reply.accepted is True
        assert dm_with_devices.devices.bpm4i.enabled is False
        assert dm_with_devices.devices.bpm4i.obj._destroyed is True
    finally:
        handler.shutdown()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_config_callback_cancel_request(config_handler):
    """Cancellation signals the scheduled work and acknowledges its completion."""
    future = Future()
    cancel_event = schedule_request(config_handler, future)
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "cancel"})

    def finish_when_cancelled(futures, timeout):
        assert cancel_event.is_set()
        future.set_result(None)
        return SimpleNamespace(not_done=set())

    with (
        mock.patch.object(config_handler, "send_config_request_reply") as reply,
        mock.patch("concurrent.futures.wait", side_effect=finish_when_cancelled),
    ):
        config_handler._device_config_callback(SimpleNamespace(value=msg))
    reply.assert_called_once_with(accepted=True, error_msg="", metadata=msg.metadata)
    assert config_handler._active_request is None
    assert config_handler._requests == {}


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_completed_request_cannot_be_cancelled(config_handler):
    """Completing a scheduled future removes it from cancellation tracking."""
    future = Future()
    schedule_request(config_handler, future)
    future.set_result(None)
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "cancel"})
    with mock.patch.object(config_handler, "send_config_request_reply") as reply:
        config_handler._device_config_callback(SimpleNamespace(value=msg))
    reply.assert_called_once_with(
        accepted=False, error_msg="No active request found to cancel", metadata=msg.metadata
    )
    assert config_handler._requests == {}


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cancel_config_request_without_active_request(config_handler):
    """Cancellation without scheduled work receives a rejected reply."""
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "cancel"})
    with mock.patch.object(config_handler, "send_config_request_reply") as reply:
        config_handler._device_config_callback(SimpleNamespace(value=msg))
    reply.assert_called_once_with(
        accepted=False, error_msg="No active request found to cancel", metadata=msg.metadata
    )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cancel_config_request_with_exception(config_handler):
    """A cancellation wait error is returned to the requester."""
    future = Future()
    cancel_event = schedule_request(config_handler, future)
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "cancel"})
    with (
        mock.patch.object(config_handler, "send_config_request_reply") as reply,
        mock.patch("concurrent.futures.wait", side_effect=RuntimeError("Test error")),
    ):
        config_handler._device_config_callback(SimpleNamespace(value=msg))
    assert cancel_event.is_set()
    reply.assert_called_once_with(
        accepted=False, error_msg="Error during cancellation: Test error", metadata=msg.metadata
    )
    future.set_result(None)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_parse_config_request_flushes_on_cancelled_error(config_handler):
    """Cancelled updates flush devices and publish a rejected reply."""
    handler = config_handler
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={"RID": "update"}
    )
    cancel_event = threading.Event()
    cancel_event.set()
    devices = [device.obj for device in handler.device_manager.devices.values()]
    with mock.patch.object(handler, "send_config_request_reply") as reply:
        handler.parse_config_request(msg, cancel_event)
    assert not handler.device_manager.devices
    assert all(device._destroyed for device in devices)
    reply.assert_called_once_with(
        accepted=False, error_msg="Request was cancelled", metadata=msg.metadata
    )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cancelled_config_reports_cleanup_failure(config_handler):
    """A failed cancellation cleanup is reported to the requester."""
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={"RID": "update"}
    )
    cancelled = threading.Event()
    cancelled.set()
    with (
        mock.patch.object(
            config_handler.device_manager.configuration,
            "destroy_device",
            side_effect=RuntimeError("cleanup failed"),
        ),
        mock.patch.object(config_handler.connector, "set") as publish,
    ):
        config_handler.parse_config_request(msg, cancelled)
    reply = publish.call_args.args[1]
    assert reply.accepted is False
    assert "cleanup failed" in reply.message
    assert reply.metadata["RID"] == "update"


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_cancel_config_request_timeout_sends_alarm_and_flushes(config_handler):
    """Timed-out cancellation warns, flushes devices and waits for completion."""
    handler = config_handler
    future = Future()
    cancel_event = schedule_request(handler, future)
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "cancel"})

    def wait_for_completion(futures, timeout=None):
        assert futures == [future]
        assert cancel_event.is_set()
        if timeout is not None:
            return SimpleNamespace(not_done={future})
        assert not handler.device_manager.devices
        future.set_result(None)
        return SimpleNamespace(not_done=set())

    with (
        mock.patch.object(handler, "send_config_request_reply") as reply,
        mock.patch.object(handler.connector, "raise_alarm") as raise_alarm,
        mock.patch("concurrent.futures.wait", side_effect=wait_for_completion),
    ):
        handler._cancel_config_request(msg, timeout=0)
    raise_alarm.assert_called_once()
    assert raise_alarm.call_args.kwargs["severity"] == bec_lib.alarm_handler.Alarms.WARNING
    assert raise_alarm.call_args.kwargs["info"].id == "ConfigCancellationTimeout"
    reply.assert_called_once_with(accepted=True, error_msg="", metadata=msg.metadata)
