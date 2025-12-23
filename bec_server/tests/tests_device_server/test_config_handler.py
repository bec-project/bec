import copy
import os
import threading
from unittest import mock

import pytest

import bec_lib
from bec_lib import messages
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.devicemanager import DeviceConfigError, DeviceManagerDS

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
    handler = ConfigUpdateHandler(device_manager)

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

        ConfigUpdateHandler._device_config_callback(msg_mock, parent=handler)

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
        ConfigUpdateHandler._device_config_callback(msg_mock, parent=handler)

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
