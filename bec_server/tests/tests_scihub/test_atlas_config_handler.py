# pylint: skip-file
import os
import threading
from unittest import mock

import pytest
from pydantic import ValidationError

import bec_lib
from bec_lib import messages
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.device import DeviceBaseWithConfig, OnFailure, ReadoutPriority
from bec_lib.devicemanager import CancelledError
from bec_lib.endpoints import MessageEndpoints
from bec_server.scihub.atlas.config_handler import ConfigHandler

dir_path = os.path.dirname(bec_lib.__file__)


@pytest.fixture
def available_keys_fixture(test_config_yaml):
    config_content = test_config_yaml
    config_keys_from_config = []
    for dev_config in config_content.values():
        for k in dev_config.keys():
            config_keys_from_config.append(k)
    config_keys_from_config = set(config_keys_from_config)
    # remove enabled and deviceClass, as it is not handled from config_handler
    config_keys_from_config.remove("enabled")
    config_keys_from_config.remove("deviceClass")
    return config_keys_from_config


@pytest.fixture
def mock_active_request():
    """Fixture providing a mock active request with cancel_event and future."""
    cancel_event = threading.Event()
    mock_future = mock.MagicMock()
    return {"future": mock_future, "cancel_event": cancel_event, "request_id": "active_request_id"}


@pytest.fixture
def cancel_request_mocks(config_handler):
    """Fixture providing common mocks for cancel request tests."""
    with mock.patch.object(config_handler, "_update_device_server") as update_ds:
        with mock.patch.object(config_handler, "_wait_for_device_server_update") as wait_ds:
            with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
                with mock.patch("concurrent.futures.wait") as cf_wait:
                    yield {
                        "update_device_server": update_ds,
                        "wait_device_server_update": wait_ds,
                        "send_config_request_reply": req_reply,
                        "concurrent_futures_wait": cf_wait,
                    }


def test_parse_config_request_update(config_handler):
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    cancel_event = threading.Event()
    with (
        mock.patch.object(config_handler, "_update_config") as update_config,
        mock.patch.object(config_handler.device_manager, "check_request_validity") as req_validity,
    ):
        config_handler.parse_config_request(msg, cancel_event=cancel_event)
        req_validity.assert_called_once_with(msg)
        update_config.assert_called_once_with(msg, cancel_event)


def test_parse_config_request_reload(config_handler):
    msg = messages.DeviceConfigMessage(action="reload", config={}, metadata={})
    cancel_event = threading.Event()
    with (
        mock.patch.object(config_handler, "_reload_config") as reload_config,
        mock.patch.object(config_handler.device_manager, "check_request_validity") as req_validity,
    ):
        config_handler.parse_config_request(msg, cancel_event=cancel_event)
        req_validity.assert_called_once_with(msg)
        reload_config.assert_called_once_with(msg)


def test_parse_config_request_set(config_handler):
    msg = messages.DeviceConfigMessage(
        action="set", config={"samx": {"enabled": True}}, metadata={}
    )
    cancel_event = threading.Event()
    with (
        mock.patch.object(config_handler, "_set_config") as set_config,
        mock.patch.object(config_handler.device_manager, "check_request_validity") as req_validity,
    ):
        config_handler.parse_config_request(msg, cancel_event=cancel_event)
        req_validity.assert_called_once_with(msg)
        set_config.assert_called_once_with(msg, cancel_event)


def test_parse_config_request_reset(config_handler):
    msg = messages.DeviceConfigMessage(action="reset", config=None, metadata={})
    cancel_event = threading.Event()
    with (
        mock.patch.object(config_handler, "_reset_config") as reset_config,
        mock.patch.object(config_handler.device_manager, "check_request_validity") as req_validity,
    ):
        config_handler.parse_config_request(msg, cancel_event=cancel_event)
        req_validity.assert_called_once_with(msg)
        reset_config.assert_called_once_with(msg)


def test_parse_config_request_add(config_handler):
    msg = messages.DeviceConfigMessage(
        action="add", config={"samx": {"enabled": True}}, metadata={}
    )
    cancel_event = threading.Event()
    with (
        mock.patch.object(config_handler, "_add_to_config") as add_to_config,
        mock.patch.object(config_handler.device_manager, "check_request_validity") as req_validity,
    ):
        config_handler.parse_config_request(msg, cancel_event=cancel_event)
        req_validity.assert_called_once_with(msg)
        add_to_config.assert_called_once_with(msg, cancel_event)


def test_parse_config_request_remove(config_handler):
    msg = messages.DeviceConfigMessage(
        action="remove", config={"samx": {"enabled": True}}, metadata={}
    )
    cancel_event = threading.Event()
    with (
        mock.patch.object(config_handler, "_remove_from_config") as remove_from_config,
        mock.patch.object(config_handler.device_manager, "check_request_validity") as req_validity,
    ):
        config_handler.parse_config_request(msg, cancel_event=cancel_event)
        req_validity.assert_called_once_with(msg)
        remove_from_config.assert_called_once_with(msg)


def test_parse_config_request_unknown_exception(config_handler):
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    cancel_event = threading.Event()
    with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
        with mock.patch.object(config_handler.device_manager, "check_request_validity"):
            with mock.patch.object(
                config_handler, "_update_config", side_effect=Exception("Unknown error")
            ):
                config_handler.parse_config_request(msg, cancel_event=cancel_event)
                req_reply.assert_called_once_with(accepted=False, error_msg=mock.ANY, metadata={})
                assert "Unknown error" in req_reply.call_args.kwargs["error_msg"]


def test_parse_config_request_cancelled_exception(config_handler):
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    cancel_event = threading.Event()
    with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
        with mock.patch.object(config_handler.device_manager, "check_request_validity"):
            with mock.patch.object(config_handler, "_update_config", side_effect=CancelledError()):
                config_handler.parse_config_request(msg, cancel_event=cancel_event)
                req_reply.assert_called_once_with(
                    accepted=False, error_msg="Request was cancelled", metadata={}
                )


def test_parse_config_request_exception(config_handler):
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    cancel_event = threading.Event()
    with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
        with mock.patch("bec_server.scihub.atlas.config_handler.traceback.format_exc") as exc:
            with mock.patch.object(config_handler, "_update_config", side_effect=AttributeError()):
                config_handler.parse_config_request(msg, cancel_event=cancel_event)
                req_reply.assert_called_once_with(accepted=False, error_msg=exc(), metadata={})


def test_config_handler_reload_config(config_handler):
    msg = messages.DeviceConfigMessage(action="reload", config={}, metadata={})
    cancel_event = threading.Event()
    with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
        with mock.patch.object(config_handler, "send_config") as send:
            config_handler.parse_config_request(msg, cancel_event=cancel_event)
            send.assert_called_once_with(msg)


@pytest.mark.parametrize(
    "config, expected",
    [
        (
            {
                "samx": {
                    "enabled": True,
                    "deviceClass": "ophyd.EpicsMotor",
                    "readoutPriority": "baseline",
                }
            },
            {
                "name": "samx",
                "enabled": True,
                "deviceConfig": {},
                "deviceClass": "ophyd.EpicsMotor",
                "readoutPriority": "baseline",
            },
        ),
        (
            {
                "samx": {
                    "enabled": True,
                    "deviceConfig": None,
                    "deviceClass": "ophyd.EpicsMotor",
                    "readoutPriority": "baseline",
                }
            },
            {
                "name": "samx",
                "enabled": True,
                "deviceConfig": {},
                "deviceClass": "ophyd.EpicsMotor",
                "readoutPriority": "baseline",
            },
        ),
    ],
)
def test_config_handler_set_config(config_handler, config, expected):
    msg = messages.DeviceConfigMessage(action="set", config=config, metadata={"RID": "12345"})
    cancel_event = threading.Event()
    with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
        with mock.patch.object(
            config_handler, "_wait_for_device_server_update", return_value=(True, mock.MagicMock())
        ) as wait:
            with mock.patch.object(config_handler, "send_config") as send_config:
                config_handler._set_config(msg, cancel_event=cancel_event)
                req_reply.assert_called_once_with(
                    accepted=True, error_msg=None, metadata={"RID": "12345", "updated_config": True}
                )
                send_config.assert_called_once_with(
                    messages.DeviceConfigMessage(
                        action="reload",
                        config={},
                        metadata={"RID": "12345", "updated_config": True},
                    )
                )


def test_config_handler_set_invalid_config_raises(config_handler):
    msg = messages.DeviceConfigMessage(
        action="set", config={"samx": {"status": {"enabled": True}}}, metadata={"RID": "12345"}
    )
    cancel_event = threading.Event()
    with pytest.raises(ValidationError):
        with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
            config_handler._set_config(msg, cancel_event=cancel_event)
            req_reply.assert_called_once_with(
                accepted=True, error_msg=None, metadata={"RID": "12345"}
            )


BASIC_CONFIG = {
    "enabled": True,
    "deviceClass": "TestDevice",
    "readoutPriority": ReadoutPriority.MONITORED.value,
}


@pytest.fixture
def make_samx(config_handler):
    def _func(config: dict = {}):
        dev = config_handler.device_manager.devices
        dev.samx = DeviceBaseWithConfig(name="samx", config=BASIC_CONFIG | config)
        return dev

    return _func


def test_config_handler_update_config(config_handler, make_samx):
    dev = make_samx()
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    cancel_event = threading.Event()
    with mock.patch.object(
        config_handler, "_update_device_config", return_value=True
    ) as update_device_config:
        with mock.patch.object(config_handler, "update_config_in_redis") as update_config_in_redis:
            with mock.patch.object(config_handler, "send_config") as send_config:
                with mock.patch.object(
                    config_handler, "send_config_request_reply"
                ) as send_config_request_reply:
                    config_handler._update_config(msg, cancel_event=cancel_event)
                    update_device_config.assert_called_once_with(dev["samx"], {"enabled": True})
                    update_config_in_redis.assert_called_once_with(dev["samx"])

                    send_config.assert_called_once_with(msg)
                    send_config_request_reply.assert_called_once_with(
                        accepted=True, error_msg=None, metadata={}
                    )


def test_config_handler_update_config_not_updated(config_handler, make_samx):
    dev = make_samx()
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    cancel_event = threading.Event()
    with mock.patch.object(
        config_handler, "_update_device_config", return_value=False
    ) as update_device_config:
        with mock.patch.object(config_handler, "update_config_in_redis") as update_config_in_redis:
            with mock.patch.object(config_handler, "send_config") as send_config:
                with mock.patch.object(
                    config_handler, "send_config_request_reply"
                ) as send_config_request_reply:
                    config_handler._update_config(msg, cancel_event=cancel_event)
                    update_device_config.assert_called_once_with(dev["samx"], {"enabled": True})
                    update_config_in_redis.assert_not_called()

                    send_config.assert_not_called()
                    send_config_request_reply.assert_not_called()


def test_config_handler_update_device_config_enable(config_handler, make_samx):
    dev = make_samx()
    with mock.patch.object(config_handler, "_update_device_server") as update_dev_server:
        with mock.patch.object(
            config_handler, "_wait_for_device_server_update", return_value=(True, mock.MagicMock())
        ) as wait:
            with mock.patch("bec_server.scihub.atlas.config_handler.uuid") as uuid:
                device = dev["samx"]
                rid = str(uuid.uuid4())
                config_handler._update_device_config(device, {"enabled": True})
                # mock doesn't copy the data, hence the popped result:
                update_dev_server.assert_called_once_with(rid, {device.name: {}})
                wait.assert_called_once_with(rid)


def test_config_handler_update_device_config_deviceConfig(config_handler, make_samx):
    dev = make_samx({"deviceConfig": {}})
    with mock.patch.object(config_handler, "_update_device_server") as update_dev_server:
        with mock.patch.object(
            config_handler, "_wait_for_device_server_update", return_value=(True, mock.MagicMock())
        ) as wait:
            with mock.patch("bec_server.scihub.atlas.config_handler.uuid") as uuid:
                device = dev["samx"]
                rid = str(uuid.uuid4())
                config_handler._update_device_config(
                    device, {"deviceConfig": {"something": "to_update"}}
                )
                # mock doesn't copy the data, hence the popped result:
                update_dev_server.assert_called_once_with(rid, {device.name: {}})
                wait.assert_called_once_with(rid)
                assert _all_in_a_in_b(
                    {"deviceConfig": {"something": "to_update"}}, dev.samx._config
                )


def test_config_handler_update_device_config_misc(config_handler, make_samx):
    dev = make_samx()
    with mock.patch.object(config_handler, "_validate_update") as validate_update:
        device = dev["samx"]
        config_handler._update_device_config(device, {"readOnly": True})
        validate_update.assert_called_once_with({"readOnly": True})


def test_config_handler_update_device_config_raise(config_handler, make_samx):
    dev = make_samx()
    with mock.patch.object(config_handler, "_validate_update") as validate_update:
        device = dev["samx"]
        with pytest.raises(DeviceConfigError):
            config_handler._update_device_config(device, {"doesnt_exist": False})


def test_config_handler_update_device_config_available_keys(
    config_handler, available_keys_fixture, make_samx
):
    for available_key in available_keys_fixture:
        if available_key in ["deviceConfig", "userParameter"]:
            init = {"something": "to_update"}
            dev = make_samx({available_key: init})
        elif available_key in ["softwareTrigger", "readOnly"]:
            init = True
            dev = make_samx({available_key: init})
        elif available_key in ["readoutPriority"]:
            init = ReadoutPriority.BASELINE
            dev = make_samx({available_key: init})
        elif available_key in ["onFailure"]:
            init = OnFailure.BUFFER
            dev = make_samx({available_key: init})
        elif available_key in ["deviceTags"]:
            init = ["something"]
            dev = make_samx({available_key: init})
        else:
            dev = make_samx({})

        with mock.patch.object(config_handler, "_update_device_server") as update_dev_server:
            with mock.patch.object(
                config_handler,
                "_wait_for_device_server_update",
                return_value=(True, mock.MagicMock()),
            ) as wait:
                with mock.patch("bec_server.scihub.atlas.config_handler.uuid") as uuid:
                    device = dev["samx"]
                    rid = str(uuid.uuid4())
                    if available_key in ["deviceConfig", "userParameter"]:
                        update = {"something": "to_update"}
                        config_handler._update_device_config(device, {available_key: update})
                    elif available_key in ["softwareTrigger", "readOnly"]:
                        update = True
                        config_handler._update_device_config(device, {available_key: update})
                    elif available_key in ["readoutPriority"]:
                        update = ReadoutPriority.MONITORED
                        config_handler._update_device_config(device, {available_key: update})
                    elif available_key in ["readoutPriority"]:
                        update = OnFailure.RETRY
                        config_handler._update_device_config(device, {available_key: update})
                    elif available_key in ["deviceTags"]:
                        update = ["something"]
                        config_handler._update_device_config(device, {available_key: update})
                    else:
                        update = ""
                        config_handler._update_device_config(device, {available_key: update})
                    # mock doesn't copy the data, hence the popped result:
                    if available_key == "deviceConfig":
                        update_dev_server.assert_called_once_with(rid, {device.name: {}})
                        wait.assert_called_once_with(rid)
                    assert _all_in_a_in_b({available_key: update}, dev.samx._config)


def test_config_handler_wait_for_device_server_update(config_handler):
    RID = "12345"
    with mock.patch.object(config_handler.connector, "get") as mock_get:
        mock_get.side_effect = [
            None,
            None,
            None,
            messages.RequestResponseMessage(accepted=True, message=""),
        ]
        config_handler._wait_for_device_server_update(RID)


def test_config_handler_wait_for_device_server_update_timeout(config_handler):
    RID = "12345"
    with mock.patch.object(config_handler.connector, "get", return_value=None) as mock_get:
        with pytest.raises(TimeoutError):
            config_handler._wait_for_device_server_update(RID, timeout_time=0.1)
            mock_get.assert_called()


def _all_in_a_in_b(a: dict, b: dict):
    return {k: v for k, v in b.items() if k in a} == a


def test_config_handler_update_config_in_redis(config_handler):
    with mock.patch.object(config_handler, "get_config_from_redis") as get_config:
        with mock.patch.object(config_handler, "set_config_in_redis") as set_config:
            get_config.return_value = [{"name": "samx", "config": {}}]
            dev = config_handler.device_manager.devices
            dev.samx = DeviceBaseWithConfig(
                name="samx",
                config=BASIC_CONFIG | {"deviceConfig": {"something": "to_update"}, "name": "samx"},
            )
            config_handler.update_config_in_redis(dev["samx"])
            get_config.assert_called_once()
            assert _all_in_a_in_b(
                {"deviceConfig": {"something": "to_update"}, "name": "samx"},
                set_config.call_args.args[0][0],
            )


def test_config_helper_get_config_from_redis(config_handler):
    with mock.patch.object(config_handler.connector, "get") as get:
        get.return_value = messages.AvailableResourceMessage(
            resource=[{"name": "samx", "config": {}}]
        )
        out = config_handler.get_config_from_redis()
        get.assert_called_once_with(MessageEndpoints.device_config())
        assert out == [{"name": "samx", "config": {}}]


def test_config_helper_set_config_in_redis(config_handler):
    with mock.patch.object(config_handler.connector, "set") as set:
        config = [{"name": "samx", "config": {}}]
        config_handler.set_config_in_redis(config)
        msg = messages.AvailableResourceMessage(resource=config)
        set.assert_called_once_with(MessageEndpoints.device_config(), msg)


def test_config_handler_add_devices_to_redis(config_handler):
    with mock.patch.object(config_handler, "get_config_from_redis") as get_config:
        with mock.patch.object(config_handler, "set_config_in_redis") as set_config:
            get_config.return_value = [{"name": "samx", "deviceConfig": {}}]
            config_handler.add_devices_to_redis(
                {"samy": {"deviceConfig": {"something": "to_update"}, "name": "samy"}}
            )
            get_config.assert_called_once()
            set_config.assert_called_once_with(
                [
                    {"name": "samx", "deviceConfig": {}},
                    {"name": "samy", "deviceConfig": {"something": "to_update"}},
                ]
            )


def test_config_handler_remove_devices_from_redis(config_handler):
    with mock.patch.object(config_handler, "get_config_from_redis") as get_config:
        with mock.patch.object(config_handler, "set_config_in_redis") as set_config:
            get_config.return_value = [
                {"name": "samx", "deviceConfig": {}},
                {"name": "samy", "deviceConfig": {}},
            ]
            config_handler.remove_devices_from_redis({"samx": {}})
            get_config.assert_called_once()
            set_config.assert_called_once_with([{"name": "samy", "deviceConfig": {}}])


def test_config_handler_add_to_config(config_handler):
    config = {
        "new_samx": {
            "deviceConfig": {},
            "name": "new_samx",
            "enabled": True,
            "readoutPriority": "baseline",
            "deviceClass": "SimPositioner",
        }
    }
    msg = messages.DeviceConfigMessage(action="add", config=config, metadata={"RID": "12345"})
    cancel_event = threading.Event()
    with mock.patch.object(config_handler, "add_devices_to_redis") as add_devices:
        with mock.patch.object(config_handler, "_update_device_server") as update_dev_server:
            with mock.patch.object(
                config_handler, "_wait_for_device_server_update"
            ) as wait_dev_server_update:
                wait_dev_server_update.return_value = (True, mock.MagicMock())
                with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
                    config_handler._add_to_config(msg, cancel_event=cancel_event)
                    add_devices.assert_called_once_with(config)
                    req_reply.assert_called_once_with(
                        accepted=True, error_msg=None, metadata={"RID": "12345"}
                    )


def test_config_handler_remove_from_config(config_handler):
    msg = messages.DeviceConfigMessage(
        action="remove", config={"samx": {}}, metadata={"RID": "12345"}
    )
    config_handler.device_manager.devices.samx = DeviceBaseWithConfig(
        name="samx", config=BASIC_CONFIG
    )
    with mock.patch.object(config_handler, "remove_devices_from_redis") as remove_devices:
        with mock.patch.object(config_handler, "_update_device_server") as update_dev_server:
            with mock.patch.object(
                config_handler, "_wait_for_device_server_update"
            ) as wait_dev_server_update:
                wait_dev_server_update.return_value = (True, mock.MagicMock())
                with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
                    config_handler._remove_from_config(msg)
                    remove_devices.assert_called_once_with({"samx": {}})
                    req_reply.assert_called_once_with(
                        accepted=True, error_msg=None, metadata={"RID": "12345"}
                    )


def test_config_handler_reset_config(config_handler):
    msg = messages.DeviceConfigMessage(action="reset", config=None, metadata={"RID": "12345"})
    with mock.patch.object(config_handler, "set_config_in_redis") as set_config:
        with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
            config_handler._reset_config(msg)
            set_config.assert_called_once_with([])
            req_reply.assert_called_once_with(
                accepted=True, error_msg=None, metadata={"RID": "12345"}
            )


def test_handle_config_request_callback_normal_request(config_handler):
    """Test handle_config_request_callback with a normal (non-cancel) request."""
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={"RID": "12345"}
    )
    with mock.patch.object(config_handler.executor, "submit") as submit:
        with mock.patch.object(config_handler, "_remove_active_request"):
            mock_future = mock.MagicMock()
            submit.return_value = mock_future

            config_handler.handle_config_request_callback(msg)

            # Verify executor.submit was called with parse_config_request
            submit.assert_called_once()
            call_args = submit.call_args
            assert call_args[0][0] == config_handler.parse_config_request
            assert call_args[0][1] == msg
            # Check that a cancel_event was passed
            assert isinstance(call_args[0][2], threading.Event)

            # Verify active request was set
            assert config_handler._active_request is not None
            assert config_handler._active_request["future"] == mock_future
            assert config_handler._active_request["request_id"] == "12345"
            assert isinstance(config_handler._active_request["cancel_event"], threading.Event)


def test_handle_config_request_callback_cancel_request(config_handler):
    """Test handle_config_request_callback with a cancel request."""
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "12345"})
    with mock.patch.object(config_handler, "_cancel_config_request") as cancel_request:
        config_handler.handle_config_request_callback(msg)

        # Verify _cancel_config_request was called
        cancel_request.assert_called_once_with(msg)


def test_cancel_config_request_with_active_request(
    config_handler, mock_active_request, cancel_request_mocks
):
    """Test _cancel_config_request when there is an active request."""
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "12345"})

    # Set up the active request
    config_handler._active_request = mock_active_request
    cancel_request_mocks["wait_device_server_update"].return_value = (True, mock.MagicMock())

    config_handler._cancel_config_request(msg)

    # Verify cancel_event was set
    assert mock_active_request["cancel_event"].is_set()

    # Verify device server was notified
    cancel_request_mocks["update_device_server"].assert_called_once()
    cancel_request_mocks["wait_device_server_update"].assert_called_once()

    # Verify we waited for the future
    cancel_request_mocks["concurrent_futures_wait"].assert_called_once_with(
        [mock_active_request["future"]]
    )

    # Verify success reply was sent
    cancel_request_mocks["send_config_request_reply"].assert_called_once_with(
        accepted=True, error_msg="", metadata={"RID": "12345"}
    )


def test_cancel_config_request_without_active_request(config_handler, cancel_request_mocks):
    """Test _cancel_config_request when there is no active request."""
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "12345"})

    # No active request
    config_handler._active_request = None
    cancel_request_mocks["wait_device_server_update"].return_value = (True, mock.MagicMock())

    config_handler._cancel_config_request(msg)

    # Verify device server was still notified
    cancel_request_mocks["update_device_server"].assert_called_once()
    cancel_request_mocks["wait_device_server_update"].assert_called_once()

    # Verify success reply was sent (no local request to cancel)
    cancel_request_mocks["send_config_request_reply"].assert_called_once_with(
        accepted=True, error_msg="", metadata={"RID": "12345"}
    )


def test_cancel_config_request_device_server_timeout(
    config_handler, mock_active_request, cancel_request_mocks
):
    """Test _cancel_config_request when device server times out."""
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "12345"})

    # Set up the active request
    config_handler._active_request = mock_active_request
    cancel_request_mocks["wait_device_server_update"].side_effect = TimeoutError()

    config_handler._cancel_config_request(msg)

    # Verify cancel_event was set
    assert mock_active_request["cancel_event"].is_set()

    # Verify device server was attempted
    cancel_request_mocks["update_device_server"].assert_called_once()
    cancel_request_mocks["wait_device_server_update"].assert_called_once()

    # Verify we still waited for the future
    cancel_request_mocks["concurrent_futures_wait"].assert_called_once_with(
        [mock_active_request["future"]]
    )

    # Verify success reply was sent (timeout is handled gracefully)
    cancel_request_mocks["send_config_request_reply"].assert_called_once_with(
        accepted=True, error_msg="", metadata={"RID": "12345"}
    )


def test_cancel_config_request_device_server_error(
    config_handler, mock_active_request, cancel_request_mocks
):
    """Test _cancel_config_request when device server returns error."""
    msg = messages.DeviceConfigMessage(action="cancel", config={}, metadata={"RID": "12345"})

    # Set up the active request
    config_handler._active_request = mock_active_request

    # Device server returns failure
    mock_response = mock.MagicMock()
    mock_response.message = "Device server error"
    cancel_request_mocks["wait_device_server_update"].return_value = (False, mock_response)

    config_handler._cancel_config_request(msg)

    # Verify cancel_event was set
    assert mock_active_request["cancel_event"].is_set()

    # Verify device server was notified
    cancel_request_mocks["update_device_server"].assert_called_once()
    cancel_request_mocks["wait_device_server_update"].assert_called_once()

    # Verify we still waited for the future
    cancel_request_mocks["concurrent_futures_wait"].assert_called_once_with(
        [mock_active_request["future"]]
    )

    # Verify success reply was sent (device server error is handled gracefully)
    cancel_request_mocks["send_config_request_reply"].assert_called_once_with(
        accepted=True, error_msg="", metadata={"RID": "12345"}
    )


def test_send_config_request_reply_with_redis(connected_atlas_connector):
    """Test send_config_request_reply stores the response in Redis correctly."""
    # Create a minimal atlas_connector mock
    atlas_mock = mock.MagicMock()
    atlas_mock.scihub = mock.MagicMock()

    # Create config handler with the real redis connector
    config_handler = ConfigHandler(atlas_mock, connected_atlas_connector)

    # Test data
    request_id = "test-request-123"
    metadata = {"RID": request_id}
    error_msg = "Test error message"

    # Call send_config_request_reply
    config_handler.send_config_request_reply(accepted=False, error_msg=error_msg, metadata=metadata)

    # Verify the message was stored in Redis
    response = connected_atlas_connector.get(
        MessageEndpoints.device_config_request_response(request_id)
    )

    assert response is not None
    assert response.content["accepted"] is False
    assert response.content["message"] == error_msg
    assert response.metadata == metadata


def test_send_config_request_reply_expiry_with_redis(connected_atlas_connector):
    """Test send_config_request_reply sets expiry on Redis key."""
    # Create a minimal atlas_connector mock
    atlas_mock = mock.MagicMock()
    atlas_mock.scihub = mock.MagicMock()

    # Create config handler with the real redis connector
    config_handler = ConfigHandler(atlas_mock, connected_atlas_connector)

    # Test data
    request_id = "test-request-expiry-456"
    metadata = {"RID": request_id}

    # Call send_config_request_reply
    config_handler.send_config_request_reply(accepted=True, error_msg=None, metadata=metadata)

    # Verify the message was stored
    response = connected_atlas_connector.get(
        MessageEndpoints.device_config_request_response(request_id)
    )
    assert response is not None
    assert response.content["accepted"] is True

    # Verify TTL is set (fakeredis should support this)
    key = MessageEndpoints.device_config_request_response(request_id).endpoint
    ttl = connected_atlas_connector._redis_conn.ttl(key)
    assert ttl > 0
    assert ttl <= 60  # Should be 60 seconds or less
