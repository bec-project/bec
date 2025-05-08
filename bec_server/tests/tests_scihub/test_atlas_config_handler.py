# pylint: skip-file
import os
from unittest import mock

import pytest
from pydantic import ValidationError

import bec_lib
from bec_lib import messages
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.device import DeviceBase, OnFailure, ReadoutPriority
from bec_lib.endpoints import MessageEndpoints

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


def test_parse_config_request_update(config_handler):
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    with (
        mock.patch.object(config_handler, "_update_config") as update_config,
        mock.patch.object(config_handler.device_manager, "check_request_validity") as req_validity,
    ):
        config_handler.parse_config_request(msg)
        req_validity.assert_called_once_with(msg)
        update_config.assert_called_once_with(msg)


def test_parse_config_request_reload(config_handler):
    msg = messages.DeviceConfigMessage(action="reload", config={}, metadata={})
    with (
        mock.patch.object(config_handler, "_reload_config") as reload_config,
        mock.patch.object(config_handler.device_manager, "check_request_validity") as req_validity,
    ):
        config_handler.parse_config_request(msg)
        req_validity.assert_called_once_with(msg)
        reload_config.assert_called_once_with(msg)


def test_parse_config_request_set(config_handler):
    msg = messages.DeviceConfigMessage(
        action="set", config={"samx": {"enabled": True}}, metadata={}
    )
    with (
        mock.patch.object(config_handler, "_set_config") as set_config,
        mock.patch.object(config_handler.device_manager, "check_request_validity") as req_validity,
    ):
        config_handler.parse_config_request(msg)
        req_validity.assert_called_once_with(msg)
        set_config.assert_called_once_with(msg)


def test_parse_config_request_exception(config_handler):
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
        with mock.patch("bec_server.scihub.atlas.config_handler.traceback.format_exc") as exc:
            with mock.patch.object(config_handler, "_update_config", side_effect=AttributeError()):
                config_handler.parse_config_request(msg)
                req_reply.assert_called_once_with(accepted=False, error_msg=exc(), metadata={})


def test_config_handler_reload_config(config_handler):
    msg = messages.DeviceConfigMessage(action="reload", config={}, metadata={})
    with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
        with mock.patch.object(config_handler, "send_config") as send:
            config_handler.parse_config_request(msg)
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
    with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
        with mock.patch.object(
            config_handler, "_wait_for_device_server_update", return_value=(True, mock.MagicMock())
        ) as wait:
            with mock.patch.object(config_handler, "send_config") as send_config:
                config_handler._set_config(msg)
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
    with pytest.raises(ValidationError):
        with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
            config_handler._set_config(msg)
            req_reply.assert_called_once_with(
                accepted=True, error_msg=None, metadata={"RID": "12345"}
            )


def test_config_handler_update_config(config_handler):
    dev = config_handler.device_manager.devices
    dev.samx = DeviceBase(name="samx", config={})
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    with mock.patch.object(
        config_handler, "_update_device_config", return_value=True
    ) as update_device_config:
        with mock.patch.object(config_handler, "update_config_in_redis") as update_config_in_redis:
            with mock.patch.object(config_handler, "send_config") as send_config:
                with mock.patch.object(
                    config_handler, "send_config_request_reply"
                ) as send_config_request_reply:
                    config_handler._update_config(msg)
                    update_device_config.assert_called_once_with(dev["samx"], {"enabled": True})
                    update_config_in_redis.assert_called_once_with(dev["samx"])

                    send_config.assert_called_once_with(msg)
                    send_config_request_reply.assert_called_once_with(
                        accepted=True, error_msg=None, metadata={}
                    )


def test_config_handler_update_config_not_updated(config_handler):
    dev = config_handler.device_manager.devices
    dev.samx = DeviceBase(name="samx", config={})
    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"enabled": True}}, metadata={}
    )
    with mock.patch.object(
        config_handler, "_update_device_config", return_value=False
    ) as update_device_config:
        with mock.patch.object(config_handler, "update_config_in_redis") as update_config_in_redis:
            with mock.patch.object(config_handler, "send_config") as send_config:
                with mock.patch.object(
                    config_handler, "send_config_request_reply"
                ) as send_config_request_reply:
                    config_handler._update_config(msg)
                    update_device_config.assert_called_once_with(dev["samx"], {"enabled": True})
                    update_config_in_redis.assert_not_called()

                    send_config.assert_not_called()
                    send_config_request_reply.assert_not_called()


def test_config_handler_update_device_config_enable(config_handler):
    dev = config_handler.device_manager.devices
    dev.samx = DeviceBase(name="samx", config={})
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


def test_config_handler_update_device_config_deviceConfig(config_handler):
    dev = config_handler.device_manager.devices
    dev.samx = DeviceBase(name="samx", config={"deviceConfig": {}})
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
                assert dev.samx._config == {"deviceConfig": {"something": "to_update"}}


def test_config_handler_update_device_config_misc(config_handler):
    dev = config_handler.device_manager.devices
    dev.samx = DeviceBase(name="samx", config={})
    with mock.patch.object(config_handler, "_validate_update") as validate_update:
        device = dev["samx"]
        config_handler._update_device_config(device, {"readOnly": True})
        validate_update.assert_called_once_with({"readOnly": True})


def test_config_handler_update_device_config_raise(config_handler):
    dev = config_handler.device_manager.devices
    dev.samx = DeviceBase(name="samx", config={})
    with mock.patch.object(config_handler, "_validate_update") as validate_update:
        device = dev["samx"]
        with pytest.raises(DeviceConfigError):
            config_handler._update_device_config(device, {"doesnt_exist": False})


def test_config_handler_update_device_config_available_keys(config_handler, available_keys_fixture):
    for available_key in available_keys_fixture:
        dev = config_handler.device_manager.devices
        if available_key in ["deviceConfig", "userParameter"]:
            init = {"something": "to_update"}
            dev.samx = DeviceBase(name="samx", config={available_key: init})
        elif available_key in ["softwareTrigger", "readOnly"]:
            init = True
            dev.samx = DeviceBase(name="samx", config={available_key: init})
        elif available_key in ["readoutPriority"]:
            init = ReadoutPriority.BASELINE
            dev.samx = DeviceBase(name="samx", config={available_key: init})
        elif available_key in ["onFailure"]:
            init = OnFailure.BUFFER
            dev.samx = DeviceBase(name="samx", config={available_key: init})
        elif available_key in ["deviceTags"]:
            init = ["something"]
            dev.samx = DeviceBase(name="samx", config={available_key: init})
        else:
            dev.samx = DeviceBase(name="samx", config={})
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
                    assert dev.samx._config == {available_key: update}


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


def test_config_handler_update_config_in_redis(config_handler):
    with mock.patch.object(config_handler, "get_config_from_redis") as get_config:
        with mock.patch.object(config_handler, "set_config_in_redis") as set_config:
            get_config.return_value = [{"name": "samx", "config": {}}]
            dev = config_handler.device_manager.devices
            dev.samx = DeviceBase(
                name="samx", config={"deviceConfig": {"something": "to_update"}, "name": "samx"}
            )
            config_handler.update_config_in_redis(dev["samx"])
            get_config.assert_called_once()
            set_config.assert_called_once_with(
                [{"deviceConfig": {"something": "to_update"}, "name": "samx"}]
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
    with mock.patch.object(config_handler, "add_devices_to_redis") as add_devices:
        with mock.patch.object(config_handler, "_update_device_server") as update_dev_server:
            with mock.patch.object(
                config_handler, "_wait_for_device_server_update"
            ) as wait_dev_server_update:
                wait_dev_server_update.return_value = (True, mock.MagicMock())
                with mock.patch.object(config_handler, "send_config_request_reply") as req_reply:
                    config_handler._add_to_config(msg)
                    add_devices.assert_called_once_with(config)
                    req_reply.assert_called_once_with(
                        accepted=True, error_msg=None, metadata={"RID": "12345"}
                    )


def test_config_handler_remove_from_config(config_handler):
    msg = messages.DeviceConfigMessage(
        action="remove", config={"samx": {}}, metadata={"RID": "12345"}
    )
    config_handler.device_manager.devices.samx = DeviceBase(name="samx", config={})
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
