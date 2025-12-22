import os
import shutil
from unittest import mock

import pytest
import yaml

import bec_lib
from bec_lib import messages
from bec_lib.bec_errors import DeviceConfigError, ServiceConfigError
from bec_lib.config_helper import ConfigHelper
from bec_lib.service_config import ServiceConfigModel

dir_path = os.path.dirname(bec_lib.__file__)

# pylint: disable=protected-access


@pytest.fixture
def config_helper_plain(dm_with_devices) -> ConfigHelper:
    connector = mock.MagicMock()
    config_helper_inst = ConfigHelper(connector, device_manager=dm_with_devices)
    return config_helper_inst


@pytest.fixture
def config_helper(config_helper_plain):
    config_helper_inst = config_helper_plain
    with mock.patch.object(config_helper_inst, "wait_for_config_reply"):
        with mock.patch.object(config_helper_inst, "wait_for_service_response"):
            yield config_helper_inst


def test_load_demo_config(config_helper):
    with mock.patch.object(config_helper, "update_session_with_file") as mock_update:
        config_helper.load_demo_config()
        dirpath = os.path.dirname(bec_lib.__file__)
        fpath = os.path.join(dirpath, "configs/demo_config.yaml")
        mock_update.assert_called_once_with(fpath, force=False)


def test_config_helper_update_session_with_file(config_helper):
    with mock.patch.object(config_helper, "send_config_request") as mock_send_config_request:
        with mock.patch.object(
            config_helper, "_load_config_from_file"
        ) as mock_load_config_from_file:
            mock_load_config_from_file.return_value = {"test": "test"}
            config_helper._base_path_recovery = "."
            config_helper._writer_mixin = mock.MagicMock()
            config_helper.update_session_with_file("test.yaml")
            mock_send_config_request.assert_called_once_with(action="set", config={"test": "test"})


@pytest.mark.parametrize("config_file", ["test.yaml", "test.yml"])
def test_config_helper_load_config_from_file(
    config_helper, tmp_path, test_config_yaml_file_path, config_file
):
    orig_cfg_file = test_config_yaml_file_path
    test_cfg_file = tmp_path / config_file
    shutil.copyfile(orig_cfg_file, test_cfg_file)
    config_helper._load_config_from_file(test_cfg_file)


def test_config_helper_save_current_session(config_helper):
    config = [
        {
            "id": "648c817f67d3c7cd6a354e8e",
            "createdAt": "2023-06-16T15:36:31.215Z",
            "createdBy": "unknown user",
            "name": "pinz",
            "sessionId": "648c817d67d3c7cd6a354df2",
            "enabled": True,
            "readOnly": False,
            "deviceClass": "SimPositioner",
            "deviceTags": {"user motors"},
            "deviceConfig": {
                "delay": 1,
                "labels": "pinz",
                "limits": [-50, 50],
                "name": "pinz",
                "tolerance": 0.01,
                "update_frequency": 400,
            },
            "readoutPriority": "baseline",
            "onFailure": "retry",
        },
        {
            "id": "648c817f67d3c7cd6a354ec5",
            "createdAt": "2023-06-16T15:36:31.764Z",
            "createdBy": "unknown user",
            "name": "transd",
            "sessionId": "648c817d67d3c7cd6a354df2",
            "enabled": True,
            "readOnly": False,
            "deviceClass": "SimMonitor",
            "deviceTags": {"beamline"},
            "deviceConfig": {"labels": "transd", "name": "transd", "tolerance": 0.5},
            "readoutPriority": "monitored",
            "onFailure": "retry",
        },
    ]
    msg = messages.AvailableResourceMessage(resource=config)
    with mock.patch("builtins.open", mock.mock_open()) as mock_open:
        with mock.patch.object(config_helper._device_manager.connector, "get") as mock_get:
            mock_get.return_value = msg
            config_helper.save_current_session("test.yaml")
            out_data = {
                "pinz": {
                    "deviceClass": "SimPositioner",
                    "deviceTags": ["user motors"],
                    "enabled": True,
                    "deviceConfig": {
                        "delay": 1,
                        "labels": "pinz",
                        "limits": [-50, 50],
                        "name": "pinz",
                        "tolerance": 0.01,
                        "update_frequency": 400,
                    },
                    "readoutPriority": "baseline",
                },
                "transd": {
                    "deviceClass": "SimMonitor",
                    "deviceTags": ["beamline"],
                    "enabled": True,
                    "deviceConfig": {"labels": "transd", "name": "transd", "tolerance": 0.5},
                    "readoutPriority": "monitored",
                },
            }
            call = mock_open().write.call_args[0][0]
            assert yaml.safe_load(call) == out_data


def test_send_config_request_raises_with_empty_config(config_helper):
    with pytest.raises(DeviceConfigError):
        config_helper.send_config_request(action="update")
        config_helper.wait_for_config_reply.assert_called_once_with(mock.ANY)


def test_send_config_request(config_helper):
    config_helper.send_config_request(action="update", config={"test": "test"})
    config_helper.wait_for_config_reply.return_value = messages.RequestResponseMessage(
        accepted=True, message={"msg": "test"}
    )
    config_helper.wait_for_config_reply.assert_called_once_with(
        mock.ANY, timeout=32, send_cancel_on_interrupt=True
    )
    config_helper.wait_for_service_response.assert_called_once_with(mock.ANY, 32)


def test_send_config_request_raises_for_rejected_update(config_helper):
    config_helper.wait_for_config_reply.return_value = messages.RequestResponseMessage(
        accepted=False, message={"msg": "test"}
    )
    with pytest.raises(DeviceConfigError):
        config_helper.send_config_request(action="update", config={"test": "test"})
        config_helper.wait_for_config_reply.assert_called_once_with(mock.ANY)


def test_wait_for_config_reply(config_helper_plain):
    config_helper_plain._connector.get.return_value = messages.RequestResponseMessage(
        accepted=True, message={"msg": "test"}
    )

    res = config_helper_plain.wait_for_config_reply("test")
    assert res == messages.RequestResponseMessage(accepted=True, message={"msg": "test"})


def test_wait_for_config_raises_timeout(config_helper_plain):
    config_helper_plain._connector.get.return_value = None

    with pytest.raises(DeviceConfigError):
        config_helper_plain.wait_for_config_reply("test", timeout=0.3)


def test_wait_for_service_response(config_helper):
    config_helper._connector.lrange.side_effect = [
        [],
        [
            messages.ServiceResponseMessage(
                response={"service": "DeviceServer"}, metadata={"RID": "test"}
            ),
            messages.ServiceResponseMessage(
                response={"service": "ScanServer"}, metadata={"RID": "test"}
            ),
        ],
    ]

    config_helper.wait_for_service_response("test", timeout=0.3)


def test_wait_for_service_response_raises_timeout(config_helper_plain):
    config_helper = config_helper_plain
    config_helper._connector.lrange.return_value = []

    with pytest.raises(DeviceConfigError):
        config_helper.wait_for_service_response("test", timeout=0.3)


def test_wait_for_service_response_handles_one_by_one(config_helper_plain):
    mock_msg_1, mock_msg_2, mock_msg_3 = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
    mock_msg_1.content = {"response": {"service": "DeviceServer"}}
    mock_msg_2.content = {"response": {"service": "ScanServer"}}
    mock_msg_3.content = {"response": {"service": "ServiceName123"}}

    config_helper = config_helper_plain
    connector = config_helper._connector
    config_helper._service_name = "ServiceName123"
    connector.lrange = mock.MagicMock(
        side_effect=[(mock_msg_1,), (mock_msg_1, mock_msg_2), (mock_msg_1, mock_msg_2, mock_msg_3)]
    )

    config_helper.wait_for_service_response("test", timeout=0.3)


def test_update_base_path_recovery(config_helper_plain):
    with mock.patch("bec_lib.bec_service.SERVICE_CONFIG") as mock_service_config:
        with mock.patch("bec_lib.config_helper.DeviceConfigWriter") as mock_device_config_writer:
            config = ServiceConfigModel(**{"log_writer": {"base_path": "./"}}).model_dump()
            mock_service_config.config = config
            config_helper = config_helper_plain
            dir_path = os.path.join(
                config["log_writer"]["base_path"], "device_configs/recovery_configs"
            )
            instance = mock_device_config_writer.get_recovery_directory
            instance.return_value = dir_path
            config_helper._update_base_path_recovery()
            assert mock_device_config_writer.call_args == mock.call(config["log_writer"])
            mock_service_config.config = {}
            with pytest.raises(ServiceConfigError):
                config_helper._update_base_path_recovery()


@pytest.mark.parametrize(
    "new_config, current_config, expected_conflicts",
    [
        (
            {
                "pinz": {
                    "deviceClass": "SimPositioner",
                    "deviceTags": {"user motors"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {
                        "delay": 1,
                        "labels": "pinz",
                        "limits": [-50, 50],
                        "name": "pinz",
                        "tolerance": 0.01,
                        "update_frequency": 400,
                    },
                    "readoutPriority": "baseline",
                    "onFailure": "retry",
                },
                "transd": {
                    "deviceClass": "SimMonitor",
                    "deviceTags": {"beamline"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {"labels": "transd", "name": "transd", "tolerance": 0.5},
                    "readoutPriority": "monitored",
                    "onFailure": "retry",
                },
            },
            {
                "pinz": {
                    "deviceClass": "SimPositioner",
                    "deviceTags": {"user motors"},
                    "enabled": True,
                    "readOnly": True,
                    "deviceConfig": {
                        "delay": 1,
                        "labels": "pinz",
                        "limits": [-50, 50],
                        "name": "pinz",
                        "tolerance": 0.01,
                        "update_frequency": 400,
                    },
                    "readoutPriority": "baseline",
                    "onFailure": "retry",
                },
                "transd": {
                    "deviceClass": "SimMonitor",
                    "deviceTags": {"beamline"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {"labels": "transd", "name": "transd", "tolerance": 0.5},
                    "readoutPriority": "monitored",
                    "onFailure": "retry",
                },
            },
            {"pinz": {"readOnly": {"new": False, "current": True}}},
        ),
        (
            {
                "pinz": {
                    "deviceClass": "SimPositioner",
                    "deviceTags": {"user motors"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {
                        "delay": 1,
                        "labels": "pinz",
                        "limits": [-50, 50],
                        "name": "pinz",
                        "tolerance": 0.01,
                        "update_frequency": 400,
                    },
                    "readoutPriority": "baseline",
                    "onFailure": "retry",
                },
                "transd": {
                    "deviceClass": "SimMonitor",
                    "deviceTags": {"beamline"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {"labels": "transd", "name": "transd", "tolerance": 0.5},
                    "readoutPriority": "monitored",
                    "onFailure": "retry",
                },
            },
            {
                "pinz": {
                    "deviceClass": "SimPositioner",
                    "deviceTags": {"user motors"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {
                        "delay": 1,
                        "labels": "pinz",
                        "limits": [-20, 20],
                        "name": "pinz",
                        "tolerance": 0.01,
                        "update_frequency": 400,
                    },
                    "readoutPriority": "baseline",
                    "onFailure": "retry",
                },
                "transd": {
                    "deviceClass": "SimMonitor",
                    "deviceTags": {"beamline"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {"labels": "transd", "name": "transd", "tolerance": 0.5},
                    "readoutPriority": "monitored",
                    "onFailure": "retry",
                },
            },
            {"pinz": {"deviceConfig": {"limits": {"new": [-50, 50], "current": [-20, 20]}}}},
        ),
        (
            {
                "pinz": {
                    "deviceClass": "SimPositioner",
                    "deviceTags": {"user motors"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {
                        "delay": 1,
                        "labels": "pinz",
                        "limits": [-50, 50],
                        "name": "pinz",
                        "tolerance": 0.01,
                        "update_frequency": 400,
                    },
                    "readoutPriority": "baseline",
                    "onFailure": "retry",
                },
                "transd": {
                    "deviceClass": "SimMonitor",
                    "deviceTags": {"beamline"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {"labels": "transd", "name": "transd", "tolerance": 0.5},
                    "readoutPriority": "monitored",
                    "onFailure": "retry",
                },
            },
            {
                "pinz": {
                    "deviceClass": "SimPositioner",
                    "deviceTags": {"user motors"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {
                        "delay": 1,
                        "labels": "pinz",
                        "limits": [-50, 50],
                        "name": "pinz",
                        "tolerance": 0.01,
                        "update_frequency": 400,
                    },
                    "readoutPriority": "baseline",
                    "onFailure": "retry",
                },
                "transd": {
                    "deviceClass": "SimMonitor",
                    "deviceTags": {"beamline"},
                    "enabled": True,
                    "readOnly": False,
                    "deviceConfig": {"labels": "transd", "name": "transd", "tolerance": 0.5},
                    "readoutPriority": "monitored",
                    "onFailure": "retry",
                    "userParameter": {"in": 123},
                },
            },
            {"transd": {"userParameter": {"in": {"new": None, "current": 123}}}},
        ),
    ],
)
def test_config_helper_get_config_conflicts(
    config_helper: ConfigHelper, new_config: dict, current_config: dict, expected_conflicts: dict
):

    config_in_redis = []
    for dev_name, dev_cfg in current_config.items():
        config = {}
        config["name"] = dev_cfg.pop("name", dev_name)
        config.update(dev_cfg)
        config_in_redis.append(config)
    with mock.patch.object(config_helper._device_manager.connector, "get") as mock_get:
        mock_get.return_value = messages.AvailableResourceMessage(resource=config_in_redis)
        conflicts = config_helper._get_config_conflicts(new_config)
        assert conflicts == expected_conflicts


def test_format_value_for_display(config_helper):
    """Test value formatting for display in conflict resolution."""
    # Test None
    assert config_helper._format_value_for_display(None) == "None"

    # Test list (should be single line)
    result = config_helper._format_value_for_display([1, 2, 3])
    assert "\n" not in result
    assert "1" in result and "2" in result and "3" in result

    # Test dict (can be multi-line)
    result = config_helper._format_value_for_display({"key": "value"})
    assert "key" in result and "value" in result

    # Test set
    result = config_helper._format_value_for_display({"a", "b"})
    assert "\n" not in result

    # Test string
    assert config_helper._format_value_for_display("test") == "test"


def test_apply_config_value_simple(config_helper):
    """Test applying a simple config value."""
    config = {}
    config_helper._apply_config_value(config, "device1", "enabled", None, True)
    assert config == {"device1": {"enabled": True}}


def test_apply_config_value_nested(config_helper):
    """Test applying a nested config value."""
    config = {}
    config_helper._apply_config_value(config, "device1", "deviceConfig", "timeout", 30)
    assert config == {"device1": {"deviceConfig": {"timeout": 30}}}

    # Add another nested value to same device
    config_helper._apply_config_value(config, "device1", "deviceConfig", "delay", 5)
    assert config == {"device1": {"deviceConfig": {"timeout": 30, "delay": 5}}}


def test_apply_all_current_values(config_helper):
    """Test applying all current values for conflicts."""
    config = {
        "device1": {"enabled": True, "deviceConfig": {"timeout": 30}},
        "device2": {"readOnly": False},
    }

    conflicts = {
        "device1": {
            "enabled": {"new": True, "current": False},
            "deviceConfig": {
                "timeout": {"new": 30, "current": 60},
                "delay": {"new": 5, "current": 10},
            },
        },
        "device2": {"readOnly": {"new": False, "current": True}},
    }

    config_helper._apply_all_current_values(config, conflicts)

    # All values should now be the "current" values
    assert config["device1"]["enabled"] is False
    assert config["device1"]["deviceConfig"]["timeout"] == 60
    assert config["device1"]["deviceConfig"]["delay"] == 10
    assert config["device2"]["readOnly"] is True


def test_merge_conflicts_accept_all(config_helper):
    """Test accepting all new values in conflict resolution."""
    config = {"device1": {"enabled": True}}
    conflicts = {"device1": {"enabled": {"new": True, "current": False}}}

    with mock.patch("bec_lib.config_helper.Prompt.ask", return_value="a"):
        config_helper._merge_conflicts_with_user_input(config, conflicts)

    # Config should still have new value (no changes needed)
    assert config["device1"]["enabled"] is True


def test_merge_conflicts_keep_all(config_helper):
    """Test keeping all current values in conflict resolution."""
    config = {"device1": {"enabled": True}}
    conflicts = {"device1": {"enabled": {"new": True, "current": False}}}

    with mock.patch("bec_lib.config_helper.Prompt.ask", return_value="k"):
        config_helper._merge_conflicts_with_user_input(config, conflicts)

    # Config should now have current value
    assert config["device1"]["enabled"] is False


def test_merge_conflicts_individual_resolution(config_helper):
    """Test individual conflict resolution."""
    config = {"device1": {"enabled": True}}
    conflicts = {"device1": {"enabled": {"new": True, "current": False}}}

    # Mock: first prompt returns "r" for individual, second returns "k" to keep current
    with mock.patch("bec_lib.config_helper.Prompt.ask", side_effect=["r", "k"]):
        config_helper._merge_conflicts_with_user_input(config, conflicts)

    # Config should have current value after individual resolution
    assert config["device1"]["enabled"] is False
