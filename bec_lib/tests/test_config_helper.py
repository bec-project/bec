import os
import shutil
from unittest import mock

import pytest
import yaml

import bec_lib
from bec_lib import messages
from bec_lib.bec_errors import DeviceConfigError, ServiceConfigError
from bec_lib.config_helper import ConfigHelper

dir_path = os.path.dirname(bec_lib.__file__)


def test_load_demo_config():
    connector = mock.MagicMock()
    config_helper = ConfigHelper(connector)
    with mock.patch.object(config_helper, "update_session_with_file") as mock_update:
        config_helper.load_demo_config()
        dirpath = os.path.dirname(bec_lib.__file__)
        fpath = os.path.join(dirpath, "configs/demo_config.yaml")
        mock_update.assert_called_once_with(fpath)


def test_config_helper_update_session_with_file():
    connector = mock.MagicMock()
    config_helper = ConfigHelper(connector)
    with mock.patch.object(config_helper, "send_config_request") as mock_send_config_request:
        with mock.patch.object(
            config_helper, "_load_config_from_file"
        ) as mock_load_config_from_file:
            mock_load_config_from_file.return_value = {"test": "test"}
            config_helper._base_path_recovery = "."
            config_helper.update_session_with_file("test.yaml")
            mock_send_config_request.assert_called_once_with(action="set", config={"test": "test"})


@pytest.mark.parametrize("config_file", ["test.yaml", "test.yml"])
def test_config_helper_load_config_from_file(tmp_path, test_config_yaml_file_path, config_file):
    orig_cfg_file = test_config_yaml_file_path
    test_cfg_file = tmp_path / config_file
    shutil.copyfile(orig_cfg_file, test_cfg_file)
    connector = mock.MagicMock()
    config_helper = ConfigHelper(connector)
    config = config_helper._load_config_from_file(test_cfg_file)


def test_config_helper_save_current_session():
    connector = mock.MagicMock()

    config_helper = ConfigHelper(connector)
    connector.get.return_value = messages.AvailableResourceMessage(
        resource=[
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
    )
    with mock.patch("builtins.open", mock.mock_open()) as mock_open:
        config_helper.save_current_session("test.yaml")
        out_data = {
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
        }
        mock_open().write.assert_called_once_with(yaml.dump(out_data))


@pytest.fixture
def config_helper():
    connector = mock.MagicMock()
    config_helper_inst = ConfigHelper(connector)
    with mock.patch.object(config_helper_inst, "wait_for_config_reply"):
        with mock.patch.object(config_helper_inst, "wait_for_service_response"):
            yield config_helper_inst


def test_send_config_request_raises_with_empty_config(config_helper):
    with pytest.raises(DeviceConfigError):
        config_helper.send_config_request(action="update")
        config_helper.wait_for_config_reply.assert_called_once_with(mock.ANY)


def test_send_config_request(config_helper):
    config_helper.send_config_request(action="update", config={"test": "test"})
    config_helper.wait_for_config_reply.return_value = messages.RequestResponseMessage(
        accepted=True, message={"msg": "test"}
    )
    config_helper.wait_for_config_reply.assert_called_once_with(mock.ANY, timeout=32)
    config_helper.wait_for_service_response.assert_called_once_with(mock.ANY, 32)


def test_send_config_request_raises_for_rejected_update(config_helper):
    config_helper.wait_for_config_reply.return_value = messages.RequestResponseMessage(
        accepted=False, message={"msg": "test"}
    )
    with pytest.raises(DeviceConfigError):
        config_helper.send_config_request(action="update", config={"test": "test"})
        config_helper.wait_for_config_reply.assert_called_once_with(mock.ANY)


def test_wait_for_config_reply():
    connector = mock.MagicMock()
    config_helper = ConfigHelper(connector)
    connector.get.return_value = messages.RequestResponseMessage(
        accepted=True, message={"msg": "test"}
    )

    res = config_helper.wait_for_config_reply("test")
    assert res == messages.RequestResponseMessage(accepted=True, message={"msg": "test"})


def test_wait_for_config_raises_timeout():
    connector = mock.MagicMock()
    config_helper = ConfigHelper(connector)
    connector.get.return_value = None

    with pytest.raises(DeviceConfigError):
        config_helper.wait_for_config_reply("test", timeout=0.3)


def test_wait_for_service_response():
    connector = mock.MagicMock()
    config_helper = ConfigHelper(connector)
    connector.lrange.side_effect = [
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


def test_wait_for_service_response_raises_timeout():
    connector = mock.MagicMock()
    config_helper = ConfigHelper(connector)
    connector.lrange.return_value = []

    with pytest.raises(DeviceConfigError):
        config_helper.wait_for_service_response("test", timeout=0.3)


def test_wait_for_service_response_handles_one_by_one():
    mock_msg_1, mock_msg_2, mock_msg_3 = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
    mock_msg_1.content = {"response": {"service": "DeviceServer"}}
    mock_msg_2.content = {"response": {"service": "ScanServer"}}
    mock_msg_3.content = {"response": {"service": "ServiceName123"}}

    connector = mock.MagicMock()
    config_helper = ConfigHelper(connector)
    config_helper._service_name = "ServiceName123"
    connector.lrange = mock.MagicMock(
        side_effect=[(mock_msg_1,), (mock_msg_1, mock_msg_2), (mock_msg_1, mock_msg_2, mock_msg_3)]
    )

    config_helper.wait_for_service_response("test", timeout=0.3)


def test_update_base_path_recovery():
    with mock.patch("bec_lib.bec_service.SERVICE_CONFIG") as mock_service_config:
        with mock.patch("bec_lib.config_helper.DeviceConfigWriter") as mock_device_config_writer:
            config = {"log_writer": {"base_path": "./"}}
            mock_service_config.config = {"service_config": config}
            connector = mock.MagicMock()
            config_helper = ConfigHelper(connector)
            dir_path = os.path.join(
                config["log_writer"]["base_path"], "device_configs/recovery_configs"
            )
            instance = mock_device_config_writer.get_recovery_directory
            instance.return_value = dir_path
            config_helper._update_base_path_recovery()
            assert mock_device_config_writer.call_args == mock.call(config["log_writer"])
            mock_service_config.config = {"service_config": {}}
            with pytest.raises(ServiceConfigError):
                config_helper._update_base_path_recovery()
