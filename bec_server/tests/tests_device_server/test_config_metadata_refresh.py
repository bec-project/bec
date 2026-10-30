"""Runtime config updates publish signal metadata before acknowledgement."""

import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib.bec_errors import DeviceConfigError
from bec_lib.messages import DeviceConfigMessage
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler


def make_handler():
    device = SimpleNamespace(save_roi_data=True)
    manager = SimpleNamespace(
        devices={
            "camera": SimpleNamespace(
                obj=device, enabled=True, _config={"deviceConfig": {"save_roi_data": True}}
            )
        },
        update_config=mock.Mock(
            side_effect=lambda obj, cfg: setattr(obj, "save_roi_data", cfg["save_roi_data"])
        ),
        publish_device_info=mock.Mock(return_value=None),
    )
    return SimpleNamespace(device_manager=manager), device


def test_update_config_republishes_metadata_after_applying_values():
    handler, device = make_handler()
    observed = []
    handler.device_manager.publish_device_info.side_effect = lambda obj: observed.append(
        obj.save_roi_data
    )
    msg = DeviceConfigMessage(
        action="update", config={"camera": {"deviceConfig": {"save_roi_data": False}}}
    )
    ConfigUpdateHandler._update_config(handler, msg, threading.Event())
    assert observed == [False]
    handler.device_manager.publish_device_info.assert_called_once_with(device)


def test_failed_metadata_publication_restores_old_config_and_metadata():
    handler, device = make_handler()
    handler.device_manager.publish_device_info.side_effect = [
        RuntimeError("Metadata unavailable"),
        None,
    ]
    msg = DeviceConfigMessage(
        action="update", config={"camera": {"deviceConfig": {"save_roi_data": False}}}
    )
    with pytest.raises(DeviceConfigError, match="Metadata unavailable"):
        ConfigUpdateHandler._update_config(handler, msg, threading.Event())
    assert device.save_roi_data is True
    assert handler.device_manager.publish_device_info.call_count == 2
