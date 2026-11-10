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
    wrapper = SimpleNamespace(
        obj=device, enabled=True, _config={"deviceConfig": {"save_roi_data": True}}
    )
    providers = SimpleNamespace(
        update_device_config=mock.Mock(
            side_effect=lambda target, cfg: setattr(
                target.obj, "save_roi_data", cfg["save_roi_data"]
            )
        ),
        get_device_info=mock.Mock(
            side_effect=lambda target: {"save_roi_data": target.obj.save_roi_data}
        ),
    )
    manager = SimpleNamespace(
        devices={"camera": wrapper},
        device_provider_manager=providers,
        publish_device_info=mock.Mock(),
    )
    return SimpleNamespace(device_manager=manager), device


def test_update_config_republishes_metadata_after_applying_values():
    handler, _ = make_handler()
    observed = []
    handler.device_manager.publish_device_info.side_effect = lambda _, info: observed.append(
        info["save_roi_data"]
    )
    msg = DeviceConfigMessage(
        action="update", config={"camera": {"deviceConfig": {"save_roi_data": False}}}
    )
    ConfigUpdateHandler._update_config(handler, msg, threading.Event())
    assert observed == [False]
    handler.device_manager.publish_device_info.assert_called_once_with(
        handler.device_manager.devices["camera"], {"save_roi_data": False}
    )


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
