import copy
import functools
import os
import time
from unittest import mock

import numpy as np
import pytest
import yaml

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.devices.devicemanager import DeviceManagerDS

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


class ControllerMock:
    def __init__(self, parent) -> None:
        self.parent = parent

    def on(self):
        self.parent._connected = True

    def off(self):
        self.parent._connected = False


class DeviceMock:
    def __init__(self) -> None:
        self._connected = False
        self.name = "name"

    @property
    def connected(self):
        return self._connected


class DeviceControllerMock(DeviceMock):
    def __init__(self) -> None:
        super().__init__()
        self.controller = ControllerMock(self)


class EpicsDeviceMock(DeviceMock):
    def wait_for_connection(self, timeout):
        self._connected = True


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_init(dm_with_devices):
    device_manager = dm_with_devices
    for dev in device_manager.devices.values():
        if not dev.enabled:
            continue
        assert dev.initialized is True


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_proxy_init(dm_with_devices):
    device_manager = dm_with_devices
    assert "sim_proxy_test" in device_manager.devices.keys()
    assert "proxy_cam_test" in device_manager.devices.keys()
    assert "image" in device_manager.devices["proxy_cam_test"].obj.registered_proxies.values()
    assert (
        "sim_proxy_test" in device_manager.devices["proxy_cam_test"].obj.registered_proxies.keys()
    )


@pytest.mark.parametrize(
    "obj,raises_error",
    [(DeviceMock(), True), (DeviceControllerMock(), False), (EpicsDeviceMock(), False)],
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_connect_device(dm_with_devices, obj, raises_error):
    device_manager = dm_with_devices
    if raises_error:
        with pytest.raises(ConnectionError):
            device_manager.connect_device(obj)
        return
    device_manager.connect_device(obj)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_disable_unreachable_devices(device_manager, session_from_test_config):
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
                    with mock.patch("bec_server.device_server.devices.devicemanager.DataStore"):
                        device_manager.initialize("")
                        assert device_manager.config_update_handler is not None
                        assert device_manager.devices.samx.enabled is False
                        msg = messages.DeviceConfigMessage(
                            action="update", config={"samx": {"enabled": False}}
                        )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_flyer_event_callback(dm_with_devices):
    device_manager = dm_with_devices
    samx = device_manager.devices.samx
    samx.metadata = {"scan_id": "12345"}

    device_manager._obj_flyer_callback(
        obj=samx.obj, value={"data": {"idata": np.random.rand(20), "edata": np.random.rand(20)}}
    )
    pipe = device_manager.connector.pipeline()
    bundle, progress = pipe._pipe_buffer[-2:]

    # check connector method
    assert bundle[0] == "send"
    assert progress[0] == "set_and_publish"

    # check endpoint
    assert bundle[1][0] == MessageEndpoints.device_read("samx").endpoint
    assert progress[1][0] == MessageEndpoints.device_progress("samx").endpoint

    # check message
    bundle_msg = bundle[1][1]
    assert len(bundle_msg) == 20

    progress_msg = progress[1][1]
    assert progress_msg.content["status"] == 20


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_obj_progress_callback(dm_with_devices):
    device_manager = dm_with_devices
    samx = device_manager.devices.samx
    samx.metadata = {"scan_id": "12345"}

    with mock.patch.object(device_manager, "connector") as mock_connector:
        device_manager._obj_progress_callback(obj=samx.obj, value=1, max_value=2, done=False)
        mock_connector.set_and_publish.assert_called_once_with(
            MessageEndpoints.device_progress("samx"),
            messages.ProgressMessage(
                value=1, max_value=2, done=False, metadata={"scan_id": "12345"}
            ),
        )


@pytest.mark.parametrize(
    "value", [np.empty(shape=(10, 10)), np.empty(shape=(100, 100)), np.empty(shape=(1000, 1000))]
)
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_obj_device_monitor_2d_callback(dm_with_devices, value):
    device_manager = dm_with_devices
    eiger = device_manager.devices.eiger
    eiger.metadata = {"scan_id": "12345"}
    value_size = len(value.tobytes()) / 1e6  # MB
    max_size = 1000
    timestamp = time.time()
    with mock.patch.object(device_manager, "connector") as mock_connector:
        device_manager._obj_callback_device_monitor_2d(
            obj=eiger.obj, value=value, timestamp=timestamp
        )
        stream_msg = {
            "data": messages.DeviceMonitor2DMessage(
                device=eiger.name, data=value, metadata={"scan_id": "12345"}, timestamp=timestamp
            )
        }

        assert mock_connector.xadd.call_count == 1
        assert mock_connector.xadd.call_args == mock.call(
            MessageEndpoints.device_monitor_2d(eiger.name),
            stream_msg,
            max_size=min(100, int(max_size // value_size)),
        )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_manager_ds_reset_config(dm_with_devices):
    with mock.patch.object(dm_with_devices, "connector") as mock_connector:
        device_manager = dm_with_devices
        config = device_manager._session["devices"]
        device_manager._reset_config()

        config_msg = messages.AvailableResourceMessage(
            resource=config, metadata=mock_connector.lpush.call_args[0][1].metadata
        )
        mock_connector.lpush.assert_called_once_with(
            MessageEndpoints.device_config_history(), config_msg, max_size=50
        )
