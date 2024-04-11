import copy
import functools
import os
from unittest import mock

import numpy as np
import pytest
import yaml

import bec_lib
from bec_lib import MessageEndpoints, messages
from bec_lib.tests.utils import ConnectorMock, create_session_from_config
from bec_server.device_server.devices.devicemanager import DeviceManagerDS

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

dir_path = os.path.dirname(bec_lib.__file__)


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


@functools.lru_cache()
def load_test_config():
    with open(f"{dir_path}/tests/test_config.yaml", "r", encoding="utf-8") as session_file:
        return create_session_from_config(yaml.safe_load(session_file))


def load_device_manager():
    service_mock = mock.MagicMock()
    service_mock.connector = ConnectorMock("", store_data=False)
    device_manager = DeviceManagerDS(service_mock, "")
    device_manager.connector = service_mock.connector
    device_manager.config_update_handler = mock.MagicMock()
    device_manager._session = copy.deepcopy(load_test_config())
    device_manager._load_session()
    return device_manager


@pytest.fixture(scope="function")
def device_manager():
    device_manager = load_device_manager()
    yield device_manager
    device_manager.shutdown()


def test_device_init(device_manager):
    for dev in device_manager.devices.values():
        if not dev.enabled:
            continue
        assert dev.initialized is True


def test_device_proxy_init(device_manager):
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
def test_conntect_device(device_manager, obj, raises_error):
    if raises_error:
        with pytest.raises(ConnectionError):
            device_manager.connect_device(obj)
        return
    device_manager.connect_device(obj)


def test_disable_unreachable_devices():
    service_mock = mock.MagicMock()
    service_mock.connector = ConnectorMock("")
    device_manager = DeviceManagerDS(service_mock)

    def get_config_from_mock():
        device_manager._session = copy.deepcopy(load_test_config())
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
                    assert device_manager.config_update_handler is not None
                    assert device_manager.devices.samx.enabled is False
                    msg = messages.DeviceConfigMessage(
                        action="update", config={"samx": {"enabled": False}}
                    )


def test_flyer_event_callback():
    device_manager = load_device_manager()
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


def test_obj_progress_callback():
    device_manager = load_device_manager()
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
def test_obj_monitor_callback(value):
    device_manager = load_device_manager()
    eiger = device_manager.devices.eiger
    eiger.metadata = {"scan_id": "12345"}
    value_size = len(value.tobytes()) / 1e6  # MB
    max_size = 100
    with mock.patch.object(device_manager, "connector") as mock_connector:
        device_manager._obj_callback_monitor(obj=eiger.obj, value=value)
        mock_connector.xadd.assert_called_once_with(
            MessageEndpoints.device_monitor(eiger.name),
            {
                "data": messages.DeviceMonitorMessage(
                    device=eiger.name, data=value, metadata={"scan_id": "12345"}
                )
            },
            max_size=int(min(100, max_size / value_size)),
        )