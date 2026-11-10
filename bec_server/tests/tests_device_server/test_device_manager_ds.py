import copy
import threading
import time
from types import SimpleNamespace
from unittest import mock

import numpy as np
import ophyd
import ophyd_devices as opd
import pytest
from ophyd_devices.devices.psi_motor import EpicsMotor
from ophyd_devices.tests.utils import patched_device

from bec_lib import messages
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.device_providers.device_provider_manager import DeviceProviderManager
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.device_info import DeviceInfo
from bec_server.device_server.devices.devicemanager import DeviceManagerDS
from bec_server.device_server.devices.ds_device import DSDevice, DSDeviceConfig

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("metadata", [{}, {"RID": "reload-request"}])
def test_reload_notification_does_not_change_device_server_status(device_manager, metadata):
    device_manager._service._service_name = "DeviceServer"
    msg = messages.DeviceConfigMessage(action="reload", config={}, metadata=metadata)
    with (
        mock.patch.object(device_manager, "update_status") as update_status,
        mock.patch.object(device_manager, "_reload_action") as reload_action,
        mock.patch.object(device_manager.connector, "lpush") as lpush,
    ):
        device_manager._device_config_update_callback(MessageObject(value=msg, topic=""))

    update_status.assert_not_called()
    reload_action.assert_not_called()
    if metadata:
        lpush.assert_called_once_with(
            MessageEndpoints.service_response(metadata["RID"]),
            messages.ServiceResponseMessage(
                response={"accepted": True, "service": device_manager._service._service_name}
            ),
            expire=100,
        )
    else:
        lpush.assert_not_called()


@pytest.fixture
def dm_with_devices_and_status(dm_with_devices):
    """
    Fixture that adds the scan_info message to the device manager
    """
    device_manager = dm_with_devices
    device_manager.scan_info.msg = messages.ScanStatusMessage(
        scan_id="12345", status="open", info={"num_points": 10, "RID": "RID123"}
    )
    yield device_manager


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
    """Mock device with controller attribute that manages connection via wait_for_connection"""

    def __init__(self) -> None:
        super().__init__()
        self.controller = ControllerMock(self)

    def wait_for_connection(self, timeout):
        self.controller.on()


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
    device = DSDevice(name=obj.name, obj=obj, config=None)
    if raises_error:
        assert isinstance(
            device_manager.device_provider_manager.providers["ophyd"].connect_device(device),
            Exception,
        )
    else:
        assert (
            device_manager.device_provider_manager.providers["ophyd"].connect_device(device) is None
        )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_connect_device_waits_for_all_signals(dm_with_devices):
    """Connect all signals with the default timeout when no timeout is configured."""
    device_manager = dm_with_devices
    obj = EpicsDeviceMock()
    device = DSDevice(name=obj.name, obj=obj, config=None)

    with mock.patch.object(obj, "wait_for_connection") as mock_wait_for_connection:
        device_manager.device_provider_manager.providers["ophyd"].connect_device(device)
        mock_wait_for_connection.assert_called_once_with(all_signals=True, timeout=5)
        mock_wait_for_connection.reset_mock()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_disable_unreachable_devices(device_manager, session_from_test_config):
    def get_config_from_mock():
        device_manager._session = copy.deepcopy(session_from_test_config)
        device_manager._load_session()

    def mocked_failed_connection(obj, **kwargs):
        if obj.name == "samx":
            return ConnectionError("Failed to connect to samx device")
        return None

    config_reply = messages.RequestResponseMessage(accepted=True, message="")

    with mock.patch.object(
        device_manager.device_provider_manager.providers["ophyd"],
        "connect_device",
        wraps=mocked_failed_connection,
    ):
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


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("reload_config", [False, True])
@pytest.mark.parametrize("cleanup_error", [False, True])
def test_load_unreachable_device_cleans_up_and_disables_config(
    device_manager, connected_connector, reload_config, cleanup_error
):
    config = {
        "name": "unreachable_signal",
        "deviceClass": "ophyd.Signal",
        "deviceConfig": {},
        "enabled": True,
        "readoutPriority": "monitored",
    }
    device_manager.connector = connected_connector
    handler = ConfigUpdateHandler(device_manager)
    device_manager.config_update_handler = handler
    # Other suites can override the shared fixture with a manager that already has devices.
    handler._flush_config()
    connected_connector.set(
        MessageEndpoints.device_config(), messages.AvailableResourceMessage(resource=[config])
    )
    providers = device_manager.device_provider_manager
    device, obj_config = providers.construct_device(DSDeviceConfig.model_validate(config))
    obj = device.obj
    original_error = "original connection failure"
    reload_msg = messages.DeviceConfigMessage(
        action="reload", config={}, metadata={"RID": "unreachable-reload"}
    )

    with (
        mock.patch.object(providers, "construct_device", return_value=(device, obj_config)),
        mock.patch.object(
            type(obj), "connected", new_callable=mock.PropertyMock, return_value=False
        ),
        mock.patch.object(obj, "wait_for_connection", side_effect=ConnectionError(original_error)),
        mock.patch.object(
            obj,
            "destroy",
            wraps=obj.destroy,
            side_effect=RuntimeError("cleanup failure") if cleanup_error else None,
        ) as destroy,
        mock.patch("bec_server.device_server.devices.config_update_handler.reload_plugin_modules"),
    ):
        if reload_config:
            handler.parse_config_request(reload_msg, cancel_event=threading.Event())
        else:
            device_manager._get_config()

        destroy.assert_called_once_with()
        device = device_manager.devices[config["name"]]
        assert device.obj is obj
        assert device.enabled is False
        assert device.initialized is False
        assert device_manager.current_session["devices"][0]["enabled"] is False
        redis_config = connected_connector.get(MessageEndpoints.device_config())
        assert redis_config.resource[0]["enabled"] is False
        failure = device_manager.failed_devices[config["name"]]
        assert original_error in failure
        assert "cleanup failure" not in failure
        if reload_config:
            reply = connected_connector.get(
                MessageEndpoints.device_config_request_response(reload_msg.metadata["RID"])
            )
            assert reply.accepted is True
            assert reply.metadata["failed_devices"][config["name"]] == failure


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_obj_callback_configuration(dm_with_devices, connected_connector):
    device_manager = dm_with_devices
    samx = device_manager.devices.samx
    samx.metadata = {"scan_id": "12345"}
    device_manager.connector = connected_connector

    device_manager._obj_callback_configuration(obj=samx.obj)

    msg = connected_connector.get(MessageEndpoints.device_read_configuration("samx"))
    expected = messages.DeviceMessage(
        signals=samx.obj.read_configuration(), metadata={"scan_id": "12345"}
    )
    assert msg == expected


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


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_subscribe_to_device_events(dm_with_devices):
    opaas_obj = mock.MagicMock()
    opaas_obj.enabled = False
    obj = mock.MagicMock()
    for event_type in ("readback", "value", "done_moving"):
        obj.event_types = (event_type,)
        obj.subscribe.reset_mock()
        with mock.patch.object(dm_with_devices, "_obj_callback_readback") as mock_callback:
            dm_with_devices._subscribe_to_device_events(obj=obj, opaas_obj=opaas_obj)
            obj.subscribe.assert_called_once_with(mock_callback, event_type=event_type, run=False)

    obj.event_types = ("device_monitor_1d", "device_monitor_2d", "file_event", "flyer", "progress")
    obj.subscribe.reset_mock()
    dm_with_devices._subscribe_to_device_events(obj=obj, opaas_obj=opaas_obj)
    obj.subscribe.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize(
    "value",
    [
        None,
        messages.DevicePreviewMessage(
            data=np.random.rand(10, 10), device="bec_signals_device", signal="preview"
        ),
        "some string",
    ],
)
def test_device_manager_ds_obj_callback_preview(dm_with_devices, value):
    device_manager = dm_with_devices
    device = dm_with_devices.devices.bec_signals_device.obj
    with mock.patch.object(device_manager.connector, "xadd") as mock_xadd:
        device_manager._obj_callback_bec_message_signal(obj=device.preview, value=value)

        if not isinstance(value, messages.DevicePreviewMessage):
            mock_xadd.assert_not_called()
        else:
            rot90 = device.preview.num_rotation_90
            transpose = device.preview.transpose
            if rot90:
                value.data = np.rot90(value.data, k=rot90, axes=(0, 1))
            if transpose:
                value.data = np.transpose(value.data)
            mock_xadd.assert_called_once_with(
                MessageEndpoints.device_preview(device="bec_signals_device", signal="preview"),
                {"data": value},
                max_size=100,  # Assuming a default max size
                expire=3600,
            )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize(
    "value",
    [
        None,
        messages.FileMessage(
            file_path="some/path/to/file.h5",
            done=True,
            successful=True,
            hinted_h5_entries={"my_entry": "entry/data/data"},
            is_master_file=False,
            metadata={"user_info": "my_info"},
        ),
        "some string",
    ],
)
def test_device_manager_ds_obj_callback_file_event_signal(dm_with_devices_and_status, value):
    device_manager = dm_with_devices_and_status
    device = device_manager.devices.bec_signals_device.obj
    with mock.patch.object(device_manager._bec_message_handler, "connector") as mock_connector:
        device_manager._obj_callback_bec_message_signal(obj=device.file_event, value=value)

        if not isinstance(value, messages.FileMessage):
            mock_connector.set_and_publish.assert_not_called()
        else:
            pipe = mock_connector.pipeline.return_value
            assert mock_connector.set_and_publish.call_args_list == [
                mock.call(MessageEndpoints.file_event(device.name), value, pipe=pipe, expire=3600),
                mock.call(
                    MessageEndpoints.public_file(scan_id="12345", name=device.name),
                    value,
                    pipe=pipe,
                    expire=3600,
                ),
            ]
            pipe.execute.assert_called_once_with()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize(
    "value", [None, messages.ProgressMessage(value=1, max_value=2, done=False), "some string"]
)
def test_device_manager_ds_obj_callback_progress_signal(dm_with_devices_and_status, value):
    device_manager = dm_with_devices_and_status
    device = device_manager.devices.bec_signals_device.obj
    with mock.patch.object(device_manager._bec_message_handler, "connector") as mock_connector:
        device_manager._obj_callback_bec_message_signal(obj=device.progress, value=value)

        if not isinstance(value, messages.ProgressMessage):
            mock_connector.set_and_publish.assert_not_called()
        else:
            mock_connector.set_and_publish.assert_called_once_with(
                MessageEndpoints.device_progress(device.name), value, expire=3600
            )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_manager_ds_obj_callback_progress_signal_disabled_device(dm_with_devices):
    device_manager = dm_with_devices
    device = dm_with_devices.devices.bec_signals_device.obj
    with mock.patch.object(
        type(device.progress), "connected", new_callable=mock.PropertyMock
    ) as mock_connected:
        mock_connected.return_value = False
        with mock.patch.object(device_manager._bec_message_handler, "emit") as mock_emit:
            device_manager._obj_callback_bec_message_signal(
                obj=device.progress,
                value=messages.ProgressMessage(
                    value=1, max_value=2, done=False, metadata={"scan_id": "12345"}
                ),
            )
            mock_emit.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize(
    "value",
    [
        None,
        messages.DeviceMessage(
            signals={"async_signal": {"value": np.random.rand(10), "timestamp": time.time()}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
        "some string",
    ],
)
def test_device_manager_ds_obj_callback_async_signal(dm_with_devices_and_status, value):
    device_manager = dm_with_devices_and_status
    device = device_manager.devices.bec_signals_device.obj
    with mock.patch.object(device_manager.connector, "xadd") as mock_xadd:
        device_manager._obj_callback_bec_message_signal(obj=device.async_signal, value=value)

        if not isinstance(value, messages.DeviceMessage):
            mock_xadd.assert_not_called()
        else:
            mock_xadd.assert_called_once()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("metadata", [{}, {"scan_id": 12345}])  # Invalid scan_id type
def test_device_manager_ds_obj_callback_async_signal_incomplete_info(dm_with_devices, metadata):
    device_manager = dm_with_devices
    device = dm_with_devices.devices.bec_signals_device.obj
    dm_with_devices.devices.bec_signals_device.metadata = metadata

    msg = messages.DeviceMessage(
        signals={"async_signal": {"value": np.random.rand(10), "timestamp": time.time()}},
        metadata={"async_update": {"type": "add", "max_shape": [None]}},
    )
    with mock.patch.object(device_manager.connector, "xadd") as mock_xadd:
        device_manager._obj_callback_bec_message_signal(obj=device.async_signal, value=msg)

        mock_xadd.assert_not_called()


@pytest.fixture
def epics_motor_config():
    return {
        "name": "test_motor",
        "description": "Test Epics Motor",
        "deviceClass": "ophyd_devices.devices.psi_motor.EpicsMotor",
        "deviceConfig": {"prefix": "TEST:MOTOR"},
        "deviceTags": ["test", "motor"],
        "onFailure": "buffer",
        "enabled": True,
        "readoutPriority": "baseline",
        "softwareTrigger": False,
    }


@pytest.fixture
def epics_motor():
    with patched_device(EpicsMotor, prefix="TEST:MOTOR", name="test_motor") as motor:
        yield motor


@pytest.mark.parametrize(
    "device_manager_class, timeout, enabled",
    [
        (DeviceManagerDS, 5, True),
        (DeviceManagerDS, 10, True),
        (DeviceManagerDS, None, True),
        (DeviceManagerDS, 5, False),
    ],
)
def test_initialize_device(dm_with_devices, epics_motor, epics_motor_config, timeout, enabled):
    """Test to initialize an EpicsMotor device, check if all necessary subscriptions are made."""
    cfg = {"name": "test_motor", "prefix": "TEST:MOTOR"}
    epics_motor_config["enabled"] = enabled
    if timeout is not None:
        epics_motor_config["connectionTimeout"] = timeout
    else:
        timeout = 5  # Default timeout in connect_device
    with (
        mock.patch.object(
            dm_with_devices, "publish_device_info", return_value=None
        ) as mock_publish_device_info,
        mock.patch.object(
            dm_with_devices.device_provider_manager.providers["ophyd"],
            "get_device_info",
            return_value=DeviceInfo(device_base_class="positioner", device_class="EpicsMotor"),
        ),
        mock.patch.object(
            dm_with_devices, "initialize_enabled_device"
        ) as mock_initialize_enabled_device,
        mock.patch(
            "bec_server.device_server.device_providers.ophyd.ophyd_device_provider.connect_native_device",
            return_value=None,
        ) as mock_connect_native_device,
    ):
        with (
            mock.patch.object(epics_motor.low_limit_travel, "subscribe") as mock_low_subscribe,
            mock.patch.object(epics_motor.high_limit_travel, "subscribe") as mock_high_subscribe,
        ):
            device = DSDevice(
                name=epics_motor.name,
                obj=epics_motor,
                config=epics_motor_config,
                parent=dm_with_devices,
            )
            assert dm_with_devices.device_provider_manager.initialize_device(device, cfg) is device

            mock_publish_device_info.assert_called_once_with(
                dm_with_devices.devices[epics_motor_config["name"]], mock.ANY, pipe=mock.ANY
            )
            if enabled:
                mock_initialize_enabled_device.assert_called_once()
                mock_connect_native_device.assert_called_once_with(
                    epics_motor, wait_for_all=True, timeout=timeout
                )
                # Limit updates are queued through the auto-monitor thread callback.
                mock_low_subscribe.assert_called_once_with(
                    dm_with_devices._obj_callback_auto_monitor_limits, run=False
                )
                mock_high_subscribe.assert_called_once_with(
                    dm_with_devices._obj_callback_auto_monitor_limits, run=False
                )
            else:
                mock_initialize_enabled_device.assert_not_called()
                mock_connect_native_device.assert_not_called()
                mock_low_subscribe.assert_not_called()
                mock_high_subscribe.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_obj_callback_auto_monitor_limits_queues_root_device(dm_with_devices):
    samx = dm_with_devices.devices.samx
    dm_with_devices._limit_change_updates.clear()

    dm_with_devices._obj_callback_auto_monitor_limits(obj=samx.obj.low_limit_travel)

    assert samx.name in dm_with_devices._limit_change_updates


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_obj_callback_auto_monitor_queueing(dm_with_devices):
    samx = dm_with_devices.devices.samx
    dm_with_devices._auto_monitor_readback_updates.clear()
    dm_with_devices._auto_monitor_configuration_updates.clear()

    dm_with_devices._obj_callback_auto_monitor_readback(obj=samx.obj.readback)
    dm_with_devices._obj_callback_auto_monitor_configuration(obj=samx.obj.velocity)

    assert samx.name in dm_with_devices._auto_monitor_readback_updates
    assert samx.name in dm_with_devices._auto_monitor_configuration_updates


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_subscribe_to_auto_monitors_recurses_and_subscribes_by_kind(dm_with_devices):
    service = mock.MagicMock()
    service.connector = mock.MagicMock()
    service._service_name = "device_server"
    device_manager = DeviceManagerDS(service)

    normal_component = SimpleNamespace(
        _auto_monitor=True, kind=ophyd.Kind.normal, subscribe=mock.MagicMock()
    )
    hinted_component = SimpleNamespace(
        _auto_monitor=True, kind=ophyd.Kind.hinted, subscribe=mock.MagicMock()
    )
    config_component = SimpleNamespace(
        _auto_monitor=True, kind=ophyd.Kind.config, subscribe=mock.MagicMock()
    )
    ignored_component = SimpleNamespace(
        _auto_monitor=False, kind=ophyd.Kind.normal, subscribe=mock.MagicMock()
    )

    nested = SimpleNamespace(component_names=["hinted_component"])
    nested.hinted_component = hinted_component

    obj = SimpleNamespace(
        component_names=["normal_component", "config_component", "ignored_component", "nested"],
        normal_component=normal_component,
        config_component=config_component,
        ignored_component=ignored_component,
        nested=nested,
    )

    device_manager._auto_monitor_update_thread = mock.MagicMock()
    device_manager._auto_monitor_update_thread.ident = None
    thread_state = {"alive": False}
    device_manager._auto_monitor_update_thread.is_alive.side_effect = lambda: thread_state["alive"]
    device_manager._auto_monitor_update_thread.start.side_effect = lambda: thread_state.__setitem__(
        "alive", True
    )

    device_manager._subscribe_to_auto_monitors(obj)

    device_manager._auto_monitor_update_thread.start.assert_called_once_with()
    normal_component.subscribe.assert_called_once_with(
        device_manager._obj_callback_auto_monitor_readback, run=False
    )
    hinted_component.subscribe.assert_called_once_with(
        device_manager._obj_callback_auto_monitor_readback, run=False
    )
    config_component.subscribe.assert_called_once_with(
        device_manager._obj_callback_auto_monitor_configuration, run=False
    )
    ignored_component.subscribe.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_subscribe_to_limit_updates_starts_thread_and_handles_missing_signals(dm_with_devices):
    service = mock.MagicMock()
    service.connector = mock.MagicMock()
    service._service_name = "device_server"
    device_manager = DeviceManagerDS(service)

    low_limit = mock.MagicMock()
    high_limit = mock.MagicMock()
    obj = SimpleNamespace(low_limit_travel=low_limit, high_limit_travel=high_limit)

    device_manager._auto_monitor_update_thread = mock.MagicMock()
    device_manager._auto_monitor_update_thread.ident = None
    thread_state = {"alive": False}
    device_manager._auto_monitor_update_thread.is_alive.side_effect = lambda: thread_state["alive"]
    device_manager._auto_monitor_update_thread.start.side_effect = lambda: thread_state.__setitem__(
        "alive", True
    )

    device_manager._subscribe_to_limit_updates(obj)

    device_manager._auto_monitor_update_thread.start.assert_called_once_with()
    low_limit.subscribe.assert_called_once_with(
        device_manager._obj_callback_auto_monitor_limits, run=False
    )
    high_limit.subscribe.assert_called_once_with(
        device_manager._obj_callback_auto_monitor_limits, run=False
    )

    device_manager._auto_monitor_update_thread.start.reset_mock()
    device_manager._subscribe_to_limit_updates(SimpleNamespace())
    device_manager._auto_monitor_update_thread.start.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_ensure_auto_monitor_update_thread_recreates_dead_thread(dm_with_devices):
    service = mock.MagicMock()
    service.connector = mock.MagicMock()
    service._service_name = "device_server"
    device_manager = DeviceManagerDS(service)

    dead_thread = mock.MagicMock()
    dead_thread.is_alive.return_value = False
    dead_thread.ident = 123

    replacement_thread = mock.MagicMock()
    replacement_thread.is_alive.side_effect = [False, True]

    device_manager._auto_monitor_update_thread = dead_thread
    with mock.patch.object(
        device_manager, "_create_auto_monitor_update_thread", return_value=replacement_thread
    ) as mock_create_thread:
        device_manager._ensure_auto_monitor_update_thread()

    mock_create_thread.assert_called_once_with()
    replacement_thread.start.assert_called_once_with()
    assert device_manager._auto_monitor_update_thread is replacement_thread


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_auto_monitor_callbacks_ignore_disconnected_objects(dm_with_devices):
    disconnected_root = SimpleNamespace(name="samx")
    disconnected_obj = SimpleNamespace(connected=False, root=disconnected_root)
    dm_with_devices._auto_monitor_readback_updates.clear()
    dm_with_devices._auto_monitor_configuration_updates.clear()
    dm_with_devices._limit_change_updates.clear()

    dm_with_devices._obj_callback_auto_monitor_readback(obj=disconnected_obj)
    dm_with_devices._obj_callback_auto_monitor_configuration(obj=disconnected_obj)
    dm_with_devices._obj_callback_auto_monitor_limits(obj=disconnected_obj)

    assert not dm_with_devices._auto_monitor_readback_updates
    assert not dm_with_devices._auto_monitor_configuration_updates
    assert not dm_with_devices._limit_change_updates


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_auto_monitor_update_loop_batches_updates_and_skips_missing_devices(dm_with_devices):
    service = mock.MagicMock()
    service.connector = mock.MagicMock()
    service._service_name = "device_server"
    device_manager = DeviceManagerDS(service)
    samx_obj = mock.MagicMock()
    samx_obj.connected = False
    device_manager.devices["samx"] = SimpleNamespace(name="samx", obj=samx_obj)
    device_manager._auto_monitor_readback_updates = {"samx", "missing"}
    device_manager._auto_monitor_configuration_updates = {"samx"}
    device_manager._limit_change_updates = {"samx"}
    device_manager._shutdown_event = mock.MagicMock()
    device_manager._shutdown_event.wait.side_effect = [False, True]

    pipe = mock.MagicMock()
    device_manager.connector = mock.MagicMock()
    device_manager.connector.pipeline.return_value = pipe

    with (
        mock.patch.object(device_manager, "_obj_callback_readback") as mock_readback,
        mock.patch.object(device_manager, "_obj_callback_configuration") as mock_configuration,
        mock.patch.object(device_manager, "_obj_callback_limit_change") as mock_limit_change,
    ):
        device_manager._auto_monitor_update_loop()

        mock_readback.assert_called_once_with(obj=samx_obj, pipe=pipe)
        mock_configuration.assert_called_once_with(obj=samx_obj, pipe=pipe)
        mock_limit_change.assert_called_once_with(obj=samx_obj, pipe=pipe)
        pipe.execute.assert_called_once()
        assert not device_manager._auto_monitor_readback_updates
        assert not device_manager._auto_monitor_configuration_updates
        assert not device_manager._limit_change_updates


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_dependency_resolution(dm_with_devices):
    devices = [
        {"name": "dev1", "deviceClass": "SomeClass", "deviceConfig": {}, "enabled": True},
        {
            "name": "dev2",
            "deviceClass": "SomeClass",
            "deviceConfig": {},
            "needs": ["dev1"],
            "enabled": True,
        },
        {
            "name": "dev3",
            "deviceClass": "SomeClass",
            "deviceConfig": {},
            "needs": ["dev2"],
            "enabled": True,
        },
    ]

    sorted_devices = dm_with_devices.resolve_device_dependencies(devices)
    sorted_names = [dev["name"] for dev in sorted_devices]
    assert sorted_names == ["dev1", "dev2", "dev3"]


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_dependency_resolution_with_unknown_dependency(dm_with_devices):
    devices = [
        {"name": "dev1", "deviceClass": "SomeClass", "deviceConfig": {}, "enabled": True},
        {
            "name": "dev2",
            "deviceClass": "SomeClass",
            "deviceConfig": {},
            "needs": ["unknown_dev"],
            "enabled": True,
        },
    ]

    with pytest.raises(DeviceConfigError, match="needs unknown device"):
        dm_with_devices.resolve_device_dependencies(devices)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_dependency_resolution_with_cyclic_dependency(dm_with_devices):
    devices = [
        {
            "name": "dev1",
            "deviceClass": "SomeClass",
            "deviceConfig": {},
            "needs": ["dev3"],
            "enabled": True,
        },
        {
            "name": "dev2",
            "deviceClass": "SomeClass",
            "deviceConfig": {},
            "needs": ["dev1"],
            "enabled": True,
        },
        {
            "name": "dev3",
            "deviceClass": "SomeClass",
            "deviceConfig": {},
            "needs": ["dev2"],
            "enabled": True,
        },
    ]

    with pytest.raises(DeviceConfigError, match="Cyclic dependency detected"):
        dm_with_devices.resolve_device_dependencies(devices)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_get_device_order(dm_with_devices):
    device_manager = dm_with_devices
    devices = ["samx", "eiger"]
    ordered_devices = device_manager.get_device_order(devices)
    assert len(ordered_devices) == len(devices)
    assert ordered_devices == ["eiger", "samx"]

    devices = ["samx.readback", "eiger"]
    ordered_devices = device_manager.get_device_order(devices)
    assert len(ordered_devices) == len(devices)
    assert ordered_devices == ["eiger", "samx.readback"]


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_get_device_order_without_order_map(dm_with_devices):
    device_manager = dm_with_devices
    device_manager._device_order_map = {}
    devices = ["samx", "eiger"]
    with pytest.raises(RuntimeError, match="Device order map is not initialized"):
        device_manager.get_device_order(devices)


@pytest.mark.parametrize("device_config", [None, {"value": 3, "custom_setting": 7}])
def test_construct_device_through_provider(device_config):
    config = {
        "name": "provider_signal",
        "deviceClass": "ophyd.Signal",
        "enabled": False,
        "readoutPriority": "monitored",
    }
    if device_config is not None:
        config["deviceConfig"] = device_config
    original_config = copy.deepcopy(config)

    manager = DeviceProviderManager(device_manager=None)
    device, remaining_config = manager.construct_device(DSDeviceConfig.model_validate(config))
    obj = device.obj

    try:
        assert isinstance(device, DSDevice)
        assert device.name == obj.name
        assert device.enabled is False
        assert isinstance(obj, ophyd.Signal)
        assert obj.name == "provider_signal"
        assert obj.get() == (3 if device_config else 0)
        assert remaining_config == ({"custom_setting": 7} if device_config else {})
        assert config == original_config
    finally:
        obj.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_legacy_duck_typed_plugin_initializes(device_manager):
    class Plugin:
        connected = True
        hints = {}

        def __init__(self, name):
            self.name = name

        def read(self):
            return {self.name: {"value": 1, "timestamp": 0}}

        read_configuration = read

        def describe(self):
            return {self.name: {"dtype": "number", "shape": [], "source": "sim"}}

        describe_configuration = describe

        def destroy(self):
            pass

    providers = device_manager.device_provider_manager
    config = DSDeviceConfig(
        name="legacy_plugin", enabled=True, deviceClass="Plugin", readoutPriority="monitored"
    )
    with mock.patch.object(providers, "_get_device_class", return_value=Plugin):
        device, remaining = providers.construct_device(config)
    providers.initialize_device(device, remaining)
    assert isinstance(device.obj, Plugin)
    assert device.initialized


@pytest.mark.parametrize(
    "base_class", [ophyd.OphydObject, opd.BECDeviceBase, opd.DeviceProxy, opd.ComputedSignal]
)
def test_ophyd_provider_supports_device_base_classes(base_class):
    class CustomDevice(base_class):
        pass

    manager = DeviceProviderManager(device_manager=None)

    assert manager.get_provider(CustomDevice) is manager.providers["ophyd"]


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("has_lookup", [False, True])
@pytest.mark.parametrize("delayed", [False, True])
def test_init_device_registers_proxy(dm_with_devices, has_lookup, delayed):
    device_config = {
        "name": "test_proxy",
        "deviceClass": "ophyd.Signal",
        "enabled": False,
        "readoutPriority": "monitored",
    }
    obj = SimpleNamespace()
    if has_lookup:
        obj.lookup = {}
    provider_manager = dm_with_devices.device_provider_manager
    device = DSDevice(name=device_config["name"], obj=obj, config=device_config)
    progress = mock.Mock()

    with (
        mock.patch.object(provider_manager, "construct_device", return_value=(device, {})),
        mock.patch.object(
            provider_manager, "initialize_device", return_value=device
        ) as initialize_device,
        mock.patch.object(
            provider_manager, "register_device_proxy", wraps=provider_manager.register_device_proxy
        ) as register_proxy,
    ):
        dm_with_devices._init_device(device_config, delayed=delayed, progress=progress)

    initialize_device.assert_called_once_with(device, {})
    if delayed:
        register_proxy.assert_called_once_with(device)
    else:
        register_proxy.assert_not_called()
    progress.update_progress.assert_called_with(
        device_name="test_proxy", finished=True, success=True
    )


@pytest.mark.parametrize("raises_error", [False, True])
def test_disconnect_device_through_provider(raises_error):
    obj = ophyd.Signal(name="disconnect_signal")
    device = DSDevice(name=obj.name, obj=obj, config=None)
    manager = DeviceProviderManager(device_manager=None)
    error = RuntimeError("destroy failed")

    with mock.patch.object(obj, "destroy", side_effect=error if raises_error else None) as destroy:
        if raises_error:
            with pytest.raises(RuntimeError) as exc:
                manager.disconnect_device(device)
            assert exc.value is error
        else:
            assert manager.disconnect_device(device) is None
        destroy.assert_called_once_with()
    obj.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("extraction_fails", [False, True])
def test_publish_provider_device_info(dm_with_devices, extraction_fails):
    original_device = dm_with_devices.devices.samx
    provider = dm_with_devices.device_provider_manager.providers["ophyd"]
    info = DeviceInfo(device_base_class="positioner", device_class="TestDevice")
    error = RuntimeError("describe failed")
    results = [error, info] if extraction_fails else [info]

    with (
        mock.patch.object(provider, "connect_device", return_value=None),
        mock.patch.object(provider, "get_device_info", side_effect=results) as extract,
        mock.patch.object(provider, "initialize_enabled_device") as finish_setup,
        mock.patch.object(dm_with_devices.connector, "set") as publish,
    ):
        if extraction_fails:
            with pytest.raises(RuntimeError) as exc:
                dm_with_devices.device_provider_manager.initialize_device(original_device, {})
            assert exc.value is error
            finish_setup.assert_not_called()
        else:
            dm_with_devices.device_provider_manager.initialize_device(original_device, {})
            finish_setup.assert_called_once()

    device = dm_with_devices.devices.samx
    assert device is original_device
    expected_calls = [mock.call(device, connect=True)]
    if extraction_fails:
        expected_calls.append(mock.call(device, connect=False))
    assert extract.call_args_list == expected_calls
    publish.assert_called_once_with(
        MessageEndpoints.device_info(device.name),
        messages.DeviceInfoMessage(device=device.name, info=info.to_wire()),
        mock.ANY,
    )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_provider_lifecycle_preserves_device_wrapper(dm_with_devices):
    """Use the constructed wrapper for registration, setup, and teardown."""
    providers = dm_with_devices.device_provider_manager
    config = DSDeviceConfig.model_validate(
        {
            "name": "wrapped_signal",
            "deviceClass": "ophyd.Signal",
            "enabled": True,
            "readoutPriority": "monitored",
            "deviceConfig": {"value": 3},
        }
    )
    device, remaining_config = providers.construct_device(config)
    assert isinstance(device, DSDevice)
    assert device.initialized is False
    provider = providers.providers["ophyd"]
    with mock.patch.object(provider, "connect_device", wraps=provider.connect_device) as connect:
        initialized = providers.initialize_device(device, remaining_config)

    connect.assert_called_once_with(device)
    assert initialized is device
    assert dm_with_devices.devices[device.name] is device
    assert device.initialized is True
    assert device.obj.get() == 3
    providers.disconnect_device(device)
    providers.reset_device(device)
    assert device.obj._destroyed is True
    assert device.initialized is False


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("enabled", [False, True])
def test_provider_initializes_bec_device_base(device_manager, connected_connector, enabled):
    """Keep the minimal BEC device family supported for enabled and disabled devices."""
    dm = device_manager
    dm.connector = connected_connector
    config = DSDeviceConfig.model_validate(
        {
            "name": "minimal_device",
            "deviceClass": "ophyd_devices.BECDeviceBase",
            "enabled": enabled,
            "readoutPriority": "monitored",
            "id": "device-id",
            "sessionId": "session-id",
            "accessGroups": "customer",
        }
    )
    providers = dm.device_provider_manager
    device, remaining = providers.construct_device(config)
    assert providers.initialize_device(device, remaining) is device
    assert isinstance(device.obj, opd.BECDeviceBase)
    assert device.initialized is enabled
    assert dm.devices[device.name] is device
    for key in ("id", "sessionId", "accessGroups"):
        assert device._config[key] == getattr(config, key)
    info = connected_connector.get(MessageEndpoints.device_info(device.name))
    assert info.info["device_base_class"] == "device"


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("failure", ["extraction", "validation", "encoding"])
def test_load_bad_device_metadata_falls_back_and_disables_device(
    device_manager, connected_connector, failure
):
    """Extraction and real MessagePack encoding errors must disable only the failed device."""

    class BadDescriptionSignal(ophyd.Signal):
        def describe(self):
            if failure == "extraction":
                raise RuntimeError("describe failed")
            if failure == "validation":
                return []
            return {self.name: {"dtype": "number", "shape": [], "source": object()}}

    dm = device_manager
    dm.connector = connected_connector
    dm.config_update_handler = ConfigUpdateHandler(dm)
    # Other suites can override the shared fixture with already initialized devices.
    dm.config_update_handler._flush_config()
    config = {
        "name": "bad_metadata",
        "deviceClass": "BadDescriptionSignal",
        "deviceConfig": {},
        "enabled": True,
        "readoutPriority": "monitored",
        "id": "device-id",
        "sessionId": "session-id",
        "accessGroups": "customer",
    }
    healthy_config = config | {
        "name": "healthy_metadata",
        "deviceClass": "ophyd.Signal",
        "id": "healthy-device-id",
    }
    connected_connector.set(
        MessageEndpoints.device_config(),
        messages.AvailableResourceMessage(resource=[healthy_config, config]),
    )
    providers = dm.device_provider_manager
    provider = providers.providers["ophyd"]
    device_classes = {"ophyd.Signal": ophyd.Signal, "BadDescriptionSignal": BadDescriptionSignal}
    with (
        mock.patch.object(providers, "_get_device_class", side_effect=device_classes.__getitem__),
        mock.patch.object(provider, "get_device_info", wraps=provider.get_device_info) as inspect,
        mock.patch.object(dm, "_reset_config") as reset_config,
    ):
        dm._get_config()

    device = dm.devices[config["name"]]
    assert device.enabled is False
    assert device.initialized is False
    assert device.obj._destroyed is True
    assert config["name"] in dm.failed_devices
    healthy_device = dm.devices[healthy_config["name"]]
    assert healthy_device.enabled is True
    assert healthy_device.initialized is True
    assert inspect.call_args_list == [
        mock.call(healthy_device, connect=True),
        mock.call(device, connect=True),
        mock.call(device, connect=False),
    ]
    reset_config.assert_not_called()
    info = connected_connector.get(MessageEndpoints.device_info(device.name))
    assert info.info["describe"] == {}
    stored = {
        item["name"]: item
        for item in connected_connector.get(MessageEndpoints.device_config()).resource
    }
    assert stored[device.name]["enabled"] is False
    assert stored[healthy_device.name]["enabled"] is True
    for original in (healthy_config, config):
        for key in ("id", "sessionId", "accessGroups"):
            assert stored[original["name"]][key] == original[key]
            assert dm.devices[original["name"]]._config[key] == original[key]


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("connection_fails", [False, True])
@pytest.mark.parametrize("failure", ["extraction", "publication"])
def test_provider_metadata_fallback_preserves_error_precedence(
    device_manager, connection_fails, failure
):
    """Register the failed wrapper before raising, retaining the connection error first."""
    dm = device_manager
    providers = dm.device_provider_manager
    provider = providers.providers["ophyd"]
    device, remaining = providers.construct_device(
        DSDeviceConfig.model_validate(
            {
                "name": "failed_metadata",
                "deviceClass": "ophyd.Signal",
                "enabled": True,
                "readoutPriority": "monitored",
            }
        )
    )
    connection_error = ConnectionError("connection failed") if connection_fails else None
    metadata_error = RuntimeError("metadata failed")
    info = DeviceInfo(device_base_class="signal", device_class="Signal")
    extraction = [metadata_error, info] if failure == "extraction" else [info, info]
    publication = [metadata_error, None] if failure == "publication" else [None]
    with (
        mock.patch.object(provider, "connect_device", return_value=connection_error),
        mock.patch.object(provider, "get_device_info", side_effect=extraction) as inspect,
        mock.patch.object(dm, "publish_device_info", side_effect=publication),
        mock.patch.object(provider, "initialize_enabled_device") as finish,
    ):
        with pytest.raises(Exception) as exc:
            providers.initialize_device(device, remaining)

    assert exc.value is (connection_error if connection_fails else metadata_error)
    assert dm.devices[device.name] is device
    assert inspect.call_args_list == [
        mock.call(device, connect=not connection_fails),
        mock.call(device, connect=False),
    ]
    finish.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("initial_failure", ["extraction", "publication"])
@pytest.mark.parametrize("fallback_failure", ["extraction", "publication"])
def test_provider_metadata_fallback_failure_is_not_retried(
    device_manager, initial_failure, fallback_failure
):
    """A failed fallback propagates immediately without registration or a third attempt."""
    dm = device_manager
    providers = dm.device_provider_manager
    provider = providers.providers["ophyd"]
    device, remaining = providers.construct_device(
        DSDeviceConfig.model_validate(
            {
                "name": "failed_fallback",
                "deviceClass": "ophyd.Signal",
                "enabled": True,
                "readoutPriority": "monitored",
            }
        )
    )
    initial_error = RuntimeError("initial metadata failed")
    fallback_error = RuntimeError("fallback metadata failed")
    info = DeviceInfo(device_base_class="signal", device_class="Signal")
    extraction = [
        initial_error if initial_failure == "extraction" else info,
        fallback_error if fallback_failure == "extraction" else info,
    ]
    publication = [initial_error] if initial_failure == "publication" else []
    if fallback_failure == "publication":
        publication.append(fallback_error)
    try:
        with (
            mock.patch.object(provider, "connect_device", return_value=ConnectionError()),
            mock.patch.object(provider, "get_device_info", side_effect=extraction) as inspect,
            mock.patch.object(dm, "publish_device_info", side_effect=publication) as publish,
        ):
            with pytest.raises(RuntimeError) as exc:
                providers.initialize_device(device, remaining)
        assert exc.value is fallback_error
        assert device.name not in dm.devices
        assert inspect.call_count == 2
        assert publish.call_count == len(publication)
    finally:
        providers.disconnect_device(device)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("name", ["_motor", "motor-one", "class"])
def test_provider_preserves_existing_runtime_device_names(device_manager, name):
    """Runtime configuration must retain the name acceptance of the existing YAML loader."""
    providers = device_manager.device_provider_manager
    config = DSDeviceConfig.model_validate(
        {
            "name": name,
            "deviceClass": "ophyd.Signal",
            "enabled": False,
            "readoutPriority": "monitored",
        }
    )
    device, remaining = providers.construct_device(config)
    assert providers.initialize_device(device, remaining) is device
    assert device_manager.devices[name] is device
    assert device.name == name
    assert device.obj.name == name
