import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest import mock

import numpy as np
import ophyd
import pytest
from ophyd_devices.devices.psi_motor import EpicsMotor
from ophyd_devices.tests.utils import patched_device

from bec_lib import messages
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.devicemanager import DeviceManagerDS

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


@pytest.fixture
def metadata_preview_signal():
    """Provide the metadata event contract without requiring an unreleased ophyd API."""
    from ophyd_devices.utils.bec_signals import PreviewSignal

    class MetadataPreviewSignal(PreviewSignal):
        """Preview signal whose test helper explicitly emits metadata changes."""

        SUB_SIGNAL_INFO_CHANGED = "signal_info_changed"

        def update_signal_info(self, **changes) -> None:
            """Update the shared message schema and emit the server's metadata event."""
            for name, value in changes.items():
                setattr(self.signal_info, name, value)
            self._run_subs(sub_type=self.SUB_SIGNAL_INFO_CHANGED)

    return MetadataPreviewSignal


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
    if raises_error:
        assert isinstance(device_manager.connect_device(obj), Exception)
    else:
        assert device_manager.connect_device(obj) is None


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_connect_device_with_kwargs(dm_with_devices):
    """Test connect with timeout, wait_for_all and force"""
    device_manager = dm_with_devices
    obj = EpicsDeviceMock()

    with mock.patch.object(obj, "wait_for_connection") as mock_wait_for_connection:
        device_manager.connect_device(obj, wait_for_all=True)
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
    obj, obj_config = device_manager.construct_device_obj(config, device_manager=device_manager)
    original_error = "original connection failure"
    reload_msg = messages.DeviceConfigMessage(
        action="reload", config={}, metadata={"RID": "unreachable-reload"}
    )

    with (
        mock.patch.object(device_manager, "construct_device_obj", return_value=(obj, obj_config)),
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
            dm_with_devices, "initialize_enabled_device"
        ) as mock_initialize_enabled_device,
        mock.patch.object(
            dm_with_devices, "connect_device", return_value=None
        ) as mock_connect_device,
    ):
        with (
            mock.patch.object(epics_motor.low_limit_travel, "subscribe") as mock_low_subscribe,
            mock.patch.object(epics_motor.high_limit_travel, "subscribe") as mock_high_subscribe,
        ):
            dm_with_devices.initialize_device(epics_motor_config, cfg, epics_motor)

            mock_publish_device_info.assert_called_once_with(
                epics_motor, connect=enabled, pipe=mock.ANY
            )
            if enabled:
                mock_initialize_enabled_device.assert_called_once()
                mock_connect_device.assert_called_once_with(
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
                mock_connect_device.assert_not_called()
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


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.parametrize("failure", [None, "connect", "hook"])
def test_device_info_refreshes_after_connection(
    device_manager, connected_connector, enabled, failure, metadata_preview_signal
):
    class Detector(ophyd.Device):
        preview = ophyd.Component(metadata_preview_signal, ndim=2)
        value = ophyd.Component(ophyd.Signal, value=42)

        def on_connected(self):
            for port in (5555, 5556):
                # Hooks may wait for workers which emit metadata events.
                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(
                        self.preview.update_signal_info,
                        transports=[
                            messages.ZMQSignalTransport(
                                address=f"tcp://localhost:{port}", encoder="adzmq-array-1.0"
                            )
                        ],
                    ).result(timeout=3)
                # Bootstrap metadata remains unchanged until the connection hook completes.
                assert len(published) == 1
                assert (
                    connected_connector.get(MessageEndpoints.device_info("detector"))
                    == published[0]
                )
            if failure == "hook":
                raise RuntimeError("Connection hook failed")

    detector = Detector(name="detector")
    previous = messages.DeviceInfoMessage(device="detector", info={"previous": True})
    connected_connector.set_and_publish(MessageEndpoints.device_info("detector"), previous)
    published = []
    publish = connected_connector.set_and_publish

    def record_publish(topic, msg, pipe=None, **kwargs):
        if topic == MessageEndpoints.device_info("detector"):
            published.append(msg)
        return publish(topic, msg, pipe, **kwargs)

    def configure(obj, _config):
        assert len(published) == 2
        obj.preview.update_signal_info(
            signal_metadata={**obj.preview.signal_info.signal_metadata, "transpose": True}
        )

    config = {
        "name": "detector",
        "deviceClass": "ophyd.Device",
        "enabled": enabled,
        "readoutPriority": "monitored",
    }
    failed = enabled and failure is not None
    try:
        with (
            mock.patch.object(device_manager, "connector", connected_connector),
            mock.patch.object(connected_connector, "set_and_publish", side_effect=record_publish),
            mock.patch.object(device_manager, "update_config", side_effect=configure),
            mock.patch.object(
                device_manager,
                "connect_device",
                return_value=ConnectionError("Connection failed") if failure == "connect" else None,
            ) as connect,
        ):
            if failed:
                with pytest.raises((ConnectionError, RuntimeError), match="failed"):
                    device_manager.initialize_device(config, {}, detector)
            else:
                device_manager.initialize_device(config, {}, detector)

            cached = connected_connector.get(MessageEndpoints.device_info("detector"))
            assert published[-1] == cached
            if enabled and not failed:
                assert len(published) == 3
                info = cached.info["signals"]["preview"]["describe"]["signal_info"]
                assert info["transports"][0]["address"] == "tcp://localhost:5556"
                assert info["signal_metadata"]["transpose"] is True
                # Publishing the final description must preserve freshly initialized buffers.
                assert connected_connector.get(MessageEndpoints.device_readback("detector"))
                detector.preview.update_signal_info(transports=None)
                assert len(published) == 4
            else:
                assert len(published) == (2 if enabled and failure == "hook" else 1)
                assert cached.info["signals"] == {}
            if not enabled:
                connect.assert_not_called()
                assert detector.preview.signal_info.transports is None
    finally:
        device_manager.devices.pop("detector", None)
        detector.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_info_publication_failure_restores_previous_device(device_manager):
    config = {
        "name": "detector",
        "deviceClass": "ophyd.Signal",
        "enabled": False,
        "readoutPriority": "monitored",
    }
    old_signal = ophyd.Signal(name="detector", value=1)
    new_signal = ophyd.Signal(name="detector", value=2)
    try:
        previous = device_manager.initialize_device(config, {}, old_signal)
        with mock.patch.object(
            device_manager, "publish_device_info", side_effect=RuntimeError("Publish failed")
        ):
            with pytest.raises(RuntimeError, match="Publish failed"):
                device_manager.initialize_device({**config, "enabled": True}, {}, new_signal)
        assert device_manager.devices["detector"] is previous
        assert previous.enabled is False
        assert not old_signal._destroyed
    finally:
        device_manager.devices.pop("detector", None)
        old_signal.destroy()
        new_signal.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_info_callback_ignores_root_removed_while_waiting_to_publish(
    device_manager, connected_connector
):
    class Detector(ophyd.Device):
        value = ophyd.Component(ophyd.Signal, value=0)

    detector = Detector(name="detector")
    signal = detector.value
    device_manager.devices["detector"] = SimpleNamespace(obj=detector, enabled=True)
    cached = messages.DeviceInfoMessage(device="detector", info={"current": True})
    connected_connector.set_and_publish(MessageEndpoints.device_info("detector"), cached)
    callback_passed_root_check = threading.Event()
    publish = device_manager.publish_device_info

    def publish_after_root_check(obj, **kwargs):
        callback_passed_root_check.set()
        return publish(obj, **kwargs)

    try:
        with (
            mock.patch.object(device_manager, "connector", connected_connector),
            mock.patch.object(
                device_manager, "publish_device_info", side_effect=publish_after_root_check
            ),
            mock.patch(
                "bec_server.device_server.devices.devicemanager.get_device_info", return_value={}
            ) as describe,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            with device_manager._device_info_lock:
                callback = executor.submit(
                    device_manager._obj_callback_signal_info_changed, obj=signal
                )
                assert callback_passed_root_check.wait(3)
                device_manager.devices.pop("detector")
                detector.destroy()
            callback.result(timeout=3)

            describe.assert_not_called()
            assert connected_connector.get(MessageEndpoints.device_info("detector")) == cached
    finally:
        device_manager.devices.pop("detector", None)
        detector.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_info_publication_ignores_replaced_root(device_manager, connected_connector):
    previous = ophyd.Device(name="detector")
    current = ophyd.Device(name="detector")
    device_manager.devices["detector"] = SimpleNamespace(obj=current, enabled=True)
    cached = messages.DeviceInfoMessage(device="detector", info={"current": True})
    connected_connector.set_and_publish(MessageEndpoints.device_info("detector"), cached)
    try:
        with (
            mock.patch.object(device_manager, "connector", connected_connector),
            mock.patch(
                "bec_server.device_server.devices.devicemanager.get_device_info"
            ) as describe,
        ):
            # A callback may have passed its root check before waiting to publish.
            device_manager.publish_device_info(previous)
            describe.assert_not_called()
            assert connected_connector.get(MessageEndpoints.device_info("detector")) == cached
    finally:
        device_manager.devices.pop("detector", None)
        previous.destroy()
        current.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_signal_info_subscription_is_not_duplicated(
    device_manager, connected_connector, metadata_preview_signal
):
    class Detector(ophyd.Device):
        preview = ophyd.Component(metadata_preview_signal, ndim=2)

    detector = Detector(name="detector")
    holder = SimpleNamespace(obj=detector, enabled=True)
    device_manager.devices["detector"] = holder
    try:
        with (
            mock.patch.object(device_manager, "connector", connected_connector),
            mock.patch.object(
                connected_connector, "set_and_publish", wraps=connected_connector.set_and_publish
            ) as publish,
        ):

            def metadata_publications():
                # The full-suite fixture also runs monitors for other devices.
                return [
                    call
                    for call in publish.call_args_list
                    if call.args and call.args[0] == MessageEndpoints.device_info("detector")
                ]

            device_manager._subscribe_to_bec_signal_info(detector)
            detector.preview.update_signal_info(
                transports=[
                    messages.ZMQSignalTransport(
                        address="tcp://localhost:5555", encoder="adzmq-array-1.0"
                    )
                ]
            )
            cached = connected_connector.get(MessageEndpoints.device_info("detector"))
            info = cached.info["signals"]["preview"]["describe"]["signal_info"]
            assert info["transports"][0]["address"] == "tcp://localhost:5555"
            assert metadata_publications() == [
                mock.call(MessageEndpoints.device_info("detector"), cached, None)
            ]
            # Repeated registration replaces the subscription instead of duplicating it.
            publish.reset_mock()
            device_manager._subscribe_to_bec_signal_info(detector)
            detector.preview.update_signal_info(
                signal_metadata={**detector.preview.signal_info.signal_metadata, "transpose": True}
            )
            assert len(metadata_publications()) == 1
            detector.preview.update_signal_info(transports=None)
            assert len(metadata_publications()) == 2
            assert (
                connected_connector.get(MessageEndpoints.device_info("detector")).info["signals"][
                    "preview"
                ]["describe"]["signal_info"]["transports"]
                is None
            )
    finally:
        device_manager.devices.pop("detector", None)
        detector.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("enabled", [True, False])
def test_signal_info_callback_respects_root_device_enabled_flag(device_manager, enabled):
    detector = ophyd.Device(name="detector")
    signal = ophyd.Signal(name="signal", parent=detector)
    device_manager.devices["detector"] = SimpleNamespace(obj=detector, enabled=enabled)
    try:
        with mock.patch.object(device_manager, "publish_device_info") as publish:
            device_manager._obj_callback_signal_info_changed(obj=signal)
            publish.assert_called_once_with(detector, connect=enabled)
    finally:
        device_manager.devices.pop("detector", None)
        signal.destroy()
        detector.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("replaced", [False, True])
def test_signal_info_callback_ignores_removed_or_replaced_devices(device_manager, replaced):
    detector = ophyd.Device(name="detector")
    signal = ophyd.Signal(name="signal", parent=detector)
    if replaced:
        device_manager.devices["detector"] = SimpleNamespace(obj=object(), enabled=True)
    try:
        with mock.patch.object(device_manager, "publish_device_info") as publish:
            device_manager._obj_callback_signal_info_changed(obj=signal)
            publish.assert_not_called()
    finally:
        device_manager.devices.pop("detector", None)
        signal.destroy()
        detector.destroy()
