import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
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
from bec_server.device_server.device_providers.device_interfaces import RefreshSection
from bec_server.device_server.device_providers.device_provider_manager import DeviceProviderManager
from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
    OphydDeviceProvider,
)
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

    device_manager.publish_refresh(samx, RefreshSection.CONFIGURATION)

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


@pytest.mark.parametrize("event_type", ["readback", "value", "done_moving", "unsupported"])
def test_subscribe_to_device_events(event_type):
    obj = SimpleNamespace(event_types=(event_type,), subscribe=mock.Mock())
    device = SimpleNamespace(obj=obj, enabled=False)
    provider = OphydDeviceProvider(None)
    provider.subscribe(device, mock.Mock()).result()

    if event_type == "unsupported":
        obj.subscribe.assert_not_called()
    else:
        obj.subscribe.assert_called_once_with(mock.ANY, event_type=event_type, run=False)


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
        device.preview._run_subs(sub_type=device.preview.SUB_VALUE, value=value)

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
        device.file_event._run_subs(sub_type=device.file_event.SUB_VALUE, value=value)

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
        device.progress._run_subs(sub_type=device.progress.SUB_VALUE, value=value)

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
        with mock.patch.object(device_manager._bec_message_handler, "publish") as mock_emit:
            device.progress._run_subs(
                sub_type=device.progress.SUB_VALUE,
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
        device.async_signal._run_subs(sub_type=device.async_signal.SUB_VALUE, value=value)

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
        device.async_signal._run_subs(sub_type=device.async_signal.SUB_VALUE, value=msg)

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

            expected = [mock.call(device, mock.ANY, pipe=mock.ANY)]
            if enabled:
                expected.append(mock.call(device, mock.ANY, pipe=None))
            assert mock_publish_device_info.call_args_list == expected
            if enabled:
                mock_initialize_enabled_device.assert_called_once()
                mock_connect_native_device.assert_called_once_with(
                    epics_motor, wait_for_all=True, timeout=timeout
                )
                # Limit updates are queued through the auto-monitor thread callback.
                mock_low_subscribe.assert_called_once_with(mock.ANY, run=False)
                mock_high_subscribe.assert_called_once_with(mock.ANY, run=False)
            else:
                mock_initialize_enabled_device.assert_not_called()
                mock_connect_native_device.assert_not_called()
                mock_low_subscribe.assert_not_called()
                mock_high_subscribe.assert_not_called()


def test_subscribe_to_auto_monitors_recurses_and_subscribes_by_kind():
    normal = SimpleNamespace(_auto_monitor=True, kind=ophyd.Kind.normal)
    hinted = SimpleNamespace(_auto_monitor=True, kind=ophyd.Kind.hinted)
    config = SimpleNamespace(_auto_monitor=True, kind=ophyd.Kind.config)
    ignored = SimpleNamespace(_auto_monitor=False, kind=ophyd.Kind.normal)
    obj = SimpleNamespace(
        component_names=["normal", "nested", "ignored"],
        normal=normal,
        nested=SimpleNamespace(component_names=["hinted", "config"], hinted=hinted, config=config),
        ignored=ignored,
    )
    provider = OphydDeviceProvider(None)
    bindings = list(provider._auto_monitor_bindings(obj))
    assert [(binding.source, binding.category) for binding in bindings] == [
        (normal, RefreshSection.READBACK),
        (hinted, RefreshSection.READBACK),
        (config, RefreshSection.CONFIGURATION),
    ]
    assert all(binding.deferred and not binding.run for binding in bindings)


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


def test_auto_monitor_callbacks_ignore_disconnected_objects():
    component = SimpleNamespace(
        _auto_monitor=True, kind=ophyd.Kind.normal, connected=False, subscribe=mock.Mock()
    )
    obj = SimpleNamespace(event_types=(), component_names=["component"], component=component)
    component.root = obj
    device = SimpleNamespace(obj=obj, enabled=True)
    sink = mock.Mock()

    OphydDeviceProvider(None).subscribe(device, sink).result()
    callback = component.subscribe.call_args.args[0]
    callback(obj=component)
    sink.request_refresh.assert_not_called()

    component.connected = True
    callback(obj=component)
    sink.request_refresh.assert_called_once_with(device, frozenset({RefreshSection.READBACK}))


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_auto_monitor_update_loop_batches_updates_and_skips_missing_devices(dm_with_devices):
    service = mock.MagicMock()
    service.connector = mock.MagicMock()
    service._service_name = "device_server"
    device_manager = DeviceManagerDS(service)
    samx_obj = mock.MagicMock()
    samx_obj.connected = True
    device = SimpleNamespace(name="samx", obj=samx_obj)
    removed = SimpleNamespace(name="missing", obj=mock.MagicMock())
    device_manager.devices["samx"] = device
    device_manager._refresh_scheduler.request_refresh(device, frozenset(RefreshSection))
    device_manager._refresh_scheduler.request_refresh(removed, frozenset(RefreshSection))
    device_manager._shutdown_event = mock.MagicMock()
    device_manager._shutdown_event.wait.side_effect = [False, True]

    pipe = mock.MagicMock()
    device_manager.connector = mock.MagicMock()
    device_manager.connector.pipeline.return_value = pipe

    with (
        mock.patch.object(device_manager, "_publish_refresh") as publish,
        mock.patch.object(
            device_manager.device_provider_manager, "is_connected", return_value=True
        ),
    ):
        device_manager._auto_monitor_update_loop()

    assert publish.call_args_list == [
        mock.call(device, section, pipe) for section in RefreshSection
    ]
    pipe.execute.assert_called_once()
    assert device_manager._refresh_scheduler.drain() == []


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
        mock.patch.object(dm_with_devices.connector, "set_and_publish") as publish,
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
    device.initialized = False
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


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_generic_refresh_ingress_starts_worker_once_and_rejects_retired_devices(dm_with_devices):
    """Concurrent async notifications share one worker and cannot restart a stopped manager."""
    manager = dm_with_devices
    manager._shutdown_event.set()
    if manager._auto_monitor_update_thread.is_alive():
        manager._auto_monitor_update_thread.join(timeout=3)
    assert not manager._auto_monitor_update_thread.is_alive()
    manager._shutdown_event.clear()
    manager._refresh_scheduler.clear()
    manager._auto_monitor_update_thread = manager._create_auto_monitor_update_thread()
    monitor = manager._auto_monitor_update_thread
    device = manager.devices.samx
    barrier = threading.Barrier(3)
    errors = []

    def notify():
        try:
            barrier.wait(timeout=3)
            manager.request_refresh(device, frozenset({RefreshSection.READBACK}))
        except Exception as exc:
            errors.append(exc)

    workers = [threading.Thread(target=notify) for _ in range(2)]
    try:
        with mock.patch.object(monitor, "start", wraps=monitor.start) as start:
            for worker in workers:
                worker.start()
            barrier.wait(timeout=3)
            for worker in workers:
                worker.join(timeout=3)
                assert not worker.is_alive()
            assert not errors
            start.assert_called_once_with()
    finally:
        manager._shutdown_event.set()
        for worker in workers:
            if worker.ident is not None:
                worker.join(timeout=3)
        if monitor.ident is not None:
            monitor.join(timeout=3)
        assert not monitor.is_alive()

    manager._refresh_scheduler.clear()
    manager._shutdown_event.clear()
    replacement = DSDevice(name=device.name, obj=device.obj, config=None)
    with mock.patch.object(manager, "_ensure_auto_monitor_update_thread") as start:
        manager.request_refresh(replacement, frozenset({RefreshSection.READBACK}))
        manager._shutdown_event.set()
        manager.request_refresh(device, frozenset({RefreshSection.READBACK}))
        start.assert_not_called()
    assert manager._refresh_scheduler.drain() == []


@pytest.mark.parametrize("with_manager", [False, True])
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_legacy_offline_construction_delegates_without_registration(dm_with_devices, with_manager):
    """Keep the native-object constructor signature used by downstream offline tools."""
    manager = dm_with_devices if with_manager else None
    config = {"name": "offline", "deviceClass": "ophyd.Signal", "deviceConfig": {"value": 4}}
    original = copy.deepcopy(config)
    before = set(dm_with_devices.devices)
    obj, remaining = DeviceManagerDS.construct_device_obj(config, manager)
    try:
        assert isinstance(obj, ophyd.Signal)
        assert obj.name == "offline"
        assert obj.get() == 4
        assert remaining == {}
        assert config == original
        assert set(dm_with_devices.devices) == before
        assert DeviceManagerDS.connect_device(obj, wait_for_all=True, timeout=1, force=True) is None
    finally:
        obj.destroy()


def test_downstream_static_device_checker_constructs_and_connects(monkeypatch):
    """Exercise the actual ophyd_test consumer rather than just the compatibility methods."""
    from ophyd_devices.utils import static_device_test

    # Pytest imports BEC before this optional downstream import can finish. Bind the
    # manager as a fresh ophyd_test process does after its import cycle completes.
    monkeypatch.setattr(static_device_test, "device_manager", DeviceManagerDS)
    StaticDeviceTest = static_device_test.StaticDeviceTest
    config = {
        "deviceClass": "ophyd.Signal",
        "enabled": True,
        "readoutPriority": "monitored",
        "deviceConfig": {"value": 4},
    }
    output = StringIO()
    checker = StaticDeviceTest(output_file=output, config_dict={"offline": config})
    obj = checker.construct_device_obj("offline", config)
    assert obj is not None, output.getvalue()
    try:
        assert checker.connect_device("offline", obj, timeout_per_device=1) == 0, output.getvalue()
    finally:
        if not obj._destroyed:
            obj.destroy()


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
def test_initial_load_cleans_up_on_device_info_publication_failure(device_manager):
    config = {
        "name": "detector",
        "deviceClass": "ophyd.Signal",
        "enabled": True,
        "readoutPriority": "monitored",
    }
    detector = ophyd.Signal(name="detector", value=1)
    providers = device_manager.device_provider_manager
    device = DSDevice("detector", detector, config, parent=device_manager)
    try:
        with (
            mock.patch.object(providers, "construct_device", return_value=(device, {})),
            mock.patch.object(
                device_manager, "publish_device_info", side_effect=RuntimeError("Publish failed")
            ),
            mock.patch.object(detector, "destroy", wraps=detector.destroy) as destroy,
        ):
            with pytest.raises(RuntimeError, match="Publish failed"):
                device_manager._init_device(config, delayed=False, progress=mock.Mock())
            destroy.assert_called_once_with()
            assert "detector" not in device_manager.devices
    finally:
        if not detector._destroyed:
            detector.destroy()


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

    def configure(device, _config):
        obj = device.obj
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
    providers = device_manager.device_provider_manager
    provider = providers.providers["ophyd"]
    device = DSDevice("detector", detector, config, parent=device_manager)
    failed = enabled and failure is not None
    try:
        with (
            mock.patch.object(device_manager, "connector", connected_connector),
            mock.patch.object(connected_connector, "set_and_publish", side_effect=record_publish),
            mock.patch.object(provider, "update_device_config", side_effect=configure),
            mock.patch.object(
                provider,
                "connect_device",
                return_value=ConnectionError("Connection failed") if failure == "connect" else None,
            ) as connect,
        ):
            if failed:
                with pytest.raises((ConnectionError, RuntimeError), match="failed"):
                    providers.initialize_device(device, {})
            else:
                providers.initialize_device(device, {})

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
                # Release the subscription tokens owned by the provider.
                providers.disconnect_device(device)
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
        providers = device_manager.device_provider_manager
        previous = DSDevice("detector", old_signal, config, parent=device_manager)
        replacement = DSDevice(
            "detector", new_signal, {**config, "enabled": True}, parent=device_manager
        )
        providers.initialize_device(previous, {})
        with mock.patch.object(
            device_manager, "publish_device_info", side_effect=RuntimeError("Publish failed")
        ):
            with pytest.raises(RuntimeError, match="Publish failed"):
                providers.initialize_device(replacement, {})
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
    providers = device_manager.device_provider_manager
    device = DSDevice(
        "detector",
        detector,
        {"enabled": True, "deviceClass": "ophyd.Device", "readoutPriority": "monitored"},
        parent=device_manager,
    )
    device_manager.devices["detector"] = device
    cached = messages.DeviceInfoMessage(device="detector", info={"current": True})
    connected_connector.set_and_publish(MessageEndpoints.device_info("detector"), cached)
    callback_passed_root_check = threading.Event()
    publish = providers.publish_device_info

    def publish_after_root_check(obj, **kwargs):
        callback_passed_root_check.set()
        return publish(obj, **kwargs)

    try:
        with (
            mock.patch.object(device_manager, "connector", connected_connector),
            mock.patch.object(
                providers, "publish_device_info", side_effect=publish_after_root_check
            ),
            mock.patch.object(providers, "get_device_info") as describe,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            with providers._device_info_lock:
                callback = executor.submit(device_manager.refresh_device_info, device)
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
@pytest.mark.parametrize("action", ["remove", "disable", "flush", "cleanup"])
def test_device_destruction_waits_for_active_metadata_snapshot(
    device_manager, connected_connector, action
):
    class Detector(ophyd.Device):
        value = ophyd.Component(ophyd.Signal, value=0)

    detector = Detector(name="detector")
    config = {
        "name": "detector",
        "deviceClass": "ophyd.Device",
        "enabled": True,
        "readoutPriority": "monitored",
    }
    device = DSDevice(name="detector", obj=detector, config=config, parent=device_manager)
    device_manager.devices["detector"] = device
    providers = device_manager.device_provider_manager
    handler = ConfigUpdateHandler(device_manager)
    describing = threading.Event()
    resume_description = threading.Event()
    teardown_started = threading.Event()
    destroyed = threading.Event()
    destroy = detector.destroy

    get_device_info = providers.get_device_info

    def describe(device, **kwargs):
        describing.set()
        assert resume_description.wait(3)
        assert not device.obj._destroyed
        return get_device_info(device, **kwargs)

    def destroy_detector():
        destroyed.set()
        destroy()

    def teardown():
        teardown_started.set()
        if action == "remove":
            handler._remove_config(
                messages.DeviceConfigMessage(action="remove", config={"detector": {}}),
                cancel_event=threading.Event(),
            )
        elif action == "disable":
            handler._update_config(
                messages.DeviceConfigMessage(
                    action="update", config={"detector": {"enabled": False}}
                ),
                cancel_event=threading.Event(),
            )
        elif action == "flush":
            handler._flush_config()
        else:
            handler._cleanup_failed_device_init(device)

    try:
        with (
            mock.patch.object(device_manager, "connector", connected_connector),
            mock.patch.object(providers, "get_device_info", side_effect=describe) as snapshot,
            mock.patch.object(detector, "destroy", side_effect=destroy_detector),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            callback = executor.submit(device_manager.refresh_device_info, device)
            try:
                assert describing.wait(3)
                closing = executor.submit(teardown)
                assert teardown_started.wait(3)
                assert not destroyed.wait(0.1), "Destroyed a device during metadata serialization"
            finally:
                resume_description.set()
            callback.result(timeout=3)
            closing.result(timeout=3)
            assert detector._destroyed
            device_manager.refresh_device_info(device)
            assert snapshot.call_count == 1
    finally:
        resume_description.set()
        handler.executor.shutdown(wait=True)
        device_manager.devices.pop("detector", None)
        detector.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_destruction_can_wait_for_metadata_callback(device_manager):
    class Detector(ophyd.Device):
        value = ophyd.Component(ophyd.Signal, value=0)

    detector = Detector(name="detector")
    providers = device_manager.device_provider_manager
    device = DSDevice(
        "detector",
        detector,
        {"enabled": True, "deviceClass": "ophyd.Device", "readoutPriority": "monitored"},
        parent=device_manager,
    )
    device_manager.devices["detector"] = device
    destroy = detector.destroy
    try:
        with (
            mock.patch.object(providers, "get_device_info") as describe,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):

            def destroy_after_callback():
                callback = executor.submit(device_manager.refresh_device_info, device)
                callback.result(timeout=3)
                destroy()

            with mock.patch.object(detector, "destroy", side_effect=destroy_after_callback):
                providers.disconnect_device(device)
            describe.assert_not_called()
            assert detector._destroyed
    finally:
        device_manager.devices.pop("detector", None)
        detector.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_failed_device_destruction_restores_metadata_updates(dm_with_devices):
    device = dm_with_devices.devices.samx
    providers = dm_with_devices.device_provider_manager
    with mock.patch.object(device.obj, "destroy", side_effect=RuntimeError("Destroy failed")):
        with pytest.raises(RuntimeError, match="Destroy failed"):
            providers.disconnect_device(device)
    with mock.patch.object(
        providers, "get_device_info", wraps=providers.get_device_info
    ) as describe:
        providers.publish_device_info(device, connect=False)
        describe.assert_called_once_with(device, connect=False)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_device_info_publication_ignores_replaced_root(device_manager, connected_connector):
    previous = ophyd.Device(name="detector")
    current = ophyd.Device(name="detector")
    providers = device_manager.device_provider_manager
    device_manager.devices["detector"] = DSDevice("detector", current, config=None)
    stale = DSDevice("detector", previous, config=None)
    cached = messages.DeviceInfoMessage(device="detector", info={"current": True})
    connected_connector.set_and_publish(MessageEndpoints.device_info("detector"), cached)
    try:
        with (
            mock.patch.object(device_manager, "connector", connected_connector),
            mock.patch.object(providers, "get_device_info") as describe,
        ):
            # A callback may have passed its root check before waiting to publish.
            providers.publish_device_info(stale)
            describe.assert_not_called()
            assert connected_connector.get(MessageEndpoints.device_info("detector")) == cached
    finally:
        device_manager.devices.pop("detector", None)
        previous.destroy()
        current.destroy()
