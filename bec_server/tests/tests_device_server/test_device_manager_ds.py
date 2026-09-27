from __future__ import annotations

import copy
import logging
import threading
import time
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, Literal, cast
from unittest import mock

import numpy as np
import ophyd
import pytest
from ophyd.device import OrderedDictType
from ophyd_devices.devices.psi_motor import EpicsMotor
from ophyd_devices.tests.utils import patched_device
from redis.client import Pipeline

from bec_lib import messages
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.devicemanager import DeviceManagerDS, DSDevice

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


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
def test_flyer_event_callback(dm_with_devices, connected_connector):
    device_manager = dm_with_devices
    samx = device_manager.devices.samx
    samx.metadata = {"scan_id": "12345"}
    # Use here fake redis connector to avoid complications with PipelineMock
    device_manager.connector = connected_connector
    device_manager._obj_flyer_callback(
        obj=samx.obj,
        value={"data": {"idata": np.random.rand(20), "edata": np.random.rand(20)}},
        metadata={"scan_id": "test_scan_id"},
    )
    msg = connected_connector.get(MessageEndpoints.device_read("samx"))
    assert "signals" in msg.content
    assert "idata" in msg.content["signals"]
    assert "edata" in msg.content["signals"]
    msg = connected_connector.get(MessageEndpoints.device_status("samx"))
    assert msg.metadata["scan_id"] == "12345"
    assert msg.content["device"] == "samx"
    assert msg.content["status"] == 20


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_obj_callback_progress(dm_with_devices):
    device_manager = dm_with_devices
    samx = device_manager.devices.samx
    samx.metadata = {"scan_id": "12345"}

    with mock.patch.object(device_manager, "connector") as mock_connector:
        device_manager._obj_callback_progress(obj=samx.obj, value=1, max_value=2, done=False)
        mock_connector.set_and_publish.assert_called_once_with(
            MessageEndpoints.device_progress("samx"),
            messages.ProgressMessage(
                value=1, max_value=2, done=False, metadata={"scan_id": "12345"}
            ),
            expire=3600,
        )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_obj_callback_configuration(
    dm_with_devices: DeviceManagerDS, connected_connector: RedisConnector
) -> None:
    """Verify obj callback configuration.

    Args:
        dm_with_devices (DeviceManagerDS): Device manager with initialized simulated devices.
        connected_connector (RedisConnector): Redis connector backed by the test server.
    """
    device_manager = dm_with_devices
    samx = device_manager.devices.samx
    samx.metadata = {"scan_id": "12345"}
    device_manager.connector = connected_connector

    device_manager._obj_callback_configuration(obj=samx.obj)
    assert device_manager.event_dispatcher.wait_idle(timeout=5, obj=samx.obj)

    msg = connected_connector.get(MessageEndpoints.device_read_configuration("samx"))
    expected = messages.DeviceMessage(
        signals=samx.obj.read_configuration(), metadata={"scan_id": "12345"}
    )
    assert msg == expected


@pytest.mark.parametrize("blocked_operation", ["redis", "read"])
def test_root_callbacks_leave_ophyd_monitor_thread_free(
    connected_connector: RedisConnector, blocked_operation: Literal["redis", "read"]
) -> None:
    """Verify root callbacks leave ophyd monitor thread free.

    Args:
        connected_connector (RedisConnector): Redis connector backed by the test server.
        blocked_operation (Literal["redis", "read"]): Worker operation to hold while callbacks run.
    """
    from ophyd._dispatch import EventDispatcher  # pylint: disable=import-outside-toplevel

    class Motor(ophyd.Device):
        """Provide monitored readback and moving-state signals."""

        readback = ophyd.Component(ophyd.Signal, value=0)
        motor_is_moving = ophyd.Component(ophyd.Signal, value=0, kind=ophyd.Kind.omitted)

    service = mock.Mock(connector=connected_connector, _service_name="device_server")
    manager = DeviceManagerDS(service)
    obj = Motor(name="monitor_motor")
    cast(SimpleNamespace, obj.readback)._auto_monitor = blocked_operation != "read"
    device = DSDevice(
        obj.name,
        obj,
        {"enabled": True, "deviceClass": "Motor", "readoutPriority": "monitored"},
        parent=manager,
    )
    manager.devices._add_device(obj.name, device)
    manager.initialize_enabled_device(device)
    monitor = EventDispatcher(context=None, logger=logging.getLogger(__name__), utility_threads=0)
    context = monitor.get_thread_context("monitor")
    entered = threading.Event()
    release = threading.Event()
    unrelated_complete = threading.Event()
    unrelated_status = ophyd.StatusBase()
    original_read = obj.read
    original_pipeline = connected_connector.pipeline

    def slow_read() -> OrderedDictType:
        """Block a device read while unrelated monitor callbacks run.

        Returns:
            OrderedDictType: Values returned by the device reader.
        """
        entered.set()
        assert release.wait(5)
        return original_read()

    def slow_pipeline() -> Pipeline:
        """Wrap a real pipeline with a controllably blocked execution.

        Returns:
            Pipeline: Real pipeline with a blocked execution callback.
        """
        pipeline = original_pipeline()
        execute = pipeline.execute

        def slow_execute(raise_on_error: bool = True) -> list[Any]:
            """Block Redis execution until the test releases it.

            Args:
                raise_on_error (bool): Whether Redis command errors should raise.

            Returns:
                list[Any]: Redis responses after the execution barrier is released.
            """
            entered.set()
            assert release.wait(5)
            return execute(raise_on_error=raise_on_error)

        pipeline.execute = slow_execute
        return pipeline

    def finish_unrelated_move() -> None:
        """Complete an unrelated move on the shared monitor thread."""
        assert threading.current_thread().name == "monitor"
        unrelated_status.set_finished()
        unrelated_complete.set()

    target, method, replacement = (
        (connected_connector, "pipeline", slow_pipeline)
        if blocked_operation == "redis"
        else (obj, "read", slow_read)
    )
    try:
        with mock.patch.object(target, method, side_effect=replacement):
            context.run(manager._obj_callback_readback, obj=obj)
            assert entered.wait(2)
            # Both root callback paths used to do I/O on this shared monitor thread.
            for value in range(100):
                context.run(manager._obj_callback_readback, obj=obj)
                context.run(
                    manager._obj_callback_is_moving, obj=obj.motor_is_moving, value=value % 2
                )
            context.run(manager._obj_callback_is_moving, obj=obj.motor_is_moving, value=0)
            context.run(finish_unrelated_move)
            assert unrelated_complete.wait(2)
            assert unrelated_status.done
            assert monitor.threads["monitor"].queue.empty()
            release.set()
            assert manager.event_dispatcher.wait_idle(timeout=5, obj=obj)
        status = connected_connector.get(MessageEndpoints.device_status(obj.name))
        assert status.status == 0
    finally:
        release.set()
        monitor.stop()
        manager.shutdown()


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
            expire=3600,
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


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_obj_callback_file_event(dm_with_devices, connected_connector):
    device_manager = dm_with_devices
    eiger = device_manager.devices.eiger
    eiger.metadata = {"scan_id": "12345"}
    # Use here fake redis connector, pipe is used and checks pydantic models
    device_manager.connector = connected_connector
    device_manager._obj_callback_file_event(
        obj=eiger.obj,
        file_path="test_file_path",
        done=True,
        successful=True,
        hinted_h5_entries={"my_entry": "entry/data/data"},
        metadata={"user_info": "my_info"},
    )
    msg = connected_connector.get(MessageEndpoints.file_event(name="eiger"))
    msg2 = connected_connector.get(MessageEndpoints.public_file(scan_id="12345", name="eiger"))
    assert msg == msg2
    assert msg.content["file_path"] == "test_file_path"
    assert msg.content["done"] is True
    assert msg.content["successful"] is True
    assert msg.content["hinted_h5_entries"] == {"my_entry": "entry/data/data"}
    assert msg.content["file_type"] == "h5"
    assert msg.metadata == {"scan_id": "12345", "user_info": "my_info"}
    assert msg.content["is_master_file"] is False


def subscribe_directly(
    obj: ophyd.OphydObject,
    callback: Callable[..., None],
    *,
    event_type: str | None = None,
    run: bool = False,
    domain: str | None = None,
) -> int:
    """Exercise callback routing independently of registered dispatcher state.

    Args:
        obj (ophyd.OphydObject): Object or test double receiving the subscription.
        callback (Callable[..., None]): Selected event handler.
        event_type (str | None): Explicit event type, or None for the default event.
        run (bool): Whether to replay the cached event.
        domain (str | None): Dispatcher domain, unused by this routing double.

    Returns:
        int: Subscription identifier returned by the object.
    """
    del domain
    if event_type is None:
        return obj.subscribe(callback, run=run)
    return obj.subscribe(callback, event_type=event_type, run=run)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_subscribe_to_device_events(
    dm_with_devices: DeviceManagerDS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Route each advertised event to its existing callback.

    Args:
        dm_with_devices (DeviceManagerDS): Manager with simulated devices.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring the subscription test double.
    """
    monkeypatch.setattr(dm_with_devices.event_dispatcher, "subscribe", subscribe_directly)
    opaas_obj = mock.MagicMock()
    opaas_obj.enabled = False
    obj = mock.MagicMock()
    # Test 2 event types together
    obj.event_types = ("file_event", "device_monitor_1d")
    with mock.patch.object(dm_with_devices, "_obj_callback_file_event") as mock_callback_file_event:
        with mock.patch.object(
            dm_with_devices, "_obj_callback_device_monitor_1d"
        ) as mock_callback_device_monitor_1d:
            dm_with_devices._subscribe_to_device_events(obj=obj, opaas_obj=opaas_obj)
            assert obj.subscribe.call_count == 0
            dm_with_devices._subscribe_to_bec_device_events(obj=obj)
            assert obj.subscribe.call_count == 2
            assert (
                mock.call(mock_callback_file_event, event_type="file_event", run=False)
                in obj.subscribe.call_args_list
            )
            assert (
                mock.call(
                    mock_callback_device_monitor_1d, event_type="device_monitor_1d", run=False
                )
                in obj.subscribe.call_args_list
            )

    # Test all event types
    for ii, event_type in enumerate(
        [
            "readback",
            "value",
            "device_monitor_1d",
            "device_monitor_2d",
            "file_event",
            "done_moving",
            "progress",
        ]
    ):
        obj.event_types = (event_type,)
        callback_name = (
            f"_obj_callback_{event_type}" if event_type != "value" else "_obj_callback_readback"
        )
        with mock.patch.object(dm_with_devices, callback_name) as mock_callback:
            dm_with_devices._subscribe_to_device_events(obj=obj, opaas_obj=opaas_obj)
            dm_with_devices._subscribe_to_bec_device_events(obj=obj)
            assert obj.subscribe.call_args == mock.call(
                mock_callback, event_type=event_type, run=False
            )


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
def test_initialize_device(
    dm_with_devices: DeviceManagerDS,
    epics_motor: EpicsMotor,
    epics_motor_config: dict[str, Any],
    timeout: int | None,
    enabled: bool,
) -> None:
    """Verify initialize device.

    Args:
        dm_with_devices (DeviceManagerDS): Device manager with initialized simulated devices.
        epics_motor (EpicsMotor): Motor with patched EPICS connections.
        epics_motor_config (dict[str, Any]): Mutable configuration for the motor.
        timeout (int | None): Connection timeout override, or the default when absent.
        enabled (bool): Whether initialization should connect and subscribe the motor.
    """
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
            else:
                mock_initialize_enabled_device.assert_not_called()
                mock_connect_device.assert_not_called()
                mock_low_subscribe.assert_not_called()
                mock_high_subscribe.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_subscribe_to_auto_monitors_recurses_and_subscribes_by_kind(
    dm_with_devices: DeviceManagerDS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify subscribe to auto monitors recurses and subscribes by kind.

    Args:
        dm_with_devices (DeviceManagerDS): Device manager with initialized simulated devices.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring the subscription test double.
    """
    service = mock.MagicMock()
    service.connector = mock.MagicMock()
    service._service_name = "device_server"
    device_manager = DeviceManagerDS(service)
    monkeypatch.setattr(device_manager.event_dispatcher, "subscribe", subscribe_directly)

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

    device_manager._subscribe_to_auto_monitors(cast(ophyd.OphydObject, obj))

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


class SnapshotRecoveryDevice(ophyd.Device):
    """Provide monitored reading and configuration fields for lifecycle regressions.

    Attributes:
        readback (ophyd.Signal): Monitored value included in ordinary readings.
        setting (ophyd.Signal): Monitored value included in configuration readings.
        SUB_FILE_EVENT (str): Legacy file event used to verify subscription continuity.
    """

    readback = ophyd.Component(ophyd.Signal, value=0, kind=ophyd.Kind.normal)
    setting = ophyd.Component(ophyd.Signal, value=1, kind=ophyd.Kind.config)
    SUB_FILE_EVENT = "file_event"


def add_snapshot_recovery_device(
    device_manager: DeviceManagerDS, name: str, *, initialize: bool = True
) -> DSDevice:
    """Register a real ophyd device with the supplied test manager.

    Args:
        device_manager (DeviceManagerDS): Manager that owns the device and its subscriptions.
        name (str): Unique root device name.
        initialize (bool): Whether to subscribe and publish the initial baseline.

    Returns:
        DSDevice: Managed wrapper retained by the manager for teardown.
    """
    obj = SnapshotRecoveryDevice(name=name)
    for signal in (obj.readback, obj.setting):
        cast(SimpleNamespace, signal)._auto_monitor = True
    device = DSDevice(
        name,
        obj,
        {
            "enabled": True,
            "deviceClass": "SnapshotRecoveryDevice",
            "readoutPriority": "monitored",
            "deviceConfig": {},
        },
        parent=device_manager,
    )
    device_manager.devices._add_device(name, device)
    if initialize:
        device_manager.initialize_enabled_device(device)
    return device


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
@pytest.mark.parametrize("old_config", [{}, {"labels": ["old"]}], ids=["empty", "nonempty"])
@pytest.mark.parametrize("failure", ["read", "read_configuration", "redis"])
def test_config_rebind_recovers_after_apply_and_rollback_baselines_fail(
    device_manager: DeviceManagerDS,
    connected_connector: RedisConnector,
    old_config: dict[str, list[str]],
    failure: Literal["read", "read_configuration", "redis"],
) -> None:
    """Recover pending final values after both configuration baselines fail.

    Args:
        device_manager (DeviceManagerDS): Manager using the device-server implementation.
        connected_connector (RedisConnector): Connector backed by a fake Redis server.
        old_config (dict[str, list[str]]): Configuration restored by the rejected request.
        failure (Literal["read", "read_configuration", "redis"]): Baseline operation to fail.
    """
    device_manager.connector = connected_connector
    device = add_snapshot_recovery_device(device_manager, "recovery")
    obj = cast(SnapshotRecoveryDevice, device.obj)
    device._config["deviceConfig"] = copy.deepcopy(old_config)
    handler = ConfigUpdateHandler(device_manager)
    device_manager.config_update_handler = handler
    available = threading.Event()
    original_read = getattr(obj, failure) if failure != "redis" else obj.read
    original_pipeline = connected_connector.pipeline

    def unavailable_read() -> OrderedDictType:
        """Reject baseline reads until the simulated hardware recovers.

        Returns:
            OrderedDictType: Current device readings after recovery.

        Raises:
            RuntimeError: The simulated hardware remains unavailable.
        """
        if not available.is_set():
            raise RuntimeError("baseline unavailable")
        return original_read()

    def unavailable_pipeline() -> Pipeline:
        """Create a pipeline that remains unavailable through configuration rollback.

        Returns:
            Pipeline: Pipeline with a recoverable execution failure.
        """
        pipeline = original_pipeline()
        execute = pipeline.execute

        def execute_when_available(raise_on_error: bool = True) -> list[Any]:
            """Execute only once the simulated Redis service recovers.

            Args:
                raise_on_error (bool): Whether individual Redis failures should raise.

            Returns:
                list[Any]: Results returned by the restored Redis connection.

            Raises:
                RuntimeError: The simulated Redis connection remains unavailable.
            """
            if not available.is_set():
                raise RuntimeError("baseline unavailable")
            return execute(raise_on_error=raise_on_error)

        pipeline.execute = execute_when_available
        return pipeline

    # Hardware may change while bindings are replaced; supply no rescuing callback.
    obj.readback._readback = 7
    obj.setting._readback = 9
    request = messages.DeviceConfigMessage(
        action="update", config={obj.name: {"deviceConfig": {"labels": ["new"]}}}
    )
    target, attribute, replacement = (
        (connected_connector, "pipeline", unavailable_pipeline)
        if failure == "redis"
        else (obj, failure, unavailable_read)
    )
    with mock.patch.object(target, attribute, side_effect=replacement):
        with pytest.raises(RuntimeError, match="baseline unavailable"):
            handler._update_config(request, threading.Event())
        assert device.enabled and device.initialized
        assert device_manager.event_dispatcher._states[id(obj)].active
        available.set()
        assert device_manager.event_dispatcher.wait_idle(timeout=5, obj=obj)

        readback = connected_connector.get(MessageEndpoints.device_readback(obj.name))
        configuration = connected_connector.get(
            MessageEndpoints.device_read_configuration(obj.name)
        )
        assert readback.signals[obj.readback.name]["value"] == 7
        assert configuration.signals[obj.setting.name]["value"] == 9

        obj.readback.put(11)
        obj.setting.put(13)
        assert device_manager.event_dispatcher.wait_idle(timeout=5, obj=obj)
        readback = connected_connector.get(MessageEndpoints.device_readback(obj.name))
        configuration = connected_connector.get(
            MessageEndpoints.device_read_configuration(obj.name)
        )
        assert readback.signals[obj.readback.name]["value"] == 11
        assert configuration.signals[obj.setting.name]["value"] == 13


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_empty_config_rollback_rebinds_after_subscription_failure(
    device_manager: DeviceManagerDS, connected_connector: RedisConnector
) -> None:
    """Restore subscriptions when binding fails before baseline initialization.

    Args:
        device_manager (DeviceManagerDS): Empty manager using the device-server implementation.
        connected_connector (RedisConnector): Connector backed by a fake Redis server.
    """
    device_manager.connector = connected_connector
    device = add_snapshot_recovery_device(device_manager, "subscription_recovery")
    obj = cast(SnapshotRecoveryDevice, device.obj)
    handler = ConfigUpdateHandler(device_manager)
    device_manager.config_update_handler = handler
    subscribe = device_manager._subscribe_to_auto_monitors
    failed = False

    def fail_first_subscription(root: ophyd.OphydObject) -> None:
        """Fail once, then install the real component subscriptions during rollback.

        Args:
            root (ophyd.OphydObject): Root whose monitored components need subscriptions.

        Raises:
            RuntimeError: The first binding attempt encounters the injected driver failure.
        """
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("subscription unavailable")
        subscribe(root)

    request = messages.DeviceConfigMessage(
        action="update", config={obj.name: {"deviceConfig": {"labels": ["new"]}}}
    )
    with (
        mock.patch.object(
            device_manager, "_subscribe_to_auto_monitors", side_effect=fail_first_subscription
        ) as subscribe_mock,
        pytest.raises(DeviceConfigError, match="subscription unavailable"),
    ):
        handler._update_config(request, threading.Event())
    assert subscribe_mock.call_count == 2
    assert device.enabled and device.initialized
    obj.readback.put(17)
    obj.setting.put(19)
    assert device_manager.event_dispatcher.wait_idle(timeout=5, obj=obj)
    readback = connected_connector.get(MessageEndpoints.device_readback(obj.name))
    configuration = connected_connector.get(MessageEndpoints.device_read_configuration(obj.name))
    assert readback.signals[obj.readback.name]["value"] == 17
    assert configuration.signals[obj.setting.name]["value"] == 19


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_failed_first_baseline_does_not_activate_device(device_manager: DeviceManagerDS) -> None:
    """Keep a device inactive when its first baseline could not be initialized.

    Args:
        device_manager (DeviceManagerDS): Empty manager using the device-server implementation.
    """
    device = add_snapshot_recovery_device(device_manager, "failed_initialization", initialize=False)
    obj = cast(SnapshotRecoveryDevice, device.obj)
    with mock.patch.object(obj, "read", side_effect=RuntimeError("first baseline failed")) as read:
        with pytest.raises(RuntimeError, match="first baseline failed"):
            device_manager.initialize_enabled_device(device)
        obj.readback.put(23)
        assert not device.initialized
        assert not device_manager.event_dispatcher._states[id(obj)].active
        read.assert_called_once_with()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_manager_shutdown_skips_busy_roots_without_per_device_wait(
    device_manager: DeviceManagerDS,
) -> None:
    """Destroy free roots promptly while leaving devices with active operations intact.

    Args:
        device_manager (DeviceManagerDS): Empty manager using the device-server implementation.
    """
    busy_devices = [
        add_snapshot_recovery_device(device_manager, f"busy_{index}") for index in range(2)
    ]
    free_device = add_snapshot_recovery_device(device_manager, "free_root")
    dispatcher = device_manager.event_dispatcher
    release = threading.Event()
    entered = [threading.Event() for _device in busy_devices]

    def hold_operation(index: int) -> None:
        """Hold a root gate to simulate an explicit hardware operation.

        Args:
            index (int): Index of the busy device and its synchronization event.
        """
        with dispatcher.read_context(busy_devices[index].obj):
            entered[index].set()
            release.wait(4)

    threads = [threading.Thread(target=hold_operation, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    try:
        assert all(event.wait(1) for event in entered)
        started = time.monotonic()
        device_manager.shutdown()
        elapsed = time.monotonic() - started
        assert elapsed < 2
        assert free_device.obj._destroyed
        assert all(not device.obj._destroyed for device in busy_devices)
    finally:
        release.set()
        for thread in threads:
            thread.join(1)
            assert not thread.is_alive()
        for device in busy_devices:
            assert dispatcher.remove(device.obj, timeout=0)
            device.obj.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_refresh_keeps_one_shot_events_and_existing_subscription_ids(
    device_manager: DeviceManagerDS,
) -> None:
    """Keep file callbacks continuously installed while updating the same root.

    Args:
        device_manager (DeviceManagerDS): Manager using the device-server implementation.
    """
    with mock.patch.object(device_manager, "_obj_callback_file_event") as received:
        device = add_snapshot_recovery_device(device_manager, "continuous_events")
        obj = cast(SnapshotRecoveryDevice, device.obj)
        dispatcher = device_manager.event_dispatcher
        state = dispatcher._states[id(obj)]
        bindings = state.subscriptions.copy()
        bind = device_manager._bind_event_subscriptions

        def bind_while_emitting(managed: DSDevice) -> None:
            """Emit a one-shot event at the former remove/rebind boundary.

            Args:
                managed (DSDevice): Existing root whose subscriptions are refreshed.
            """
            obj._run_subs(sub_type=obj.SUB_FILE_EVENT, value="during-refresh.h5")
            bind(managed)

        with mock.patch.object(
            device_manager, "_bind_event_subscriptions", side_effect=bind_while_emitting
        ):
            device_manager.update_config(obj, {"labels": ["changed"]})
        assert received.call_count == 1
        assert received.call_args.kwargs["value"] == "during-refresh.h5"
        assert dispatcher._states[id(obj)] is state
        assert state.subscriptions == bindings


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_existing_monitors_survive_subscription_failure_during_apply_and_rollback(
    device_manager: DeviceManagerDS, connected_connector: RedisConnector
) -> None:
    """Retain working monitors even when both attempts to refresh bindings fail.

    Args:
        device_manager (DeviceManagerDS): Manager using the device-server implementation.
        connected_connector (RedisConnector): Connector backed by a fake Redis server.
    """
    device_manager.connector = connected_connector
    device = add_snapshot_recovery_device(device_manager, "retained_monitors")
    obj = cast(SnapshotRecoveryDevice, device.obj)
    dispatcher = device_manager.event_dispatcher
    state = dispatcher._states[id(obj)]
    bindings = state.subscriptions.copy()
    handler = ConfigUpdateHandler(device_manager)
    device_manager.config_update_handler = handler
    request = messages.DeviceConfigMessage(
        action="update", config={obj.name: {"deviceConfig": {"labels": ["changed"]}}}
    )
    with mock.patch.object(
        device_manager,
        "_subscribe_to_auto_monitors",
        side_effect=RuntimeError("subscription unavailable"),
    ) as subscribe:
        with pytest.raises(RuntimeError, match="subscription unavailable"):
            handler._update_config(request, threading.Event())
        assert subscribe.call_count == 2
        assert dispatcher._states[id(obj)] is state
        assert state.subscriptions == bindings
        obj.readback.put(17)
        obj.setting.put(19)
        assert dispatcher.wait_idle(timeout=5, obj=obj)
    assert (
        connected_connector.get(MessageEndpoints.device_readback(obj.name)).signals[
            obj.readback.name
        ]["value"]
        == 17
    )
    assert (
        connected_connector.get(MessageEndpoints.device_read_configuration(obj.name)).signals[
            obj.setting.name
        ]["value"]
        == 19
    )


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_refresh_reconciles_kind_and_monitor_changes(
    device_manager: DeviceManagerDS, connected_connector: RedisConnector
) -> None:
    """Move fields between domains and restore dirty reads when monitoring is disabled.

    Args:
        device_manager (DeviceManagerDS): Manager using the device-server implementation.
        connected_connector (RedisConnector): Connector backed by a fake Redis server.
    """
    device_manager.connector = connected_connector
    device = add_snapshot_recovery_device(device_manager, "changed_coverage")
    obj = cast(SnapshotRecoveryDevice, device.obj)
    dispatcher = device_manager.event_dispatcher
    obj.readback.kind = ophyd.Kind.config
    device_manager._refresh_event_subscriptions(obj)
    obj.readback.put(23)
    assert dispatcher.wait_idle(obj=obj)
    configuration = connected_connector.get(MessageEndpoints.device_read_configuration(obj.name))
    assert configuration.signals[obj.readback.name]["value"] == 23
    assert (
        obj.readback.name
        not in connected_connector.get(MessageEndpoints.device_readback(obj.name)).signals
    )
    obj.readback.kind = ophyd.Kind.normal | ophyd.Kind.config
    obj.setting._auto_monitor = False
    device_manager._refresh_event_subscriptions(obj)
    obj.setting.put(27)
    obj.readback.put(29)
    assert dispatcher.wait_idle(obj=obj)
    configuration = connected_connector.get(MessageEndpoints.device_read_configuration(obj.name))
    assert configuration.signals[obj.setting.name]["value"] == 27
    assert configuration.signals[obj.readback.name]["value"] == 29
    assert not dispatcher._states[id(obj)].domains["configuration"].callback_only
