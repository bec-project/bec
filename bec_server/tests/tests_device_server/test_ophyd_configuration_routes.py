"""Regression coverage for the Ophyd configuration and serialization routes."""

from __future__ import annotations

import threading
from concurrent.futures import Future
from types import SimpleNamespace
from unittest import mock

import ophyd
import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.devices.config_update_handler import (
    ConfigUpdateHandler as LegacyConfigUpdateHandler,
)
from bec_server.device_server.devices.device_serializer import (
    get_device_info as legacy_get_device_info,
)
from bec_server.device_server.devices.devicemanager import DeviceManagerDS as LegacyDeviceManagerDS
from bec_server.device_server.devices.devicemanager import DSDevice as LegacyDSDevice
from bec_server.device_server.ophyd import serialization
from bec_server.device_server.ophyd.configuration import ConfigUpdateHandler, OphydConfiguration
from bec_server.device_server.ophyd.device_manager import DeviceManagerDS, DSDevice
from bec_server.device_server.ophyd.serialization import get_device_info


def test_legacy_imports_preserve_identity_and_static_construction():
    """Existing downstream imports and class lookup monkeypatches keep working."""
    assert LegacyConfigUpdateHandler is ConfigUpdateHandler
    assert LegacyDSDevice is DSDevice
    assert LegacyDeviceManagerDS is DeviceManagerDS
    assert legacy_get_device_info is get_device_info
    config = {"name": "test", "deviceClass": "signal", "deviceConfig": {"value": 3}}
    with mock.patch.object(LegacyDeviceManagerDS, "_get_device_class", return_value=ophyd.Signal):
        obj, remaining = LegacyDeviceManagerDS.construct_device_obj(config, None)
    try:
        assert obj.name == "test"
        assert obj.get() == 3
        assert remaining == {}
    finally:
        obj.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_configuration_and_serialization_are_bound_before_initialization(device_manager):
    """Native config application remains real when only request handling is replaced."""
    assert isinstance(device_manager.configuration, OphydConfiguration)
    assert device_manager.serialization is serialization
    obj = ophyd.Signal(name="test")
    try:
        device_manager.update_config(obj, {"labels": ["group"]})
        assert obj._ophyd_labels_ == {"group"}
        device_manager.config_update_handler.apply_device_config.assert_not_called()
    finally:
        obj.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_manager_publishes_through_serialization_route(device_manager, connected_connector):
    """Serialization route data reaches the existing proxy-info endpoint unchanged."""
    device_manager.connector = connected_connector
    obj = ophyd.Signal(name="test")
    interface = {"device_class": "Signal", "signals": {}, "ownership_mode": "claimable"}
    try:
        with mock.patch.object(
            device_manager.serialization, "get_device_info", return_value=interface
        ) as serialize:
            assert device_manager.publish_device_info(obj, connect=False) is None
        serialize.assert_called_once_with(obj, connect=False)
        published = device_manager.connector.get(MessageEndpoints.device_info(obj.name))
        assert published.info == interface
    finally:
        obj.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_callback_uses_its_own_reply_handler(dm_with_devices, connected_connector):
    """A separately injected handler cannot receive another handler's request reply."""
    device_manager = dm_with_devices
    device_manager.connector = connected_connector
    installed_handler = device_manager.config_update_handler
    handler = ConfigUpdateHandler(device_manager)
    msg = messages.DeviceConfigMessage(
        action="update", config={"bpm4i": {"enabled": False}}, metadata={"RID": "route"}
    )
    completed = threading.Event()
    original_send = handler.send_config_request_reply

    def send_reply(**kwargs):
        original_send(**kwargs)
        completed.set()

    try:
        with mock.patch.object(handler, "send_config_request_reply", side_effect=send_reply):
            handler._device_config_callback(SimpleNamespace(value=msg))
            assert completed.wait(timeout=5)
        reply = connected_connector.get(MessageEndpoints.device_config_request_response("route"))
        assert reply.accepted is True
        assert device_manager.devices.bpm4i.enabled is False
        installed_handler.parse_config_request.assert_not_called()
    finally:
        handler.shutdown()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_route_accepts_direct_requests(dm_with_devices):
    """Native application works without invoking the installed transport handler."""
    msg = messages.DeviceConfigMessage(action="update", config={"bpm4i": {"enabled": False}})
    dm_with_devices.configuration.parse_config_request(msg, threading.Event())
    device = dm_with_devices.devices.bpm4i
    assert device.enabled is False
    assert device.initialized is False
    assert device.obj._destroyed is True
    dm_with_devices.config_update_handler.parse_config_request.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_failed_init_cleanup_uses_configuration_route(device_manager):
    """Failed initialization releases native resources through the configuration owner."""
    obj = mock.Mock(name="test")
    device_manager.configuration._cleanup_failed_device_init(obj)
    obj.destroy.assert_called_once_with()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_completed_config_future_does_not_deadlock_or_leave_active_request(device_manager):
    """Already-complete executor results invoke cleanup after request registration."""
    handler = ConfigUpdateHandler(device_manager)
    future = Future()
    future.set_result(None)
    msg = messages.DeviceConfigMessage(action="update", config={"test": {"enabled": True}})
    try:
        with mock.patch.object(handler.executor, "submit", return_value=future):
            handler._device_config_callback(SimpleNamespace(value=msg))
        assert handler._active_request is None
        assert handler._requests == {}
    finally:
        handler.shutdown()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_previous_config_completion_retains_newer_request(device_manager):
    """Completion of one queued request cannot clear the next request's cancellation state."""
    handler = ConfigUpdateHandler(device_manager)
    previous = Future()
    current = Future()
    msg = messages.DeviceConfigMessage(action="update", config={"test": {"enabled": True}})
    try:
        with mock.patch.object(handler.executor, "submit", side_effect=[previous, current]):
            handler._device_config_callback(SimpleNamespace(value=msg))
            handler._device_config_callback(SimpleNamespace(value=msg))
        previous.set_result(None)
        assert handler._active_request["future"] is current
        assert set(handler._requests) == {current}
        current.set_result(None)
        assert handler._active_request is None
        assert handler._requests == {}
    finally:
        handler.shutdown()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_config_shutdown_rejects_late_callbacks_and_cancels_all_requests(device_manager):
    """Shutdown drains admitted work and prevents submissions into the closed executor."""
    handler = ConfigUpdateHandler(device_manager)
    msg = messages.DeviceConfigMessage(action="update", config={"test": {"enabled": True}})
    try:
        with mock.patch.object(handler.executor, "submit", side_effect=[Future(), Future()]):
            handler._device_config_callback(SimpleNamespace(value=msg))
            handler._device_config_callback(SimpleNamespace(value=msg))
        requests = list(handler._requests.values())
        with (
            mock.patch.object(handler.executor, "shutdown") as shutdown,
            mock.patch.object(handler.connector, "unregister") as unregister,
        ):
            handler.shutdown()
            handler.shutdown()
        assert all(request["cancel_event"].is_set() for request in requests)
        unregister.assert_called_once_with(
            MessageEndpoints.device_server_config_request(), cb=handler._device_config_callback
        )
        shutdown.assert_called_once_with(wait=True, cancel_futures=True)
        with mock.patch.object(handler.executor, "submit") as submit:
            handler._device_config_callback(SimpleNamespace(value=msg))
        submit.assert_not_called()
    finally:
        handler.executor.shutdown(wait=True, cancel_futures=True)


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_manager_shutdown_quiesces_config_before_destroy_and_retains_transport(device_manager):
    """The layer owns hardware teardown; the surrounding service owns the connector."""
    events = []
    obj = ophyd.Signal(name="test")
    wrapper = DSDevice(
        name="test",
        obj=obj,
        config={"enabled": True, "deviceClass": "ophyd.Signal", "readoutPriority": "monitored"},
        parent=device_manager,
    )
    device_manager.devices._add_device("test", wrapper)
    device_manager.config_update_handler.shutdown.side_effect = lambda: events.append("config")
    with (
        mock.patch.object(obj, "destroy", wraps=obj.destroy) as destroy,
        mock.patch.object(device_manager.connector, "shutdown") as close_transport,
    ):
        destroy.side_effect = lambda: events.append("destroy")
        device_manager.shutdown()
        device_manager.shutdown()
        device_manager.__del__()
        close_transport.assert_not_called()
    assert events == ["config", "destroy"]
    assert not device_manager.devices
    obj.destroy()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_manager_shutdown_ignores_late_native_and_transport_callbacks(device_manager):
    """Callbacks already dispatched by another thread cannot publish after teardown."""
    device_manager.shutdown()
    with mock.patch.object(device_manager.connector, "set_and_publish") as publish:
        device_manager._obj_callback_readback(obj=mock.Mock())
        device_manager._obj_callback_bec_message_signal(obj=mock.Mock(), value=mock.Mock())
        device_manager._device_config_update_callback(mock.Mock())
        device_manager._update_scan_info(mock.Mock())
    publish.assert_not_called()
