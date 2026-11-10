"""Validate transient readings, refresh scheduling, and BEC publication."""

from __future__ import annotations

from concurrent.futures import Future
from threading import Event, Thread, get_ident
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
from ophyd import Signal

from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.device_providers.device_interfaces import (
    DeviceTarget,
    RefreshSection,
    wait_for_completion,
)
from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
    OphydDeviceProvider,
)
from bec_server.device_server.devices.devicemanager import DeviceManagerDS
from bec_server.device_server.devices.ds_device import DSDevice
from bec_server.device_server.refresh_scheduler import RefreshScheduler


@pytest.fixture
def root_device():
    """Use a soft root and release its native resources."""
    signal = Signal(name="refresh_signal", value=np.array([1, 2]))
    try:
        yield DSDevice(name=signal.name, obj=signal, config=None)
    finally:
        signal.destroy()


def test_scheduler_coalesces_by_identity_and_preserves_section_order(root_device):
    """Same-name replacement wrappers must produce distinct acquisition requests."""
    scheduler = RefreshScheduler()
    replacement = DSDevice(name=root_device.name, obj=root_device.obj, config=None)
    for _ in range(3):
        scheduler.request_refresh(root_device, frozenset(RefreshSection))
    scheduler.request_refresh(replacement, frozenset({RefreshSection.READBACK}))
    batch = scheduler.drain()
    assert [section for _, section in batch] == [
        RefreshSection.READBACK,
        RefreshSection.READBACK,
        RefreshSection.CONFIGURATION,
        RefreshSection.LIMITS,
    ]
    assert {id(device) for device, _ in batch[:2]} == {id(root_device), id(replacement)}
    assert scheduler.drain() == []
    scheduler.request_refresh(root_device, frozenset({RefreshSection.READBACK}))
    next_batch = scheduler.drain()
    assert len(next_batch) == 1
    assert next_batch[0][0] is root_device
    assert next_batch[0][1] is RefreshSection.READBACK
    assert scheduler.drain() == []


def test_scheduler_notification_after_drain_survives_for_next_batch(root_device):
    """No scheduler lock is held during acquisition or publication."""
    scheduler = RefreshScheduler()
    scheduler.request_refresh(root_device, frozenset({RefreshSection.READBACK}))
    first = scheduler.drain()
    scheduler.request_refresh(root_device, frozenset({RefreshSection.READBACK}))
    second = scheduler.drain()
    assert first[0][0] is second[0][0]
    assert len(first) == len(second) == 1
    assert first[0] is not second[0]
    assert first[0][1] is second[0][1] is RefreshSection.READBACK
    scheduler.request_refresh(root_device, frozenset(RefreshSection))
    scheduler.clear()
    assert scheduler.drain() == []


def test_ophyd_read_forwards_current_reading_without_copying(root_device):
    """The transient read result retains native buffers for immediate publication."""
    provider = OphydDeviceProvider(None)
    target = DeviceTarget(root_device)
    data = np.array([1, 2])
    reading = {"value": {"value": data}}
    with mock.patch.object(root_device.obj, "read", return_value=reading) as read:
        result = wait_for_completion(provider.read(target))
    read.assert_called_once_with()
    assert result is reading
    assert result["value"]["value"] is data


def test_ophyd_root_signal_has_no_separate_configuration(root_device):
    """No raw read_configuration invocation is made for standalone signals."""
    provider = OphydDeviceProvider(None)
    target = DeviceTarget(root_device)
    with mock.patch.object(root_device.obj, "read_configuration") as read:
        assert wait_for_completion(provider.read_configuration(target, separate_only=True)) is None
    read.assert_not_called()


def test_coordinator_waits_for_foreign_completion_without_owning_its_runtime():
    """A future async backend may finish elsewhere; only the coordinator publishes."""
    readings = {}
    future = Future()
    registered = Event()
    completion = future
    original_add_callback = completion.add_done_callback

    def register(callback):
        original_add_callback(callback)
        registered.set()

    def complete():
        assert registered.wait(5)
        future.set_result(readings)

    worker = Thread(target=complete)
    worker.start()
    try:
        with mock.patch.object(completion, "add_done_callback", side_effect=register):
            assert wait_for_completion(completion) is readings
    finally:
        worker.join(timeout=5)
        assert not worker.is_alive()


def test_classic_acquisition_stays_on_calling_thread(root_device):
    """The provider returns a terminal completion without transferring thread ownership."""
    provider = OphydDeviceProvider(None)
    target = DeviceTarget(root_device)
    calling_thread = get_ident()
    native_read = root_device.obj.read
    threads = []

    def read():
        threads.append(get_ident())
        return native_read()

    with mock.patch.object(root_device.obj, "read", side_effect=read):
        completion = provider.read(target)
    assert completion.done()
    assert completion.exception(timeout=0) is None
    assert root_device.name in completion.result(timeout=0)
    assert threads == [calling_thread]


def test_classic_acquisition_returns_read_error_as_terminal_completion(root_device):
    """Native exceptions remain accessible through the shared completion contract."""
    provider = OphydDeviceProvider(None)
    error = RuntimeError("read failed")
    target = DeviceTarget(root_device)
    with mock.patch.object(root_device.obj, "read", side_effect=error):
        completion = provider.read(target)
    assert completion.done()
    assert completion.exception(timeout=0) is not None
    assert completion.exception(timeout=0) is error
    with pytest.raises(RuntimeError, match="read failed"):
        completion.result(timeout=0)


@pytest.mark.parametrize("section", list(RefreshSection))
def test_direct_reads_forward_memoryview_in_every_section(root_device, section):
    """Readback, configuration, and limits retain values accepted by Redis serialization."""
    backing = bytearray(b"abc")
    view = memoryview(backing)
    reading = {"s": {"value": view, "timestamp": 1}}
    obj = SimpleNamespace(
        read=lambda: reading,
        read_configuration=lambda: reading,
        low_limit_travel=SimpleNamespace(get=lambda: view),
        high_limit_travel=SimpleNamespace(get=lambda: 10),
    )
    device = DSDevice(name=root_device.name, obj=obj, config=None)
    provider = OphydDeviceProvider(None)
    if section is RefreshSection.READBACK:
        readings = wait_for_completion(provider.read(DeviceTarget(device)))
    elif section is RefreshSection.CONFIGURATION:
        readings = wait_for_completion(provider.read_configuration(DeviceTarget(device)))
    else:
        readings = provider.get_device_limits(device)
    value = (readings["low"] if section is RefreshSection.LIMITS else readings["s"])["value"]
    assert value is view


def test_limits_return_none_only_when_unsupported(root_device):
    provider = OphydDeviceProvider(None)
    assert provider.get_device_limits(root_device) is None

    error = TimeoutError("limit read failed")
    low = SimpleNamespace(get=mock.Mock(side_effect=error))
    high = SimpleNamespace(get=mock.Mock())
    obj = SimpleNamespace(low_limit_travel=low, high_limit_travel=high)
    device = DSDevice(name="motor", obj=obj, config=None)

    with pytest.raises(TimeoutError) as exc:
        provider.get_device_limits(device)
    assert exc.value is error
    high.get.assert_not_called()


@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_initial_buffer_reads_and_queues_in_classic_order(dm_with_devices):
    """Seed readback, configuration and limits in one ordered Redis pipeline."""
    calls = mock.Mock()
    calls.native.read.return_value = {}
    calls.native.read_configuration.return_value = {}
    calls.native.low_limit_travel.get.return_value = -1
    calls.native.high_limit_travel.get.return_value = 1
    connector = calls.connector
    pipe = connector.pipeline.return_value
    device = DSDevice(name="motor", obj=calls.native, config=None, parent=dm_with_devices)

    device.initialize_device_buffer(connector)

    assert calls.mock_calls == [
        mock.call.native.read(),
        mock.call.native.low_limit_travel.get(),
        mock.call.native.high_limit_travel.get(),
        mock.call.connector.pipeline(),
        mock.call.connector.set_and_publish(
            MessageEndpoints.device_readback("motor"), mock.ANY, pipe=pipe
        ),
        mock.call.connector.set_and_publish(
            topic=MessageEndpoints.device_read("motor"), msg=mock.ANY, pipe=pipe
        ),
        mock.call.native.read_configuration(),
        mock.call.connector.set_and_publish(
            MessageEndpoints.device_read_configuration("motor"), mock.ANY, pipe=pipe
        ),
        mock.call.connector.set_and_publish(
            MessageEndpoints.device_limits("motor"), mock.ANY, pipe=pipe
        ),
        mock.call.connector.pipeline().execute(),
    ]
    assert device.initialized
