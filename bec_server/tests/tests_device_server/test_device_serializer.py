from unittest import mock

import pytest
from ophyd import Component as Cpt
from ophyd import Device, EpicsSignal, EpicsSignalRO, Kind, Signal
from ophyd_devices import ComputedSignal, PSIDeviceBase
from ophyd_devices.sim.sim_signals import ReadOnlySignal
from ophyd_devices.tests.utils import patched_device
from ophyd_devices.utils.bec_signals import ProgressSignal

from bec_lib.bec_errors import DeviceConfigError
from bec_server.device_server.devices.device_serializer import get_device_info

# pylint: disable=protected-access


class LazySubDevice(Device):

    lazy_signal = Cpt(EpicsSignal, "sub_signal", lazy=True)


class LazySubDeviceWithNoLazyLoading(LazySubDevice):

    lazy_wait_for_connection = False

    lazy_signal = Cpt(EpicsSignal, "sub_signal", lazy=True)


class LazyDevice(Device):

    lazy_signal = Cpt(EpicsSignal, "signal", lazy=True)
    lazy_sub_device = Cpt(LazySubDevice, "test_device,", lazy=True)
    lazy_sub_device_no_lazy = Cpt(LazySubDeviceWithNoLazyLoading, "test_device_lazy", kind="normal")


class MyDevice(Device):
    custom = Cpt(Signal, value=0)


class EpicsDevice(Device):
    custom = Cpt(EpicsSignal, ":CUSTOM", auto_monitor=True)


class DummyDeviceWithConflictingSignalNames(Device):
    # This device has a signal with the same name as a protected method
    # in the Device class
    enabled = Cpt(Signal, value=0)


class DummyDeviceWithConflictingName(Device):
    """This device will be assigned a protected name"""


class DummyDeviceWithConflictingUserAccess(Device):
    """This device will be assigned a protected name"""

    USER_ACCESS = ["enabled"]

    def enabled(self):
        pass


class DummyDeviceWithConflictingUserAccessProperty(Device):
    """This device will be assigned a protected name"""

    USER_ACCESS = ["enabled"]

    @property
    def enabled(self):
        pass


class DummyDeviceWithConflictingSubDevice(Device):
    """This device will be assigned a protected name"""

    sub_device = Cpt(DummyDeviceWithConflictingSignalNames)


class DummyDeviceWithConflictingDuplicateSignalNames(Device):
    """This device will be assigned a protected name"""

    signal = Cpt(MyDevice, "signal", lazy=True)
    signal_custom = Cpt(MyDevice, "custom", lazy=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Imitate Positioner behavior with conflicting signal names
        self.signal_custom.custom.name = self.signal_custom.name


class DummyDeviceWithAliasedSignalNames(Device):
    """Expose the same signal object via two component names."""

    signal_alias = signal = Cpt(Signal, value=1)


class DummyReadOnlySignal(Signal):
    @property
    def write_access(self):
        return False


class DummyWritableSignal(Signal):
    @property
    def write_access(self):
        return True


class DummyExplicitOwnershipDevice(Device):
    ownership_mode = "free"


class DummyInvalidOwnershipDevice(Device):
    ownership_mode = "not-a-mode"


class DummyPSIDevice(PSIDeviceBase):
    pass


@pytest.mark.parametrize(
    "obj",
    [
        DummyDeviceWithConflictingSignalNames(name="test"),
        DummyDeviceWithConflictingSignalNames(name="enabled"),
        DummyDeviceWithConflictingSubDevice(name="test"),
        DummyDeviceWithConflictingUserAccess(name="test"),
        DummyDeviceWithConflictingUserAccessProperty(name="test"),
    ],
)
def test_get_device_info(obj):
    with pytest.raises(DeviceConfigError):
        _ = get_device_info(obj)


def test_get_device_info_without_connection():
    device = MyDevice(name="test")
    with (
        mock.patch.object(device, "describe", side_effect=TimeoutError),
        mock.patch.object(device, "describe_configuration", side_effect=TimeoutError),
        mock.patch.object(device.custom, "describe", side_effect=TimeoutError),
        mock.patch.object(device.custom, "describe_configuration", side_effect=TimeoutError),
        mock.patch.object(device, "walk_components", side_effect=TimeoutError),
    ):
        _ = get_device_info(device, connect=False)


def test_get_device_info_lazy_signal():
    device = LazyDevice(name="test")
    _ = get_device_info(device, connect=False)
    assert device.lazy_sub_device_no_lazy.lazy_wait_for_connection is False
    assert device.lazy_sub_device.lazy_wait_for_connection is True


def test_get_device_info_USER_ACCESS():
    device = DummyDeviceWithConflictingDuplicateSignalNames(name="test")
    _ = get_device_info(device, connect=False)
    with pytest.raises(DeviceConfigError):
        _ = get_device_info(device, connect=True)


def test_get_device_info_allows_aliased_signal_names():
    device = DummyDeviceWithAliasedSignalNames(name="test")

    info = get_device_info(device, connect=True)

    assert info["signals"]["signal"]["obj_name"] == "test_signal"
    assert info["signals"]["signal_alias"]["obj_name"] == "test_signal"


@pytest.mark.parametrize("auto_monitor", [None, False, True])
@pytest.mark.parametrize("signal_class", [EpicsSignal, EpicsSignalRO])
@pytest.mark.parametrize("kind", [Kind.normal, Kind.hinted, Kind.config, Kind.omitted, 3])
def test_get_device_info_auto_publish_for_auto_monitors(auto_monitor, signal_class, kind):
    """Only monitored EPICS signals in supported kinds advertise automatic publication."""

    class MonitoredDevice(Device):
        custom = Cpt(signal_class, ":CUSTOM", auto_monitor=auto_monitor, kind=kind)

    with patched_device(MonitoredDevice, name="test", prefix="TEST") as device:
        try:
            info = get_device_info(device)
        finally:
            device.destroy()

    assert info["signals"]["custom"]["auto_publish"] is (
        auto_monitor is True and kind in (Kind.normal, Kind.hinted, Kind.config)
    )


@pytest.mark.parametrize("auto_monitor", [False, True])
@pytest.mark.parametrize("event_type", ["readback", "value", "done_moving"])
def test_get_device_info_root_events_do_not_auto_publish_non_epics_signals(
    auto_monitor, event_type
):
    """Neither event declarations nor a monitor flag qualify a non-EPICS signal."""

    class EventDevice(MyDevice):
        SUB_AUTO_PUBLISH = event_type

    device = EventDevice(name="test")
    device.custom._auto_monitor = auto_monitor

    info = get_device_info(device)

    assert info["signals"]["custom"]["auto_publish"] is False


@pytest.mark.parametrize("parent_kind", [Kind.normal, Kind.omitted])
def test_get_device_info_auto_publish_respects_nested_read_attrs(parent_kind):
    """Signals excluded by their parent are absent from the root's Redis cache."""

    class NestedDevice(Device):
        sub = Cpt(EpicsDevice)

    with patched_device(NestedDevice, name="test", prefix="TEST") as device:
        try:
            device.sub.kind = parent_kind
            info = get_device_info(device)
        finally:
            device.destroy()

    expected = parent_kind == Kind.normal
    assert info["signals"]["sub.custom"]["auto_publish"] is expected
    assert info["sub_devices"][0]["signals"]["custom"]["auto_publish"] is expected


@pytest.mark.parametrize("num_subdevices", [10, 100])
def test_get_device_info_describes_root_once(num_subdevices):
    """Wide device trees must not repeat root-wide descriptions for each child."""

    class Leaf(Device):
        reading = Cpt(Signal, value=0)
        setting = Cpt(Signal, value=0, kind="config")

    root_class = type(
        "Root", (Device,), {f"leaf_{index}": Cpt(Leaf) for index in range(num_subdevices)}
    )
    device = root_class(name="test")
    try:
        with (
            mock.patch.object(device, "describe", wraps=device.describe) as root_describe,
            mock.patch.object(
                device, "describe_configuration", wraps=device.describe_configuration
            ) as root_configuration,
            mock.patch.object(
                Signal, "describe", autospec=True, side_effect=Signal.describe
            ) as signal_describe,
        ):
            info = get_device_info(device)

        root_describe.assert_called_once_with()
        root_configuration.assert_called_once_with()
        assert signal_describe.call_count <= 8 * num_subdevices
        assert len(info["sub_devices"]) == num_subdevices
        assert len(info["signals"]) == 2 * num_subdevices
    finally:
        device.destroy()


def test_get_device_info_preserves_empty_root_and_subdevice_descriptions():
    """Empty root descriptions and per-device units survive description reuse."""

    class Leaf(Device):
        egu = "s"
        reading = Cpt(Signal, value=0)
        setting = Cpt(Signal, value=0, kind="config")

    class Root(Device):
        egu = "mm"
        sub = Cpt(Leaf, kind="omitted")

    device = Root(name="test")
    try:
        with (
            mock.patch.object(device, "describe", wraps=device.describe) as root_describe,
            mock.patch.object(
                device, "describe_configuration", wraps=device.describe_configuration
            ) as root_configuration,
        ):
            info = get_device_info(device)

        root_describe.assert_called_once_with()
        root_configuration.assert_called_once_with()
        assert info["describe"] == {}
        assert info["describe_configuration"] == {"egu": "mm"}
        sub_info = info["sub_devices"][0]
        assert sub_info["describe"] == device.sub.describe()
        assert sub_info["describe_configuration"] == device.sub.describe_configuration() | {
            "egu": "s"
        }
    finally:
        device.destroy()


@pytest.mark.parametrize("serialize_subdevice", [False, True])
def test_get_device_info_refreshes_root_descriptions_between_calls(serialize_subdevice):
    """Shared root descriptions must not outlive a single serialization request."""

    class NestedDevice(Device):
        sub = Cpt(EpicsDevice)

    with patched_device(NestedDevice, name="test", prefix="TEST") as device:
        try:
            target = device.sub if serialize_subdevice else device
            signal_name = "custom" if serialize_subdevice else "sub.custom"
            with (
                mock.patch.object(device, "describe", wraps=device.describe) as root_describe,
                mock.patch.object(
                    device, "describe_configuration", wraps=device.describe_configuration
                ) as root_configuration,
            ):
                before = get_device_info(target)
                device.sub.kind = Kind.omitted
                after = get_device_info(target)

            assert before["signals"][signal_name]["auto_publish"] is True
            assert after["signals"][signal_name]["auto_publish"] is False
            assert root_describe.call_count == 2
            assert root_configuration.call_count == 2
        finally:
            device.destroy()


def test_get_device_info_auto_publish_ignores_nested_device_events():
    """Only root device events are subscribed independently of auto monitors."""

    class EventDevice(MyDevice):
        SUB_READBACK = "readback"

    class NestedDevice(Device):
        sub = Cpt(EventDevice)

    device = NestedDevice(name="test")

    info = get_device_info(device)

    assert info["signals"]["sub.custom"]["auto_publish"] is False
    assert info["sub_devices"][0]["signals"]["custom"]["auto_publish"] is False


def test_get_device_info_bec_message_signals_do_not_use_readback_cache():
    """Dedicated message endpoints cannot serve ordinary cached signal reads."""

    class MessageDevice(Device):
        progress = Cpt(ProgressSignal, signals=["progress"])

    device = MessageDevice(name="test")
    device.progress._auto_monitor = True

    info = get_device_info(device)

    assert info["signals"]["progress"]["auto_publish"] is False


@pytest.mark.parametrize("connect", [False, True])
@pytest.mark.parametrize("signal_class", [Signal, ComputedSignal, ReadOnlySignal])
def test_get_device_info_non_epics_root_signals_are_not_auto_published(connect, signal_class):
    """Standalone non-EPICS signals retain RPC reads even with a monitor flag."""
    signal = signal_class(name="test")
    signal._auto_monitor = True

    try:
        info = get_device_info(signal, connect=connect)
    finally:
        signal.destroy()

    assert not info["signals"]
    assert "auto_publish" not in info


@pytest.mark.parametrize("connect", [False, True])
@pytest.mark.parametrize("auto_monitor", [None, False, True])
@pytest.mark.parametrize("signal_class", [EpicsSignal, EpicsSignalRO])
def test_get_device_info_root_epics_signal_is_not_auto_published(
    connect, auto_monitor, signal_class
):
    """Standalone EPICS signals are excluded from automatic client caching for now."""
    with patched_device(EpicsDevice, name="test", prefix="TEST") as device:
        signal = signal_class(
            "TEST:ROOT", name="root_signal", auto_monitor=auto_monitor, cl=device.custom.cl
        )
        try:
            info = get_device_info(signal, connect=connect)
        finally:
            signal.destroy()
            device.destroy()

    assert not info["signals"]
    assert "auto_publish" not in info


def test_get_device_info_marks_read_only_plain_signal_as_free():
    signal = DummyReadOnlySignal(name="test")

    info = get_device_info(signal, connect=False)

    assert info["ownership_mode"] == "free"


def test_get_device_info_marks_writable_plain_signal_as_claimable():
    signal = DummyWritableSignal(name="test")

    info = get_device_info(signal, connect=False)

    assert info["ownership_mode"] == "claimable"


def test_get_device_info_marks_psi_devices_as_pinned():
    device = DummyPSIDevice(name="test")

    info = get_device_info(device, connect=False)

    assert info["ownership_mode"] == "pinned"


def test_get_device_info_prefers_explicit_class_ownership_mode():
    device = DummyExplicitOwnershipDevice(name="test")

    info = get_device_info(device, connect=False)

    assert info["ownership_mode"] == "free"


def test_get_device_info_raises_device_config_error_for_invalid_explicit_ownership_mode():
    device = DummyInvalidOwnershipDevice(name="test")

    with pytest.raises(
        DeviceConfigError,
        match="Invalid ownership_mode 'not-a-mode' configured on DummyInvalidOwnershipDevice",
    ):
        get_device_info(device, connect=False)
