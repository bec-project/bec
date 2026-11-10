"""Exercise the internal provider contracts without migrating live instruction paths."""

from __future__ import annotations

import gc
import weakref
from concurrent.futures import CancelledError, Future, TimeoutError
from threading import Event, Thread, get_ident
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
from ophyd import Component as Cpt
from ophyd import Device, Kind, Signal, StatusBase
from ophyd_devices import BECDeviceBase

from bec_lib import messages
from bec_lib.bec_errors import DeviceConfigError
from bec_server.device_server.device_providers.device_interfaces import (
    DeviceKind,
    DeviceMessageEvent,
    DeviceTarget,
    MessageCategory,
    wait_for_completion,
)
from bec_server.device_server.device_providers.device_provider_manager import DeviceProviderManager
from bec_server.device_server.device_providers.ophyd.ophyd_completion import (
    OphydCompletion,
    to_instruction_execution,
)
from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
    OphydDeviceProvider,
)
from bec_server.device_server.devices.devicemanager import DeviceManagerDS
from bec_server.device_server.devices.ds_device import DSDevice
from bec_server.device_server.instruction_execution import InstructionExecution


class _NestedDevice(Device):
    """Soft nested targets for kind mapping and capability discovery."""

    reading = Cpt(Signal, value=1, kind=Kind.hinted)
    configuration = Cpt(Signal, value=2, kind=Kind.config)
    ignored = Cpt(Signal, value=3, kind=Kind.omitted)
    low_limit_travel = Cpt(Signal, value=-1, kind=Kind.omitted)
    high_limit_travel = Cpt(Signal, value=1, kind=Kind.omitted)


@pytest.fixture
def root_device():
    """Supply a standalone wrapper without service initialization or background workers."""
    obj = _NestedDevice(name="contract_root")
    try:
        yield DSDevice(name=obj.name, obj=obj, config=None)
    finally:
        obj.destroy()


@pytest.mark.parametrize(
    "native, expected",
    [
        (Kind.omitted, DeviceKind.OMITTED),
        (Kind.normal, DeviceKind.READOUT),
        (Kind.config, DeviceKind.CONFIGURATION),
        (Kind.hinted, DeviceKind.READOUT | DeviceKind.HINTED),
        (Kind.normal | Kind.config, DeviceKind.READOUT | DeviceKind.CONFIGURATION),
        (
            Kind.hinted | Kind.config,
            DeviceKind.READOUT | DeviceKind.CONFIGURATION | DeviceKind.HINTED,
        ),
        (Kind.hinted ^ Kind.normal, DeviceKind.HINTED),
        ((Kind.hinted ^ Kind.normal) | Kind.config, DeviceKind.HINTED | DeviceKind.CONFIGURATION),
    ],
)
def test_ophyd_kind_mapping_is_explicit_and_independent(root_device, native, expected):
    """Native flags map to BEC roles without making BEC kind a native enum alias."""
    root_device.obj.reading.kind = native
    mapped = OphydDeviceProvider(None).get_kind(DeviceTarget(root_device, "reading"))
    assert mapped == expected
    assert isinstance(mapped, DeviceKind)
    assert not isinstance(mapped, Kind)
    assert mapped != native


@pytest.mark.parametrize(
    "path, expected",
    [
        ("reading", DeviceKind.READOUT | DeviceKind.HINTED),
        ("configuration", DeviceKind.CONFIGURATION),
        ("ignored", DeviceKind.OMITTED),
    ],
)
def test_nested_target_kind_uses_root_wrapper_identity(root_device, path, expected):
    """Nested paths retain the registered wrapper and resolve the requested backend target."""
    target = DeviceTarget(root_device, path)
    assert target.device is root_device
    assert target.name == f"{root_device.name}.{path}"
    assert OphydDeviceProvider(None).get_kind(target) == expected


def test_unknown_native_kind_is_not_silently_treated_as_omitted(root_device):
    """An unmapped native flag must not become a cache-exclusion decision."""
    root_device.obj.kind = Kind(8)
    with pytest.raises(ValueError, match="Unsupported Ophyd kind"):
        OphydDeviceProvider(None).get_kind(DeviceTarget(root_device))


@pytest.mark.parametrize("kind", [Kind.normal, Kind.config, Kind.omitted])
def test_standalone_signal_has_no_separate_configuration(kind):
    """Data role does not invent configuration capabilities or forbid direct reads."""
    signal = Signal(name="standalone", value=3, kind=kind)
    try:
        device = DSDevice(name=signal.name, obj=signal, config=None)
        provider = OphydDeviceProvider(None)
        target = DeviceTarget(device)
        assert provider.read(target).result()[signal.name]["value"] == 3
        assert provider.read_configuration(target, separate_only=True).result() is None
        assert provider.read_configuration(target).result() == signal.read_configuration()
        if kind == Kind.omitted:
            assert provider.get_kind(DeviceTarget(device)) == DeviceKind.OMITTED
    finally:
        signal.destroy()


def test_minimal_bec_device_has_readout_kind():
    """The existing minimal device family also participates in the shared interfaces."""
    obj = BECDeviceBase(name="minimal")
    try:
        device = DSDevice(name=obj.name, obj=obj, config=None)
        provider = OphydDeviceProvider(None)
        assert provider.get_kind(DeviceTarget(device)) == DeviceKind.READOUT
    finally:
        obj.destroy()


@pytest.mark.parametrize("already_done", [False, True])
@pytest.mark.parametrize("fails", [False, True])
def test_ophyd_completion_forwards_result_without_mutating_status(already_done, fails):
    """Pending and already-finished statuses expose identical backend-neutral completions."""
    status = StatusBase()
    error = RuntimeError("operation failed")

    def finish():
        if fails:
            status.set_exception(error)
        else:
            status.set_finished()

    if already_done:
        finish()
    completion = OphydCompletion(status)
    callbacks = []
    completion.add_done_callback(callbacks.append)
    if not already_done:
        assert completion.done() is False
        assert callbacks == []
        with pytest.raises(TimeoutError):
            completion.result(timeout=0)
        with pytest.raises(TimeoutError):
            completion.exception(timeout=0)
        finish()
    assert callbacks == [completion]
    assert completion.done() is True
    assert completion.cancelled() is False
    assert completion.exception(timeout=0) is (error if fails else None)
    if fails:
        with pytest.raises(RuntimeError) as exc:
            completion.result(timeout=0)
        assert exc.value is error
    else:
        assert completion.result(timeout=0) is None
    for attribute in ("instruction", "obj_ref", "sub_id"):
        assert not hasattr(status, attribute)


def test_raw_future_satisfies_completion_contract_across_threads():
    """Completion bridges are independent of an event loop and preserve the produced value."""
    readings = {"value": {"value": 1, "timestamp": 10}}
    future = Future()
    completion = future
    callbacks = []
    threads = []
    completion.add_done_callback(
        lambda handle: callbacks.append((handle.result(timeout=0), get_ident()))
    )

    def produce():
        threads.append(get_ident())
        future.set_result(readings)

    worker = Thread(target=produce)
    worker.start()
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert wait_for_completion(completion) is readings
    assert callbacks == [(readings, threads[0])]
    assert completion.exception(timeout=0) is None
    late_callbacks = []
    completion.add_done_callback(late_callbacks.append)
    assert late_callbacks == [completion]


def test_raw_future_cancellation_is_a_failed_instruction_execution(root_device):
    """Cancelled execution is a terminal failure without implying a hardware stop."""
    future = Future()
    completion = future
    callbacks = []
    completion.add_done_callback(callbacks.append)
    assert future.cancel() is True
    assert callbacks == [completion]
    assert completion.done() is True
    assert completion.cancelled() is True
    assert InstructionExecution(completion, DeviceTarget(root_device)).success is False
    assert isinstance(
        InstructionExecution(completion, DeviceTarget(root_device)).exception(), CancelledError
    )
    with pytest.raises(CancelledError):
        completion.result(timeout=0)


def test_payload_event_keeps_wire_identity_separate_from_backend_path(root_device):
    """Payload event data is independent of cache refresh kind and Redis destination selection."""
    message = messages.DevicePreviewMessage(
        device=root_device.name, signal="wire_preview", data=np.zeros(2)
    )
    target = DeviceTarget(root_device, "nested.preview")
    event = DeviceMessageEvent(target, MessageCategory.PREVIEW, "wire_preview", message)
    assert event.target.path == "nested.preview"
    assert event.signal_name == "wire_preview"
    assert event.message is message
    assert event.signal_info is None


@pytest.mark.parametrize("native", [Kind.normal, Kind.hinted])
def test_ophyd_device_kind_retains_native_configuration_inclusion(root_device, native):
    """Device.kind adds configuration inclusion; mapping must preserve the effective roles."""
    root_device.obj.kind = native
    expected = DeviceKind.READOUT | DeviceKind.CONFIGURATION
    if native == Kind.hinted:
        expected |= DeviceKind.HINTED
    assert OphydDeviceProvider(None).get_kind(DeviceTarget(root_device)) == expected


def test_provider_manager_routes_introspection_through_root_provider(root_device):
    """Shared callers can inspect nested roles without raw access."""
    manager = DeviceProviderManager(None)
    assert manager.get_kind(DeviceTarget(root_device, "configuration")) == DeviceKind.CONFIGURATION


def test_ophyd_completion_owns_native_status_until_completion():
    """The returned adapter must retain the native object supplied by the backend."""
    native = StatusBase()
    reference = weakref.ref(native)
    completion = OphydCompletion(native)
    del native
    gc.collect()
    assert reference() is not None
    reference().set_finished()
    assert completion.done() is True
    assert completion.exception(timeout=0) is None


@pytest.mark.parametrize("already_done", [False, True])
def test_ophyd_status_result_remains_pending_variant_even_if_done(already_done, root_device):
    """Result type records status provenance, independently of the current done flag."""
    status = StatusBase()
    if already_done:
        status.set_finished()
    result = to_instruction_execution(status, DeviceTarget(root_device))
    assert isinstance(result, InstructionExecution)
    if not already_done:
        assert result.completion.done() is False
        status.set_finished()
    assert result.completion.done() is True
    assert result.completion.result() is None


def test_target_lookup_distinguishes_replacement_wrappers(root_device):
    """Scheduler keys coalesce one target but never merge a same-name replacement."""
    replacement_obj = _NestedDevice(name=root_device.name)
    try:
        replacement = DSDevice(name=root_device.name, obj=replacement_obj, config=None)
        original = DeviceTarget(root_device, "reading")
        repeated = DeviceTarget(root_device, "reading")
        replaced = DeviceTarget(replacement, "reading")
        different_path = DeviceTarget(root_device, "configuration")
        assert original == repeated
        assert hash(original) == hash(repeated)
        assert original != replaced
        assert original != different_path
        lookups = {original: "original", replaced: "replacement", different_path: "configuration"}
        assert len(lookups) == 3
        assert lookups[repeated] == "original"
        assert lookups[replaced] == "replacement"
        original_name = root_device.name
        try:
            root_device.name = "renamed_wrapper"
            assert lookups[original] == "original"
        finally:
            root_device.name = original_name
    finally:
        replacement_obj.destroy()


class _UnpreparedSignal(Signal):
    """Detect accidental construction or connection during introspection."""

    def __init__(self, *args, **kwargs):
        raise AssertionError("Introspection must not construct a lazy signal")


class _LazyDevice(Device):
    """Expose cold nested components and limits."""

    reading = Cpt(_UnpreparedSignal, lazy=True, kind=Kind.hinted)
    low_limit_travel = Cpt(_UnpreparedSignal, lazy=True)
    high_limit_travel = Cpt(_UnpreparedSignal, lazy=True)


class _LazyRoot(Device):
    """Keep the intermediate lazy device cold as well."""

    child = Cpt(_LazyDevice, lazy=True)


@pytest.mark.parametrize("nested", [False, True])
def test_kind_introspection_requires_prepared_lazy_target(nested):
    """Exact effective kind cannot be inferred by constructing cold hardware."""
    obj = (_LazyRoot if nested else _LazyDevice)(name="cold")
    try:
        wrapper = DSDevice(name=obj.name, obj=obj, config=None)
        path = "child.reading" if nested else "reading"
        with pytest.raises(RuntimeError, match="requires runtime preparation"):
            OphydDeviceProvider(None).get_kind(DeviceTarget(wrapper, path))
        assert obj._signals == {}
        assert obj.lazy_wait_for_connection is True
    finally:
        obj.destroy()


def test_kind_introspection_uses_prepared_lazy_instance_kind():
    """Runtime kind overrides take precedence once a lazy target is prepared."""

    class PreparedDevice(Device):
        reading = Cpt(Signal, lazy=True, kind=Kind.hinted)

    obj = PreparedDevice(name="prepared")
    try:
        obj.reading.kind = Kind.config
        wrapper = DSDevice(name=obj.name, obj=obj, config=None)
        assert OphydDeviceProvider(None).get_kind(DeviceTarget(wrapper, "reading")) == (
            DeviceKind.CONFIGURATION
        )
    finally:
        obj.destroy()


def test_ophyd_callbacks_observe_terminal_state_during_failure_cleanup():
    """Late callbacks use the native terminal state even before queued callbacks run."""
    entered, released, callbacks_finished = Event(), Event(), Event()
    failure = RuntimeError("original hardware failure")

    class PausingFailure(StatusBase):
        def _handle_failure(self):
            entered.set()
            assert released.wait(5)

    native = PausingFailure()
    completion = OphydCompletion(native)
    observed = []

    def observe(handle):
        observed.append(
            (handle.done(), handle.exception(timeout=0) is None, handle.exception(timeout=0))
        )
        with pytest.raises(RuntimeError) as exc:
            handle.result(timeout=0)
        assert exc.value is failure

    completion.add_done_callback(lambda handle: (observe(handle), callbacks_finished.set()))
    worker = Thread(target=native.set_exception, args=(failure,))
    worker.start()
    try:
        assert entered.wait(2)
        assert native.done
        completion.add_done_callback(observe)
        completion.add_done_callback(observe)
        assert observed == [(True, False, failure)] * 2
        assert not callbacks_finished.is_set()

        def callback_error(_handle):
            raise ValueError("inline callback failure")

        with pytest.raises(ValueError, match="inline callback failure"):
            completion.add_done_callback(callback_error)
    finally:
        released.set()
        worker.join(timeout=2)
        assert not worker.is_alive()
    assert callbacks_finished.wait(2)
    assert observed == [(True, False, failure)] * 3


def test_instruction_execution_keeps_one_identity_and_gates_bec_completion(root_device):
    """Binding adds context to the original execution; backend done alone cannot finish BEC work."""
    future = Future()
    target = DeviceTarget(root_device)
    execution = InstructionExecution(future, target, status_type="backend")
    assert execution.target is target
    instruction = messages.DeviceInstructionMessage(
        device=root_device.name, action="trigger", parameter={}, metadata={"device_instr_id": "one"}
    )
    with pytest.raises(RuntimeError, match="not bound"):
        _ = execution.instruction
    assert execution.bind(instruction) is execution
    assert execution.completion is future
    assert execution.target is target
    assert execution.instruction is instruction
    assert execution.target.name == target.name
    with pytest.raises(RuntimeError, match="already bound"):
        execution.bind(instruction)
    callbacks = []
    execution.add_callback(callbacks.append)
    future.set_result(None)
    assert callbacks == [execution]
    assert future.done()
    assert execution.success
    assert not execution.done
    execution.mark_callback_processed()
    assert execution.done
    assert not {"instruction", "obj_ref", "sub_id"}.intersection(future.__dict__)


def test_wait_for_completion_retains_pending_native_callback_delivery_boundary():
    """A pending coordinator wait must not escape while native failure cleanup holds callbacks."""
    entered, release, registered, returned = (Event() for _ in range(4))
    error = RuntimeError("native failure")

    class PausingFailure(StatusBase):
        def _handle_failure(self):
            entered.set()
            assert release.wait(5)

    native = PausingFailure()
    completion = OphydCompletion(native)
    original_add = completion.add_done_callback
    observed = []

    def add(callback):
        original_add(callback)
        registered.set()

    def wait():
        try:
            wait_for_completion(completion)
        except RuntimeError as exc:
            observed.append(exc)
        finally:
            returned.set()

    waiter = Thread(target=wait)
    failing = Thread(target=native.set_exception, args=(error,))
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(completion, "add_done_callback", add)
        waiter.start()
        try:
            assert registered.wait(2)
            failing.start()
            assert entered.wait(2)
            assert completion.done()
            assert completion.exception(timeout=0) is error
            assert not returned.is_set()
        finally:
            release.set()
            waiter.join(timeout=2)
            if failing.ident is not None:
                failing.join(timeout=2)
    assert not waiter.is_alive()
    assert not failing.is_alive()
    assert returned.is_set()
    assert observed == [error]


@pytest.mark.parametrize("auto_monitor", [None, False, True])
def test_ophyd_configuration_application_retains_native_order(root_device, auto_monitor):
    """Native mutation stays in the provider; BEC receives only the applied notification."""
    obj = root_device.obj
    signal = obj.configuration
    if auto_monitor is not None:
        signal._auto_monitor = auto_monitor
    events = []
    obj.scalar = 0
    obj.apply_value = lambda value: events.append(("call", value))
    manager = SimpleNamespace(
        on_configuration_applied=lambda device, *, refresh_buffer: events.append(
            ("applied", device, refresh_buffer)
        )
    )
    provider = OphydDeviceProvider(manager)

    def setter(signal):
        def set_value(value):
            events.append(("set", signal.dotted_name, value))
            signal.put(value)
            status = mock.Mock()
            status.wait.side_effect = lambda *, timeout: events.append(
                ("wait", signal.dotted_name, timeout)
            )
            return status

        return set_value

    config = {
        "limits": [-2, 5],
        "labels": ["label"],
        "configuration": 6,
        "apply_value": 7,
        "scalar": 8,
    }
    with (
        mock.patch.object(obj.low_limit_travel, "set", side_effect=setter(obj.low_limit_travel)),
        mock.patch.object(obj.high_limit_travel, "set", side_effect=setter(obj.high_limit_travel)),
        mock.patch.object(signal, "set", side_effect=setter(signal)),
    ):
        provider.update_device_config(root_device, config)
    assert events == [
        ("set", "low_limit_travel", -2),
        ("set", "high_limit_travel", 5),
        ("wait", "low_limit_travel", 2),
        ("wait", "high_limit_travel", 2),
        ("set", "configuration", 6),
        ("wait", "configuration", 2),
        ("call", 7),
        ("applied", root_device, auto_monitor is None),
    ]
    assert signal.get() == 6
    assert obj.scalar == 8
    assert obj._ophyd_labels_ == {"label"}
    assert config["limits"] == [-2, 5]
    assert config["labels"] == ["label"]


@pytest.mark.parametrize("failure", [False, True])
def test_custom_configuration_hook_owns_its_follow_up(root_device, failure):
    """Custom hooks retain their early return, without standard buffer or metrics work."""
    callback = mock.Mock()
    manager = SimpleNamespace(on_configuration_applied=callback)
    provider = OphydDeviceProvider(manager)
    config = {"custom": 3}
    hook = mock.Mock(side_effect=RuntimeError("custom failed") if failure else None)
    with mock.patch.object(root_device.obj, "_update_device_config", hook, create=True):
        if failure:
            with pytest.raises(RuntimeError, match="custom failed"):
                provider.update_device_config(root_device, config)
        else:
            provider.update_device_config(root_device, config)
    hook.assert_called_once_with(config)
    assert hook.call_args.args[0] is config
    callback.assert_not_called()


@pytest.mark.parametrize("failure", ["unknown", "wait"])
def test_failed_configuration_does_not_notify_bec(root_device, failure):
    """Validation and native wait failures propagate before standard BEC follow-up."""
    callback = mock.Mock()
    provider = OphydDeviceProvider(SimpleNamespace(on_configuration_applied=callback))
    if failure == "unknown":
        with pytest.raises(DeviceConfigError, match="Unknown config parameter"):
            provider.update_device_config(root_device, {"unknown": 1})
    else:
        status = mock.Mock()
        status.wait.side_effect = TimeoutError("configuration timed out")
        with mock.patch.object(root_device.obj.configuration, "set", return_value=status):
            with pytest.raises(TimeoutError, match="configuration timed out"):
                provider.update_device_config(root_device, {"configuration": 3})
        status.wait.assert_called_once_with(timeout=2)
    callback.assert_not_called()


@pytest.mark.parametrize("refresh_buffer", [False, True])
def test_configuration_follow_up_stays_in_bec(root_device, refresh_buffer):
    """BEC refreshes synchronously before metrics, preserving the original ordering."""
    connector = mock.Mock()
    manager = SimpleNamespace(connector=connector, devices={root_device.name: root_device})
    events = mock.Mock()
    with mock.patch.object(root_device, "initialize_device_buffer") as refresh:
        events.attach_mock(refresh, "refresh")
        events.attach_mock(connector.publish_metrics, "metrics")
        DeviceManagerDS.on_configuration_applied(
            manager, root_device, refresh_buffer=refresh_buffer
        )
        expected = [mock.call.refresh(connector)] if refresh_buffer else []
        assert events.mock_calls == [
            *expected,
            mock.call.metrics("device_server", {"num_devices": 1}),
        ]
