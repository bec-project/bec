"""Regression coverage for callback capture, read isolation, and retained events."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Generator
from contextlib import ExitStack, nullcontext
from functools import partial
from types import SimpleNamespace
from typing import Any, Literal, NoReturn, cast, overload
from unittest import mock

import numpy as np
import ophyd
import pytest
from ophyd.device import OrderedDictType
from ophyd_devices.sim.sim_monitor import SimMonitor
from ophyd_devices.sim.sim_signals import ReadOnlySignal
from ophyd_devices.utils.dynamic_pseudo import ComputedSignal
from redis.client import Pipeline as RedisPipeline
from redis.exceptions import ResponseError, WatchError

from bec_lib import messages
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_server.device_server.devices.devicemanager import DSDevice
from bec_server.device_server.devices.event_dispatcher import (
    DeviceEventDispatcher,
    Domain,
    Readings,
    _Publication,
)

# pylint: disable=protected-access


class Pair(ophyd.Device):
    """Provide independent readback and configuration signals."""

    first = ophyd.Component(ophyd.Signal, value=0, kind=ophyd.Kind.normal)
    second = ophyd.Component(ophyd.Signal, value=1, kind=ophyd.Kind.normal)
    setting = ophyd.Component(ophyd.Signal, value=2, kind=ophyd.Kind.config)


OphydReadings = OrderedDictType
EventMessage = messages.DeviceMessage | messages.DeviceStatusMessage
Operation = Literal["set", "publish"]
Command = tuple[Operation, EndpointInfo, EventMessage]


class Connector:
    """Record publication commands and inject controlled transport failures."""

    def __init__(self) -> None:
        """Initialize the test double."""
        self.writes: list[tuple[EndpointInfo, EventMessage]] = []
        self.executions: list[list[Command]] = []
        self.before_write: Callable[[EndpointInfo, EventMessage], None] = (
            lambda _topic, _message: None
        )
        self.before_command: Callable[[Operation, EndpointInfo, EventMessage], None] = (
            lambda _operation, _topic, _message: None
        )
        self.written = threading.Event()
        self.versions: dict[str, int] = {}

    def pipeline(self) -> Pipeline:
        """Create a pipeline with the requested failure behavior.

        Returns:
            Pipeline: Empty pipeline bound to this recording connector.
        """
        return Pipeline(self)

    def set_and_publish(self, topic: EndpointInfo, message: EventMessage, pipe: Pipeline) -> None:
        """Queue one SET and one PUBLISH for the same snapshot.

        Args:
            topic (EndpointInfo): Destination endpoint.
            message (EventMessage): Snapshot message to publish.
            pipe (Pipeline): Pipeline receiving staged commands.
        """
        pipe.command_stack.extend([("set", topic, message), ("publish", topic, message)])

    def set(self, topic: EndpointInfo, message: EventMessage, pipe: Pipeline | None = None) -> None:
        """Queue or record a SET operation.

        Args:
            topic (EndpointInfo): Destination endpoint.
            message (EventMessage): Snapshot message to publish.
            pipe (Pipeline | None): Pipeline receiving staged commands.
        """
        if pipe is not None:
            pipe.command_stack.append(("set", topic, message))
            return
        self.before_write(topic, message)
        self.writes.append((topic, message))
        self.versions[topic.endpoint] = self.versions.get(topic.endpoint, 0) + 1
        self.written.set()

    @overload
    def reading(
        self, name: str, domain: Literal["readback", "configuration"] = "readback"
    ) -> messages.DeviceMessage: ...

    @overload
    def reading(self, name: str, domain: Literal["status"]) -> messages.DeviceStatusMessage: ...

    def reading(
        self, name: str, domain: Literal["readback", "configuration", "status"] = "readback"
    ) -> EventMessage:
        """Return the latest recorded message for a device domain.

        Args:
            name (str): Device name used in Redis endpoints.
            domain (Literal['readback', 'configuration', 'status']): Snapshot domain to retrieve.

        Returns:
            EventMessage: Latest snapshot written to the requested endpoint.
        """
        endpoint = {
            "readback": MessageEndpoints.device_readback,
            "configuration": MessageEndpoints.device_read_configuration,
            "status": MessageEndpoints.device_status,
        }[domain](name)
        return next(message for topic, message in reversed(self.writes) if topic == endpoint)


class Pipeline:
    """Stage connector commands and expose Redis-compatible command outcomes."""

    def __init__(self, connector: Connector) -> None:
        """Initialize the test double.

        Args:
            connector (Connector): Connector that executes staged commands.
        """
        self.connector = connector
        self.command_stack: list[Command] = []
        self.watched: dict[str, int] = {}

    def watch(self, *names: str) -> None:
        """Remember current endpoint revisions for conflict injection.

        Args:
            *names (str): Endpoint keys to watch.
        """
        self.watched = {name: self.connector.versions.get(name, 0) for name in names}

    def multi(self) -> None:
        """Keep commands queued until execution."""

    def reset(self) -> None:
        """Release watched endpoints and discard abandoned commands."""
        self.command_stack.clear()
        self.watched.clear()

    def execute(self, raise_on_error: bool = True) -> list[bool | int | ResponseError]:
        """Run queued commands and retain their individual outcomes.

        Args:
            raise_on_error (bool): Whether to raise command errors instead of returning them.

        Returns:
            list[bool | int | ResponseError]: One success value or error per staged command.
        """
        commands, self.command_stack = self.command_stack, []
        if any(
            self.connector.versions.get(name, 0) != version
            for name, version in self.watched.items()
        ):
            raise WatchError("Concurrent explicit write")
        self.connector.executions.append(commands)
        results: list[bool | int | ResponseError] = []
        for operation, topic, message in commands:
            try:
                self.connector.before_command(operation, topic, message)
                if operation == "set":
                    self.connector.set(topic, message)
                results.append(True if operation == "set" else 0)
            except ResponseError as exc:
                if raise_on_error:
                    raise
                results.append(exc)
        return results


class DeviceDouble(SimpleNamespace):
    """Expose the device-manager fields needed by dispatcher tests."""

    obj: Pair
    name: str
    metadata: dict[str, Any]
    enabled: bool


class Uncopyable:
    """Reject ownership copies of an otherwise reusable payload."""

    def __deepcopy__(self, _memo: dict[int, object]) -> NoReturn:
        """Fail if a code path tries to retain this payload.

        Args:
            _memo (dict[int, object]): Objects already visited during copying.

        Raises:
            TypeError: This payload cannot be copied.
        """
        raise TypeError("Cannot copy payload")


SetupDevice = Callable[..., DeviceDouble]
DispatcherSetup = tuple[DeviceEventDispatcher, Connector, SetupDevice]


@pytest.fixture
def setup_dispatcher() -> Generator[DispatcherSetup, None, None]:
    """Create a dispatcher and clean up its workers and devices.

    Yields:
        DispatcherSetup: Fixture resources for this test.
    """
    connector = Connector()
    # These doubles implement only the interfaces used by the dispatcher.
    dispatcher = DeviceEventDispatcher(
        lambda: cast(RedisConnector, connector), workers=2, retry_delay=0.01
    )
    devices: list[Pair] = []

    def setup(
        name: str = "motor", mixed: bool = False, cls: type[Pair] = Pair, activate: bool = True
    ) -> DeviceDouble:
        """Register and seed a device with the requested monitor coverage.

        Args:
            name (str): Device name used in Redis endpoints.
            mixed (bool): Whether one readback signal lacks monitoring.
            cls (type[Pair]): Device class to construct.
            activate (bool): Whether to start dispatching immediately.

        Returns:
            DeviceDouble: Registered device with initialized snapshot state.
        """
        obj = cls(name=name)
        devices.append(obj)
        for attr in ("first", "second", "setting"):
            if hasattr(obj, attr):
                getattr(obj, attr)._auto_monitor = not (mixed and attr == "second")
        device = DeviceDouble(obj=obj, name=name, metadata={}, enabled=True)
        dispatcher.register(cast(DSDevice, device))
        domains: tuple[Domain, ...] = ("readback", "configuration")
        for domain in domains:
            snapshot = dispatcher._states[id(obj)].domains[domain]
            for signal in snapshot.coverage.signals.values():
                if not getattr(signal, "_auto_monitor", False):
                    continue
                dispatcher.subscribe(
                    signal, partial(dispatcher.enqueue, domain=domain), domain=domain
                )
        dispatcher.seed(obj, "readback", cast(Readings, obj.read()))
        dispatcher.seed(obj, "configuration", cast(Readings, obj.read_configuration()))
        if activate:
            dispatcher.activate(obj)
        return device

    yield dispatcher, connector, setup
    dispatcher.shutdown(timeout=1)
    for obj in devices:
        obj.destroy()


def test_fully_monitored_burst_uses_complete_snapshots_with_live_values(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Publish complete snapshots with live values and captured metadata.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    device = setup(activate=False)
    obj = device.obj
    obj.read = mock.Mock(side_effect=AssertionError("Callback caused a read"))
    device.metadata = {"point_id": 3, "nested": {"labels": ["captured"]}}
    values = np.array([5, 6])
    dispatcher.enqueue(obj.first, value=values, timestamp=123)
    values[:] = -1
    for value in range(100):
        dispatcher.enqueue(obj.second, value=value, timestamp=124 + value)
    device.metadata["point_id"] = 4
    device.metadata["nested"]["labels"].append("later")
    dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    message = connector.reading(obj.name)
    np.testing.assert_equal(message.signals[obj.first.name]["value"], [-1, -1])
    assert message.signals[obj.first.name]["timestamp"] == 123
    assert message.signals[obj.second.name] == {"value": 99, "timestamp": 223}
    assert message.metadata == {"point_id": 3, "nested": {"labels": ["captured"]}}
    obj.read.assert_not_called()


@pytest.mark.parametrize("invalid", ["metadata", "status"])
def test_invalid_callback_leaves_snapshot_and_pending_state_unchanged(
    setup_dispatcher: DispatcherSetup, invalid: Literal["metadata", "status"]
) -> None:
    """Verify invalid callback leaves snapshot and pending state unchanged.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        invalid (Literal["metadata", "status"]): Malformed callback input to exercise.
    """
    dispatcher, connector, setup = setup_dispatcher
    device = setup()
    if invalid == "metadata":
        device.metadata = {"bad": Uncopyable()}
        with pytest.raises(TypeError):
            dispatcher.enqueue(device.obj.first, value=7, timestamp=7)
    else:
        with pytest.raises(ValueError):
            dispatcher.enqueue(device.obj, "status", value="invalid")
    assert dispatcher.is_idle()
    assert not dispatcher._pending
    device.metadata = {}
    device.obj.first.put(9)
    assert dispatcher.wait_idle()
    assert connector.reading(device.name).signals[device.obj.first.name]["value"] == 9


def test_callback_payload_is_retained_without_deepcopy(setup_dispatcher: DispatcherSetup) -> None:
    """Retain callback values directly while capturing the surrounding event record.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    value = Uncopyable()
    dispatcher.enqueue(obj.first, value=value, timestamp=7)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] is value


@pytest.mark.parametrize("lifecycle", ["unregistered", "retired"])
def test_inactive_read_tokens_do_not_copy_unused_results(
    setup_dispatcher: DispatcherSetup, lifecycle: Literal["unregistered", "retired"]
) -> None:
    """Discard explicit results that have no live snapshot destination.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        lifecycle (Literal["unregistered", "retired"]): Token destination state.
    """
    dispatcher, connector, _ = setup_dispatcher
    obj = Pair(name="inactive")
    try:
        if lifecycle == "retired":
            device = DeviceDouble(obj=obj, name=obj.name, metadata={}, enabled=True)
            dispatcher.register(cast(DSDevice, device))
        with dispatcher.read_context(obj) as token:
            if lifecycle == "retired":
                assert dispatcher.remove(obj)
            token.update({obj.first.name: {"value": Uncopyable()}}, {"unused": Uncopyable()})
        assert dispatcher.is_idle()
        assert not connector.writes
    finally:
        dispatcher.remove(obj)
        obj.destroy()


def test_buffered_read_acknowledges_without_copying_or_replacing_live_state(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Acknowledge buffered publication without retaining its discarded payload.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, _, setup = setup_dispatcher
    obj = setup(mixed=True, activate=False).obj
    obj.first.put(5)
    snapshot = dispatcher._states[id(obj)].domains["readback"]
    readings = snapshot.readings.copy()
    assert dispatcher._pending
    assert snapshot.dirty_version is not None
    with dispatcher.read_context(obj) as token:
        token.update({"unused": {"value": Uncopyable()}}, {"unused": Uncopyable()}, live=False)
    assert snapshot.readings == readings
    assert snapshot.dirty_version is not None
    assert snapshot.published == snapshot.version
    assert not dispatcher._pending


@pytest.mark.parametrize("operation", ["seed", "explicit"])
def test_live_read_results_keep_value_references_and_capture_record_structure(
    setup_dispatcher: DispatcherSetup, operation: Literal["seed", "explicit"]
) -> None:
    """Share mutable values while isolating reading records and event metadata.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        operation (Literal["seed", "explicit"]): Path installing the live snapshot.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(activate=False).obj
    values = np.array([5, 6])
    readings = cast(Readings, obj.read())
    readings[obj.first.name] = {"value": values, "timestamp": 7}
    metadata = {"nested": {"labels": ["captured"]}}
    original_record = readings[obj.first.name]
    with dispatcher.read_context(obj) if operation == "explicit" else nullcontext() as token:
        if token is None:
            dispatcher.seed(obj, "readback", readings, metadata)
        else:
            token.update(readings, metadata)
        values[:] = -1
        original_record["timestamp"] = 99
        original_record["value"] = np.array([99, 99])
        readings.pop(obj.second.name)
        metadata["nested"]["labels"].append("later")
    snapshot = dispatcher._states[id(obj)].domains["readback"]
    assert snapshot.readings is not readings
    assert snapshot.readings[obj.first.name] is not original_record
    assert snapshot.readings[obj.first.name]["timestamp"] == 7
    assert obj.second.name in snapshot.readings
    assert snapshot.readings[obj.first.name]["value"] is values
    assert snapshot.metadata == {"nested": {"labels": ["captured"]}}
    dispatcher.enqueue(obj)
    dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    np.testing.assert_equal(connector.reading(obj.name).signals[obj.first.name]["value"], [-1, -1])


def test_refresh_keeps_value_references_and_captures_records_and_metadata(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Retain refreshed values by reference while isolating records and event metadata.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    device = setup(mixed=True, activate=False)
    obj = device.obj
    values = np.array([5, 6])
    readings = cast(Readings, obj.read())
    readings[obj.second.name] = {"value": values, "timestamp": 7}
    record = readings[obj.second.name]
    obj.read = mock.Mock(return_value=readings)
    device.metadata = {"nested": {"labels": ["captured"]}}
    obj.first.put(7)
    device.metadata["nested"]["labels"].append("later")
    dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    snapshot = dispatcher._states[id(obj)].domains["readback"]
    assert snapshot.readings[obj.second.name]["value"] is values
    assert connector.reading(obj.name).metadata == {"nested": {"labels": ["captured"]}}
    values[:] = -1
    record["timestamp"] = 99
    readings.pop(obj.first.name)
    np.testing.assert_equal(snapshot.readings[obj.second.name]["value"], [-1, -1])
    assert snapshot.readings[obj.second.name]["timestamp"] == 7
    assert obj.first.name in snapshot.readings
    assert snapshot.metadata == {"nested": {"labels": ["captured"]}}


def test_each_mixed_event_generation_refreshes_unmonitored_values(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify each mixed event generation refreshes unmonitored values.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(mixed=True).obj
    obj.read = mock.Mock(wraps=obj.read)
    for value in (10, 20):
        obj.second.put(value)
        obj.first.put(value + 1, timestamp=value)
        assert dispatcher.wait_idle()
        assert connector.reading(obj.name).signals[obj.second.name]["value"] == value
    assert obj.read.call_count == 2


@pytest.mark.parametrize("failure_first", [True, False])
def test_failed_read_does_not_lose_healthy_final_event(
    setup_dispatcher: DispatcherSetup, failure_first: bool
) -> None:
    """Verify failed read does not lose healthy final event.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        failure_first (bool): Whether the failing device is queued first.
    """
    dispatcher, connector, setup = setup_dispatcher
    bad = setup("bad", mixed=True, activate=False).obj
    good = setup("good", mixed=True, activate=False).obj
    readings = bad.read()
    bad.read = mock.Mock(side_effect=[TimeoutError("PV unavailable"), readings])
    ordered = (bad, good) if failure_first else (good, bad)
    for obj in ordered:
        obj.first.put(5, timestamp=123)
        dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert connector.reading("good").signals[good.first.name]["value"] == 5
    assert connector.reading("bad")
    assert bad.read.call_count == 2


def test_blocked_dirty_read_does_not_block_clean_publication_or_callbacks(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify blocked dirty read does not block clean publication or callbacks.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    bad = setup("slow", mixed=True).obj
    good = setup("healthy").obj
    entered, release = threading.Event(), threading.Event()
    original = bad.read

    def blocked_read() -> OphydReadings:
        """Hold a compatibility read until the test releases it.

        Returns:
            OphydReadings: Device readings returned by ophyd.
        """
        entered.set()
        assert release.wait(3)
        return original()

    bad.read = blocked_read
    try:
        bad.first.put(3)
        assert entered.wait(1)
        good.first.put(9, timestamp=42)
        assert connector.written.wait(1)
        assert connector.reading(good.name).signals[good.first.name]["value"] == 9
        assert dispatcher.is_idle(good)
        assert not dispatcher.is_idle(bad)
    finally:
        release.set()
    assert dispatcher.wait_idle()


def test_blocked_redis_keeps_callbacks_and_final_status_bounded(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify blocked redis keeps callbacks and final status bounded.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    entered, release = threading.Event(), threading.Event()

    def blocked_write(_topic: EndpointInfo, _message: EventMessage) -> None:
        """Hold publication until the test releases it.

        Args:
            _topic (EndpointInfo): Unused destination endpoint.
            _message (EventMessage): Unused snapshot message.
        """
        entered.set()
        assert release.wait(3)

    connector.before_write = blocked_write
    try:
        obj.first.put(2)
        assert entered.wait(1)
        for value in range(200):
            dispatcher.enqueue(obj, "status", value=value % 2)
            obj.first.put(value, timestamp=value)
        dispatcher.enqueue(obj, "status", value=0)
        state = dispatcher._states[id(obj)]
        assert len(state.domains) == 4
        assert len(state.domains["readback"].readings) == 2
        assert len(state.domains["status"].readings) == 1
        assert len(dispatcher._pending) == 2
    finally:
        release.set()
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 199
    assert connector.reading(obj.name, "status").status == 0


def test_redis_failure_retries_without_further_hardware_event(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify redis failure retries without further hardware event.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    bad = setup("bad").obj
    good = setup("good").obj
    failed = set()

    def intermittent_failure(topic: EndpointInfo, _message: EventMessage) -> None:
        """Fail the first write for the selected device.

        Args:
            topic (EndpointInfo): Destination endpoint.
            _message (EventMessage): Unused snapshot message.
        """
        if topic == MessageEndpoints.device_readback("bad") and not failed:
            failed.add(True)
            raise ConnectionError("Redis connection lost after ambiguous execution")

    connector.before_write = intermittent_failure
    bad.first.put(3)
    good.first.put(7)
    assert dispatcher.wait_idle()
    assert connector.reading("good").signals[good.first.name]["value"] == 7
    assert connector.reading("bad").signals[bad.first.name]["value"] == 3


def test_callback_during_refresh_wins_and_later_dirty_generation_survives(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify callback during refresh wins and later dirty generation survives.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(mixed=True).obj
    entered, release = threading.Event(), threading.Event()
    original = obj.read
    calls = []

    def read() -> OphydReadings:
        """Read device values while exercising the selected callback race.

        Returns:
            OphydReadings: Device readings returned by ophyd.
        """
        readings = original()
        calls.append(True)
        if len(calls) == 1:
            entered.set()
            assert release.wait(3)
        return readings

    obj.read = read
    try:
        obj.first.put(3, timestamp=3)
        assert entered.wait(1)
        obj.first.put(9, timestamp=9)
        obj.second.put(20)
    finally:
        release.set()
    assert dispatcher.wait_idle()
    assert len(calls) == 2
    readings = connector.reading(obj.name).signals
    assert readings[obj.first.name] == {"value": 9, "timestamp": 9}
    assert readings[obj.second.name]["value"] == 20


def test_explicit_read_invalidates_old_event_and_keeps_racing_callback(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify explicit read invalidates old event and keeps racing callback.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(activate=False).obj
    obj.first.put(3, timestamp=3)
    with dispatcher.read_context(obj) as token:
        explicit = obj.read()
        obj.first.put(9, timestamp=9)
        token.update(cast(Readings, explicit), {"point_id": 1})
    dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name] == {"value": 9, "timestamp": 9}
    with dispatcher.read_context(obj) as token:
        token.update(cast(Readings, obj.read()))
    assert dispatcher.is_idle()


def test_failed_explicit_context_does_not_acknowledge_pending_event(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify failed explicit context does not acknowledge pending event.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(activate=False).obj
    obj.first.put(7)
    with pytest.raises(ConnectionError), dispatcher.read_context(obj) as token:
        token.update(cast(Readings, obj.read()))
        raise ConnectionError("explicit write failed")
    dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 7


def test_unknown_signal_get_and_custom_read_use_fallback(setup_dispatcher: DispatcherSetup) -> None:
    """Verify unknown signal get and custom read use fallback.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """

    class Computed(ophyd.Signal):
        """Expose a computed signal requiring compatibility reads."""

        def get(self, **kwargs: object) -> float | int:
            """Return a transformed value to require compatibility reads.

            Args:
                **kwargs (object): Keyword arguments forwarded to the device.

            Returns:
                float | int: Twice the signal value.
            """
            value = super().get(**kwargs)
            assert isinstance(value, (float, int))
            return value * 2

    class CustomPair(Pair):
        """Combine a computed signal with ordinary monitored signals."""

        first = ophyd.Component(Computed, value=1)

    dispatcher, connector, setup = setup_dispatcher
    obj = setup(cls=CustomPair).obj
    obj.read = mock.Mock(wraps=obj.read)
    obj.first.put(6, timestamp=5)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 12
    obj.read.assert_called_once()


def test_configuration_and_readback_coverage_are_independent(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify configuration and readback coverage are independent.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    obj.read = mock.Mock(side_effect=AssertionError("Unexpected data read"))
    obj.read_configuration = mock.Mock(wraps=obj.read_configuration)
    obj.setting.put(7, timestamp=9)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name, "configuration").signals[obj.setting.name]["value"] == 7
    obj.read.assert_not_called()
    obj.read_configuration.assert_not_called()


def test_kind_combination_and_nested_parent_domain_are_honored(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify kind combination and nested parent domain are honored.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """

    class Nested(ophyd.Device):
        """Expose a signal belonging to both reading domains."""

        both = ophyd.Component(ophyd.Signal, value=1, kind=ophyd.Kind.normal | ophyd.Kind.config)

    class Parent(ophyd.Device):
        """Mix omitted and included nested devices."""

        data_only = ophyd.Component(Nested, kind=ophyd.Kind.omitted)
        both = ophyd.Component(Nested, kind=ophyd.Kind.normal | ophyd.Kind.config)

    dispatcher, _, _ = setup_dispatcher
    obj = Parent(name="parent")
    try:
        device = SimpleNamespace(obj=obj, name=obj.name, metadata={})
        dispatcher.register(cast(DSDevice, device))
        domains = dispatcher._states[id(obj)].domains
        assert set(domains["readback"].coverage.fields.values()) == {obj.both.both.name}
        assert set(domains["configuration"].coverage.fields.values()) == {obj.both.both.name}
    finally:
        dispatcher.remove(obj)
        obj.destroy()


def test_disconnect_invalidates_and_reconnect_refreshes_baseline(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify disconnect invalidates and reconnect refreshes baseline.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    state = dispatcher._states[id(obj)]
    dispatcher._connection_changed(state, obj.first, False)
    obj.first.put(8, timestamp=2)
    assert not state.domains["readback"].initialized
    assert not connector.writes
    obj.read = mock.Mock(wraps=obj.read)
    dispatcher._connection_changed(state, obj.first, True)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 8
    obj.read.assert_called_once()


def test_retired_identity_and_shutdown_remain_safe_with_blocked_read(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify retired identity and shutdown remain safe with blocked read.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(mixed=True).obj
    entered, release = threading.Event(), threading.Event()
    original = obj.read

    def blocked_read() -> OphydReadings:
        """Hold a compatibility read until the test releases it.

        Returns:
            OphydReadings: Device readings returned by ophyd.
        """
        entered.set()
        assert release.wait(3)
        return original()

    obj.read = blocked_read
    try:
        obj.first.put(3)
        assert entered.wait(1)
        assert not dispatcher.remove(obj, timeout=0.01)
        dispatcher.enqueue(obj.first, value=9, timestamp=9)
        start = time.monotonic()
        stuck = dispatcher.shutdown(timeout=0.01)
        assert time.monotonic() - start < 0.5
        assert stuck
        assert not dispatcher.remove(obj, timeout=0)
        assert not connector.writes
    finally:
        release.set()
    assert not dispatcher.shutdown(timeout=1)
    assert dispatcher.remove(obj)
    assert not connector.writes


def test_retirement_timeout_leaves_device_subscribed(setup_dispatcher: DispatcherSetup) -> None:
    """Verify retirement timeout leaves device subscribed.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(mixed=True).obj
    entered, release = threading.Event(), threading.Event()
    original = obj.read

    def blocked_read() -> OphydReadings:
        """Hold a compatibility read until the test releases it.

        Returns:
            OphydReadings: Device readings returned by ophyd.
        """
        entered.set()
        assert release.wait(3)
        return original()

    obj.read = blocked_read
    try:
        obj.first.put(3)
        assert entered.wait(1)
        assert not dispatcher.remove(obj, timeout=0.01)
        obj.first.put(8)
        release.set()
        assert dispatcher.wait_idle()
        assert connector.reading(obj.name).signals[obj.first.name]["value"] == 8
    finally:
        release.set()


def test_missing_timestamp_for_mapped_field_requires_read(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify missing timestamp for mapped field requires read.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    obj.read = mock.Mock(wraps=obj.read)
    dispatcher.enqueue(obj.first, value=9)
    assert dispatcher.wait_idle()
    obj.read.assert_called_once()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 0


def test_custom_device_read_cannot_be_overwritten_by_raw_callback(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify custom device read cannot be overwritten by raw callback.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    entered, release = threading.Event(), threading.Event()
    calls = []

    class Scaled(Pair):
        """Return scaled values from a custom root read."""

        def read(self) -> OphydReadings:
            """Read device values while exercising the selected callback race.

            Returns:
                OphydReadings: Device readings returned by ophyd.
            """
            readings = super().read()
            cast(Readings, readings)[self.first.name]["value"] *= 10
            if calls:
                calls.append(True)
                if len(calls) == 2:
                    entered.set()
                    assert release.wait(3)
            return readings

    dispatcher, connector, setup = setup_dispatcher
    obj = setup(cls=Scaled).obj
    calls.append(True)
    try:
        obj.first.put(3, timestamp=3)
        assert entered.wait(1)
        obj.first.put(9, timestamp=9)
    finally:
        release.set()
    assert dispatcher.wait_idle()
    values = [
        cast(messages.DeviceMessage, message).signals[obj.first.name]["value"]
        for _, message in connector.writes
    ]
    assert values[-1] == 90
    assert all(value in (30, 90) for value in values)


def test_incomplete_refresh_remains_pending_and_retries(setup_dispatcher: DispatcherSetup) -> None:
    """Verify incomplete refresh remains pending and retries.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(mixed=True).obj
    complete = cast(Readings, obj.read())
    obj.read = mock.Mock(side_effect=[{obj.first.name: complete[obj.first.name]}, complete])
    obj.first.put(2)
    assert dispatcher.wait_idle()
    obj.read.assert_called_with()
    assert obj.read.call_count == 2
    assert len(connector.writes) == 1
    assert set(connector.reading(obj.name).signals) == set(complete)


def test_retired_read_context_rejects_use_and_same_name_replacement_is_distinct(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify retired read context rejects use and same name replacement is distinct.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    old = setup("motor").obj
    assert dispatcher.remove(old)
    with pytest.raises(RuntimeError, match="retired"), dispatcher.read_context(old):
        pytest.fail("A retired device must not perform I/O")
    new = setup("motor").obj
    dispatcher.enqueue(old.first, value=99, timestamp=9)
    new.first.put(5)
    assert dispatcher.wait_idle()
    assert connector.reading("motor").signals[new.first.name]["value"] == 5
    assert len(connector.writes) == 1


def test_initialization_merge_keeps_callback_that_arrives_during_baseline(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify initialization merge keeps callback that arrives during baseline.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(activate=False).obj
    with dispatcher.read_context(obj) as token:
        baseline = obj.read()
        obj.first.put(8, timestamp=8)
        token.update(cast(Readings, baseline))
    dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name] == {"value": 8, "timestamp": 8}


def test_root_trigger_without_field_mapping_publishes_full_snapshot_without_read(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify root trigger without field mapping publishes full snapshot without read.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    obj.read = mock.Mock(side_effect=AssertionError("Root events must use snapshot"))
    dispatcher.enqueue(obj, value=999, timestamp=9)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 0
    obj.read.assert_not_called()


def test_buffered_explicit_read_does_not_mark_snapshot_live(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify buffered explicit read does not mark snapshot live.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, _, setup = setup_dispatcher
    obj = setup(mixed=True, activate=False).obj
    obj.first.put(5)
    state = dispatcher._states[id(obj)]
    with dispatcher.read_context(obj) as token:
        token.update(cast(Readings, {}), live=False)
    assert state.domains["readback"].dirty_version is not None
    assert state.domains["readback"].readings


def test_completed_refresh_can_publish_under_continuous_invalidation(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify completed refresh can publish under continuous invalidation.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(mixed=True).obj
    original = obj.read
    remaining = [20]

    def read() -> OphydReadings:
        """Read device values while exercising the selected callback race.

        Returns:
            OphydReadings: Device readings returned by ophyd.
        """
        readings = original()
        if remaining[0]:
            remaining[0] -= 1
            obj.first.put(remaining[0])
        return readings

    obj.read = read
    obj.first.put(21)
    assert connector.written.wait(1)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 0


def test_duplicate_names_cannot_patch_a_field_owned_by_another_signal(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify duplicate names cannot patch a field owned by another signal.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """

    class DuplicatePair(Pair):
        """Give two signals the same reading key."""

        def __init__(self, *args: str, **kwargs: object) -> None:
            """Initialize the test double.

            Args:
                *args (str): Positional prefix arguments forwarded to the device.
                **kwargs (object): Keyword arguments forwarded to the device.
            """
            super().__init__(*args, **kwargs)
            self.second.name = self.first.name

    dispatcher, connector, setup = setup_dispatcher
    obj = setup(cls=DuplicatePair).obj
    obj.read = mock.Mock(wraps=obj.read)
    coverage = dispatcher._states[id(obj)].domains["readback"].coverage
    assert not coverage.trusted
    assert not coverage.patch_fields
    obj.first.put(9)
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 1
    obj.read.assert_called_once()


def test_pipeline_response_exception_retains_the_only_pending_event(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify pipeline response exception retains the only pending event.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    original = connector.pipeline
    calls = []

    def pipeline() -> Pipeline:
        """Create a pipeline with the requested failure behavior.

        Returns:
            Pipeline: Empty pipeline bound to this recording connector.
        """
        pipe = original()
        calls.append(True)
        if len(calls) == 1:
            pipe.execute = mock.Mock(return_value=[True, ResponseError("Redis command rejected")])
        return pipe

    connector.pipeline = pipeline
    obj.first.put(9)
    assert dispatcher.wait_idle()
    assert len(calls) == 2
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 9


def test_ready_snapshots_share_one_pipeline_across_devices_and_domains(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify ready snapshots share one pipeline across devices and domains.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    devices = [setup(name, activate=False).obj for name in ("one", "two")]
    for obj in devices:
        obj.first.put(5)
        obj.setting.put(6)
        dispatcher.enqueue(obj, "status", value=0)
    with dispatcher._condition:
        for obj in devices:
            dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert len(connector.executions) == 1
    # Readback/configuration each need SET + PUBLISH; status needs only SET.
    assert len(connector.executions[0]) == 10
    for obj in devices:
        assert connector.reading(obj.name).signals[obj.first.name]["value"] == 5
        assert connector.reading(obj.name, "configuration").signals[obj.setting.name]["value"] == 6
        assert connector.reading(obj.name, "status").status == 0


def test_publication_batches_have_a_snapshot_limit(setup_dispatcher: DispatcherSetup) -> None:
    """Verify publication batches have a snapshot limit.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    dispatcher._max_batch_size = 2
    devices = [setup(f"device{index}", activate=False).obj for index in range(5)]
    with dispatcher._condition:
        for obj in devices:
            obj.first.put(5)
            dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert [len(commands) for commands in connector.executions] == [4, 4, 2]


@pytest.mark.parametrize("mixed", [True, False])
def test_successful_updates_have_no_per_device_cooldown(
    setup_dispatcher: DispatcherSetup, mixed: bool
) -> None:
    """Verify successful updates have no per device cooldown.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        mixed (bool): Whether one readback signal lacks monitoring.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(mixed=mixed).obj
    for value in (5, 6):
        obj.first.put(value)
        assert dispatcher.wait_idle(timeout=1)
        assert connector.reading(obj.name).signals[obj.first.name]["value"] == value
        snapshot = dispatcher._states[id(obj)].domains["readback"]
        assert snapshot.refresh_retry_at == snapshot.publish_retry_at == 0


def test_hot_readback_cannot_starve_configuration_refresh(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify hot readback cannot starve configuration refresh.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(mixed=True, activate=False).obj
    original_read, original_config = obj.read, obj.read_configuration
    calls = []

    def hot_read() -> OphydReadings:
        """Keep readback dirty until configuration receives a refresh.

        Returns:
            OphydReadings: Device readings returned by ophyd.
        """
        calls.append("readback")
        readings = original_read()
        if "configuration" not in calls and len(calls) < 100:
            obj.first.put(len(calls))
        return readings

    def read_config() -> OphydReadings:
        """Record and return the configuration refresh.

        Returns:
            OphydReadings: Device readings returned by ophyd.
        """
        calls.append("configuration")
        return original_config()

    obj.read, obj.read_configuration = hot_read, read_config
    obj.first.put(5)
    # A missing timestamp needs one fallback read even for a monitored signal.
    dispatcher.enqueue(obj.setting, "configuration", value=2)
    dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert calls[:2] == ["readback", "configuration"]
    assert connector.reading(obj.name, "configuration").signals[obj.setting.name]["value"] == 2


@pytest.mark.parametrize("read_failed", [False, True])
def test_gate_release_wakes_pending_publisher_without_another_event(
    setup_dispatcher: DispatcherSetup, read_failed: bool
) -> None:
    """Verify gate release wakes pending publisher without another event.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        read_failed (bool): Whether the explicit read context raises.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    attempted = threading.Event()
    original = dispatcher._next_publication_batch

    def select(now: float) -> list[_Publication]:
        """Record when ready publication waits for an explicit read gate.

        Args:
            now (float): Scheduler time used to select ready work.

        Returns:
            list[_Publication]: Publications whose operation gates are available.
        """
        batch = original(now)
        if dispatcher._pending and not batch:
            attempted.set()
        return batch

    with mock.patch.object(dispatcher, "_next_publication_batch", side_effect=select):
        with (
            pytest.raises(TimeoutError) if read_failed else nullcontext(),
            dispatcher.read_context(obj),
        ):
            obj.first.put(7)
            assert attempted.wait(1)
            if read_failed:
                raise TimeoutError("Explicit read failed")
        assert dispatcher.wait_idle(timeout=1)
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 7


@pytest.mark.parametrize("read_failed", [False, True])
def test_busy_read_gates_do_not_starve_refreshes(
    setup_dispatcher: DispatcherSetup, read_failed: bool
) -> None:
    """Refresh healthy devices while all worker slots could otherwise wait on gates.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, connector, and device factory.
        read_failed (bool): Whether the explicit reads fail before releasing their gates.
    """
    dispatcher, connector, setup = setup_dispatcher
    blocked = [setup(name, mixed=True).obj for name in ("first", "second")]
    healthy = setup("healthy", mixed=True).obj
    with pytest.raises(TimeoutError) if read_failed else nullcontext(), ExitStack() as stack:
        for obj in blocked:
            stack.enter_context(dispatcher.read_context(obj))
        with dispatcher._condition:
            for obj in [*blocked, healthy]:
                obj.first.put(7)
        assert dispatcher.wait_idle(timeout=1, obj=healthy)
        assert connector.reading(healthy.name).signals[healthy.first.name]["value"] == 7
        assert all(not dispatcher.is_idle(obj) for obj in blocked)
        if read_failed:
            raise TimeoutError("Explicit batch failed")
    # Gate release must wake retained work without a later callback to rescue it.
    assert dispatcher.wait_idle(timeout=1)
    for obj in blocked:
        assert connector.reading(obj.name).signals[obj.first.name]["value"] == 7


@pytest.mark.parametrize("kind", ["signal", "device", "one_limit", "two_limits"])
def test_activation_refreshes_only_supported_missing_baselines(
    kind: Literal["signal", "device", "one_limit", "two_limits"],
) -> None:
    """Recover missing baselines without inventing unsupported reads.

    Args:
        kind (Literal["signal", "device", "one_limit", "two_limits"]): Root capabilities.
    """
    connector = Connector()
    dispatcher = DeviceEventDispatcher(lambda: cast(RedisConnector, connector))
    obj = ophyd.Signal(name="root", value=1) if kind == "signal" else Pair(name="root")
    device = SimpleNamespace(obj=obj, name=obj.name, metadata={})
    if kind in ("one_limit", "two_limits"):
        cast(SimpleNamespace, obj).low_limit_travel = ophyd.Signal(name="low", value=-1)
    if kind == "two_limits":
        cast(SimpleNamespace, obj).high_limit_travel = ophyd.Signal(name="high", value=1)
    try:
        dispatcher.register(cast(DSDevice, device))
        dispatcher.activate(obj)
        assert dispatcher.wait_idle(timeout=1)
        domains = dispatcher._states[id(obj)].domains
        assert domains["readback"].initialized
        assert domains["configuration"].initialized == (kind != "signal")
        assert domains["limits"].initialized == (kind == "two_limits")
        assert domains["status"].version == 0
        assert connector.reading(obj.name).signals == obj.read()
    finally:
        dispatcher.shutdown()
        obj.destroy()


def test_fully_monitored_callbacks_do_not_recalculate_coverage(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify fully monitored callbacks do not recalculate coverage.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    snapshot = dispatcher._states[id(obj)].domains["readback"]
    with mock.patch.object(snapshot, "update_coverage") as coverage:
        for value in range(100):
            obj.first.put(value)
        assert dispatcher.wait_idle()
        coverage.assert_not_called()
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 99


@pytest.mark.parametrize("operation", ["refresh", "publish"])
def test_retry_deadline_crossing_does_not_lose_the_only_pending_event(
    setup_dispatcher: DispatcherSetup, operation: Literal["refresh", "publish"]
) -> None:
    """Verify retry deadline crossing does not lose the only pending event.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        operation (Literal["refresh", "publish"]): Operation selected by the test.
    """
    dispatcher, _, setup = setup_dispatcher
    obj = setup(mixed=operation == "refresh", activate=False).obj
    obj.first.put(7)
    state = dispatcher._states[id(obj)]
    state.active = True
    attribute: Literal["refresh_retry_at", "publish_retry_at"] = (
        "refresh_retry_at" if operation == "refresh" else "publish_retry_at"
    )
    setattr(state.domains["readback"], attribute, 101)
    select = (
        dispatcher._next_refresh if operation == "refresh" else dispatcher._next_publication_batch
    )
    # Selection and its timed wait use one sample even if the deadline passes between them.
    with mock.patch(
        "bec_server.device_server.devices.event_dispatcher.time.monotonic", return_value=102
    ):
        assert not select(100)
        assert dispatcher._retry_timeout(attribute, 100) == 1


def test_batch_uses_real_connector_serialization_and_pipeline(
    setup_dispatcher: DispatcherSetup, connected_connector: RedisConnector
) -> None:
    """Verify batch uses real connector serialization and pipeline.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        connected_connector (RedisConnector): Redis connector backed by the test server.
    """
    dispatcher, _, setup = setup_dispatcher
    dispatcher._get_connector = lambda: connected_connector
    devices = [setup(name, activate=False).obj for name in ("one", "two")]
    with mock.patch.object(
        connected_connector, "pipeline", wraps=connected_connector.pipeline
    ) as pipeline:
        with dispatcher._condition:
            for obj in devices:
                obj.first.put(5)
                obj.setting.put(6)
                dispatcher.enqueue(obj, "status", value=0)
                dispatcher.activate(obj)
        assert dispatcher.wait_idle()
        pipeline.assert_called_once()
    for obj in devices:
        readback = connected_connector.get(MessageEndpoints.device_readback(obj.name))
        assert readback.signals[obj.first.name]["value"] == 5
        config = connected_connector.get(MessageEndpoints.device_read_configuration(obj.name))
        assert config.signals[obj.setting.name]["value"] == 6
        assert connected_connector.get(MessageEndpoints.device_status(obj.name)).status == 0


def test_small_batches_rotate_domains_under_continuous_updates(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify small batches rotate domains under continuous updates.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    dispatcher._max_batch_size = 1
    obj = setup(activate=False).obj
    obj.first.put(5)
    obj.setting.put(6)
    dispatcher.enqueue(obj, "status", value=0)
    remaining = [10]

    def update_again(topic: EndpointInfo, _message: EventMessage) -> None:
        """Create another readback while a previous update is published.

        Args:
            topic (EndpointInfo): Destination endpoint.
            _message (EventMessage): Unused snapshot message.
        """
        if topic == MessageEndpoints.device_readback(obj.name) and remaining[0]:
            remaining[0] -= 1
            obj.first.put(remaining[0])

    connector.before_write = update_again
    dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert {commands[0][1] for commands in connector.executions[:3]} == {
        MessageEndpoints.device_readback(obj.name),
        MessageEndpoints.device_read_configuration(obj.name),
        MessageEndpoints.device_status(obj.name),
    }
    assert connector.reading(obj.name).signals[obj.first.name]["value"] == 0


@pytest.mark.parametrize("failure_first", [True, False])
def test_partial_staging_failure_keeps_healthy_commands(
    setup_dispatcher: DispatcherSetup, failure_first: bool
) -> None:
    """Verify partial staging failure keeps healthy commands.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        failure_first (bool): Whether the failing device is queued first.
    """
    dispatcher, connector, setup = setup_dispatcher
    names = ("bad", "good") if failure_first else ("good", "bad")
    devices = [setup(name, activate=False).obj for name in names]
    original = connector.set_and_publish
    failed = []

    def stage(topic: EndpointInfo, message: EventMessage, pipe: Pipeline) -> None:
        """Fail after staging commands for one snapshot.

        Args:
            topic (EndpointInfo): Destination endpoint.
            message (EventMessage): Snapshot message to publish.
            pipe (Pipeline): Pipeline receiving staged commands.
        """
        original(topic, message, pipe)
        if topic == MessageEndpoints.device_readback("bad") and not failed:
            failed.append(True)
            raise ValueError("Failed after staging part of a snapshot")

    connector.set_and_publish = stage
    with dispatcher._condition:
        for obj in devices:
            obj.first.put(7)
            dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert [len(commands) for commands in connector.executions] == [2, 2]
    assert connector.executions[0][0][1] == MessageEndpoints.device_readback("good")
    assert connector.executions[1][0][1] == MessageEndpoints.device_readback("bad")
    assert len(connector.writes) == 2


@pytest.mark.parametrize("failure_first", [True, False])
@pytest.mark.parametrize("failed_operation", ["set", "publish"])
def test_partial_command_failure_retries_only_unconfirmed_snapshot(
    setup_dispatcher: DispatcherSetup, failure_first: bool, failed_operation: Operation
) -> None:
    """Verify partial command failure retries only unconfirmed snapshot.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        failure_first (bool): Whether the failing device is queued first.
        failed_operation (Operation): Redis command that should fail.
    """
    dispatcher, connector, setup = setup_dispatcher
    names = ("bad", "good") if failure_first else ("good", "bad")
    devices = [setup(name, activate=False).obj for name in names]
    failed = []

    def fail_command(operation: Operation, topic: EndpointInfo, _message: EventMessage) -> None:
        """Reject one selected Redis command once.

        Args:
            operation (Operation): Operation selected by the test.
            topic (EndpointInfo): Destination endpoint.
            _message (EventMessage): Unused snapshot message.
        """
        if (
            topic == MessageEndpoints.device_readback("bad")
            and operation == failed_operation
            and not failed
        ):
            failed.append(True)
            raise ResponseError("Redis rejected one command")

    connector.before_command = fail_command
    with dispatcher._condition:
        for obj in devices:
            obj.first.put(7)
            dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert [len(commands) for commands in connector.executions] == [4, 2]
    assert connector.executions[1][0][1] == MessageEndpoints.device_readback("bad")
    for obj in devices:
        assert connector.reading(obj.name).signals[obj.first.name]["value"] == 7
    assert (
        sum(topic == MessageEndpoints.device_readback("good") for topic, _ in connector.writes) == 1
    )


@pytest.mark.parametrize("failure_first", [True, False])
def test_ambiguous_batch_failure_retries_all_snapshots(
    setup_dispatcher: DispatcherSetup, failure_first: bool
) -> None:
    """Verify ambiguous batch failure retries all snapshots.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        failure_first (bool): Whether the failing device is queued first.
    """
    dispatcher, connector, setup = setup_dispatcher
    dispatcher._retry_delay = 0
    names = ("bad", "good") if failure_first else ("good", "bad")
    devices = [setup(name, activate=False).obj for name in names]
    failed = []

    def lose_connection(operation: Operation, topic: EndpointInfo, _message: EventMessage) -> None:
        """Lose the first batch acknowledgement after publication.

        Args:
            operation (Operation): Operation selected by the test.
            topic (EndpointInfo): Destination endpoint.
            _message (EventMessage): Unused snapshot message.
        """
        if (
            topic == MessageEndpoints.device_readback("bad")
            and operation == "publish"
            and not failed
        ):
            failed.append(True)
            raise ConnectionError("Lost the batch acknowledgement")

    connector.before_command = lose_connection
    with dispatcher._condition:
        for obj in devices:
            obj.first.put(7)
            dispatcher.activate(obj)
    assert dispatcher.wait_idle()
    assert [len(commands) for commands in connector.executions] == [4, 4]
    for obj in devices:
        assert connector.reading(obj.name).signals[obj.first.name]["value"] == 7


def test_read_started_before_disconnect_cannot_reinitialize_after_reconnect(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify read started before disconnect cannot reinitialize after reconnect.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup(mixed=True).obj
    state = dispatcher._states[id(obj)]
    first_entered, first_release = threading.Event(), threading.Event()
    second_entered, second_release = threading.Event(), threading.Event()
    original = obj.read
    calls = []

    def read() -> OphydReadings:
        """Read device values while exercising the selected callback race.

        Returns:
            OphydReadings: Device readings returned by ophyd.
        """
        readings = original()
        calls.append(True)
        if len(calls) == 1:
            first_entered.set()
            assert first_release.wait(3)
        else:
            second_entered.set()
            assert second_release.wait(3)
        return readings

    obj.read = read
    try:
        obj.first.put(2)
        assert first_entered.wait(1)
        dispatcher._connection_changed(state, obj.first, False)
        obj.second.put(20)
        dispatcher._connection_changed(state, obj.first, True)
        first_release.set()
        assert second_entered.wait(1)
        assert not state.domains["readback"].initialized
        assert not connector.writes
    finally:
        first_release.set()
        second_release.set()
    assert dispatcher.wait_idle()
    assert connector.reading(obj.name).signals[obj.second.name]["value"] == 20


def test_explicit_read_started_before_disconnect_does_not_reseed_snapshot(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Verify explicit read started before disconnect does not reseed snapshot.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, _, setup = setup_dispatcher
    obj = setup(activate=False).obj
    state = dispatcher._states[id(obj)]
    with dispatcher.read_context(obj) as token:
        readings = obj.read()
        dispatcher._connection_changed(state, obj.first, False)
        dispatcher._connection_changed(state, obj.first, True)
        token.update(cast(Readings, readings))
    assert not state.domains["readback"].initialized
    assert state.domains["readback"].dirty_version is not None


def test_simulated_computed_graph_publishes_callbacks_without_refresh_feedback(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Keep the real simulator dependency graph on the snapshot path.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, _ = setup_dispatcher
    left, right = SimMonitor(name="left"), SimMonitor(name="right")
    manager = SimpleNamespace(devices=SimpleNamespace(left=left, right=right))
    derived = ComputedSignal(name="derived", device_manager=manager)
    latest: Readings = {}

    def capture(
        *, obj: ophyd.Signal, value: Any, timestamp: float | None = None, **_kwargs: Any
    ) -> None:
        """Remember the latest emitted value without triggering another measurement.

        Args:
            obj (ophyd.Signal): Signal emitting the callback.
            value (Any): Latest callback value.
            timestamp (float | None): Supplied timestamp, or None to use the cached property.
            **_kwargs (Any): Unused callback fields.
        """
        latest[obj.name] = {
            "value": value,
            "timestamp": obj.timestamp if timestamp is None else timestamp,
        }

    try:
        for signal in (left, right, derived):
            device = SimpleNamespace(obj=signal, name=signal.name, metadata={})
            dispatcher.register(cast(DSDevice, device))
            dispatcher.subscribe(signal, dispatcher.enqueue, domain="readback")
            signal.subscribe(capture, run=False)
            if signal is derived:
                derived.compute_method = "def calculate(a, b): return a.get() + b.get()"
                derived.input_signals = ["left", "right"]
            dispatcher.seed(signal, "readback", cast(Readings, signal.read()))
            dispatcher.activate(signal)
        assert dispatcher.wait_idle()
        with mock.patch.object(dispatcher, "_read", wraps=dispatcher._read) as refresh:
            for _ in range(100):
                left.get()
            assert dispatcher.wait_idle()
            refresh.assert_not_called()
        for signal in (left, right, derived):
            assert connector.reading(signal.name).signals[signal.name] == latest[signal.name]
    finally:
        derived.input_signals = []
        for signal in (derived, left, right):
            dispatcher.remove(signal)
            signal.destroy()


@pytest.mark.parametrize("shared", [False, True])
def test_read_generated_events_do_not_repeat_mixed_domain_reads(
    setup_dispatcher: DispatcherSetup, shared: bool
) -> None:
    """Read unmonitored siblings once without feeding synthetic events back into reads.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        shared (bool): Whether the read-emitting signal belongs to both reading domains.
    """
    kind = ophyd.Kind.normal | ophyd.Kind.config if shared else ophyd.Kind.normal

    class Mixed(Pair):
        """Combine a real read-emitting driver with an unmonitored sibling."""

        first = ophyd.Component(ReadOnlySignal, value=0, kind=kind)
        second = ophyd.Component(ophyd.Signal, value=1, kind=kind)

    dispatcher, connector, setup = setup_dispatcher
    obj = setup(cls=Mixed, mixed=True).obj
    assert dispatcher.wait_idle()
    with mock.patch.object(dispatcher, "_read", wraps=dispatcher._read) as refresh:
        with dispatcher._condition:
            obj.second.put(99)
            obj.first.get()
        assert dispatcher.wait_idle()
        assert refresh.call_count == (2 if shared else 1)
    assert connector.reading(obj.name).signals[obj.second.name]["value"] == 99
    if shared:
        assert connector.reading(obj.name, "configuration").signals[obj.second.name]["value"] == 99


@pytest.mark.parametrize("custom_readback", [False, True])
@pytest.mark.parametrize("external_during_configuration", [False, True])
def test_read_generated_events_refresh_custom_other_domains_once(
    setup_dispatcher: DispatcherSetup, custom_readback: bool, external_during_configuration: bool
) -> None:
    """Refresh transformed domains without cycling between read-emitting getters.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        custom_readback (bool): Whether both domains transform their readings.
        external_during_configuration (bool): Whether a new pass starts during configuration.
    """
    configuration_reads: list[OphydReadings] = []
    readback_reads: list[OphydReadings] = []
    entered, release = threading.Event(), threading.Event()
    block_configuration = False

    class CustomConfiguration(Pair):
        """Transform a shared simulator field in configuration readings."""

        first = ophyd.Component(ReadOnlySignal, kind=ophyd.Kind.normal | ophyd.Kind.config)

        def read_configuration(self) -> OphydReadings:
            """Scale the configuration measurement before returning it.

            Returns:
                OphydReadings: Configuration readings with the shared field scaled.
            """
            readings = super().read_configuration()
            readings[self.first.name]["value"] *= 10
            configuration_reads.append(readings)
            if block_configuration and len(configuration_reads) == 1:
                entered.set()
                assert release.wait(3)
            return readings

    class CustomBoth(CustomConfiguration):
        """Transform readback as well, preventing either domain from patching fields."""

        def read(self) -> OphydReadings:
            """Scale the readback measurement independently of configuration.

            Returns:
                OphydReadings: Readback readings with the shared field scaled.
            """
            readings = super().read()
            readings[self.first.name]["value"] *= 100
            readback_reads.append(readings)
            return readings

    dispatcher, connector, setup = setup_dispatcher
    obj = setup(cls=CustomBoth if custom_readback else CustomConfiguration, mixed=True).obj
    assert dispatcher.wait_idle()
    for value in (99, 100):
        configuration_reads.clear()
        readback_reads.clear()
        entered.clear()
        release.clear()
        block_configuration = external_during_configuration
        with mock.patch.object(dispatcher, "_read", wraps=dispatcher._read) as refresh:
            obj.second.put(value)
            dispatcher.enqueue(obj)
            try:
                if external_during_configuration:
                    assert entered.wait(1)
                    obj.second.put(value + 1)
                    dispatcher.enqueue(obj)
            finally:
                release.set()
            assert dispatcher.wait_idle()
            assert refresh.call_count == (4 if external_during_configuration else 2)
        assert len(configuration_reads) == (2 if external_during_configuration else 1)
        assert connector.reading(obj.name, "configuration").signals == configuration_reads[-1]
        assert connector.reading(obj.name).signals[obj.second.name]["value"] == (
            value + int(external_during_configuration)
        )
        if custom_readback:
            assert len(readback_reads) == (2 if external_during_configuration else 1)
            assert connector.reading(obj.name).signals == readback_reads[-1]


def test_external_callback_during_read_generated_events_still_requires_refresh(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Keep an external thread's final invalidation while filtering synthetic read feedback.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """

    class Mixed(Pair):
        """Provide a monitored simulator and an unmonitored sibling."""

        first = ophyd.Component(ReadOnlySignal, value=0)

    dispatcher, connector, setup = setup_dispatcher
    obj = setup(cls=Mixed, mixed=True).obj
    entered, release = threading.Event(), threading.Event()
    original = obj.read
    calls = 0

    def blocked_read() -> OphydReadings:
        """Pause the first completed reading while another thread changes the hardware.

        Returns:
            OphydReadings: Reading captured before the first pause or after recovery.
        """
        nonlocal calls
        readings = original()
        calls += 1
        if calls == 1:
            entered.set()
            assert release.wait(3)
        return readings

    obj.read = blocked_read
    try:
        obj.first.get()
        assert entered.wait(1)
        obj.second.put(99)
        obj.first.get()
    finally:
        release.set()
    assert dispatcher.wait_idle()
    assert calls == 2
    assert connector.reading(obj.name).signals[obj.second.name]["value"] == 99


def test_final_moving_status_retries_while_sibling_is_disconnected(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Publish a final moving state independently of a disconnected configuration signal.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    dispatcher.enqueue(obj, "status", value=1)
    assert dispatcher.wait_idle()

    connector.written.clear()
    failures = 0

    def fail_once(topic: EndpointInfo, _message: EventMessage) -> None:
        """Fail one status write before allowing its retained retry.

        Args:
            topic (EndpointInfo): Destination endpoint.
            _message (EventMessage): Unused pending status message.
        """
        nonlocal failures
        if topic == MessageEndpoints.device_status(obj.name) and failures == 0:
            failures += 1
            raise ConnectionError("Temporary Redis failure")

    connector.before_write = fail_once
    obj.setting._metadata["connected"] = False
    obj.setting._run_subs(sub_type=obj.setting.SUB_META, connected=False)
    try:
        assert not obj.connected
        dispatcher.enqueue(obj, "status", value=0)
        assert connector.written.wait(1)
        assert connector.reading(obj.name, "status").status == 0
        assert failures == 1
    finally:
        obj.setting._metadata["connected"] = True
        obj.setting._run_subs(sub_type=obj.setting.SUB_META, connected=True)
    assert dispatcher.wait_idle()


@pytest.mark.parametrize("phase", ["serialization", "execution"])
def test_explicit_write_passes_blocked_event_batch_without_stale_overwrite(
    setup_dispatcher: DispatcherSetup,
    connected_connector: RedisConnector,
    phase: Literal["serialization", "execution"],
) -> None:
    """Let an explicit read finish while unrelated event I/O is blocked.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        connected_connector (RedisConnector): Connector backed by a fake Redis server.
        phase (Literal['serialization', 'execution']): Event pipeline phase to pause.
    """
    dispatcher, _, setup = setup_dispatcher
    dispatcher._get_connector = lambda: connected_connector
    motor, detector = [setup(name, activate=False).obj for name in ("motor", "detector")]
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()
    pipeline_factory = connected_connector.pipeline
    stage = connected_connector.set_and_publish
    errors: list[Exception] = []
    conflicts: list[WatchError] = []

    def stage_with_pause(topic: EndpointInfo, message: EventMessage, pipe: RedisPipeline) -> None:
        """Pause serialization of the unrelated detector once.

        Args:
            topic (EndpointInfo): Destination endpoint.
            message (EventMessage): Snapshot being serialized.
            pipe (RedisPipeline): Shared event pipeline.
        """
        stage(topic, message, pipe=pipe)
        if phase == "serialization" and topic == MessageEndpoints.device_readback(detector.name):
            entered.set()
            assert release.wait(3)

    def pipeline_with_pause() -> RedisPipeline:
        """Pause a watched event transaction before Redis receives EXEC.

        Returns:
            RedisPipeline: Real pipeline with a controlled execution boundary.
        """
        pipe = pipeline_factory()
        execute = pipe.execute

        def execute_with_pause(raise_on_error: bool = True) -> list[Any]:
            """Hold the first event commit while an explicit write changes the watched key.

            Args:
                raise_on_error (bool): Whether Redis command errors should raise.

            Returns:
                list[Any]: Redis transaction outcomes.
            """
            if phase == "execution" and pipe.watching and not entered.is_set():
                entered.set()
                assert release.wait(3)
            try:
                return execute(raise_on_error=raise_on_error)
            except WatchError as exc:
                conflicts.append(exc)
                raise

        pipe.execute = execute_with_pause
        return pipe

    def explicit_read() -> None:
        """Read and publish a newer value through the actual explicit-read token."""
        try:
            with dispatcher.read_context(motor) as token:
                motor.first._readback = 99
                readings = cast(Readings, motor.read())
                message = messages.DeviceMessage(signals=readings, metadata={"point_id": 7})
                pipe = pipeline_factory()
                stage(MessageEndpoints.device_readback(motor.name), message, pipe=pipe)
                pipe.execute()
                token.update(readings, message.metadata)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            completed.set()

    worker = threading.Thread(target=explicit_read)
    with (
        mock.patch.object(connected_connector, "set_and_publish", side_effect=stage_with_pause),
        mock.patch.object(connected_connector, "pipeline", side_effect=pipeline_with_pause),
    ):
        try:
            with dispatcher._condition:
                for obj in (motor, detector):
                    obj.first.put(3)
                    dispatcher.activate(obj)
            assert entered.wait(1)
            worker.start()
            assert completed.wait(1), "An event batch blocked the explicit read"
            assert not errors
        finally:
            release.set()
            if worker.ident is not None:
                worker.join(3)
        assert dispatcher.wait_idle()
    actual = connected_connector.get(MessageEndpoints.device_readback(motor.name))
    assert actual.signals[motor.first.name]["value"] == 99
    assert actual.metadata == {"point_id": 7}
    assert (
        connected_connector.get(MessageEndpoints.device_readback(detector.name)).signals[
            detector.first.name
        ]["value"]
        == 3
    )
    assert bool(conflicts) == (phase == "execution")


def test_watch_conflict_does_not_starve_an_unrelated_final_update(
    setup_dispatcher: DispatcherSetup, connected_connector: RedisConnector
) -> None:
    """Split a conflicted batch so a healthy root commits without another callback.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
        connected_connector (RedisConnector): Connector backed by a fake Redis server.
    """
    dispatcher, _, setup = setup_dispatcher
    dispatcher._get_connector = lambda: connected_connector
    bad, good = [setup(name, activate=False).obj for name in ("conflicting", "healthy")]
    original = connected_connector.pipeline
    recovered, healthy = threading.Event(), threading.Event()
    conflict_count = 0
    bad_key = MessageEndpoints.device_readback(bad.name)
    good_key = MessageEndpoints.device_readback(good.name)

    def pipeline() -> RedisPipeline:
        """Create real transactions with an independently changing watched root.

        Returns:
            RedisPipeline: Pipeline whose conflicting root changes immediately before EXEC.
        """
        pipe = original()
        execute = pipe.execute

        def execute_with_conflict(raise_on_error: bool = True) -> list[Any]:
            """Inject competing writes only for the selected root.

            Args:
                raise_on_error (bool): Whether Redis command errors should raise.

            Returns:
                list[Any]: Successful transaction outcomes.
            """
            nonlocal conflict_count
            keys = {args[1] for args, _ in pipe.command_stack if args[0] == "SET"}
            if bad_key.endpoint in keys and not recovered.is_set():
                competing = original()
                connected_connector.set_and_publish(
                    bad_key, messages.DeviceMessage(signals={}, metadata={}), pipe=competing
                )
                competing.execute()
                conflict_count += 1
            results = execute(raise_on_error=raise_on_error)
            if good_key.endpoint in keys:
                healthy.set()
            return results

        pipe.execute = execute_with_conflict
        return pipe

    with mock.patch.object(connected_connector, "pipeline", side_effect=pipeline):
        try:
            with dispatcher._condition:
                for obj in (bad, good):
                    obj.first.put(5)
                    dispatcher.activate(obj)
            assert healthy.wait(1)
            assert dispatcher.wait_idle(timeout=1, obj=good)
            assert conflict_count >= 2
            assert connected_connector.get(good_key).signals[good.first.name]["value"] == 5
        finally:
            recovered.set()
        assert dispatcher.wait_idle()
    assert connected_connector.get(bad_key).signals[bad.first.name]["value"] == 5


def test_reconnected_baseline_rejects_a_snapshot_still_being_serialized(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Discard a staged pre-disconnect value even after a fresh baseline becomes available.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, connector, setup = setup_dispatcher
    obj = setup().obj
    state = dispatcher._states[id(obj)]
    snapshot = state.domains["readback"]
    entered, release = threading.Event(), threading.Event()
    stage = connector.set_and_publish

    def pause_staging(topic: EndpointInfo, message: EventMessage, pipe: Pipeline) -> None:
        """Hold the first serialized snapshot before its final validation.

        Args:
            topic (EndpointInfo): Destination endpoint.
            message (EventMessage): Snapshot being serialized.
            pipe (Pipeline): Recording pipeline receiving the commands.
        """
        stage(topic, message, pipe)
        if not entered.is_set():
            entered.set()
            assert release.wait(3)

    connector.set_and_publish = pause_staging
    try:
        obj.first.put(1)
        assert entered.wait(1)
        dispatcher._connection_changed(state, obj.first, False)
        obj.first._readback = 99
        dispatcher._connection_changed(state, obj.first, True)
        with dispatcher._condition:
            assert dispatcher._condition.wait_for(
                lambda: snapshot.initialized and snapshot.readings[obj.first.name]["value"] == 99,
                timeout=1,
            )
    finally:
        release.set()
    assert dispatcher.wait_idle()
    assert connector.writes
    assert all(message.signals[obj.first.name]["value"] == 99 for _, message in connector.writes)


def test_retirement_cleans_subscription_installed_after_admission_closed(
    setup_dispatcher: DispatcherSetup,
) -> None:
    """Remove a late driver subscription when retirement wins the installation race.

    Args:
        setup_dispatcher (DispatcherSetup): Dispatcher, recording connector, and device factory.
    """
    dispatcher, _, setup = setup_dispatcher
    obj = setup().obj
    entered, release = threading.Event(), threading.Event()
    subscribe = obj.first.subscribe
    errors: list[Exception] = []

    def observer(**_kwargs: Any) -> None:
        """Accept events for the extra binding used by this race.

        Args:
            **_kwargs (Any): Unused callback payload.
        """

    def paused_subscribe(callback: Callable[..., None], **kwargs: Any) -> int:
        """Pause after ophyd installs the binding but before dispatcher ownership.

        Args:
            callback (Callable[..., None]): Callback to install.
            **kwargs (Any): Subscription options passed through to ophyd.

        Returns:
            int: Newly installed subscription identifier.
        """
        cid = subscribe(callback, **kwargs)
        entered.set()
        assert release.wait(3)
        return cid

    def install() -> None:
        """Attempt installation while retaining its expected retirement error."""
        try:
            dispatcher.subscribe(obj.first, observer)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    worker = threading.Thread(target=install)
    with mock.patch.object(obj.first, "subscribe", side_effect=paused_subscribe):
        try:
            worker.start()
            assert entered.wait(1)
            assert dispatcher.remove(obj, timeout=0.1)
        finally:
            release.set()
            worker.join(3)
    assert len(errors) == 1 and isinstance(errors[0], RuntimeError)
    assert not obj.first._callbacks[obj.first.SUB_VALUE]
