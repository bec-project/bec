"""Coalesce device events without reading hardware or Redis on callback threads.

The dispatcher is composed into the device manager. Its fixed daemon workers own
compatibility reads and publication; callback work consists only of capturing a
payload and updating bounded state. Explicit reads share the per-device operation
gate through :meth:`DeviceEventDispatcher.read_context`.
"""

from __future__ import annotations

import copy
import inspect
import threading
import time
import weakref
from collections import Counter, OrderedDict
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import ophyd
from ophyd.signal import EpicsSignalBase
from ophyd_devices.sim.sim_signals import ReadOnlySignal
from ophyd_devices.utils.dynamic_pseudo import ComputedSignal
from redis.client import Pipeline
from redis.exceptions import WatchError

from bec_lib import messages
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector

if TYPE_CHECKING:
    from bec_server.device_server.devices.devicemanager import DSDevice

# The token is an internal component of the dispatcher and ophyd exposes domain
# traversal through _get_components_of_kind rather than a public equivalent.
# pylint: disable=protected-access
# Keep this component together; its typed docstrings account for the extra lines.
# pylint: disable=too-many-lines

logger = bec_logger.logger
_MISSING = object()
Domain = Literal["readback", "configuration", "limits", "status"]
Readings = dict[str, dict[Literal["value", "timestamp"], Any]]
Metadata = dict[str, Any]
SubscriptionKey = tuple[ophyd.OphydObject, Callable[..., None], str | None]
_DOMAINS: tuple[Domain, ...] = ("readback", "configuration", "limits", "status")
_ENDPOINTS: dict[Domain, Callable[[str], EndpointInfo]] = {
    "readback": MessageEndpoints.device_readback,
    "configuration": MessageEndpoints.device_read_configuration,
    "limits": MessageEndpoints.device_limits,
    "status": MessageEndpoints.device_status,
}


@dataclass
class _Coverage:
    """Field mapping established when a device is registered.

    Attributes:
        fields (dict[int, str]): Signal identities mapped to emitted reading keys.
        signals (dict[int, ophyd.Signal]): Signals used to inspect connection state.
        patch_fields (set[int]): Signal identities whose callback values match readings.
        cached_timestamps (set[int]): Signals whose callbacks use a known cached timestamp.
        trusted (bool): Whether the domain's read semantics are fully understood.
    """

    fields: dict[int, str] = field(default_factory=dict)
    signals: dict[int, ophyd.Signal] = field(default_factory=dict)
    patch_fields: set[int] = field(default_factory=set)
    cached_timestamps: set[int] = field(default_factory=set)
    trusted: bool = True


@dataclass(frozen=True)
class _Publication:
    """Retain a snapshot version for retries and Redis batching.

    Attributes:
        state (_DeviceState): Registered device identity owning this publication.
        domain (Domain): Snapshot domain represented by the message.
        version (int): Captured version acknowledged after successful publication.
        readings (Readings): Captured field records whose mutable values may remain shared.
        metadata (Metadata): Metadata captured for this version.
    """

    state: _DeviceState
    domain: Domain
    version: int
    readings: Readings
    metadata: Metadata


@dataclass
class _Snapshot:
    """Hold mutable domain state under the dispatcher's condition lock.

    Attributes:
        domain (Domain): Reading or status domain represented by this snapshot.
        coverage (_Coverage): Mapping and trust information for callback updates.
        readings (Readings): Latest complete reading and captured field updates.
        metadata (Metadata): Metadata associated with the latest event.
        subscribed (set[int]): Signal identities with successful value subscriptions.
        field_versions (dict[str, int]): Latest callback version for each reading key.
        version (int): Latest locally captured event version.
        published (int): Latest version acknowledged by Redis or an explicit write.
        dirty_version (int | None): Latest invalidation requiring a compatibility read.
        invalidated_version (int): Latest baseline reset, used to reject staged old readings.
        initialized (bool): Whether a valid complete baseline exists.
        callback_only (bool): Whether subscribed callbacks cover the complete reading.
        candidate (_Publication | None): Completed version available for publication.
        refresh_retry_at (float): Monotonic deadline after a failed compatibility read.
        publish_retry_at (float): Monotonic deadline after a failed Redis publication.
        refresh_failures (int): Consecutive compatibility read failures.
        publish_failures (int): Consecutive Redis publication failures.
    """

    domain: Domain
    coverage: _Coverage
    readings: Readings = field(default_factory=dict)
    metadata: Metadata = field(default_factory=dict)
    subscribed: set[int] = field(default_factory=set)
    field_versions: dict[str, int] = field(default_factory=dict)
    version: int = 0
    published: int = 0
    dirty_version: int | None = None
    invalidated_version: int = 0
    initialized: bool = False
    callback_only: bool = False
    candidate: _Publication | None = None
    refresh_retry_at: float = 0
    publish_retry_at: float = 0
    refresh_failures: int = 0
    publish_failures: int = 0

    def update_coverage(self) -> None:
        """Recompute callback coverage after subscriptions or baseline changes."""
        self.callback_only = (
            self.initialized
            and self.coverage.trusted
            and set(self.readings) == set(self.coverage.fields.values())
            and set(self.coverage.fields) <= self.subscribed
        )

    def invalidate(self, metadata: Metadata) -> None:
        """Invalidate the baseline after a connection change.

        Args:
            metadata (Metadata): Current device metadata to capture for the invalidation.
        """
        metadata = copy.deepcopy(metadata)
        self.version += 1
        self.dirty_version = self.version
        self.invalidated_version = self.version
        self.initialized = self.callback_only = False
        self.candidate = None
        self.metadata = metadata
        self.refresh_retry_at = 0

    def record(self, source: int, value: Any, timestamp: Any, metadata: Metadata) -> None:
        """Apply a callback and mark any required compatibility read.

        Args:
            source (int): Identity of the signal or device that emitted the callback.
            value (Any): Callback payload retained by reference, or the missing-value sentinel.
            timestamp (Any): Source measurement timestamp, or the missing-value sentinel.
            metadata (Metadata): Current device metadata to capture with the event.

        Raises:
            TypeError: A moving-state payload cannot be converted to an integer.
            ValueError: A moving-state payload does not represent an integer.
        """
        metadata = copy.deepcopy(metadata)
        if self.domain == "status":
            value = int(value)
        self.version += 1
        self.metadata = metadata
        if self.domain == "status":
            self.readings = {"status": {"value": value}}
            self.initialized = True
            return
        key = self.coverage.fields.get(source)
        usable = value is not _MISSING and (
            self.domain == "limits" or timestamp is not _MISSING and timestamp is not None
        )
        if key is not None and usable and source in self.coverage.patch_fields:
            record: dict[Literal["value", "timestamp"], Any] = {"value": value}
            if self.domain != "limits":
                record["timestamp"] = timestamp
            self.readings[key] = record
            self.field_versions[key] = self.version
        if not self.callback_only or key is not None and not usable:
            self.dirty_version = self.version

    def merge_read(self, readings: Readings, version: int) -> None:
        """Adopt a complete read while preserving callbacks captured after it began.

        Args:
            readings (Readings): Reading structure transferred to the snapshot. Its
                values may still be shared with the driver.
            version (int): Snapshot version captured before the read started.
        """
        for key, field_version in self.field_versions.items():
            if field_version > version and key in readings:
                readings[key] = self.readings[key]
        self.readings = readings
        self.initialized = True
        if self.dirty_version is not None and self.dirty_version <= version:
            self.dirty_version = None
        self.update_coverage()

    def capture(self, state: _DeviceState, version: int, metadata: Metadata) -> _Publication:
        """Retain a publishable version while newer callbacks continue.

        Args:
            state (_DeviceState): Registered device identity owning the snapshot.
            version (int): Version represented by the completed reading.
            metadata (Metadata): Already captured metadata, shared without further mutation.

        Returns:
            _Publication: Captured reading and metadata retained for publication or retry.
        """
        self.candidate = _Publication(
            state=state,
            domain=self.domain,
            version=version,
            readings=self.readings.copy(),
            metadata=metadata,
        )
        return self.candidate

    def acknowledge(self, version: int) -> None:
        """Confirm only versions whose Redis writes succeeded.

        Args:
            version (int): Successfully published version to acknowledge.
        """
        self.published = max(self.published, version)
        if self.candidate is not None and self.candidate.version <= self.published:
            self.candidate = None
        self.publish_retry_at = 0
        self.publish_failures = 0


@dataclass
class _DeviceState:
    """Track one device identity and its operation and subscription lifetime.

    Attributes:
        device (DSDevice): Device-server wrapper supplying configuration and metadata.
        obj (ophyd.OphydObject): Root hardware object for this registered identity.
        name (str): Device name used by existing Redis endpoints.
        domains (dict[Domain, _Snapshot]): Independent reading and status snapshots.
        gate (threading.RLock): Operation gate shared by refresh, explicit reads, and writes.
        subscriptions (dict[SubscriptionKey, int]): Owned callbacks and subscription handles.
        requested_subscriptions (set[SubscriptionKey] | None): Bindings retained by an active
            configuration refresh, or None outside refresh.
        active (bool): Whether initialization is complete and dispatch is enabled.
        retired (bool): Whether this identity rejects new work and completed reads.
        refreshing (bool): Whether a refresh worker currently owns this root.
        publishing (bool): Whether a Redis transaction currently owns this root's lifetime.
        refreshed_domains (set[Domain]): Domains refreshed since the last external event,
            preventing synthetic read callbacks from cycling between custom domains.
        disconnected (set[int]): Signal identities currently reported disconnected.
        connection_epoch (int): Connection revision used to reject stale read results.
    """

    device: DSDevice
    obj: ophyd.OphydObject
    name: str
    domains: dict[Domain, _Snapshot]
    gate: threading.RLock = field(default_factory=threading.RLock)
    subscriptions: dict[SubscriptionKey, int] = field(default_factory=dict)
    requested_subscriptions: set[SubscriptionKey] | None = None
    active: bool = False
    retired: bool = False
    refreshing: bool = False
    publishing: bool = False
    refreshed_domains: set[Domain] = field(default_factory=set)
    disconnected: set[int] = field(default_factory=set)
    connection_epoch: int = 0


class ReadToken:
    """Merge a successful explicit read when its operation context exits.

    Attributes:
        dispatcher (DeviceEventDispatcher): Dispatcher coordinating the operation.
        state (_DeviceState | None): Registered identity, or None for unregistered use.
        domain (Domain): Snapshot domain updated by this explicit operation.
        result (tuple[Readings, Metadata, bool] | None): Reading structure, metadata, and live flag.
        version (int): Snapshot version captured when the operation starts.
        connection_epoch (int): Connection revision captured when the operation starts.
    """

    def __init__(
        self, dispatcher: DeviceEventDispatcher, state: _DeviceState | None, domain: Domain
    ) -> None:
        """Capture the snapshot version protected by the operation context.

        Args:
            dispatcher (DeviceEventDispatcher): Dispatcher coordinating the explicit read.
            state (_DeviceState | None): Registered identity, or None for a harmless token.
            domain (Domain): Reading domain updated when the token commits.
        """
        self.dispatcher = dispatcher
        self.state = state
        self.domain: Domain = domain
        self.result: tuple[Readings, Metadata, bool] | None = None
        with dispatcher._condition:
            self.version = state.domains[domain].version if state is not None else 0
            self.connection_epoch = state.connection_epoch if state is not None else 0

    def update(
        self, signals: Readings, metadata: Metadata | None = None, live: bool = True
    ) -> None:
        """Record data after the explicit Redis write succeeds.

        Copy the reading structure while retaining its values by reference. Capture
        metadata separately so scan and point identifiers stay associated with the
        read. Buffered results only acknowledge an older version; their contents
        never enter the live snapshot.

        Args:
            signals (Readings): Complete reading returned by the device.
            metadata (Metadata | None): Metadata used for the explicit publication.
            live (bool): Whether the result is a live reading rather than a failure buffer.
        """
        if self.state is None or self.state.retired:
            return
        if not live:
            self.result = ({}, {}, False)
            return
        self.result = (
            {name: reading.copy() for name, reading in signals.items()},
            copy.deepcopy(metadata or {}),
            True,
        )

    def commit(self) -> None:
        """Merge and acknowledge the captured version after successful explicit I/O."""
        if self.state is None or self.result is None:
            return
        readings, metadata, live = self.result
        with self.dispatcher._condition:
            if self.state.retired:
                return
            snapshot = self.state.domains[self.domain]
            if live and self.connection_epoch == self.state.connection_epoch:
                snapshot.merge_read(readings, self.version)
                if snapshot.version == self.version:
                    snapshot.metadata = metadata
            # Older event publications must not undo this explicit Redis write.
            snapshot.acknowledge(self.version)
            self.dispatcher._sync_pending(self.state, self.domain)


class DeviceEventDispatcher:
    """Coordinate bounded event capture, compatibility reads, and publication.

    Attributes:
        _max_batch_size (int): Maximum snapshots in one Redis pipeline execution.
        _get_connector (Callable[[], RedisConnector]): Accessor for the current connector.
        _retry_delay (float): Initial failure retry delay in seconds.
        _max_retry_delay (float): Maximum failure retry delay in seconds.
        _condition (threading.Condition): Short state lock and worker notification channel.
        _states (dict[int, _DeviceState]): Registered roots indexed by object identity.
        _retired (weakref.WeakValueDictionary[int, ophyd.OphydObject]): Retired identities.
        _stopping (bool): Whether admission has closed for shutdown.
        _started (bool): Whether the daemon worker threads have been started.
        _pending (OrderedDict[tuple[int, Domain], None]): Deduplicated, ordered pending work.
        _threads (list[threading.Thread]): Fixed refresh workers and the single publisher.
        _read_origins (threading.local): Roots and domains being read by the current thread.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        get_connector: Callable[[], RedisConnector],
        workers: int = 4,
        *,
        retry_delay: float = 0.05,
        max_retry_delay: float = 5.0,
        max_batch_size: int = 256,
    ) -> None:
        """Allocate bounded workers and event state without starting any threads.

        Args:
            get_connector (Callable[[], RedisConnector]): Return the current manager connector.
            workers (int): Maximum simultaneous compatibility reads; defaults to four.
            retry_delay (float): Initial failure retry delay in seconds; defaults to 0.05.
            max_retry_delay (float): Maximum failure retry delay in seconds; defaults to five.
            max_batch_size (int): Maximum snapshots per Redis pipeline; defaults to 256.

        Raises:
            ValueError: The worker count or publication batch size is less than one.
        """
        if workers < 1:
            raise ValueError("The dispatcher needs at least one refresh worker")
        if max_batch_size < 1:
            raise ValueError("The publication batch size must be positive")
        self._max_batch_size = max_batch_size
        self._get_connector = get_connector
        self._retry_delay = retry_delay
        self._max_retry_delay = max_retry_delay
        self._condition = threading.Condition()
        self._states: dict[int, _DeviceState] = {}
        self._retired: weakref.WeakValueDictionary[int, ophyd.OphydObject] = (
            weakref.WeakValueDictionary()
        )
        self._stopping = False
        self._started = False
        self._pending: OrderedDict[tuple[int, Domain], None] = OrderedDict()
        self._read_origins = threading.local()
        self._threads = [
            threading.Thread(target=self._refresh_loop, daemon=True, name=f"DeviceRefresh-{index}")
            for index in range(workers)
        ]
        self._threads.append(
            threading.Thread(target=self._publish_loop, daemon=True, name="DeviceEventPublisher")
        )

    @staticmethod
    def _root(obj: ophyd.OphydObject) -> ophyd.OphydObject:
        """Resolve a callback source to its root device identity.

        Args:
            obj (ophyd.OphydObject): Device or signal supplying the root reference.

        Returns:
            ophyd.OphydObject: Root object, or the supplied object when no root exists.
        """
        return getattr(obj, "root", None) or obj

    @staticmethod
    def _coverage(obj: ophyd.OphydObject, domain: Domain) -> _Coverage:
        """Inspect domain fields and classify conservative callback coverage.

        Args:
            obj (ophyd.OphydObject): Root object whose read behavior is inspected.
            domain (Domain): Reading or status domain to classify.

        Returns:
            _Coverage: Field mappings and evidence permitting raw callback updates.
        """
        coverage = _Coverage()
        if domain == "status":
            return coverage
        if domain == "limits":
            for attr, name in (("low_limit_travel", "low"), ("high_limit_travel", "high")):
                signal = getattr(obj, attr, None)
                if signal is None:
                    coverage.trusted = False
                    continue
                coverage.fields[id(signal)] = name
                coverage.signals[id(signal)] = signal
                trusted = DeviceEventDispatcher._trusted_signal(signal)
                coverage.trusted &= trusted
                if trusted:
                    coverage.patch_fields.add(id(signal))
                    if type(signal).get in (ReadOnlySignal.get, ComputedSignal.get):
                        coverage.cached_timestamps.add(id(signal))
                coverage.trusted &= bool(getattr(signal, "_auto_monitor", False))
            return coverage
        method = "read" if domain == "readback" else "read_configuration"
        kind = ophyd.Kind.normal if domain == "readback" else ophyd.Kind.config

        def visit(node: ophyd.OphydObject, can_patch: bool = True) -> None:
            """Collect selected leaves while retaining every ancestor's read semantics.

            Args:
                node (ophyd.OphydObject): Current device or signal in the domain traversal.
                can_patch (bool): Whether ancestor reads permit direct callback field patches.
            """
            if isinstance(node, ophyd.Signal):
                coverage.fields[id(node)] = node.name
                coverage.signals[id(node)] = node
                trusted = DeviceEventDispatcher._trusted_signal(node)
                coverage.trusted &= trusted
                if trusted and can_patch:
                    coverage.patch_fields.add(id(node))
                if trusted and type(node).get in (ReadOnlySignal.get, ComputedSignal.get):
                    coverage.cached_timestamps.add(id(node))
                # Root Signals already have a value subscription in the manager.
                coverage.trusted &= bool(getattr(node, "_auto_monitor", node is obj))
                return
            if not isinstance(node, ophyd.Device):
                coverage.trusted = False
                return
            trusted = getattr(getattr(node, method), "__func__", None) is getattr(
                ophyd.Device, method
            )
            coverage.trusted &= trusted
            for _, child in node._get_components_of_kind(kind):
                visit(child, can_patch=can_patch and trusted)

        visit(obj)
        counts = Counter(coverage.fields.values())
        ambiguous = {key for key, name in coverage.fields.items() if counts[name] > 1}
        if ambiguous:
            coverage.trusted = False
            coverage.patch_fields.difference_update(ambiguous)
        return coverage

    @staticmethod
    def _trusted_signal(signal: ophyd.OphydObject) -> bool:
        """Check that a signal exposes supported standard read behavior.

        Args:
            signal (ophyd.OphydObject): Candidate signal to inspect.

        Returns:
            bool: Whether callback values and timestamps can represent its reading.
        """
        if not isinstance(signal, ophyd.Signal):
            return False
        cls = type(signal)
        return (
            cls.read is ophyd.Signal.read
            and cls.read_configuration is ophyd.Signal.read_configuration
            and cls.get
            in (ophyd.Signal.get, EpicsSignalBase.get, ReadOnlySignal.get, ComputedSignal.get)
            and inspect.getattr_static(cls, "timestamp")
            in (
                inspect.getattr_static(ophyd.Signal, "timestamp"),
                EpicsSignalBase.timestamp,
                ReadOnlySignal.timestamp,
            )
        )

    def register(self, device: DSDevice) -> None:
        """Allocate inactive snapshots and connection subscriptions without reading.

        Args:
            device (DSDevice): Device-server wrapper to register by root object identity.

        Raises:
            RuntimeError: The same root identity is still retiring.
        """
        obj = device.obj
        domains: dict[Domain, _Snapshot] = {
            domain: _Snapshot(
                domain=domain,
                coverage=(
                    self._coverage(obj, domain) if self._supports_read(obj, domain) else _Coverage()
                ),
            )
            for domain in _DOMAINS
        }
        state = _DeviceState(device=device, obj=obj, name=device.name, domains=domains)
        with self._condition:
            if self._stopping:
                return
            previous = self._states.get(id(obj))
            if previous is not None:
                if previous.retired:
                    raise RuntimeError(f"Device {device.name} is still retiring")
                return
            self._retired.pop(id(obj), None)
            self._states[id(obj)] = state
        self._subscribe_connections(state)

    def _subscribe_connections(self, state: _DeviceState) -> None:
        """Retain one metadata subscription for each selected signal.

        Args:
            state (_DeviceState): Registered root whose coverage selects the signals.
        """
        signals = {
            key: signal
            for snapshot in state.domains.values()
            for key, signal in snapshot.coverage.signals.items()
        }
        for signal in signals.values():
            if not hasattr(signal, "SUB_META"):
                continue
            try:

                self.subscribe(
                    signal,
                    self._on_connection_changed,
                    event_type=signal.SUB_META,
                    run=False,
                    domain="status",
                )
            # Driver subscription hooks can raise arbitrary exceptions.
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"Cannot watch connection state for {signal.name}: {exc}")

    def _on_connection_changed(
        self, *, obj: ophyd.Signal, connected: bool | None = None, **_kwargs: Any
    ) -> None:
        """Route connection metadata to the current registered device identity.

        Args:
            obj (ophyd.Signal): Signal whose metadata changed.
            connected (bool | None): Reported connection state, or None when absent.
            **_kwargs (Any): Unused metadata supplied by ophyd.
        """
        with self._condition:
            state = self._states.get(id(self._root(obj)))
            if state is not None and connected is not None:
                self._connection_changed(state, obj, connected)

    def _connection_changed(
        self, state: _DeviceState, signal: ophyd.Signal, connected: bool
    ) -> None:
        """Invalidate affected snapshots when a signal changes connection state.

        Args:
            state (_DeviceState): Registered identity owning the signal.
            signal (ophyd.Signal): Signal reporting a connection change.
            connected (bool): Whether the signal is currently connected.
        """
        with self._condition:
            if self._stopping or state.retired:
                return
            if not any(id(signal) in s.coverage.fields for s in state.domains.values()):
                return
            was_connected = id(signal) not in state.disconnected
            if bool(connected) == was_connected:
                return
            state.connection_epoch += 1
            state.refreshed_domains = set()
            if connected:
                state.disconnected.discard(id(signal))
            else:
                state.disconnected.add(id(signal))
            for domain, snapshot in state.domains.items():
                if domain == "status" or id(signal) not in snapshot.coverage.fields:
                    continue
                if domain == "configuration" and isinstance(state.obj, ophyd.Signal):
                    continue
                snapshot.invalidate(state.device.metadata)
                self._sync_pending(state, domain)
            self._condition.notify_all()

    def subscribe(  # pylint: disable=too-many-arguments
        self,
        signal: ophyd.OphydObject,
        callback: Callable[..., None],
        *,
        event_type: str | None = None,
        run: bool = False,
        domain: Domain | None = None,
    ) -> int:
        """Reuse an existing callback binding or install and own a new one.

        Args:
            signal (ophyd.OphydObject): Object owning the subscription handle.
            callback (Callable[..., None]): Callback registered with the signal.
            event_type (str | None): Event name, or None for the object's default event.
            run (bool): Whether a newly installed binding replays its cached event.
            domain (Domain | None): Covered domain, or None to consider all mapped domains.

        Returns:
            int: Existing or newly installed ophyd subscription identifier.

        Raises:
            RuntimeError: The signal's root is not registered or has been retired.
        """
        key = (signal, callback, event_type)
        with self._condition:
            state = self._states.get(id(self._root(signal)))
            if state is None or state.retired:
                raise RuntimeError(f"Device {self._root(signal).name} is not registered")
            cid = state.subscriptions.get(key)
        if cid is None:
            cid = signal.subscribe(callback, event_type=event_type, run=run)
        with self._condition:
            retired = (
                self._stopping or state.retired or self._states.get(id(state.obj)) is not state
            )
            if not retired:
                state.subscriptions[key] = cid
                if state.requested_subscriptions is not None:
                    state.requested_subscriptions.add(key)
                for name in _DOMAINS if domain is None else (domain,):
                    snapshot = state.domains[name]
                    if id(signal) in snapshot.coverage.fields:
                        snapshot.subscribed.add(id(signal))
                        snapshot.update_coverage()
        if retired:
            self._unsubscribe([(signal, cid)])
            raise RuntimeError(f"Device {state.name} was retired during subscription")
        return cid

    @contextmanager
    def reconfigure(self, obj: ophyd.OphydObject) -> Generator[None, None, None]:
        """Refresh coverage while keeping working callbacks until replacement succeeds.

        Args:
            obj (ophyd.OphydObject): Initialized root whose configuration changed.

        Yields:
            None: Scope in which the manager requests bindings and seeds new baselines.

        Raises:
            TimeoutError: An active hardware operation did not release the root within five seconds.
        """
        with self.read_context(obj, timeout=5):
            state = self._states[id(self._root(obj))]
            coverage = {
                domain: (
                    self._coverage(state.obj, domain)
                    if self._supports_read(state.obj, domain)
                    else _Coverage()
                )
                for domain in _DOMAINS
            }
            with self._condition:
                state.requested_subscriptions = set()
                state.refreshed_domains = set()
                for domain, snapshot in state.domains.items():
                    if domain == "status":
                        continue
                    snapshot.coverage = coverage[domain]
                    snapshot.subscribed.clear()
                    snapshot.readings.clear()
                    snapshot.field_versions.clear()
                    snapshot.invalidate(state.device.metadata)
                    if not self._supports_read(state.obj, domain):
                        snapshot.dirty_version = None
                        snapshot.acknowledge(snapshot.version)
                    self._sync_pending(state, domain)
                selected = {key for item in coverage.values() for key in item.fields}
                state.disconnected.intersection_update(selected)
            try:
                self._subscribe_connections(state)
                yield
                with self._condition:
                    obsolete = set(state.subscriptions) - state.requested_subscriptions
                    subscriptions = [(key[0], state.subscriptions.pop(key)) for key in obsolete]
                self._unsubscribe(subscriptions)
            finally:
                with self._condition:
                    state.requested_subscriptions = None
                    self._condition.notify_all()

    @staticmethod
    def _supports_read(obj: ophyd.OphydObject, domain: Domain) -> bool:
        """Check which snapshot domains can obtain a baseline from this root.

        Args:
            obj (ophyd.OphydObject): Registered root object.
            domain (Domain): Snapshot domain to inspect.

        Returns:
            bool: Whether the root supports the domain's compatibility read.
        """
        if domain == "status" or domain == "configuration" and isinstance(obj, ophyd.Signal):
            return False
        return domain != "limits" or all(
            getattr(obj, attr, None) is not None
            for attr in ("low_limit_travel", "high_limit_travel")
        )

    def activate(self, obj: ophyd.OphydObject) -> None:
        """Enable dispatch and schedule retries for any missing supported baselines.

        Args:
            obj (ophyd.OphydObject): Device or signal resolving to the registered root.
        """
        with self._condition:
            state = self._states.get(id(self._root(obj)))
            if self._stopping or state is None or state.retired:
                return
            state.active = True
            for domain, snapshot in state.domains.items():
                snapshot.update_coverage()
                if snapshot.initialized or not self._supports_read(state.obj, domain):
                    continue
                snapshot.invalidate(state.device.metadata)
                self._sync_pending(state, domain)
            if not self._started:
                self._started = True
                for thread in self._threads:
                    thread.start()
            self._condition.notify_all()

    def seed(
        self,
        obj: ophyd.OphydObject,
        domain: Domain,
        signals: Readings,
        metadata: Metadata | None = None,
    ) -> None:
        """Install an already-published baseline for a snapshot domain.

        Prefer ``read_context`` when callbacks can race with the baseline read.

        Args:
            obj (ophyd.OphydObject): Device or signal resolving to the registered root.
            domain (Domain): Domain represented by the baseline.
            signals (Readings): Complete reading already written to Redis.
            metadata (Metadata | None): Metadata associated with the baseline publication.

        Raises:
            RuntimeError: The dispatcher is stopped or the device identity is retired.
        """
        with self.read_context(obj, domain) as token:
            token.update(signals, metadata)

    @contextmanager
    def read_context(
        self, obj: ophyd.OphydObject, domain: Domain = "readback", *, timeout: float | None = None
    ) -> Generator[ReadToken, None, None]:
        """Serialize an explicit read and its Redis writes with event I/O.

        Perform the read and Redis writes inside this context, then call ``token.update``.
        Callback capture never acquires this gate. Unregistered devices receive a token
        that leaves dispatcher state unchanged.

        Args:
            obj (ophyd.OphydObject): Device or signal resolving to the root being read.
            domain (Domain): Domain updated by the operation; defaults to readback.
            timeout (float | None): Maximum seconds to wait for the operation gate, or None
                for the existing unbounded explicit-read behavior.

        Yields:
            ReadToken: Token that commits the successful reading when the context exits.

        Raises:
            RuntimeError: The dispatcher is stopped or the root is retired before the read.
            TimeoutError: The operation gate was not acquired within the supplied timeout.
        """
        root = self._root(obj)
        with self._condition:
            state = self._states.get(id(root))
            if self._stopping:
                raise RuntimeError("Device event dispatcher is shut down")
            if (state is not None and state.retired) or self._retired.get(id(root)) is root:
                raise RuntimeError(f"Device {root.name} has been retired")
        if state is None:
            yield ReadToken(self, None, domain)
            return
        if not state.gate.acquire(timeout=-1 if timeout is None else max(0, timeout)):
            raise TimeoutError(f"Device {state.name} still has an active device operation")
        try:
            with self._condition:
                if state.retired:
                    raise RuntimeError(f"Device {state.name} has been retired")
            token = ReadToken(self, state, domain)
            yield token
            token.commit()
        finally:
            state.gate.release()
            # Notify after releasing the gate: a publisher may be waiting for it.
            with self._condition:
                self._condition.notify_all()

    def enqueue(  # pylint: disable=too-many-arguments
        self,
        obj: ophyd.OphydObject,
        domain: Domain = "readback",
        *,
        value: Any = _MISSING,
        timestamp: Any = _MISSING,
        **_kwargs: Any,
    ) -> None:
        """Capture one event without hardware I/O, Redis I/O, or an operation gate.

        Args:
            obj (ophyd.OphydObject): Callback source, resolved by root object identity.
            domain (Domain): Reading or status domain; defaults to readback.
            value (Any): Source payload, or the missing-value sentinel when absent.
            timestamp (Any): Measurement timestamp, or the missing-value sentinel when absent.
            **_kwargs (Any): Unused additional ophyd callback fields.

        Raises:
            TypeError: A moving-state payload cannot be converted to an integer.
            ValueError: A moving-state payload does not represent an integer.
        """
        if domain == "status" and value is _MISSING:
            return
        root = self._root(obj)
        if domain != "status" and not getattr(root, "connected", True):
            return
        if domain == "configuration" and isinstance(root, ophyd.Signal):
            return
        with self._condition:
            state = self._states.get(id(root))
            if self._stopping or state is None or state.retired:
                return
            snapshot = state.domains[domain]
            if domain != "status" and obj is not root and id(obj) not in snapshot.coverage.fields:
                return
            if (
                domain in ("readback", "configuration")
                and obj is not root
                and not getattr(obj, "_auto_monitor", False)
            ):
                return
            read_generated = id(obj) in snapshot.coverage.cached_timestamps
            if read_generated and (timestamp is _MISSING or timestamp is None):
                timestamp = obj.timestamp
            origin = getattr(self._read_origins, "active", None)
            synthetic = read_generated and origin is not None and origin[0] == id(root)
            dirty_version = snapshot.dirty_version
            snapshot.record(id(obj), value, timestamp, state.device.metadata)
            if not synthetic:
                # Replace rather than clear: an in-flight read retains the previous pass.
                state.refreshed_domains = set()
            if synthetic and (
                origin[1] == domain
                or id(obj) in snapshot.coverage.patch_fields
                or domain in state.refreshed_domains
            ):
                # A custom other domain still needs one read when its fields cannot be patched.
                snapshot.dirty_version = dirty_version
            self._sync_pending(state, domain)
            self._condition.notify_all()

    def _delay(self, failures: int) -> float:
        """Calculate the bounded exponential delay for a failed operation.

        Args:
            failures (int): Number of consecutive failures, starting at one.

        Returns:
            float: Delay in seconds, capped by the configured retry maximum.
        """
        return min(self._max_retry_delay, self._retry_delay * 2 ** min(failures - 1, 16))

    def _read(self, state: _DeviceState, domain: Domain) -> Readings:
        """Perform the root compatibility read selected by a refresh worker.

        Args:
            state (_DeviceState): Registered identity owning the device to read.
            domain (Domain): Readback, configuration, or limits domain requiring refresh.

        Returns:
            Readings: Complete root reading or both existing low/high limit records.
        """
        self._read_origins.active = (id(state.obj), domain)
        try:
            if domain in ("readback", "configuration"):
                method = "read" if domain == "readback" else "read_configuration"
                return getattr(state.obj, method)()
            return {
                limit: {"value": getattr(state.obj, f"{limit}_limit_travel").get()}
                for limit in ("low", "high")
            }
        finally:
            self._read_origins.active = None

    def _sync_pending(self, state: _DeviceState, domain: Domain) -> None:
        """Synchronize one domain's deduplicated pending marker under the state lock.

        Args:
            state (_DeviceState): Registered identity owning the snapshot.
            domain (Domain): Domain whose captured and published versions are compared.
        """
        key = (id(state.obj), domain)
        snapshot = state.domains[domain]
        if not state.retired and snapshot.version > snapshot.published:
            self._pending.setdefault(key, None)
        else:
            self._pending.pop(key, None)

    def _retry_timeout(
        self, attribute: Literal["refresh_retry_at", "publish_retry_at"], now: float
    ) -> float | None:
        """Calculate the next retry wait while ordinary work uses notifications.

        Args:
            attribute (Literal["refresh_retry_at", "publish_retry_at"]): Retry deadline field.
            now (float): Current monotonic time in seconds.

        Returns:
            float | None: Seconds until the next future retry, or None for an untimed wait.
        """
        deadlines = (
            getattr(self._states[key].domains[domain], attribute)
            for key, domain in self._pending
            if self._states[key].active
            and (domain == "status" or not self._states[key].disconnected)
        )
        return min((deadline - now for deadline in deadlines if deadline > now), default=None)

    def _next_refresh(self, now: float) -> tuple[_DeviceState, Domain] | None:
        """Reserve an eligible refresh whose operation gate can be acquired immediately.

        Args:
            now (float): Current monotonic time used to check failure retry deadlines.

        Returns:
            tuple[_DeviceState, Domain] | None: Root with its gate held and domain, or None
                if no refresh can start immediately.
        """
        for key, domain in self._pending:
            state = self._states[key]
            snapshot = state.domains[domain]
            if not state.active or state.retired or state.refreshing or state.disconnected:
                continue
            if snapshot.dirty_version is None or snapshot.refresh_retry_at > now:
                continue
            if not state.gate.acquire(blocking=False):
                continue
            state.refreshing = True
            self._pending.move_to_end((key, domain))
            return state, domain
        return None

    def _refresh_loop(self) -> None:
        """Run compatibility reads until shutdown, retaining each failed domain for retry."""
        while True:
            with self._condition:
                if self._stopping:
                    return
                now = time.monotonic()
                work = self._next_refresh(now)
                if work is None:
                    self._condition.wait(self._retry_timeout("refresh_retry_at", now))
                    continue
            state, domain = work
            try:
                with self._condition:
                    snapshot = state.domains[domain]
                    if state.retired or snapshot.dirty_version is None:
                        continue
                    version = snapshot.version
                    connection_epoch = state.connection_epoch
                    metadata = snapshot.metadata
                    refreshed_domains = state.refreshed_domains
                if not getattr(state.obj, "connected", True):
                    raise ConnectionError(f"Device {state.name} is disconnected")
                readings = self._read(state, domain)
                if not isinstance(readings, dict):
                    raise TypeError("A device reading must be a dictionary")
                if any(
                    not isinstance(record, dict) or "value" not in record
                    for record in readings.values()
                ):
                    raise ValueError("A refresh returned invalid field records")
                readings = {name: record.copy() for name, record in readings.items()}
                with self._condition:
                    if state.retired or state.connection_epoch != connection_epoch:
                        continue
                    if not set(snapshot.readings) <= set(readings):
                        raise ValueError("A refresh omitted previously initialized reading fields")
                    snapshot.merge_read(readings, version)
                    refreshed_domains.add(domain)
                    snapshot.refresh_retry_at = 0
                    snapshot.refresh_failures = 0
                    snapshot.capture(state, version, metadata)
                    self._condition.notify_all()
            # Isolate arbitrary driver failures and retain this domain's work.
            except Exception as exc:  # noqa: BLE001
                with self._condition:
                    snapshot = state.domains[domain]
                    snapshot.refresh_failures += 1
                    snapshot.refresh_retry_at = time.monotonic() + self._delay(
                        snapshot.refresh_failures
                    )
                    first_failure = snapshot.refresh_failures == 1
                if first_failure:
                    logger.warning(
                        f"Event refresh failed for {state.name}/{domain}; retrying: {exc}"
                    )
            finally:
                state.gate.release()
                with self._condition:
                    state.refreshing = False
                    self._condition.notify_all()

    def _next_publication_batch(self, now: float) -> list[_Publication]:
        """Capture ready snapshots without holding device gates during serialization.

        Args:
            now (float): Current monotonic time used to check publication retry deadlines.

        Returns:
            list[_Publication]: Bounded batch to serialize and validate before publication.
        """
        batch = []
        for key, domain in list(self._pending):
            state = self._states[key]
            snapshot = state.domains[domain]
            if not state.active or state.retired or domain != "status" and state.disconnected:
                continue
            if snapshot.publish_retry_at > now or not snapshot.initialized:
                continue
            if snapshot.dirty_version is None:
                snapshot.capture(state, snapshot.version, snapshot.metadata)
            candidate = snapshot.candidate
            if candidate is None or candidate.version <= snapshot.published:
                continue
            if not state.gate.acquire(blocking=False):
                continue
            state.gate.release()
            batch.append(candidate)
            self._pending.move_to_end((key, domain))
            if len(batch) == self._max_batch_size:
                return batch
        return batch

    @staticmethod
    def _stage_publication(
        connector: RedisConnector, pipe: Pipeline, publication: _Publication
    ) -> None:
        """Append one snapshot's existing Redis operations to a shared pipeline.

        Args:
            connector (RedisConnector): Connector used to serialize and queue the message.
            pipe (Pipeline): Shared pipeline receiving this snapshot's commands.
            publication (_Publication): Captured reading and metadata to stage.

        """
        state, domain = publication.state, publication.domain
        if domain == "status":
            connector.set(
                MessageEndpoints.device_status(state.name),
                messages.DeviceStatusMessage(
                    device=state.name,
                    status=publication.readings["status"]["value"],
                    metadata=publication.metadata,
                ),
                pipe=pipe,
            )
            return
        endpoint = _ENDPOINTS[domain]
        msg = messages.DeviceMessage(signals=publication.readings, metadata=publication.metadata)
        connector.set_and_publish(endpoint(state.name), msg, pipe=pipe)

    def _publication_failed(
        self, publication: _Publication, exc: Exception, *, log: bool = True
    ) -> None:
        """Retain a failed snapshot and schedule its independent retry deadline.

        Args:
            publication (_Publication): Snapshot whose publication was not confirmed.
            exc (Exception): Failure reported by serialization, staging, or Redis execution.
            log (bool): Whether the first failure should be logged. Ordinary write conflicts
                retry quietly.
        """
        state, domain = publication.state, publication.domain
        with self._condition:
            snapshot = state.domains[domain]
            snapshot.publish_failures += 1
            snapshot.publish_retry_at = time.monotonic() + self._delay(snapshot.publish_failures)
            first_failure = snapshot.publish_failures == 1
        if first_failure and log:
            logger.warning(f"Event publication failed for {state.name}/{domain}; retrying: {exc}")

    def _publication_finished(self, publication: _Publication, outcomes: Sequence[Any]) -> None:
        """Acknowledge successful commands or retain this snapshot for retry.

        Args:
            publication (_Publication): Snapshot represented by the completed Redis commands.
            outcomes (Sequence[Any]): Redis results for this snapshot's commands.
        """
        error = next((outcome for outcome in outcomes if isinstance(outcome, Exception)), None)
        if error is not None:
            self._publication_failed(publication, error)
            return
        state, domain = publication.state, publication.domain
        with self._condition:
            if state.retired:
                return
            state.domains[domain].acknowledge(publication.version)
            self._sync_pending(state, domain)

    def _reserve_publication(self, publication: _Publication) -> bool:
        """Validate a watched candidate and reserve its device lifetime without waiting.

        Args:
            publication (_Publication): Serialized snapshot whose destination is watched.

        Returns:
            bool: Whether this snapshot is still eligible for the Redis transaction.

        Raises:
            ConnectionError: The device disconnected without a metadata notification.
        """
        state, domain = publication.state, publication.domain
        with self._condition:
            if self._stopping or state.retired or not state.gate.acquire(blocking=False):
                return False
            try:
                snapshot = state.domains[domain]
                if (
                    publication.version <= snapshot.published
                    or publication.version < snapshot.invalidated_version
                    or not snapshot.initialized
                ):
                    return False
                if domain != "status" and state.disconnected:
                    return False
                if domain != "status" and not getattr(state.obj, "connected", True):
                    raise ConnectionError(f"Device {state.name} is disconnected")
                state.publishing = True
                return True
            finally:
                state.gate.release()

    def _execute_publications(
        self, pipe: Pipeline, staged: list[tuple[_Publication, list[Any]]]
    ) -> bool:
        """Commit watched snapshots without holding device gates across Redis I/O.

        Args:
            pipe (Pipeline): Empty pipeline reused for this transaction.
            staged (list[tuple[_Publication, list[Any]]]): Snapshots and serialized commands.

        Returns:
            bool: False for a concurrent write conflict; True when the attempt is resolved.
                Failed I/O and skipped snapshots retain their pending markers.
        """
        reserved: list[tuple[_Publication, slice]] = []
        try:
            pipe.watch(*{_ENDPOINTS[item.domain](item.state.name).endpoint for item, _ in staged})
            pipe.multi()
            for publication, commands in staged:
                try:
                    if not self._reserve_publication(publication):
                        continue
                except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-except
                    self._publication_failed(publication, exc)
                    continue
                start = len(pipe.command_stack)
                pipe.command_stack.extend(commands)
                reserved.append((publication, slice(start, len(pipe.command_stack))))
            if not reserved:
                return True
            command_count = len(pipe.command_stack)
            results = pipe.execute(raise_on_error=False)
            if not isinstance(results, (list, tuple)) or len(results) != command_count:
                raise RuntimeError("Incomplete Redis batch acknowledgement")
            for publication, commands in reserved:
                self._publication_finished(publication, results[commands])
            return True
        except WatchError:
            return False
        except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-except
            for publication, _ in staged:
                self._publication_failed(publication, exc)
            return True
        finally:
            try:
                pipe.reset()
            finally:
                with self._condition:
                    for publication, _ in reserved:
                        publication.state.publishing = False
                    self._condition.notify_all()

    def _publish_batch(self, batch: list[_Publication]) -> None:
        """Batch serialized snapshots and isolate conflicts to individual device roots.

        Args:
            batch (list[_Publication]): Captured snapshots ready for serialization.
        """
        connector = self._get_connector()
        pipe = connector.pipeline()
        staged: list[tuple[_Publication, list[Any]]] = []
        for publication in batch:
            start = len(pipe.command_stack)
            try:
                self._stage_publication(connector, pipe, publication)
                stop = len(pipe.command_stack)
                if stop == start:
                    raise RuntimeError("No Redis commands queued for snapshot")
            except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-except
                del pipe.command_stack[start:]
                self._publication_failed(publication, exc)
                continue
            staged.append((publication, pipe.command_stack[start:stop]))
        pipe.command_stack.clear()
        if not staged:
            pipe.reset()
            return
        if self._execute_publications(pipe, staged):
            return
        # An explicit write to one root must not starve unrelated final updates.
        roots: dict[int, list[tuple[_Publication, list[Any]]]] = {}
        for item in staged:
            roots.setdefault(id(item[0].state.obj), []).append(item)
        for publications in roots.values():
            if not self._execute_publications(pipe, publications):
                for publication, _ in publications:
                    self._publication_failed(publication, WatchError(), log=False)

    def _publish_loop(self) -> None:
        """Publish ready batches until shutdown, retaining failed work for retry."""
        while True:
            with self._condition:
                if self._stopping:
                    return
                now = time.monotonic()
                batch = self._next_publication_batch(now)
                if not batch:
                    self._condition.wait(self._retry_timeout("publish_retry_at", now))
                    continue
            try:
                self._publish_batch(batch)
            # Connector setup failures must also retain the batch for retry.
            except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-except
                for publication in batch:
                    self._publication_failed(publication, exc)
            finally:
                with self._condition:
                    self._condition.notify_all()

    def remove(self, obj: ophyd.OphydObject, timeout: float = 5.0) -> bool:
        """Quiesce and retire one identity before destroying or replacing it.

        Args:
            obj (ophyd.OphydObject): Device or signal resolving to the root being removed.
            timeout (float): Maximum seconds to wait for the root's active operation.

        Returns:
            bool: Whether destruction is safe. A timeout leaves a running device subscribed
                so a rejected configuration change cannot silently disable its updates.
                Shutdown retires all identities immediately and never waits for their gates.
        """
        deadline = time.monotonic() + timeout
        with self._condition:
            state = self._states.get(id(self._root(obj)))
            if state is None:
                return True
            if self._stopping:
                timeout = 0
                deadline = time.monotonic()
        if not state.gate.acquire(timeout=max(0, timeout)):
            return False
        try:
            with self._condition:
                while state.publishing:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._condition.wait(remaining)
                state.retired = True
                self._retired[id(state.obj)] = state.obj
                subscriptions = [(key[0], cid) for key, cid in state.subscriptions.items()]
                state.subscriptions.clear()
                for domain in state.domains:
                    self._pending.pop((id(state.obj), domain), None)
                if self._states.get(id(state.obj)) is state:
                    del self._states[id(state.obj)]
                self._condition.notify_all()
        finally:
            state.gate.release()
        self._unsubscribe(subscriptions)
        return True

    @staticmethod
    def _unsubscribe(subscriptions: list[tuple[ophyd.OphydObject, int]]) -> None:
        """Release every supplied binding even when one driver's cleanup fails.

        Args:
            subscriptions (list[tuple[ophyd.OphydObject, int]]): Objects and subscription IDs.
        """
        for signal, cid in subscriptions:
            try:
                signal.unsubscribe(cid)
            # A failed driver cleanup must not skip the remaining subscriptions.
            except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-except
                logger.warning(f"Failed to remove event subscription for {signal.name}: {exc}")

    def is_idle(self, obj: ophyd.OphydObject | None = None) -> bool:
        """Check for pending events, refreshes, or publications.

        Args:
            obj (ophyd.OphydObject | None): Root or signal to inspect, or None for all roots.

        Returns:
            bool: Whether every selected identity has no pending or active event work.
        """
        with self._condition:
            states = list(self._states.values())
            if obj is not None:
                state = self._states.get(id(self._root(obj)))
                states = [] if state is None else [state]
            return all(
                not (state.refreshing or state.publishing)
                and all(s.version <= s.published for s in state.domains.values())
                for state in states
            )

    def wait_idle(self, timeout: float = 5.0, obj: ophyd.OphydObject | None = None) -> bool:
        """Wait for admitted events to finish within a bounded timeout.

        Args:
            timeout (float): Maximum seconds to wait; defaults to five.
            obj (ophyd.OphydObject | None): Root or signal to inspect, or None for all roots.

        Returns:
            bool: Whether the selected work became idle before the timeout expired.
        """
        deadline = time.monotonic() + timeout
        with self._condition:
            while not self.is_idle(obj):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def shutdown(self, timeout: float = 5.0) -> list[str]:
        """Stop admission and join workers within one bounded deadline.

        Args:
            timeout (float): Total worker join budget in seconds; defaults to five.

        Returns:
            list[str]: Names of workers still blocked in driver or Redis I/O. Daemon workers
                do not make interpreter shutdown wait for a hung driver.
        """
        deadline = time.monotonic() + timeout
        with self._condition:
            self._stopping = True
            for state in self._states.values():
                state.retired = True
                self._retired[id(state.obj)] = state.obj
            objects = [state.obj for state in self._states.values()]
            self._condition.notify_all()
        for obj in objects:
            self.remove(obj, timeout=0)
        for thread in self._threads:
            if thread.ident is not None:
                thread.join(max(0, deadline - time.monotonic()))
        stuck = [thread.name for thread in self._threads if thread.is_alive()]
        if stuck:
            logger.warning(f"Device event workers did not stop before the deadline: {stuck}")
        return stuck
