"""BEC's service-facing hardware abstraction contract.

Providers own native device objects, execution contexts and callback bridging.
The server owns instructions, request context and publication. No HAL type is part
of this contract; a completion may be a future or an immutable/slotted observer.
"""

# Protocol bodies retain ellipses for static return-type checking.
# pylint: disable=unnecessary-ellipsis
from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, Literal, Protocol, TypeVar, runtime_checkable

from bec_lib import messages

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
Reading = dict[Literal["value", "timestamp"], Any]
Readings = dict[str, Reading]


@runtime_checkable
class Completion(Protocol[T_co]):
    """Thread-safe observation of work; waiting does not cancel hardware.

    A callback registered after completion is still delivered once. Providers
    deliver service callbacks outside their HAL owner/callback context. Callback
    failure must not change the underlying hardware result or stop another device.
    """

    @property
    def done(self) -> bool:
        """Whether work has reached a terminal outcome."""
        ...

    def result(self, timeout: float | None = None) -> T_co:
        """Return the result or raise its original failure / a bounded wait timeout."""
        ...

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Return the terminal error; a wait timeout does not stop owned work."""
        ...

    def add_callback(self, callback: Callable[[Completion[T_co]], None]) -> None:
        """Deliver one terminal notification for this registration."""
        ...


class Operation(Generic[T]):
    """Optional standard-library implementation of Completion for providers.

    Producers settle this object. Consumers need only the Completion protocol.
    A provider may supply a dispatcher so future completion merely queues service
    callbacks, never invoking them on its native owner loop.
    """

    __slots__ = ("_future", "_dispatch")

    def __init__(self, dispatch: Callable[[Callable[[], None]], None] | None = None) -> None:
        self._future: Future[T] = Future()
        self._dispatch = dispatch or (lambda callback: callback())

    @classmethod
    def finished(cls, value: T) -> Operation[T]:
        """Create an already-successful operation."""
        operation: Operation[T] = cls()
        operation.set_result(value)
        return operation

    @classmethod
    def failed(cls, error: BaseException) -> Operation[T]:
        """Create an already-failed operation without native stop side effects."""
        operation: Operation[T] = cls()
        operation.set_exception(error)
        return operation

    @property
    def done(self) -> bool:
        """Whether the producer has reached a terminal outcome."""
        return self._future.done()

    def result(self, timeout: float | None = None) -> T:
        """Return the terminal value or raise its original error or wait timeout."""
        return self._future.result(timeout)

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Return the terminal error without cancelling hardware on wait timeout."""
        return self._future.exception(timeout)

    def add_callback(self, callback: Callable[[Completion[T]], None]) -> None:
        """Notify this observer once, including registrations after completion."""
        self._future.add_done_callback(lambda _future: self._dispatch(lambda: callback(self)))

    def set_result(self, value: T) -> None:
        """Publish a successful producer result exactly once."""
        self._future.set_result(value)

    def set_exception(self, error: BaseException) -> None:
        """Publish the original producer failure exactly once."""
        self._future.set_exception(error)


@dataclass(frozen=True)
class DeviceIdentity:
    """Native-independent identity for a configured root or child target."""

    root: str
    path: str = ""
    name: str = ""

    @property
    def qualified_name(self) -> str:
        """Return the configured root and requested child path."""
        return f"{self.root}.{self.path}" if self.path else self.root


class Capability(str, Enum):
    """Optional behavior; no fake lifecycle methods are required of a HAL."""

    SIGNAL = "signal"
    STAGE = "stage"
    PRE_SCAN = "pre_scan"
    COMPLETE = "complete"
    KICKOFF = "kickoff"
    SET = "set"
    STOP = "stop"
    PROXY = "proxy"


class ReadCategory(str, Enum):
    """Cache/read membership independent of monitoring and native kind enums."""

    NORMAL = "normal"
    CONFIGURATION = "configuration"
    OMITTED = "omitted"
    MIXED = "mixed"


class EventKind(str, Enum):
    """Service actions identified by provider events."""

    READBACK = "readback"
    CONFIGURATION = "configuration"
    LIMITS = "limits"
    MOTION = "motion"
    MESSAGE = "message"


class MessageChannel(str, Enum):
    """BEC push publication channels, independent of concrete signal classes."""

    FILE = "file"
    PROGRESS = "progress"
    PREVIEW = "preview"
    DYNAMIC = "dynamic"


@dataclass(frozen=True)
class EventContext:
    """Service-owned immutable scan identity captured when a push event is received."""

    scan_id: str | None
    status: str | None


@dataclass(frozen=True)
class DeviceEvent:
    """An ordered event accepted by a provider subscription.

    Invalidations may be batched by the service. File/data messages may not be
    silently coalesced. Native objects and callback calling conventions stay in
    the provider. Subscription disposal/close prevents subsequent delivery and
    drains or explicitly accounts for accepted messages.
    """

    source: DeviceIdentity
    kind: EventKind
    batched: bool = False
    value: Any = None
    message: messages.BECMessage | None = None
    channel: MessageChannel | None = None
    signal_metadata: Mapping[str, Any] | None = None
    context: EventContext | None = None


class Subscription(Protocol):  # pylint: disable=too-few-public-methods
    """Owned event registration, independent of native subscription IDs."""

    def close(self) -> None:
        """Idempotently dispose and account for already accepted events."""
        ...


class CallbackMonitor(Protocol):
    """Optional provider diagnostics without service knowledge of callback queues."""

    @property
    def stopped(self) -> bool:
        """Whether a bounded diagnostics join finished without needing unblocking."""
        ...

    def start(self) -> None:
        """Start diagnostics."""
        ...

    def request_stop(self) -> None:
        """Request diagnostics shutdown promptly."""
        ...

    def join(self, timeout: float | None = None) -> None:
        """Wait for shutdown, optionally bounded."""
        ...


@dataclass(frozen=True)
class ConfigurationResult:
    """Whether configuration needs a manual cache refresh / metric publication."""

    refresh: bool = False
    delegated: bool = False


@dataclass(frozen=True)
class OperationMetadata:
    """Legacy RPC response metadata, not mandatory state on a HAL completion."""

    success: bool | None
    timeout: float | None = None
    done: bool = False
    settle_time: float = 0

    def as_rpc(self, rid: str | None) -> dict:
        """Project neutral metadata into the existing RPC status response."""
        return {
            "type": "status",
            "RID": rid,
            "success": self.success,
            "timeout": self.timeout,
            "done": self.done,
            "settle_time": self.settle_time,
        }


@dataclass(frozen=True)
class RPCResult:
    """Discriminated RPC payload or pending completion, already HAL-normalized."""

    value: Any = None
    completion: Completion[None] | None = None
    status: OperationMetadata | None = None
    target_path: str = ""
    refresh: bool = False
    warning: messages.ErrorInfo | None = None


@runtime_checkable
class DeviceHandler(Protocol):  # pylint: disable=too-many-public-methods
    """Bound service-facing interface for a device tree or resolved child.

    Native execution is provider-owned. Lifecycle submission is prompt for pending
    native work. Legacy sync hook execution is preserved, not silently moved to a
    new executor. Reads/connection may wait only outside HAL owner/callback contexts
    with the provider's documented bounded I/O policy. Stop remains independently
    admissible while acquisition waits for transport. Cleanup failure is observable.
    """

    @property
    def identity(self) -> DeviceIdentity:
        """Configured root, requested child path and published native name."""
        ...

    @property
    def capabilities(self) -> frozenset[Capability]:
        """Optional operations supported by this handler."""
        ...

    @property
    def connected(self) -> bool:
        """Whether the device is available and has not been closed."""
        ...

    @property
    def staged(self) -> bool:
        """Whether staging has completed according to the provider."""
        ...

    @property
    def read_category(self) -> ReadCategory:
        """Select the existing BEC read/configuration cache membership."""
        ...

    def resolve(self, path: str) -> DeviceHandler:
        """Resolve a child target, preserving requested-target identity."""
        ...

    def connect(self, *, all_signals: bool = False, timeout: float = 5) -> None:
        """Connect with bounded startup I/O, or raise the original failure."""
        ...

    def on_connected(self) -> None:
        """Apply an optional post-connection hook."""
        ...

    def describe(self, *, connected: bool = True) -> dict:
        """Return the existing BEC metadata schema; offline mode must not connect."""
        ...

    def read(self) -> Readings:
        """Return BEC readings; native kind selection is a provider detail."""
        ...

    def read_configuration(self) -> Readings:
        """Return BEC configuration readings."""
        ...

    def limits(self) -> Readings | None:
        """Return existing low/high limit readings when supported."""
        ...

    def configure(self, values: dict) -> ConfigurationResult:
        """Apply config without requiring every HAL to imitate native signal objects."""
        ...

    def set(self, value: Any) -> Completion[None]:
        """Submit a write/move; observer timeout does not stop hardware."""
        ...

    def trigger(self) -> Completion[None]:
        """Submit acquisition and report terminal native success/error."""
        ...

    def stage(self) -> Completion[None] | None:
        """Stage; None preserves an immediate legacy stage result."""
        ...

    def unstage(self) -> Completion[None] | None:
        """Unstage and acknowledge cleanup; None preserves legacy immediate results."""
        ...

    def pre_scan(self) -> Completion[None] | None:
        """Submit optional pre-scan work, or report no outstanding work."""
        ...

    def kickoff(self, configure: dict, metadata: dict) -> Completion[None] | None:
        """Start fly work; adapt legacy argument-bearing kickoff internally."""
        ...

    def complete(self) -> Completion[None] | None:
        """Complete supported work; caller retains legacy missing-result validation."""
        ...

    def stop(self) -> Completion[None]:
        """Independently submit stop; terminal success means acknowledged shutdown."""
        ...

    def subscribe(
        self,
        callback: Callable[[DeviceEvent], None],
        *,
        context: Callable[[], EventContext] | None = None,
    ) -> Subscription:
        """Deliver outside the HAL owner, sampling optional context before enqueueing.

        The context supplier must only sample immutable service state; it may run
        on an ingress/owner thread and must not perform I/O, wait or access devices.
        Actual event processing and publication run on the delivery executor.
        """
        ...

    def flush_events(self, timeout: float = 10) -> None:
        """Fence accepted delivery off the owner thread and report unobserved errors.

        Use one bounded fence, not subscription idleness, so continued production
        cannot indefinitely delay service acknowledgement. Do not mutate native
        outcomes or attribute another device's publication errors to this target.
        """
        ...

    def subscribe_readback(self, callback: Callable[[DeviceEvent], None]) -> Subscription:
        """Temporarily observe a requested nested target during an operation."""
        ...

    def runtime_info(self) -> dict[str, messages.SignalInfo]:
        """Return BEC runtime push-signal descriptions."""
        ...

    def rpc(self, path: str, args: tuple, kwargs: dict, *, rid: str | None = None) -> RPCResult:
        """Invoke exposed RPC/property access, normalizing native response types."""
        ...

    def set_property(self, path: str, value: Any) -> None:
        """Write an exposed property in the appropriate execution context."""
        ...

    def close(self) -> None:
        """Idempotently dispose events and owned resources; raise cleanup failures."""
        ...


@runtime_checkable
class DeviceProvider(Protocol):
    """HAL implementations and execution infrastructure supplied by the device library."""

    def load_class(self, specification: str) -> type:
        """Resolve an allowed configured device class through the provider."""
        ...

    def construct(
        self, device_class: type, config: dict, context: Any = None
    ) -> tuple[object, dict]:
        """Raw construction compatibility for existing devices/static-test helpers."""
        ...

    def wrap(self, device: object) -> DeviceHandler:
        """Bind a handler; raw objects are retained only for library compatibility."""
        ...

    def requires_dependencies(self, device_class: type) -> bool:
        """Whether construction needs the existing dependency-resolution pass."""
        ...

    def register_proxy(self, proxy: DeviceHandler, devices: Mapping[str, DeviceHandler]) -> None:
        """Register provider-native proxy dependencies without exposing them to BEC."""
        ...

    def callback_monitor(self) -> CallbackMonitor:
        """Create native callback diagnostics behind the neutral monitor contract."""
        ...

    def error_device(self, error: BaseException) -> str | None:
        """Identify the native owner of an error, when available."""
        ...

    def close(self) -> None:
        """Release provider-owned execution and diagnostics without leaking callbacks."""
        ...
