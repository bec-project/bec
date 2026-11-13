"""Backend-independent contracts for device execution and event delivery.

These are internal device-server interfaces, not Redis message schemas. Scheduling,
acquisition, and publication have separate ownership. Providers never choose Redis
endpoints or perform Redis writes through these contracts.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import CancelledError
from dataclasses import dataclass
from enum import Enum, Flag, auto
from threading import Event
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeAlias, TypeVar

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_server.device_server.devices.ds_device import DSDevice

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
Readings: TypeAlias = Mapping[str, Mapping[Literal["value", "timestamp"], Any]]


class DeviceKind(Flag):
    """BEC data roles, independent of backend enums and their numeric representation.

    OMITTED has no cache inclusion, but does not prohibit direct operations
    or BEC payload events. HINTED is a presentation flag; reading inclusion is
    represented separately by READOUT. Roles do not imply backend capabilities or
    prescribe refresh timing. Provider mapping preserves backend-specific policies.
    """

    OMITTED = 0
    READOUT = auto()
    CONFIGURATION = auto()
    HINTED = auto()


class RefreshSection(Enum):
    """Device cache sections that can be refreshed independently."""

    READBACK = "readback"
    CONFIGURATION = "configuration"
    LIMITS = "limits"


@dataclass(frozen=True, eq=False)
class DeviceTarget:
    """A root wrapper and optional relative backend path.

    Wrapper identity distinguishes replacement devices with the same name. A nested
    target is not a new registered DSDevice, and need not own a separate cache.
    Aliases may use an execution-local wrapper for an unmanaged native root.
    """

    device: DSDevice
    path: str = ""

    def __eq__(self, other: object) -> bool:
        """Compare wrapper identity and relative path, independently of device name equality."""
        if not isinstance(other, DeviceTarget):
            return NotImplemented
        return self.device is other.device and self.path == other.path

    def __hash__(self) -> int:
        """Hash the stable wrapper identity and path for scheduler lookup keys."""
        return hash((id(self.device), self.path))

    @property
    def name(self) -> str:
        """Return the requested root or nested target name."""
        return f"{self.device.name}.{self.path}" if self.path else self.device.name


class Completion(Protocol[T_co]):
    """Future-compatible backend completion implemented directly by concurrent.futures.Future.

    Callbacks may run inline on registration or on the completing thread. Native
    adapters retain backend callback ordering and exception policy. Result access
    follows Future semantics: timeout=None may block, timeout=0 never waits.
    Cancellation describes execution, not confirmation that hardware stopped.
    """

    def done(self) -> bool:
        """Return whether execution reached a terminal state."""

    def cancelled(self) -> bool:
        """Return whether execution was cancelled."""

    def result(self, timeout: float | None = None) -> T_co:
        """Return the result or raise its error, cancellation, or a wait TimeoutError."""

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Return the error; raise CancelledError on cancellation or TimeoutError on expiry."""

    def add_done_callback(self, fn: Callable[[Completion[T_co]], object], /) -> None:
        """Register a callback, including when execution is already complete."""


def wait_for_completion(completion: Completion[T]) -> T:
    """Wait at the coordinator boundary, preserving native callback-delivery timing.

    Registering a callback while pending retains the existing wait boundary: native
    terminal state can precede queued callback delivery during failure cleanup.
    """
    if not completion.done():
        finished = Event()
        completion.add_done_callback(lambda _completion: finished.set())
        finished.wait()
    if completion.cancelled():
        raise CancelledError("Device execution was cancelled")
    return completion.result(timeout=0)


class ReadbackRefresh(Protocol):  # pylint: disable=too-few-public-methods
    """Ingress for temporary readback notifications, separate from acquisition."""

    def __call__(self, target: DeviceTarget, *, inline: bool = False) -> None:
        """Queue a coalescible refresh by default, without waiting on a backend loop.

        The provider supplies the resolved target before any initial notification.
        Classic providers can explicitly request inline acquisition to retain native
        callback lock ownership and initial-subscription publication order.
        """


class Subscription(Protocol):  # pylint: disable=too-few-public-methods
    """An owned subscription whose idempotent close can complete asynchronously."""

    def close(self) -> Completion[None]:
        """Remove subscriptions and report completion after backend cleanup finishes."""


class MessageCategory(Enum):
    """BEC payload-event categories, independent of native signal classes and kind."""

    FILE_EVENT = "file_event"
    PROGRESS = "progress"
    PREVIEW = "preview"
    DYNAMIC_DATA = "dynamic_data"


@dataclass(frozen=True)
class DeviceMessageEvent:
    """
    Carry a native payload to the BEC publisher.

    Attributes:
        target (DeviceTarget): Registered device and native path used to check subscription
            ownership.
        category (MessageCategory): Payload category that selects the publication path.
        signal_name (str): Wire name of the source signal.
        message (messages.BECMessage): Payload to publish without coalescing.
        signal_info (messages.SignalInfo | None): Native signal metadata, when available.
        wire_device_name (str | None): Native wire name when it differs from the registered
            device name.
    """

    target: DeviceTarget
    category: MessageCategory
    signal_name: str
    message: messages.BECMessage
    signal_info: messages.SignalInfo | None = None
    wire_device_name: str | None = None


class DeviceEventSink(Protocol):
    """Shared BEC ingress for refresh notifications and individual payload events."""

    def request_refresh(self, device: DSDevice, sections: frozenset[RefreshSection]) -> None:
        """Submit a coalescible refresh notification without acquiring data inline."""

    def publish_refresh(self, device: DSDevice, section: RefreshSection) -> None:
        """Read and publish a cache section on the caller thread."""

    def publish_motion(self, device: DSDevice, status: int) -> None:
        """Publish the current native motion state."""

    def refresh_device_info(self, device: DSDevice) -> None:
        """Publish an updated description after native signal metadata changes."""

    def submit_message(self, event: DeviceMessageEvent) -> None:
        """Accept one payload event and apply BEC context policy at ingress."""
