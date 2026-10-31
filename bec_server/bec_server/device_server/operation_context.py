"""Request context owned by BEC, separate from hardware completions."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from bec_lib import messages
from bec_lib.device_backend import Completion, DeviceHandler, Subscription


@dataclass(eq=False)
class OperationContext:
    """One requested target and instruction, even for delegated/shared completion."""

    completion: Completion[None]
    instruction: messages.DeviceInstructionMessage
    target: DeviceHandler
    subscription: Subscription | None = None
    staging_state: int | None = None
    refresh: bool = False
    service_error: BaseException | None = None
    service_error_info: messages.ErrorInfo | None = None
    processed: bool = False
    processing: bool = False
    callback_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    service_done: threading.Event = field(default_factory=threading.Event, repr=False)

    @property
    def done(self) -> bool:
        """Whether the producer has reached a terminal outcome."""
        return self.completion.done

    @property
    def success(self) -> bool:
        """Whether hardware and processed service effects have succeeded."""
        return self.done and self.exception(timeout=0) is None

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Return the terminal error without cancelling hardware on wait timeout."""
        return self.service_error or self.completion.exception(timeout)

    def wait(self, timeout: float | None = None) -> None:
        """Wait for hardware completion; service processing has a separate acknowledgement."""
        self.completion.result(timeout)

    def add_callback(self, callback: Callable[[OperationContext], None]) -> None:
        """Notify this observer once, including registrations after completion."""
        self.completion.add_callback(lambda _completion: callback(self))
