"""Adapt native Ophyd statuses without attaching BEC instruction state to them."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import TimeoutError

from ophyd import OphydObject, StatusBase
from ophyd.utils.errors import WaitTimeoutError

from bec_server.device_server.device_providers.device_interfaces import Completion, DeviceTarget
from bec_server.device_server.instruction_execution import InstructionExecution


class OphydCompletion:
    """Expose Ophyd's native terminal state through the shared completion contract."""

    def __init__(self, status: StatusBase) -> None:
        self._status = status

    def done(self) -> bool:
        """Expose native terminal state even while earlier callbacks are still queued."""
        return self._status.done

    def cancelled(self) -> bool:
        """Return False; classic Ophyd has no distinct execution-cancelled state."""
        return False

    def result(self, timeout: float | None = None) -> None:
        """Return None or raise the native error, with Future-compatible timeout semantics."""
        error = self.exception(timeout=timeout)
        if error is not None:
            raise error

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Inspect native failure without waiting for callback cleanup."""
        try:
            return self._status.exception(timeout=timeout)
        except WaitTimeoutError as exc:
            raise TimeoutError() from exc

    def add_done_callback(self, fn: Callable[[Completion[None]], object]) -> None:
        """Preserve native callback order and inline exceptions for terminal statuses."""
        self._status.add_callback(lambda _native: fn(self))


def to_instruction_execution(
    status: StatusBase, target: DeviceTarget
) -> InstructionExecution[None]:
    """Adapt a validated native status, retaining callback policy and diagnostic ownership.

    Args:
        status (StatusBase): Native status returned by an Ophyd operation.
        target (DeviceTarget): Resolved operation target, independent of the status owner.

    Returns:
        InstructionExecution[None]: Adapted status, including an already completed status.
    """
    owner = getattr(status, "device", None)
    if owner is None:
        candidate = getattr(status, "obj", None)
        owner = candidate if isinstance(candidate, OphydObject) else None
    name = None
    if owner is not None:
        name = getattr(owner, "dotted_name", "") or getattr(owner, "name", None)
    return InstructionExecution(
        OphydCompletion(status),
        target,
        status_type=type(status).__name__,
        error_device_name=name,
        callback_inline=True,
    )
