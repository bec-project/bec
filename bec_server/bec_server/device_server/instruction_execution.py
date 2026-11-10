"""One BEC-owned execution model for backend outcomes and instruction coordination."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import CancelledError
from typing import TYPE_CHECKING, Generic, TypeVar

from bec_server.device_server.device_providers.device_interfaces import (
    Completion,
    DeviceKind,
    DeviceTarget,
    Subscription,
    wait_for_completion,
)

if TYPE_CHECKING:
    from bec_lib import messages

T = TypeVar("T")


class InstructionExecution(Generic[T]):
    """Own one backend completion, its diagnostics, and the BEC instruction lifecycle.

    Providers create an execution with its resolved target and backend metadata.
    The coordinator binds instruction context once before installing callbacks, without
    replacing the resolved target.
    No native status or Future is modified. Backend completion is exposed separately
    from BEC callback/publication completion through completion and done respectively.
    """

    def __init__(
        self,
        completion: Completion[T],
        target: DeviceTarget,
        *,
        status_type: str | None = None,
        error_device_name: str | None = None,
        callback_inline: bool = False,
        target_kind: Callable[[], DeviceKind] | None = None,
    ) -> None:
        """
        Create an execution that tracks one native completion.

        Args:
            completion (Completion[T]): Completion supplied by the device backend.
            target (DeviceTarget): Registered device and path for the operation.
            status_type (str | None): Native status type to use in diagnostics.
            error_device_name (str | None): Native status owner to report on failure.
            callback_inline (bool): Whether BEC completion callbacks run on the invoking backend
                thread.
            target_kind (Callable[[], DeviceKind] | None): Lookup for the resolved target's current
                data role.
        """
        self.completion = completion
        self.status_type = status_type or type(completion).__name__
        self.error_device_name = error_device_name
        self.callback_inline = callback_inline
        self.target_kind = target_kind
        self.target = target
        self.response_device_name = target.name
        self._instruction: messages.DeviceInstructionMessage | None = None
        self.subscription: Subscription | None = None
        self._callback_processed = False

    def bind(self, instruction: messages.DeviceInstructionMessage) -> InstructionExecution[T]:
        """
        Bind BEC context before registering completion callbacks.

        Args:
            instruction (messages.DeviceInstructionMessage): Request that owns this execution.

        Returns:
            InstructionExecution[T]: This execution with its instruction attached.
        """
        if self._instruction is not None:
            raise RuntimeError("Instruction execution is already bound")
        self._instruction = instruction
        return self

    @property
    def instruction(self) -> messages.DeviceInstructionMessage:
        """Return the bound instruction; reject access before coordinator binding."""
        if self._instruction is None:
            raise RuntimeError("Instruction execution is not bound")
        return self._instruction

    @property
    def done(self) -> bool:
        """Gate BEC completion on callback publication for non-inline backends."""
        return self.completion.done() and (self.callback_inline or self._callback_processed)

    @property
    def success(self) -> bool:
        """Return the backend outcome without blocking on pending work."""
        return (
            self.completion.done() and not self.completion.cancelled() and self.exception() is None
        )

    def exception(self) -> BaseException | None:
        """Inspect terminal failure without waiting, normalizing cancelled Futures."""
        try:
            return self.completion.exception(timeout=0)
        except CancelledError as exc:
            return exc

    def add_callback(self, callback: Callable[[InstructionExecution[T]], None]) -> None:
        """Deliver this execution with its bound context using backend callback timing."""
        self.completion.add_done_callback(lambda _completion: callback(self))

    def mark_callback_processed(self) -> None:
        """Permit terminal instruction responses after BEC refresh/publication completes."""
        self._callback_processed = True

    def close_subscription(self) -> None:
        """
        Close the provider-owned subscription on a terminal request path.
        """
        if self.subscription is not None:
            wait_for_completion(self.subscription.close())
