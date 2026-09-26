"""Hardware-independent tracking and publication of device instruction results."""

from __future__ import annotations

import enum
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from bec_lib import messages
from bec_lib.bec_errors import ExceptionWithErrorInfo
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.serialization import json_ext
from bec_server.device_server.device_status import DeviceStatus

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger


class ResponseState(str, enum.Enum):
    """State published for an aggregate instruction response."""

    COMPLETED = "completed"
    ERROR = "error"
    RUNNING = "running"


@dataclass
class _PendingRequest:
    """Aggregate state protected by the handler lock."""

    instruction: messages.DeviceInstructionMessage
    num_status_objects: int
    status_objects: list[DeviceStatus] = field(default_factory=list)
    completed_callbacks: int = 0
    error_info: messages.ErrorInfo | None = None


@dataclass(frozen=True)
class RequestSnapshot:
    """Read-only description of the statuses attached to a pending instruction."""

    status_objects: tuple[DeviceStatus, ...]
    num_status_objects: int


class RequestHandler:
    """Associate instructions with statuses and publish their settled outcomes.

    Mutations and publications hold the handler lock. Callback registration runs outside
    that lock because statuses may invoke callbacks immediately or from another thread.
    An event-loop backend must marshal callbacks to a worker before entering this
    synchronous publication path. Private record helpers require the lock to be held.
    """

    def __init__(self, connector: RedisConnector) -> None:
        self.connector = connector
        self._storage: dict[str, _PendingRequest] = {}
        self._lock = threading.RLock()
        self._closed = False
        self._stopped_requests: deque[str] = deque(maxlen=50)

    def add_request(
        self,
        instr: messages.DeviceInstructionMessage,
        num_status_objects: int,
        done: bool = False,
        success: bool | None = None,
    ) -> None:
        """Register an instruction before its statuses can call back.

        Args:
            instr (messages.DeviceInstructionMessage): Instruction being executed.
            num_status_objects (int): Expected status count, including pending registrations.
            done (bool): Whether execution finished synchronously.
            success (bool | None): Required outcome for synchronous completion.
        """
        if done and success is None:
            raise ValueError("If the instruction is done, the success status must be set.")
        with self._lock:
            if self._closed:
                return
            request = _PendingRequest(instr, num_status_objects)
            self._storage[instr.metadata["device_instr_id"]] = request
            if done:
                self._finish(request, success)
            else:
                self._send_response(request, success, done=False)

    def has_request(self, instr_id: str) -> bool:
        """Return whether an instruction is pending.

        Args:
            instr_id (str): Instruction identifier.
        Returns:
            bool: Whether this handler still tracks the instruction.
        """
        with self._lock:
            return instr_id in self._storage

    def get_request(self, instr_id: str) -> RequestSnapshot | None:
        """Return a frozen snapshot without exposing the mutable pending record.

        Args:
            instr_id (str): Instruction identifier.
        Returns:
            RequestSnapshot | None: Current registrations, if the request exists.
        """
        with self._lock:
            request = self._storage.get(instr_id)
            if request is None:
                return None
            return RequestSnapshot(tuple(request.status_objects), request.num_status_objects)

    def patch_num_status_objects(
        self, instr: messages.DeviceInstructionMessage, num_status_objects: int
    ) -> None:
        """Set the actual status count after dispatching an instruction.

        Args:
            instr (messages.DeviceInstructionMessage): Executed instruction.
            num_status_objects (int): Actual status count.
        """
        with self._lock:
            request = self._storage.get(instr.metadata["device_instr_id"])
            if request is not None:
                request.num_status_objects = num_status_objects
                self._complete_if_ready(request)

    def remove_request(self, instr_id: str) -> None:
        """Remove aggregate tracking while retaining per-device completion publication.

        Args:
            instr_id (str): Instruction identifier.
        """
        with self._lock:
            self._storage.pop(instr_id, None)

    def clear(self) -> None:
        """Remove aggregate tracking without discarding registered status callbacks."""
        with self._lock:
            self._storage.clear()

    def shutdown(self) -> None:
        """Drain active publication and suppress all subsequent response publication."""
        with self._lock:
            self._closed = True
            self._storage.clear()

    def add_stopped_request(self, stop_id: str) -> None:
        """Suppress aggregate responses associated with a stopped request or scan.

        Args:
            stop_id (str): Request, scan or queue identifier.
        """
        with self._lock:
            self._stopped_requests.append(stop_id)

    def add_status_object(
        self, instruction: messages.DeviceInstructionMessage, status_obj: DeviceStatus
    ) -> None:
        """Associate a status with an instruction and observe its settled completion.

        Args:
            instruction (messages.DeviceInstructionMessage): Instruction producing the status.
            status_obj (DeviceStatus): Status invoking its callback once after finalization.
        """
        instr_id = instruction.metadata["device_instr_id"]
        with self._lock:
            if self._closed:
                return
            request = self._storage.get(instr_id)
            if request is not None:
                request.status_objects.append(status_obj)
                if len(request.status_objects) == 1:
                    try:
                        self._send_response(request, success=False, done=False)
                    except Exception:  # pylint: disable=broad-except
                        # The operation has started; a failed progress notification must
                        # not prevent observation of its eventual outcome.
                        logger.exception(f"Failed to publish running status for {instr_id}.")

        def on_update(status: DeviceStatus) -> None:
            with self._lock:
                if not self._closed:
                    self._status_completed(request, instruction, status)

        # A completed status may invoke on_update before add_callback returns.
        status_obj.add_callback(on_update)

    def set_finished(
        self,
        instr_id: str,
        success: bool | None = None,
        error_info: messages.ErrorInfo | None = None,
        result: Any = None,
    ) -> None:
        """Explicitly finish a pending instruction.

        Args:
            instr_id (str): Instruction identifier.
            success (bool | None): Outcome, or None to derive it from attached statuses.
            error_info (messages.ErrorInfo | None): Failure details, when available.
            result (Any): Synchronous result returned by the instruction.
        """
        with self._lock:
            request = self._storage.get(instr_id)
            if request is not None:
                self._finish(request, success, error_info, result)

    def finish_if_untracked(
        self,
        instr_id: str,
        success: bool,
        error_info: messages.ErrorInfo | None = None,
        result: Any = None,
    ) -> bool:
        """Finish an RPC instruction only when no status owns its completion.

        Args:
            instr_id (str): Instruction identifier.
            success (bool): Synchronous RPC outcome.
            error_info (messages.ErrorInfo | None): Failure details, when available.
            result (Any): Synchronous RPC result.
        Returns:
            bool: Whether a pending instruction without attached statuses was completed.
        """
        with self._lock:
            request = self._storage.get(instr_id)
            if request is None or request.status_objects:
                return False
            self._finish(request, success, error_info, result)
            return True

    def _status_completed(
        self,
        request: _PendingRequest | None,
        instruction: messages.DeviceInstructionMessage,
        status: DeviceStatus,
    ) -> None:
        error = status.exception()
        if error is None and not status.success:
            error = RuntimeError(f"The status {status.status_type} completed unsuccessfully.")
        error_info = self.get_error_info(error, status, instruction) if error is not None else None
        try:
            self._publish_device_response(instruction, status, error_info)
        except Exception as exc:  # pylint: disable=broad-except
            error_info = self.get_error_info(exc, status, instruction)
            logger.exception(f"Failed to publish completion for device {status.device_name}.")

        # Publication remains independent of aggregate existence. A replacement with the
        # same ID belongs to another operation and cannot consume this completion.
        current = self._storage.get(instruction.metadata["device_instr_id"])
        if request is not None and current is request:
            request.completed_callbacks += 1
            if request.error_info is None:
                request.error_info = error_info
            self._complete_if_ready(request)

    def _publish_device_response(
        self,
        instruction: messages.DeviceInstructionMessage,
        status: DeviceStatus,
        error_info: messages.ErrorInfo | None,
    ) -> None:
        if not instruction.metadata.get("response"):
            return
        request_id = instruction.metadata["RID"]
        metadata = {"action": instruction.action, **instruction.metadata, "error_info": error_info}
        response = messages.DeviceReqStatusMessage(
            device=status.device_name,
            success=error_info is None,
            request_id=request_id,
            metadata=metadata,
        )
        self.connector.xadd(
            MessageEndpoints.device_req_status(request_id), {"data": response}, expire=3600
        )

    def _complete_if_ready(self, request: _PendingRequest) -> None:
        if (
            len(request.status_objects) == request.num_status_objects
            and request.completed_callbacks == request.num_status_objects
        ):
            self._finish(request, request.error_info is None, request.error_info)

    def _finish(
        self,
        request: _PendingRequest,
        success: bool | None,
        error_info: messages.ErrorInfo | None = None,
        result: Any = None,
    ) -> None:
        if success is None:
            success = request.error_info is None and all(
                status.success for status in request.status_objects
            )
        if not success and error_info is None:
            error_info = request.error_info or messages.ErrorInfo(
                error_message="Device instruction completed unsuccessfully.",
                compact_error_message="Device instruction completed unsuccessfully.",
                exception_type="DeviceInstructionError",
            )
        self._send_response(request, success, done=True, error_info=error_info, result=result)
        self._storage.pop(request.instruction.metadata["device_instr_id"], None)

    @staticmethod
    def get_error_info(
        error: Exception, status: DeviceStatus, instruction: messages.DeviceInstructionMessage
    ) -> messages.ErrorInfo:
        """Build failure details from a neutral status and its registered instruction.

        Args:
            error (Exception): Operation or publication failure.
            status (DeviceStatus): Status identifying the device and backend status type.
            instruction (messages.DeviceInstructionMessage): Instruction producing the status.
        Returns:
            messages.ErrorInfo: Failure details using the existing BEC message contract.
        """
        if isinstance(error, ExceptionWithErrorInfo):
            return error.error_info
        device_name = status.device_name
        message = (
            f"{error.__class__.__name__}: {error}\n"
            f"The status {status.status_type} from device {device_name} failed during the execution "
            f"of the following instruction:\n{json_ext.dumps(instruction, indent=2)}\n"
        )
        compact_message = f"{error.__class__.__name__}: {error}"
        if instruction.action:
            compact_message = (
                f"An error occurred during '{instruction.action}' on device '{device_name}'.\n\n"
                f"{compact_message}"
            )
        return messages.ErrorInfo(
            error_message=message,
            compact_error_message=compact_message,
            exception_type=error.__class__.__name__,
            device=device_name,
        )

    def _send_response(
        self,
        request: _PendingRequest,
        success: bool | None,
        done: bool,
        error_info: messages.ErrorInfo | None = None,
        result: Any = None,
    ) -> None:
        instruction = request.instruction
        metadata = instruction.metadata
        if any(
            metadata.get(key) in self._stopped_requests for key in ("RID", "scan_id", "queue_id")
        ):
            return
        state = ResponseState.RUNNING
        if done:
            state = ResponseState.COMPLETED if success else ResponseState.ERROR
        response = messages.DeviceInstructionResponse(
            device=instruction.device,
            status=state.value,
            error_info=error_info,
            instruction_id=metadata["device_instr_id"],
            instruction=instruction,
            result=result,
            result_is_status=True if request.status_objects else None,
            metadata=metadata,
        )
        self.connector.send(MessageEndpoints.device_instructions_response(), response)
