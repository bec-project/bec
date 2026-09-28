"""Execute one v4 scan without reading or changing its scheduling queue."""

from __future__ import annotations

import threading
import traceback
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Literal

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.logger import bec_logger

from .device_lock_registry import DeviceLockRegistry
from .errors import DeviceInstructionError, ScanAbortion, UserScanInterruption
from .scan_queue import ExitInfoType, InstructionQueueStatus
from .scans.scan_base import ScanBase

# ScanActions and DeviceManager expose the existing scan hooks as internal methods.
# pylint: disable=protected-access

logger = bec_logger.logger

SCAN_SEQUENCE = (
    "prepare_scan",
    "open_scan",
    "stage",
    "pre_scan",
    "scan_core",
    "post_scan",
    "unstage",
    "close_scan",
)
ScanOutcome = Literal["completed", "aborted", "shutdown"]


@dataclass
class ScanControl:
    """Thread-safe queue-to-task controls for one executing scan."""

    run_on_exception_hook: bool
    exit_info: ExitInfoType | None = None
    _status: InstructionQueueStatus = InstructionQueueStatus.RUNNING
    _shutdown: bool = False
    _condition: threading.Condition = field(default_factory=threading.Condition, repr=False)

    def set_status(self, status: InstructionQueueStatus) -> None:
        """Send a status change to the running scan."""
        with self._condition:
            self._status = status
            self._condition.notify_all()

    def set_cleanup_enabled(self, enabled: bool) -> None:
        """Update the exception-cleanup policy, for example when halting."""
        with self._condition:
            self.run_on_exception_hook = enabled

    def stop(self, exit_info: ExitInfoType | None = None, *, shutdown: bool = False) -> None:
        """Request cooperative interruption; shutdown skips exception cleanup."""
        with self._condition:
            if self.exit_info is None:
                self.exit_info = exit_info
            self._shutdown |= shutdown
            self._status = InstructionQueueStatus.STOPPED
            self._condition.notify_all()

    def prepare_cleanup(self) -> bool:
        """Allow exception cleanup unless queue shutdown is in progress."""
        with self._condition:
            if self._shutdown:
                return False
            self._status = InstructionQueueStatus.RUNNING
            return True

    def checkpoint(self, on_pause: Callable[[], None]) -> None:
        """Wait through pause, then raise if the queue requested a stop."""
        with self._condition:
            paused = self._status == InstructionQueueStatus.PAUSED
        if paused:
            on_pause()
        with self._condition:
            self._condition.wait_for(lambda: self._status != InstructionQueueStatus.PAUSED)
            if self._status == InstructionQueueStatus.STOPPED:
                if self.exit_info is None:
                    raise ScanAbortion()
                raise UserScanInterruption(exit_info=self.exit_info)


@dataclass(frozen=True)
class ScanTask:
    """The future and control channel for one submitted scan."""

    future: Future[ScanOutcome]
    control: ScanControl


class DirectScanWorker:
    """Run the v4 lifecycle and report its terminal outcome to a future."""

    def __init__(
        self,
        *,
        scan: ScanBase,
        control: ScanControl,
        on_status: Callable[[], None],
        device_lock_registry: DeviceLockRegistry | None = None,
    ) -> None:
        self.scan = scan
        self.control = control
        self.on_status = on_status
        self.device_lock_registry = device_lock_registry
        self.connector = scan.redis_connector
        self.device_manager = scan.device_manager

    def run(self) -> ScanOutcome:
        """Execute the scan and all cleanup before completing the future."""
        scan = self.scan
        scan.actions._interruption_callback = self.check_for_interruption
        scan.actions._update_queue_info_callback = self.on_status
        try:
            with self.device_manager._rpc_method(scan.actions.rpc_call):
                self.check_for_interruption()
                scan.actions._initialize_scan()
                for step in SCAN_SEQUENCE:
                    method = getattr(scan, step, None)
                    if method is None:
                        raise ScanAbortion(f"Scan is missing required method: {step}")
                    self.check_for_interruption()
                    method()
            return "completed"
        except Exception as exc:
            if not self.control.prepare_cleanup():
                return "shutdown"
            cleanup_succeeded = self._run_on_exception_hook(exc)
            if cleanup_succeeded and not isinstance(exc, ScanAbortion):
                self._raise_alarm(exc)
            self._publish_abortion(exc)
            return "aborted"
        finally:
            self._release_scan_locks()
            scan.actions._interruption_callback = None
            scan.actions._update_queue_info_callback = None

    def check_for_interruption(self) -> None:
        """Apply pause and stop commands at a scan checkpoint."""
        self.control.checkpoint(lambda: self.scan.actions._send_scan_status("paused"))

    def _run_on_exception_hook(self, exc: Exception) -> bool:
        """Run scan cleanup and report a failure in the cleanup hook."""
        if not self.control.run_on_exception_hook:
            return True
        hook = getattr(self.scan, "on_exception", None)
        if not callable(hook):
            return True
        try:
            self.scan._shutdown_event.clear()
            self.scan.actions._metadata_suffix = "__on-exception"
            with self.device_manager._rpc_method(self.scan.actions.rpc_call):
                hook(exc.__cause__ or exc)
        except Exception as cleanup_exc:
            self.scan.actions.send_client_info("")
            logger.exception("Failed to run direct scan on_exception hook")
            self._raise_alarm(cleanup_exc)
            return False
        return True

    def _publish_abortion(self, exc: Exception) -> None:
        exit_info = (
            exc.exit_info if isinstance(exc, UserScanInterruption) else self.control.exit_info
        )
        if exit_info:
            self.scan.actions._send_scan_status(exit_info[0], reason=exit_info[1])
        else:
            status = "aborted" if self.control.run_on_exception_hook else "halted"
            self.scan.actions._send_scan_status(status, reason="alarm")

    def _raise_alarm(self, exc: Exception) -> None:
        error_info = (
            exc.error_info
            if isinstance(exc, DeviceInstructionError)
            else messages.ErrorInfo(
                error_message=traceback.format_exc(),
                compact_error_message=f"{type(exc).__name__}: {exc}",
                exception_type=type(exc).__name__,
                device=None,
            )
        )
        metadata = {
            key: value
            for key, value in {
                "scan_id": self.scan.scan_info.scan_id,
                "scan_number": self.scan.scan_info.scan_number,
            }.items()
            if value is not None
        }
        self.connector.raise_alarm(severity=Alarms.MAJOR, info=error_info, metadata=metadata)

    def _release_scan_locks(self) -> None:
        request_id = self.scan.scan_info.metadata.get("RID")
        if self.device_lock_registry is not None and request_id is not None:
            self.device_lock_registry.release_all(request_id)
