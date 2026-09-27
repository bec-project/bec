"""Direct scan execution on the worker side of the queue channels."""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.logger import bec_logger

from .errors import DeviceInstructionError, ScanAbortion, UserScanInterruption
from .queue_channels import InstructionQueueStatus, ScanAssignment, ScanReport

if TYPE_CHECKING:
    from .scan_worker import ScanWorker
    from .scans.scan_base import ScanBase

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


def describe_scan(scan: ScanBase, msg: messages.ScanQueueMessage) -> messages.RequestBlock:
    """Capture a direct scan description on the thread that owns the scan.

    Args:
        scan (ScanBase): Prepared or executing direct scan owned by the caller.
        msg (messages.ScanQueueMessage): Original request.

    Returns:
        messages.RequestBlock: Independent queue-visible metadata.
    """
    info = scan.scan_info
    return messages.RequestBlock(
        msg=msg.model_copy(deep=True),
        RID=msg.metadata["RID"],
        readout_priority=info.readout_priority_modification,
        is_scan=bool(scan.is_scan),
        scan_number=info.scan_number,
        scan_id=info.scan_id,
        report_instructions=info.scan_report_instructions,
        owned_device_locks=scan.actions.get_owned_device_locks(),
        pending_device_locks=scan.actions.get_pending_device_locks(),
    ).model_copy(deep=True)


class DirectScanWorker:
    # Direct execution binds the ScanActions callbacks and its cancellation event.
    # pylint: disable=protected-access
    """Run direct lifecycle hooks without accessing mutable queue or item state."""

    def __init__(self, *, worker: ScanWorker) -> None:
        self.worker = worker
        self.scan: ScanBase | None = None
        self.assignment: ScanAssignment | None = None

    def run(self, assignment: ScanAssignment) -> ScanReport:
        """Execute a transferred scan and return its copied terminal report.

        Args:
            assignment (ScanAssignment): Scan, identity and cancellation capability.

        Returns:
            ScanReport: Terminal outcome after exception cleanup and lock release.
        """
        self.assignment = assignment
        scan = self.scan = assignment.scan
        control = assignment.control
        status = InstructionQueueStatus.COMPLETED
        error = None
        exit_info = None
        entered = False
        self._bind_scan(scan, assignment)
        try:
            self.check_for_interruption()
            with self.worker.device_manager._rpc_method(scan.actions.rpc_call):
                entered = True
                scan.actions._initialize_scan()
                self.update_queue_info()
                for step in SCAN_SEQUENCE:
                    self.check_for_interruption()
                    getattr(scan, step)()
                self.check_for_interruption()
        except Exception as exc:  # pylint: disable=broad-except
            status = InstructionQueueStatus.STOPPED
            error = str(exc)
            if entered and scan.scan_info.run_on_exception_hook and control.begin_cleanup():
                self._run_on_exception_hook(exc)
            if not control.shutdown_event.is_set():
                exit_info = control.exit_info
                if exit_info is None and isinstance(exc, UserScanInterruption):
                    exit_info = exc.exit_info  # pylint: disable=no-member
                terminal, reason = exit_info or (
                    "aborted" if scan.scan_info.run_on_exception_hook else "halted",
                    "alarm",
                )
                exit_info = (terminal, reason)
                scan.actions._send_scan_status(terminal, reason=reason)
                if not isinstance(exc, ScanAbortion):
                    self._raise_alarm(exc)
        finally:
            # Stop-device scope must be captured/sent before ownership is released.
            control.finish()
            if control.execution_event.is_set():
                status = InstructionQueueStatus.STOPPED
            try:
                self._release_scan_locks()
            except Exception as exc:  # pylint: disable=broad-except
                status, error = InstructionQueueStatus.STOPPED, str(exc)
                self._raise_alarm(exc)
        report = ScanReport(
            assignment.token,
            describe_scan(scan, assignment.msg),
            terminal=True,
            status=status,
            exit_info=control.exit_info or exit_info,
            error=error,
        )
        self.scan = None
        self.assignment = None
        return report

    def _bind_scan(self, scan: ScanBase, assignment: ScanAssignment) -> None:
        """Bind the assignment's identity and cancellation capability before any hook."""
        control = assignment.control
        scan.scan_info.metadata["queue_id"] = assignment.token.queue_id
        scan.scan_info.scan_queue = assignment.queue_name
        if assignment.scan_number is not None:
            scan.scan_info.scan_number = assignment.scan_number
            scan.scan_info.dataset_number = assignment.dataset_number
        scan._shutdown_event = control.execution_event
        scan.actions._shutdown_event = control.execution_event
        scan.actions._interruption_callback = self.check_for_interruption
        scan.actions._update_queue_info_callback = self.update_queue_info

    def check_for_interruption(self) -> None:
        """Pause cooperatively or interrupt a scan/device wait by assignment identity."""
        if self.assignment is None:
            return
        control = self.assignment.control
        if control.status == InstructionQueueStatus.PAUSED and self.scan is not None:
            self.scan.actions._send_scan_status("paused")
        control.checkpoint()

    def update_queue_info(self) -> None:
        """Send copied progress through the worker's report channel."""
        if self.scan is None or self.assignment is None:
            return
        self.worker.report(
            ScanReport(self.assignment.token, describe_scan(self.scan, self.assignment.msg))
        )

    def _run_on_exception_hook(self, exc: Exception) -> None:
        scan, assignment = self.scan, self.assignment
        if scan is None or assignment is None:
            return
        control = assignment.control
        # The old execution event stays set. Cleanup gets its own event and cannot
        # erase a newer interruption by clearing a shared shutdown signal.
        scan._shutdown_event = control.cleanup_event
        scan.actions._shutdown_event = control.cleanup_event
        scan.actions._metadata_suffix = "__on-exception"
        registry = getattr(self.worker.parent, "device_lock_registry", None)
        rid = assignment.msg.metadata.get("RID")
        if registry is not None and rid is not None:
            registry.allow_request(rid)
        try:
            self.check_for_interruption()
            with self.worker.device_manager._rpc_method(scan.actions.rpc_call):
                scan.on_exception(exc.__cause__ or exc)
        except Exception as cleanup_error:  # pylint: disable=broad-except
            if not isinstance(cleanup_error, UserScanInterruption):
                self._raise_alarm(cleanup_error)
            logger.warning(f"Exception cleanup stopped: {cleanup_error}")

    def _release_scan_locks(self) -> None:
        if self.assignment is None:
            return
        registry = getattr(self.worker.parent, "device_lock_registry", None)
        request_id = self.assignment.msg.metadata.get("RID")
        if registry is not None and request_id is not None:
            registry.release_all(request_id)
            registry.allow_request(request_id)

    def _raise_alarm(self, exc: Exception) -> None:
        metadata = self.get_metadata_for_alarm()
        if isinstance(exc, DeviceInstructionError):
            info = exc.error_info
        else:
            info = messages.ErrorInfo(
                error_message=traceback.format_exc(),
                compact_error_message=str(exc),
                exception_type=type(exc).__name__,
                device=None,
            )
        self.worker.connector.raise_alarm(severity=Alarms.MAJOR, info=info, metadata=metadata)

    def get_metadata_for_alarm(self) -> dict:
        """Return captured request identity even when lifecycle initialization failed."""
        if self.assignment is None:
            return {"queue": self.worker.queue_name}
        metadata = dict(self.assignment.msg.metadata)
        metadata["queue"] = self.assignment.queue_name
        metadata["queue_id"] = self.assignment.token.queue_id
        if self.scan is not None:
            metadata["scan_id"] = self.scan.scan_info.scan_id
            metadata["scan_number"] = self.scan.scan_info.scan_number
        return metadata
