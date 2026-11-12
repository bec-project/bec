from __future__ import annotations

import time
import traceback
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.logger import bec_logger
from bec_server.scan_server.errors import DeviceInstructionError, ScanAbortion, UserScanInterruption
from bec_server.scan_server.scan_queue import InstructionQueueStatus
from bec_server.scan_server.scans.scan_base import ScanBase

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_server.scan_server.scan_queue import DirectInstructionQueueItem
    from bec_server.scan_server.scan_worker import ScanWorker

SCAN_SEQUENCE = [
    "prepare_scan",
    "open_scan",
    "stage",
    "pre_scan",
    "scan_core",
    "post_scan",
    "unstage",
    "close_scan",
]


class DirectScanWorker:
    """
    DirectScanWorker runs scan lifecycle methods directly.
    Instructions are sent directly to Redis by the scan itself.
    """

    def __init__(self, *, worker: ScanWorker):
        self.worker = worker
        self.scan = None
        self._active_entry: ScanBase | None = None

    def reset(self):
        """
        Reset the state of the scan worker after a scan is completed or aborted.
        """
        self.scan = None

    def process_instructions(self, queue: DirectInstructionQueueItem) -> None:
        """
        Process the instructions in the given queue item. It runs the scan and handles any exceptions that may occur during the scan execution.

        Args:
            queue (DirectInstructionQueueItem): The queue item containing the scan instructions to process.
        """
        self.worker.current_instruction_queue_item = queue

        scan = queue.move_to_next_scan()
        if scan is None:
            logger.error("No scan found in the queue item to process.")
            return

        self.run(scan)

        queue.status = InstructionQueueStatus.COMPLETED
        self.worker.current_instruction_queue_item = None
        self.reset()

    def run(self, scan: ScanBase):
        """
        Run the scan.

        Args:
            scan (ScanBase): Scan to run
        """
        self.scan = scan

        # pylint: disable=protected-access
        scan.actions._interruption_callback = self.check_for_interruption
        scan.actions._update_queue_info_callback = self.update_queue_info
        queue = self.worker.current_instruction_queue_item
        failure = None
        try:
            if scan._entry_scans:
                self._run_entry_sequence(scan)
            else:
                self._run_acquisition(scan)
        except Exception as exc:
            failure = exc
            if not self._prepare_exception_cleanup(queue, exc):
                return
            if isinstance(exc, ScanAbortion):
                raise
            self._handle_exception(exc)
        finally:
            try:
                if self._active_entry is not None:
                    self._finish_failed_entry(queue, failure)
                elif not scan._entry_scans:
                    scan.actions._hook_broadcaster.stop()
            finally:
                self._release_scan_locks(scan)

        if queue is None:
            return

        queue.status = InstructionQueueStatus.COMPLETED
        self.worker.current_instruction_queue_item = None
        self.reset()

    def _run_acquisition(self, scan: ScanBase) -> None:
        scan.actions._hook_broadcaster.start()
        with self.worker.device_manager._rpc_method(scan.actions.rpc_call):
            scan.actions._initialize_scan()
            for step in SCAN_SEQUENCE:
                method = getattr(scan, step, None)
                if not method:
                    raise ScanAbortion(f"Scan is missing required method: {step}")
                self.check_for_interruption()
                with scan.actions._scan_hook_context(step):
                    method()
                self.run_post_scan_step(step, scan)

    def _run_entry_sequence(self, parent: ScanBase) -> None:
        self.check_for_interruption()
        parent.actions._send_scan_status("open")
        for plan, entry in zip(parent.scan_info.entry_sequence, parent._entry_scans, strict=True):
            self.check_for_interruption()
            entry.scan_info.scan_number = parent.scan_info.scan_number
            entry.scan_info.dataset_number = parent.scan_info.dataset_number
            entry._shutdown_event = parent._shutdown_event
            entry.actions._shutdown_event = parent._shutdown_event
            entry.actions._sequence_parent = parent.actions
            entry.scan_info.sequence_index = plan.entry_index
            entry.scan_info.total_sequences = len(parent._entry_scans)
            entry.actions._entry_role = plan.role
            entry.actions._interruption_callback = self.check_for_interruption
            entry.actions._update_queue_info_callback = self.update_queue_info
            entry.scan_info.scan_id = plan.scan_id
            parent.actions._entries_started = plan.entry_index + 1
            self._active_entry = entry
            if self.worker.current_instruction_queue_item is not None:
                self.update_queue_info()
            self._run_acquisition(entry)
            entry.actions._hook_broadcaster.stop()
            entry.actions._send_scan_status("closed")
            self._reset_entry(entry)
            self._active_entry = None
            self._release_scan_locks(entry)

    def _finish_failed_entry(
        self, queue: DirectInstructionQueueItem | None, failure: Exception | None
    ) -> None:
        entry = self._active_entry
        try:
            status, reason = self._abortion_status(queue, failure)
            entry.actions._send_scan_status(status, reason=reason)
        finally:
            entry.actions._hook_broadcaster.stop()
            self._reset_entry(entry)
            self._active_entry = None

    def _reset_entry(self, entry: ScanBase) -> None:
        entry.scan_info.scan_id = entry.scan_info.sequence_id
        entry.scan_info.sequence_index = 0
        entry.scan_info.total_sequences = 1
        entry.actions._sequence_parent = None

    @staticmethod
    def _abortion_status(queue, exc=None):
        exit_info = exc.exit_info if isinstance(exc, UserScanInterruption) else None
        exit_info = exit_info or (queue.exit_info if queue is not None else None)
        if exit_info:
            return exit_info
        return ("aborted" if queue is None or queue.run_on_exception_hook else "halted", "alarm")

    def _release_scan_locks(self, scan: ScanBase | None) -> None:
        if scan is None:
            return
        request_id = scan.scan_info.metadata.get("RID")
        if request_id is None:
            return
        registry = getattr(self.worker.parent, "device_lock_registry", None)
        if registry is None:
            return
        registry.release_all(request_id)

    def _prepare_exception_cleanup(
        self, queue: DirectInstructionQueueItem | None, exc: Exception
    ) -> bool:
        """Prepare exception cleanup for a failed direct scan run.

        Returns True when the caller should continue propagating/handling the
        original exception, or False when shutdown/no-queue means the run can
        exit early.
        """
        if self.worker.signal_event.is_set():
            # If the signal event is set, the worker is shutting down and does
            # not need additional exception handling.
            return False
        if queue is None:
            return False
        if queue.stopped or not queue.active_scan:
            raise exc

        queue.stopped = True
        try:
            # We reset the worker to RUNNING to allow for cleanup tasks during
            # the on_exception hook.
            self.worker.status = InstructionQueueStatus.RUNNING
            scan = self._active_entry or self.scan
            if scan is not None and (self._active_entry is not None or not scan._entry_scans):
                scan.actions._metadata_suffix = "__on-exception"
                self._run_on_exception_hook(exc)
        except Exception as exc_cleanup:
            self.worker.connector.send_client_info("")
            self._handle_exception(exc_cleanup)
        return True

    def _handle_exception(self, exc: Exception):
        content = traceback.format_exc()
        logger.error(content)

        def _raise_alarm(error_info: messages.ErrorInfo):
            self.worker.connector.raise_alarm(
                severity=Alarms.MAJOR, info=error_info, metadata=self.get_metadata_for_alarm()
            )

        if isinstance(exc, DeviceInstructionError):
            _raise_alarm(error_info=exc.error_info)
            raise ScanAbortion from exc
        error_info = messages.ErrorInfo(
            error_message=content,
            compact_error_message=traceback.format_exc(limit=0),
            exception_type=exc.__class__.__name__,
            device=None,
        )
        _raise_alarm(error_info=error_info)
        raise ScanAbortion from exc

    def check_for_interruption(self):
        """
        Check if the scan has been interrupted by checking the worker's status.
        If the status is PAUSED, it waits until the status changes to RUNNING.
        If the status is STOPPED, it raises a ScanAbortion or UserScanInterruption
        exception depending on the exit_info of the current queue item.
        """
        if self.worker.status == InstructionQueueStatus.PAUSED:
            if self.scan is not None:
                if self._active_entry is not None:
                    self._active_entry.actions._send_scan_status("paused")
                    self.scan.actions._send_scan_status("paused", parent=True)
                else:
                    self.scan.actions._send_scan_status("paused")
        while self.worker.status == InstructionQueueStatus.PAUSED:
            time.sleep(0.1)
        if self.worker.status == InstructionQueueStatus.STOPPED:
            item = self.worker.current_instruction_queue_item
            if item is None or item.exit_info is None:
                raise ScanAbortion()
            raise UserScanInterruption(exit_info=item.exit_info)

    def update_queue_info(self):
        """
        Update the queue info for the current instruction queue item.
        This is used to propagate the queue status to the client during the scan execution.
        """
        logger.info(f"Updating queue info")
        self.worker.current_instruction_queue_item.parent.queue_manager.send_queue_status()

    def _propagate_error(self, content: str, exc: Exception):
        """
        Propagate the error to the client by sending a client info message and raising an alarm with the error information.

        Args:
            content (str): The error message content to send to the client and include in the alarm information.
            exc (Exception): The exception that was raised, which will be included in the alarm information.
        """

        error_info = messages.ErrorInfo(
            error_message=content,
            compact_error_message=traceback.format_exc(limit=0),
            exception_type=exc.__class__.__name__,
            device=None,
        )
        self.worker.connector.raise_alarm(
            severity=Alarms.MAJOR, info=error_info, metadata=self.get_metadata_for_alarm()
        )

    def get_metadata_for_alarm(self) -> dict:
        """
        Get metadata for the alarm based on the current scan information.
        This can include details such as the scan ID and scan number,
        which can help with debugging and identifying the context of the error.
        """
        if self.scan is None:
            return {}
        metadata = {}
        if self._active_entry is not None:
            metadata["scan_id"] = self._active_entry.scan_info.scan_id
            metadata["entry_index"] = self._active_entry.scan_info.sequence_index
        elif self.scan.scan_info.scan_id is not None:
            metadata["scan_id"] = self.scan.scan_info.scan_id
        if self.scan.scan_info.scan_number is not None:
            metadata["scan_number"] = self.scan.scan_info.scan_number
        return metadata

    def _run_on_exception_hook(self, exc: Exception):
        """
        Run the on_exception hook implemented by the scan if the current queue item has run_on_exception_hook set to True.
        The on_exception hook allows the scan to perform cleanup tasks or other actions when an exception occurs
        """
        scan = self._active_entry or self.scan
        if scan is None:
            return
        if not self.worker.current_instruction_queue_item.run_on_exception_hook:
            return
        hook_exc = exc.__cause__ if exc.__cause__ is not None else exc
        if not hasattr(scan, "on_exception") or not callable(getattr(scan, "on_exception")):
            return
        try:
            scan._shutdown_event.clear()
            with self.worker.device_manager._rpc_method(scan.actions.rpc_call):
                # pylint: disable=protected-access
                with scan.actions._scan_hook_context("on_exception"):
                    scan.on_exception(hook_exc)

        except Exception:
            scan.actions.send_client_info("")
            logger.exception("Failed to run direct scan on_exception hook")

    def _handle_scan_abortion(self, queue: DirectInstructionQueueItem, exc: ScanAbortion):
        content = traceback.format_exc()
        logger.error(content)
        if self.scan is None:
            return

        status, reason = self._abortion_status(queue, exc)
        self.scan.actions._send_scan_status(status, reason=reason)

        self._release_scan_locks(self.scan)
        queue.status = InstructionQueueStatus.STOPPED
        queue.append_to_queue_history()
        self.worker.parent.queue_manager.queues[self.worker.queue_name].abort()
        self.reset()
        self.worker.status = InstructionQueueStatus.RUNNING

    def run_post_scan_step(self, step: str, scan: ScanBase) -> None:
        """
        Run the post-scan step for the given scan step.

        Args:
            step (str): The scan step to run the post-scan method for.
            scan (ScanBase): The scan object to run the post-scan method on.
        """
        if step == "stage":
            scan.actions._broadcast_scan_device_info()
