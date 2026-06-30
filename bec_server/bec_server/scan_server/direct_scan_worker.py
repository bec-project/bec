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
    Unlike GeneratorScanWorker, it does not interpret instructions.
    Instructions are sent directly to Redis by the scan itself.
    """

    def __init__(self, *, worker: ScanWorker):
        self.worker = worker
        self.scan = None

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
        try:
            with self.worker.device_manager._rpc_method(scan.actions.rpc_call):
                for step in SCAN_SEQUENCE:
                    method = getattr(scan, step, None)
                    if not method:
                        raise ScanAbortion(f"Scan is missing required method: {step}")
                    self.check_for_interruption()
                    method()
        except ScanAbortion as exc:
            if not self._prepare_exception_cleanup(queue, exc):
                return
            raise exc
        except Exception as exc:
            if not self._prepare_exception_cleanup(queue, exc):
                return
            self._handle_exception(exc)
        finally:
            self._release_scan_locks(scan)

        if queue is None:
            return

        queue.status = InstructionQueueStatus.COMPLETED
        self.worker.current_instruction_queue_item = None
        self.reset()

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
        if queue.stopped or not queue.active_request_block:
            raise exc

        queue.stopped = True
        try:
            # We reset the worker to RUNNING to allow for cleanup tasks during
            # the on_exception hook.
            self.worker.status = InstructionQueueStatus.RUNNING
            if self.scan is not None:
                self.scan.actions._metadata_suffix = "__on-exception"
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
        self.worker.current_instruction_queue_item.parent.queue_manager.send_queue_status()

    def _propagate_error(self, content: str, exc: Exception):
        """
        Propagate the error to the client by sending a client info message and raising an alarm with the error information.

        Args:
            content (str): The error message content to send to the client and include in the alarm information.
            exc (Exception): The exception that was raised, which will be included in the alarm information.
        """

        logger.error(content)
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
        if self.scan.scan_info.scan_id is not None:
            metadata["scan_id"] = self.scan.scan_info.scan_id
        if self.scan.scan_info.scan_number is not None:
            metadata["scan_number"] = self.scan.scan_info.scan_number
        return metadata

    def _run_on_exception_hook(self, exc: Exception):
        """
        Run the on_exception hook implemented by the scan if the current queue item has run_on_exception_hook set to True.
        The on_exception hook allows the scan to perform cleanup tasks or other actions when an exception occurs
        """
        scan = self.scan
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
                scan.on_exception(hook_exc)

        except Exception:
            scan.actions.send_client_info("")
            logger.exception("Failed to run direct scan on_exception hook")

    def _handle_scan_abortion(self, queue: DirectInstructionQueueItem, exc: ScanAbortion):
        # TODO: We currently access the method from the scan worker for being backwards compatible with
        # the generator-based worker. Once we have fully switched to the direct worker, we should move
        # the method to the run method of the direct worker and remove it from the scan worker.
        content = traceback.format_exc()
        logger.error(content)
        if self.scan is None:
            return

        exit_info = exc.exit_info if isinstance(exc, UserScanInterruption) else queue.exit_info
        if exit_info:
            self.scan.actions._send_scan_status(exit_info[0], reason=exit_info[1])
        else:
            reason = "alarm"
            if queue.run_on_exception_hook:
                self.scan.actions._send_scan_status("aborted", reason=reason)
            else:
                self.scan.actions._send_scan_status("halted", reason=reason)

        self._release_scan_locks(self.scan)
        queue.status = InstructionQueueStatus.STOPPED
        queue.append_to_queue_history()
        self.worker.parent.queue_manager.queues[self.worker.queue_name].abort()
        self.reset()
        self.worker.status = InstructionQueueStatus.RUNNING
