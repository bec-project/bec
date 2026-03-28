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
        self.scan = None

    def process_instructions(self, queue: DirectInstructionQueueItem) -> None:
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
        except Exception as exc:
            if self.worker.signal_event.is_set():
                # If the signal event is set, it means that the scan worker is shutting down, so we don't need to handle the abortion
                return
            if queue is None:
                return
            if queue.stopped or not queue.active_request_block:
                raise exc
            queue.stopped = True
            try:
                # We reset the worker to RUNNING to allow for cleanup tasks
                # during the on_exception hook.
                self.worker.status = InstructionQueueStatus.RUNNING
                self.scan.actions._metadata_suffix = "__on-exception"
                self._run_on_exception_hook(exc)
            except Exception as exc_cleanup:
                self.worker.connector.send_client_info("")
                self._handle_exception(exc_cleanup)
            self._handle_exception(exc)
        if queue is None:
            return
        queue.status = InstructionQueueStatus.COMPLETED
        self.worker.current_instruction_queue_item = None
        self.reset()

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
        self.worker.current_instruction_queue_item.parent.queue_manager.send_queue_status()

    def _propagate_error(self, content: str, exc: Exception):
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
        if self.scan is None:
            return {}
        metadata = {}
        if self.scan.scan_info.scan_id is not None:
            metadata["scan_id"] = self.scan.scan_info.scan_id
        if self.scan.scan_info.scan_number is not None:
            metadata["scan_number"] = self.scan.scan_info.scan_number
        return metadata

    def _run_on_exception_hook(self, exc: Exception):
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
                scan.on_exception(hook_exc)  # type: ignore
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

        queue.status = InstructionQueueStatus.STOPPED
        queue.append_to_queue_history()
        self.worker.parent.queue_manager.queues[self.worker.queue_name].abort()
        self.reset()
        self.worker.status = InstructionQueueStatus.RUNNING
