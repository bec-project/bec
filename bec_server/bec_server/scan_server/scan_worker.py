from __future__ import annotations

import threading
import traceback
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.logger import bec_logger

from .direct_scan_worker import DirectScanWorker
from .errors import ScanAbortion
from .scan_queue import DirectInstructionQueueItem, InstructionQueueStatus

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_server.scan_server.scan_server import ScanServer


class ScanWorker(threading.Thread):
    """
    Scan worker receives device instructions and pre-processes them before sending them to the device server
    """

    def __init__(self, *, parent: ScanServer, queue_name: str = "primary"):
        super().__init__(daemon=True)
        self.queue_name = queue_name
        self.name = f"ScanWorker-{queue_name}"
        self.parent = parent
        self.device_manager = self.parent.device_manager
        self.connector = self.parent.connector
        self.status = InstructionQueueStatus.IDLE
        self.signal_event = threading.Event()
        self.current_instruction_queue_item: DirectInstructionQueueItem | None = None

    def run(self):
        try:
            while not self.signal_event.is_set():
                try:
                    for queue in self.parent.queue_manager.queues[self.queue_name]:
                        if self.signal_event.is_set():
                            break
                        if not queue:
                            continue
                        self.current_instruction_queue_item = queue
                        worker = DirectScanWorker(worker=self)
                        worker.process_instructions(queue)
                        if not queue.stopped:
                            queue.append_to_queue_history()

                except ScanAbortion as exc:
                    if not queue:
                        # only for type checker; we should never get here
                        continue
                    worker._handle_scan_abortion(queue, exc)

        # pylint: disable=broad-except
        except Exception as exc:
            content = traceback.format_exc()
            error_info = messages.ErrorInfo(
                error_message=content,
                compact_error_message=traceback.format_exc(limit=0),
                exception_type=exc.__class__.__name__,
                device=None,
            )
            self.connector.raise_alarm(
                severity=Alarms.MAJOR, info=error_info, metadata=worker.get_metadata_for_alarm()
            )
            if self.queue_name in self.parent.queue_manager.queues:
                self.parent.queue_manager.queues[self.queue_name].abort()
            worker.reset()
            logger.critical(f"Scan worker stopped: {exc}. Unrecoverable error.")

    def shutdown(self):
        """shutdown the scan worker"""
        self.status = InstructionQueueStatus.STOPPED
        self.signal_event.set()
        if self._started.is_set():  # type: ignore ; _started is defined in threading.Thread
            self.join()
