"""Direct scan worker receiving assignments and sending reports through channels."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from .direct_scan_worker import DirectScanWorker
from .queue_channels import Channel, ChannelClosed, ScanAssignment, ScanReport

if TYPE_CHECKING:
    from .scan_queue import QueueManager
    from .scan_server import ScanServer


class ScanWorker(threading.Thread):
    """One worker consumes one queue generation's assignments; it never reads its deque."""

    def __init__(
        self,
        *,
        parent: ScanServer,
        queue_name: str,
        work: Channel[ScanAssignment],
        manager: QueueManager,
    ) -> None:
        super().__init__(name=f"ScanWorker-{queue_name}", daemon=True)
        self.parent = parent
        self.queue_name = queue_name
        self.device_manager = parent.device_manager
        self.connector = parent.connector
        self._work = work
        self._manager = manager
        self._assignment: ScanAssignment | None = None
        self.signal_event = threading.Event()

    def run(self) -> None:
        """Receive ownership, execute direct hooks, and acknowledge the exact token."""
        while True:
            try:
                assignment = self._work.receive()
            except ChannelClosed:
                return
            self._assignment = assignment
            executor = DirectScanWorker(worker=self)
            if self.signal_event.is_set():
                assignment.control.shutdown()
            try:
                report = executor.run(assignment)
            except Exception as exc:  # pylint: disable=broad-except
                # Retirement must not depend on another network operation succeeding.
                self._manager.worker_failed(assignment.token, str(exc))
                continue
            finally:
                self._assignment = None
                del assignment, executor
            self.report(report)

    def report(self, report: ScanReport) -> None:
        """Send copied progress/terminal data and wait for the owner acknowledgement."""
        self._manager.worker_report(report)

    def request_shutdown(self) -> None:
        """Wake blocked receive; the coordinator also signals the active capability."""
        self.signal_event.set()
        self._work.close()

    def shutdown(self, timeout: float | None = None) -> None:
        """Join only from a lifecycle caller outside the coordinator."""
        self.request_shutdown()
        if self._started.is_set() and threading.current_thread() is not self:
            self.join(timeout=timeout)
