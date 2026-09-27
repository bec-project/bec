"""Owner-side direct queue item; no worker or connector hidden in its setters."""

from __future__ import annotations

from dataclasses import dataclass

from .protocol import Assignment, Cancellation, DirectScan, ExecutionToken, ItemSnapshot


@dataclass
class DirectInstructionQueueItem:
    """One direct request's metadata, manipulated only by its owner-side ScanQueue.

    Before dispatch, _prepared_scan is an opaque handle. After dispatch, only the
    worker has the scan; this object retains metadata for snapshots and retirement.
    One scan per item here; direct grouping and request metadata are left to integration.
    """

    queue_id: str
    label: str
    is_scan: bool
    _prepared_scan: DirectScan | None
    run_on_exception_hook: bool = True
    status: str = "PENDING"

    def claim(self, token: ExecutionToken, cancellation: Cancellation) -> Assignment:
        """Transfer the scan once, without calling any of its methods."""
        if self._prepared_scan is None:
            raise RuntimeError("The direct scan has already been claimed")
        scan, self._prepared_scan = self._prepared_scan, None
        self.status = "RUNNING"
        return Assignment(token, scan, cancellation, self.run_on_exception_hook)

    def describe(self) -> ItemSnapshot:
        """Return copied metadata; do not ask the worker's scan to describe itself."""
        return ItemSnapshot(self.queue_id, self.label, self.is_scan, self.status)
