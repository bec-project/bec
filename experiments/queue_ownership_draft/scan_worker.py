"""Direct-only worker: run ScanBase lifecycle hooks, then report by exact token."""

from __future__ import annotations

from threading import Thread

from .protocol import Assignment, Finished, QueueRef, ScanInterrupted
from .scan_queue import QueueManager

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


class ScanWorker(Thread):
    """One direct worker per queue generation; no legacy worker factory is needed."""

    def __init__(self, manager: QueueManager, queue: QueueRef) -> None:
        super().__init__(name=f"draft-worker-{queue.name}", daemon=True)
        self._manager = manager
        self._queue = queue

    def run(self) -> None:
        """Wait outside the owner, execute a transferred payload, then acknowledge it."""
        while True:
            assignment = self._manager.request_work(self._queue).result()
            if assignment is None:
                return
            report = self._execute(assignment)
            self._manager.finished(report)

    def _execute(self, assignment: Assignment) -> Finished:
        scan = assignment.scan  # Worker owns it; the queue item no longer has it.
        control = assignment.cancellation
        outcome, error = "completed", None
        entered_lifecycle = False
        try:
            # Handles an abort/shutdown after dispatch but before the worker starts.
            control.checkpoint()
            scan.bind_cancellation(control)
            # Production: wire scan.actions callbacks, enter its RPC context, assign
            # the owner's number pair, and call scan.actions._initialize_scan().
            for step in SCAN_SEQUENCE:
                control.checkpoint()
                entered_lifecycle = True
                getattr(scan, step)()
            control.checkpoint()
        except Exception as exc:
            outcome = "aborted" if isinstance(exc, ScanInterrupted) else "failed"
            error = str(exc) or type(exc).__name__
            if (
                entered_lifecycle
                and assignment.run_on_exception_hook
                and not control.shutdown.is_set()
                and not control.cleanup.is_set()
            ):
                try:
                    # No Event.clear(): a newer stop can still interrupt this cleanup.
                    # Production actions switch to checking the cleanup-phase signal.
                    scan.on_exception(exc.__cause__ or exc)
                except Exception as cleanup_error:
                    error = f"{error}; cleanup failed: {cleanup_error}"
        finally:
            try:
                # Production: registry release goes through the stop-scope fence.
                scan.release_device_locks()
            except Exception as release_error:
                outcome = "failed"
                error = f"{error or 'scan finished'}; lock release failed: {release_error}"
        # Production: capture copied terminal description, publish terminal status,
        # then acknowledge. Never publish by traversing the scan from the owner.
        return Finished(assignment.token, outcome, error)
