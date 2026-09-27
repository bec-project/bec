"""Run an in-memory ownership example; no Redis, BEC services, or hardware."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event

from .protocol import (
    Cancellation,
    ExecutionToken,
    Finished,
    PreparedScan,
    QueueRef,
    QueueStatus,
    ScanInterrupted,
)
from .scan_queue import QueueManager
from .scan_worker import SCAN_SEQUENCE, ScanWorker


@dataclass
class DemoScan:
    """Small direct scan adapter with visible lifecycle hooks."""

    block_until_aborted: bool = False
    started: Event = field(default_factory=Event)
    cleaning: Event = field(default_factory=Event)
    finish_cleanup: Event = field(default_factory=Event)
    released: Event = field(default_factory=Event)
    steps: list[str] = field(default_factory=list)
    _cancellation: Cancellation | None = None

    def bind_cancellation(self, cancellation: Cancellation) -> None:
        """Stand in for installing callbacks on scan.actions."""
        self._cancellation = cancellation

    def prepare_scan(self) -> None:
        """Prepare this direct scan."""
        self.steps.append("prepare_scan")

    def open_scan(self) -> None:
        """Open this direct scan."""
        self.steps.append("open_scan")

    def stage(self) -> None:
        """Stand in for device staging."""
        self.steps.append("stage")

    def pre_scan(self) -> None:
        """Run the pre-scan hook."""
        self.steps.append("pre_scan")

    def scan_core(self) -> None:
        """Demonstrate interruption inside a direct scan hook."""
        self.steps.append("scan_core")
        self.started.set()
        assert self._cancellation is not None
        if self.block_until_aborted:
            assert self._cancellation.execution.wait(3), "Demo scan was never interrupted"
        self._cancellation.checkpoint()

    def post_scan(self) -> None:
        """Run the post-scan hook."""
        self.steps.append("post_scan")

    def unstage(self) -> None:
        """Stand in for device unstaging."""
        self.steps.append("unstage")

    def close_scan(self) -> None:
        """Close this direct scan."""
        self.steps.append("close_scan")

    def on_exception(self, cause: Exception) -> None:
        """Hold cleanup open so the demo can inspect the owner's state."""
        self.steps.append("on_exception")
        self.cleaning.set()
        assert self.finish_cleanup.wait(3), "Demo cleanup was not released"
        assert self._cancellation is not None
        if self._cancellation.cleanup.is_set():
            raise ScanInterrupted("Exception cleanup interrupted")

    def release_device_locks(self) -> None:
        """Stand in for a worker-side finally block releasing its device locks."""
        self.steps.append("release_device_locks")
        self.released.set()


def main() -> None:
    """Show independent queues, exact identity, and retained cleanup ownership."""
    manager = QueueManager()
    primary, secondary = manager.create_queue(), manager.create_queue("secondary")
    manager.set_admission(primary, QueueStatus.LOCKED)
    held_scan, other_scan, next_scan = DemoScan(True), DemoScan(), DemoScan()
    manager.insert_prepared(primary, PreparedScan("first scan", True, held_scan))
    manager.insert_prepared(secondary, PreparedScan("independent scan", True, other_scan))
    workers = [ScanWorker(manager, ref) for ref in (primary, secondary)]
    for worker in workers:
        worker.start()

    try:
        assert other_scan.released.wait(2)
        assert other_scan.steps == [*SCAN_SEQUENCE, "release_device_locks"]
        assert manager.snapshot(primary).active is None
        print("A locked primary queue does not block the secondary queue.")

        manager.set_admission(primary, QueueStatus.RUNNING)
        assert held_scan.started.wait(2)
        original = manager.snapshot(primary).active
        assert original is not None
        stale = ExecutionToken(QueueRef("primary", "old-generation"), original.item_id, 1)
        assert manager.finished(Finished(stale, "completed")) is False
        assert manager.snapshot(primary).active == original
        print("Completion from an old generation cannot retire the active scan.")

        manager.clear(primary)
        assert held_scan.cleaning.wait(2)
        snapshot = manager.snapshot(primary)
        assert snapshot.items == () and snapshot.active == original
        manager.insert_prepared(primary, PreparedScan("next scan", True, next_scan))
        manager.set_admission(primary, QueueStatus.RUNNING)
        assert manager.snapshot(primary).active == original
        assert not next_scan.started.is_set()
        print("Clear removes visible work; in-flight cleanup still prevents another dispatch.")

        held_scan.finish_cleanup.set()
        assert next_scan.released.wait(2)
        assert next_scan.steps == [*SCAN_SEQUENCE, "release_device_locks"]
        assert held_scan.steps == [*SCAN_SEQUENCE[:5], "on_exception", "release_device_locks"]
        print("The next scan starts after the original worker acknowledges cleanup.")
        print("Direct lifecycle order, on_exception, and finally release were exercised.")
    finally:
        # Test/demo teardown always releases barriers before joining threads.
        for scan in (held_scan, other_scan, next_scan):
            scan.finish_cleanup.set()
        manager.begin_shutdown()
        for worker in workers:
            worker.join(timeout=3)
        if any(worker.is_alive() for worker in workers):
            raise RuntimeError("Worker did not stop; leave the owner alive for late reports")
        manager.finish_shutdown()
    print("Workers joined outside the owner; coordinator stopped last.")


if __name__ == "__main__":
    main()
