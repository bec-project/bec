"""Admission-only experiment; not a replacement for the production queue.

Reuse BEC's admission policy and queue mutations, but require the test driver to
mutate under the condition and notify. Production callers do not yet honor that
contract. Worker execution, publication ordering, and idle expiry are out of scope.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from unittest import mock

import pytest

from bec_lib import messages
from bec_server.scan_server.scan_queue import InstructionQueueStatus, ScanQueue, ScanQueueStatus


class AdmissionPrototype(ScanQueue):
    """Demonstrate one condition loop using the existing admission rules."""

    def __init__(self):
        super().__init__(mock.MagicMock())
        self.changed = threading.Condition(self._lock)

    def take(self):
        """Wait for admission, or return None at shutdown (no idle expiry)."""
        with self.changed:
            while not self.signal_event.is_set():
                self._flush_deferred_inserts()
                if (
                    self.status == ScanQueueStatus.PAUSED
                    and not self.queue
                    and self.auto_reset_enabled
                ):
                    self.status = ScanQueueStatus.RUNNING
                stopped_head = (
                    self.status == ScanQueueStatus.PAUSED
                    and self.queue
                    and self.queue[0].status == InstructionQueueStatus.STOPPED
                )
                if self.queue and (self._queue_should_continue() or stopped_head):
                    self.active_instruction_queue = self.queue[0]
                    self.history_queue.append(self.active_instruction_queue)
                    return self.active_instruction_queue
                self.changed.wait()
        return None

    def shutdown_admission(self):
        """Set the shutdown predicate and wake the blocked consumer atomically."""
        with self.changed:
            self.signal_event.set()
            self.changed.notify_all()


@contextmanager
def waiting_consumer(queue):
    """Observe actual waits without sleeps and always release the consumer."""
    waiting = threading.Semaphore(0)
    results = []
    errors = []
    original_wait = queue.changed.wait

    def observed_wait(timeout=None):
        assert timeout is None, "The experiment must not depend on polling"
        waiting.release()
        return original_wait(timeout)

    def consume():
        try:
            results.append(queue.take())
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=consume, daemon=True)
    with mock.patch.object(queue.changed, "wait", side_effect=observed_wait):
        thread.start()
        try:
            assert waiting.acquire(timeout=2)
            yield waiting, results, thread
        finally:
            queue.shutdown_admission()
            thread.join(timeout=2)
    assert not thread.is_alive()
    assert not errors


def item(is_scan=True, status=InstructionQueueStatus.PENDING):
    """Create just the metadata needed by the production admission predicate."""
    return mock.Mock(is_scan=[is_scan], status=status)


def hold(allow_device_instructions=False):
    """Create a real BEC queue lock message."""
    return messages.ScanQueueLock(
        identifier="hold", reason="experiment", allow_device_instructions=allow_device_instructions
    )


@pytest.mark.parametrize("initial", ["empty", "paused", "locked"])
def test_mutation_wakes_consumer_without_polling(initial):
    queue = AdmissionPrototype()
    pending = item()
    if initial != "empty":
        queue.queue.append(pending)
    if initial == "paused":
        queue.status = ScanQueueStatus.PAUSED
    if initial == "locked":
        queue.add_lock(hold())
    with waiting_consumer(queue) as (_, results, thread):
        with queue.changed:
            if initial == "empty":
                queue.queue.append(pending)
            elif initial == "paused":
                queue.status = ScanQueueStatus.RUNNING
            else:
                queue.remove_lock(hold())
            queue.changed.notify_all()
        thread.join(timeout=2)
        assert results == [pending]


def test_rechecks_every_gate_after_notification():
    queue = AdmissionPrototype()
    pending = item()
    queue.queue.append(pending)
    queue.status = ScanQueueStatus.PAUSED
    with waiting_consumer(queue) as (waiting, results, thread):
        # A pause becoming a lock must not dispatch the item.
        with queue.changed:
            queue.add_lock(hold())
            queue.changed.notify_all()
        assert waiting.acquire(timeout=2)
        assert not results
        with queue.changed:
            queue.remove_lock(hold())  # restores PAUSED
            queue.status = ScanQueueStatus.RUNNING
            queue.changed.notify_all()
        thread.join(timeout=2)
        assert results == [pending]


def test_reorder_can_admit_device_instruction_while_locked():
    queue = AdmissionPrototype()
    scan, device_instruction = item(), item(is_scan=False)
    queue.queue.extend([scan, device_instruction])
    queue.add_lock(hold(allow_device_instructions=True))
    with waiting_consumer(queue) as (_, results, thread):
        with queue.changed:
            queue.queue.rotate(1)
            queue.changed.notify_all()
        thread.join(timeout=2)
        assert results == [device_instruction]


def test_spurious_notification_rechecks_predicate():
    queue = AdmissionPrototype()
    with waiting_consumer(queue) as (waiting, results, thread):
        with queue.changed:
            queue.changed.notify_all()
        assert waiting.acquire(timeout=2)
        assert not results
        queue.shutdown_admission()
        thread.join(timeout=2)
        assert results == [None]


def test_notification_before_wait_is_not_required_for_existing_work():
    queue = AdmissionPrototype()
    pending = item()
    with queue.changed:
        queue.queue.append(pending)
        queue.changed.notify_all()
    with mock.patch.object(queue.changed, "wait", side_effect=AssertionError("Lost wake-up")):
        assert queue.take() is pending


@pytest.mark.parametrize("initial", ["empty", "paused", "locked"])
def test_shutdown_wakes_all_admission_states(initial):
    queue = AdmissionPrototype()
    if initial == "paused":
        queue.auto_reset_enabled = False
        queue.status = ScanQueueStatus.PAUSED
    if initial == "locked":
        queue.add_lock(hold())
    with waiting_consumer(queue) as (_, results, thread):
        queue.shutdown_admission()
        thread.join(timeout=2)
        assert results == [None]


def test_condition_releases_recursive_queue_lock_but_not_manager_lock():
    manager_lock = threading.RLock()
    queue_lock = threading.RLock()
    changed = threading.Condition(queue_lock)
    observed = []

    def producer():
        with changed:
            acquired_manager = manager_lock.acquire(blocking=False)
            observed.append(acquired_manager)
            if acquired_manager:
                manager_lock.release()
            changed.notify_all()

    with manager_lock, queue_lock, changed:
        thread = threading.Thread(target=producer, daemon=True)
        thread.start()
        # wait releases both recursive acquisitions of queue_lock, but leaves
        # manager_lock held. A real producer requiring both locks would deadlock.
        assert changed.wait_for(lambda: bool(observed), timeout=2)
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert observed == [False]


def test_baseline_pause_to_lock_transition_can_admit_a_scan():
    """Characterize an existing bug, rather than claim it is desired behavior."""
    queue = ScanQueue(mock.MagicMock())
    pending = item()
    queue.queue.append(pending)
    queue.status = ScanQueueStatus.PAUSED
    waiting, resume = threading.Event(), threading.Event()
    results = []

    def controlled_wait(timeout=None):
        waiting.set()
        assert resume.wait(timeout=2)
        return False

    thread = threading.Thread(
        target=lambda: results.append(queue._next_instruction_queue()), daemon=True
    )
    with mock.patch.object(queue.signal_event, "wait", side_effect=controlled_wait):
        thread.start()
        try:
            assert waiting.wait(timeout=2)
            queue.add_lock(hold())
            resume.set()
            thread.join(timeout=2)
        finally:
            queue.signal_event.set()
            resume.set()
            thread.join(timeout=2)
    assert not thread.is_alive()
    assert queue.status == ScanQueueStatus.LOCKED
    assert queue._queue_should_continue() is False
    # The baseline nevertheless selects the pending scan after its pause loop.
    assert results == [True]
    assert queue.active_instruction_queue is pending
