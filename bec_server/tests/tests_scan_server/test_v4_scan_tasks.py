"""Behavior and regression tests for the v4 scan queue and its task workers."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any, TypeAlias, cast
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.scan_server.direct_scan_worker import DirectScanWorker, ScanControl, ScanOutcome
from bec_server.scan_server.scan_queue import (
    InstructionQueueStatus,
    QueueManager,
    ScanQueue,
    ScanQueueStatus,
)
from bec_server.scan_server.scans.scan_base import ScanBase, ScanType


def wait_for(predicate: Callable[[], bool], timeout: float = 5) -> None:
    """Wait for an asynchronous state transition with a bounded deadline."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("Expected scan task state was not reached")
        time.sleep(0.005)


class FakeScan:
    """Small v4 scan stand-in that exposes controllable lifecycle checkpoints."""

    is_scan = True

    def __init__(
        self, rid: str, parent: mock.MagicMock, core: Callable[[FakeScan], object] | None = None
    ) -> None:
        self.redis_connector = parent.connector
        self.device_manager = parent.device_manager
        self.scan_info = SimpleNamespace(
            scan_id=f"scan-{rid}",
            scan_type=ScanType.SOFTWARE_TRIGGERED,
            scan_number=None,
            dataset_number=None,
            metadata={"RID": rid},
            run_on_exception_hook=True,
            readout_priority_modification={},
            scan_report_instructions=[],
        )
        self.actions = mock.MagicMock()
        self.actions.get_owned_device_locks.return_value = []
        self.actions.get_pending_device_locks.return_value = []
        self._shutdown_event = threading.Event()
        self.started = threading.Event()
        self.finished = threading.Event()
        self.core = core
        self.cleanup_errors = []

    def prepare_scan(self) -> None:
        pass

    def open_scan(self) -> None:
        pass

    def stage(self) -> None:
        pass

    def pre_scan(self) -> None:
        pass

    def scan_core(self) -> None:
        self.started.set()
        if self.core is not None:
            self.core(self)

    def post_scan(self) -> None:
        pass

    def unstage(self) -> None:
        pass

    def close_scan(self) -> None:
        self.finished.set()

    def on_exception(self, exc: Exception) -> None:
        self.cleanup_errors.append(exc)


ScanFactory: TypeAlias = Callable[[dict[str, FakeScan]], QueueManager]


def scan_message(
    rid: str, *, queue: str = "primary", group: str | None = None
) -> messages.ScanQueueMessage:
    """Build one v4 scan request."""
    metadata = {"RID": rid}
    if group is not None:
        metadata["queue_group"] = group
    return messages.ScanQueueMessage(
        scan_type="fake_v4", parameter={"args": {}, "kwargs": {}}, queue=queue, metadata=metadata
    )


@pytest.fixture
def make_manager() -> Iterator[ScanFactory]:
    """Create queue managers with a fake v4 assembler and real task futures."""
    managers = []

    def build(scans: dict[str, FakeScan]) -> QueueManager:
        parent = mock.MagicMock()
        parent.scan_number = 0
        parent.dataset_number = 0
        parent.scan_assembler.scan_manager.scan_dict = {"fake_v4": SimpleNamespace(is_scan=True)}
        parent.scan_assembler.is_direct_scan_message.return_value = True
        parent.scan_assembler.assemble_direct_scan.side_effect = lambda msg, scan_id: scans[
            msg.metadata["RID"]
        ]
        for scan in scans.values():
            scan.redis_connector = parent.connector
            scan.device_manager = parent.device_manager
        manager = QueueManager(parent)
        manager.add_queue("primary")
        managers.append(manager)
        return manager

    yield build

    for manager in managers:
        manager.shutdown()


def test_worker_runs_without_queue_reference() -> None:
    parent = mock.MagicMock()
    scan = FakeScan("one", parent)
    control = ScanControl(run_on_exception_hook=True)
    worker = DirectScanWorker(
        scan=cast(ScanBase, scan),
        control=control,
        on_status=mock.MagicMock(),
        device_lock_registry=parent.device_lock_registry,
    )

    assert worker.run() == "completed"
    assert scan.finished.is_set()
    parent.device_lock_registry.release_all.assert_called_once_with("one")


def test_future_completion_retires_item_and_writes_history(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})

    manager.add_to_queue("primary", scan_message("one"))
    wait_for(lambda: not manager.queues["primary"].queue)

    queue = manager.queues["primary"]
    assert scan.finished.is_set()
    assert queue.active_task is None
    assert queue.history_queue[-1].status == InstructionQueueStatus.COMPLETED
    cast(mock.Mock, manager.connector.lpush).assert_called_once()
    assert manager.parent.scan_number == 1


def test_tasks_are_serial_within_queue(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})

    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))
    assert not second.started.wait(0.05)

    release.set()
    assert second.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    assert [
        item.scan_msgs[0].metadata["RID"] for item in manager.queues["primary"].history_queue
    ] == ["first", "second"]


def test_grouped_v4_scans_run_sequentially_in_one_item(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})

    manager.add_to_queue("primary", scan_message("first", group="group"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second", group="group"))
    assert len(manager.queues["primary"].queue) == 1
    assert not second.started.is_set()

    release.set()
    assert second.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    assert len(manager.queues["primary"].history_queue) == 1


def test_immediate_futures_do_not_nest_completion_callbacks(make_manager: ScanFactory) -> None:
    scans = {str(index): FakeScan(str(index), mock.MagicMock()) for index in range(25)}
    manager = make_manager(scans)
    manager.executor.shutdown()

    class ImmediateFuture(Future[ScanOutcome]):
        callback_depth = 0
        max_callback_depth = 0

        def add_done_callback(self, fn: Callable[[Future[ScanOutcome]], object]) -> None:
            ImmediateFuture.callback_depth += 1
            ImmediateFuture.max_callback_depth = max(
                ImmediateFuture.max_callback_depth, ImmediateFuture.callback_depth
            )
            try:
                super().add_done_callback(fn)
            finally:
                ImmediateFuture.callback_depth -= 1

    class ImmediateExecutor:
        def submit(self, fn: Callable[[], ScanOutcome]) -> Future[ScanOutcome]:
            future = ImmediateFuture()
            future.set_result(fn())
            return future

        def shutdown(self, **_kwargs: Any) -> None:
            pass

    manager.executor = cast(ThreadPoolExecutor, ImmediateExecutor())
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    for rid in scans:
        manager.add_to_queue("primary", scan_message(rid, group="group"))

    queue.status = ScanQueueStatus.RUNNING
    assert all(scan.finished.is_set() for scan in scans.values())
    assert not queue.queue
    assert ImmediateFuture.max_callback_depth == 1


def test_named_queues_can_execute_concurrently(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock(), lambda scan: release.wait(5))
    manager = make_manager({"first": first, "second": second})
    manager.add_queue("secondary")

    manager.add_to_queue("primary", scan_message("first"))
    manager.add_to_queue("secondary", scan_message("second", queue="secondary"))
    assert first.started.wait(5)
    assert second.started.wait(5)

    release.set()
    wait_for(lambda: not manager.queues["primary"].queue)
    wait_for(lambda: not manager.queues["secondary"].queue)


def test_queue_lock_holds_admission_until_release(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    lock = messages.ScanQueueLock(reason="test", identifier="hold", allow_device_instructions=False)
    manager.add_queue_lock("primary", lock)

    manager.add_to_queue("primary", scan_message("one"))
    assert not scan.started.wait(0.05)
    manager.remove_queue_lock("primary", lock)
    assert scan.started.wait(5)


def test_queue_status_change_waits_for_manager_lock(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))
    assert not scan.started.is_set()

    entered = threading.Event()

    def resume_queue() -> None:
        entered.set()
        queue.status = ScanQueueStatus.RUNNING

    with manager._lock:
        resume = threading.Thread(target=resume_queue)
        resume.start()
        assert entered.wait(5)
        assert not scan.started.wait(0.05)
        assert queue.status == ScanQueueStatus.PAUSED

    resume.join(5)
    assert not resume.is_alive()
    assert scan.started.wait(5)


def test_pending_abort_does_not_interrupt_active_scan(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})

    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))
    manager.set_abort(request_id="second", queue="primary")
    assert not first._shutdown_event.is_set()
    assert not second.started.is_set()

    release.set()
    wait_for(lambda: not manager.queues["primary"].queue)
    assert not second.started.is_set()


def test_halt_pending_scan_keeps_active_scan_exception_cleanup(make_manager: ScanFactory) -> None:
    release = threading.Event()

    def fail_after_halt(_scan: FakeScan) -> None:
        assert release.wait(5)
        raise ValueError("active scan failed")

    first = FakeScan("first", mock.MagicMock(), fail_after_halt)
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))

    manager.set_halt(request_id="second", queue="primary")
    release.set()
    wait_for(lambda: not manager.queues["primary"].queue)
    assert len(first.cleanup_errors) == 1
    assert not second.started.is_set()


def test_restart_stops_original_then_dispatches_replacement(make_manager: ScanFactory) -> None:
    release = threading.Event()

    def original_core(scan: FakeScan) -> None:
        assert release.wait(5)
        scan.actions._interruption_callback()

    original = FakeScan("original", mock.MagicMock(), original_core)
    replacement = FakeScan("replacement", mock.MagicMock())
    manager = make_manager({"original": original, "replacement": replacement})
    manager.add_to_queue("primary", scan_message("original"))
    assert original.started.wait(5)

    manager.set_restart(scan_id="scan-original", queue="primary", parameter={"RID": "replacement"})
    assert not replacement.started.is_set()
    release.set()

    assert replacement.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    history = list(manager.queues["primary"].history_queue)
    assert [item.status for item in history] == [
        InstructionQueueStatus.STOPPED,
        InstructionQueueStatus.COMPLETED,
    ]


def test_abort_reaches_running_task_and_future_finishes_after_cleanup(
    make_manager: ScanFactory,
) -> None:
    proceed = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_abort(queue="primary")
    proceed.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)

    assert manager.queues["primary"].history_queue[-1].status == InstructionQueueStatus.STOPPED
    assert len(scan.cleanup_errors) == 1
    scan.actions._send_scan_status.assert_called_with("aborted", reason="user")


def test_clear_keeps_exception_cleanup_for_running_scan(make_manager: ScanFactory) -> None:
    proceed = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_clear(queue="primary")
    proceed.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)
    assert len(scan.cleanup_errors) == 1


def test_clear_discards_deferred_inserts(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)

    manager.set_abort(queue="primary")
    manager.add_to_queue("primary", scan_message("second"))
    assert manager.queues["primary"]._deferred_inserts
    manager.set_clear(queue="primary")
    release.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)

    assert not manager.queues["primary"]._deferred_inserts
    assert not second.started.is_set()


def test_halt_disables_exception_hook_for_running_scan(make_manager: ScanFactory) -> None:
    proceed = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_halt(queue="primary")
    proceed.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)
    assert not scan.cleanup_errors
    scan.actions._send_scan_status.assert_called_with("halted", reason="user")


def test_pause_and_continue_use_control_channel(make_manager: ScanFactory) -> None:
    proceed = threading.Event()
    checkpoint_exited = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()
        checkpoint_exited.set()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_pause(queue="primary")
    proceed.set()
    wait_for(lambda: scan.actions._send_scan_status.call_count > 0)
    assert not checkpoint_exited.is_set()

    manager.set_continue(queue="primary")
    assert checkpoint_exited.wait(5)
    wait_for(lambda: manager.queues["primary"].active_task is None)


def test_continue_resumes_item_before_dispatching_next_scan(make_manager: ScanFactory) -> None:
    observed_status = []
    scan = FakeScan("one", mock.MagicMock(), lambda _scan: observed_status.append(item.status))
    manager = make_manager({"one": scan})
    manager.executor.shutdown()

    def submit(fn: Callable[[], ScanOutcome]) -> Future[ScanOutcome]:
        future: Future[ScanOutcome] = Future()
        future.set_result(fn())
        return future

    manager.executor = mock.MagicMock()
    manager.executor.submit.side_effect = submit
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))
    item = queue.queue[0]
    item.status = InstructionQueueStatus.PAUSED

    manager.set_continue(queue="primary")

    assert observed_status == [InstructionQueueStatus.RUNNING]
    assert not queue.queue


def test_paused_group_waits_without_occupying_pool_thread(make_manager: ScanFactory) -> None:
    first = FakeScan("first", mock.MagicMock())
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    queue = manager.queues["primary"]

    def finish_and_pause() -> None:
        first.finished.set()
        manager.set_pause(queue="primary")

    first.close_scan = finish_and_pause
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("first", group="group"))
    manager.add_to_queue("primary", scan_message("second", group="group"))
    queue.status = ScanQueueStatus.RUNNING

    assert first.finished.wait(5)
    wait_for(lambda: queue.active_task is None)
    assert not second.started.is_set()
    assert queue.worker_status == InstructionQueueStatus.PAUSED

    manager.set_continue(queue="primary")
    assert second.started.wait(5)
    wait_for(lambda: not queue.queue)


def test_worker_status_callback_publishes_queue_snapshot(make_manager: ScanFactory) -> None:
    published = threading.Event()

    def core(scan: FakeScan) -> None:
        connector = scan.redis_connector
        before = connector.set_and_publish.call_count
        scan.actions._update_queue_info_callback()
        if connector.set_and_publish.call_count > before:
            published.set()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert published.wait(5)


def test_worker_failure_is_reported_through_future_result(make_manager: ScanFactory) -> None:
    def fail(_scan: FakeScan) -> None:
        raise RuntimeError("scan failed")

    scan = FakeScan("one", mock.MagicMock(), fail)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    wait_for(lambda: manager.queues["primary"].active_task is None)

    assert manager.queues["primary"].history_queue[-1].status == InstructionQueueStatus.STOPPED
    cast(mock.Mock, manager.connector.raise_alarm).assert_called()
    assert len(scan.cleanup_errors) == 1


def test_cleanup_hook_failure_raises_alarm(make_manager: ScanFactory) -> None:
    def fail(_scan: FakeScan) -> None:
        raise RuntimeError("scan failed")

    scan = FakeScan("one", mock.MagicMock(), fail)
    scan.on_exception = mock.Mock(side_effect=ValueError("cleanup failed"))
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    wait_for(lambda: manager.queues["primary"].active_task is None)

    alarm_info = cast(mock.Mock, manager.connector.raise_alarm).call_args.kwargs["info"]
    assert alarm_info.exception_type == "ValueError"
    assert "cleanup failed" in alarm_info.error_message
    assert manager.queues["primary"].history_queue[-1].status == InstructionQueueStatus.STOPPED


def test_removed_queue_does_not_dispatch_successor_after_late_completion(
    make_manager: ScanFactory,
) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))

    task = manager.queues["primary"].active_task
    assert task is not None
    manager.remove_queue("primary", skip_primary=False)
    assert "primary" not in manager.queues
    release.set()
    assert task.future.result(timeout=5) == "shutdown"
    assert not second.started.is_set()


def test_late_completion_cannot_change_replacement_queue(make_manager: ScanFactory) -> None:
    release = threading.Event()
    old = FakeScan("old", mock.MagicMock(), lambda scan: release.wait(5))
    new = FakeScan("new", mock.MagicMock())
    manager = make_manager({"old": old, "new": new})
    manager.add_to_queue("primary", scan_message("old"))
    assert old.started.wait(5)
    old_queue = manager.queues["primary"]
    old_task = old_queue.active_task
    assert old_task is not None

    manager.remove_queue("primary", skip_primary=False)
    replacement = threading.Thread(
        target=manager.add_to_queue, args=("primary", scan_message("new"))
    )
    replacement.start()
    assert not new.started.wait(0.05)
    release.set()
    assert old_task.future.result(timeout=5) == "shutdown"
    replacement.join(timeout=5)
    assert not replacement.is_alive()
    assert new.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    assert manager.queues["primary"] is not old_queue
    assert manager.queues["primary"].history_queue[-1].scan_msgs[0].metadata["RID"] == "new"


def test_shutdown_cancels_pool_pending_task_without_false_alarm(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.executor.shutdown()
    manager.executor = ThreadPoolExecutor(max_workers=1)
    manager.add_queue("secondary")

    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("secondary", scan_message("second", queue="secondary"))
    assert not second.started.is_set()

    shutdown_done = threading.Event()
    shutdown_thread = threading.Thread(target=lambda: (manager.shutdown(), shutdown_done.set()))
    shutdown_thread.start()
    wait_for(lambda: not manager.queues)
    release.set()
    assert shutdown_done.wait(5)
    shutdown_thread.join()
    assert not second.started.is_set()
    cast(mock.Mock, manager.connector.raise_alarm).assert_not_called()


def test_removed_pool_pending_task_does_not_block_queue_recreation(
    make_manager: ScanFactory,
) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.executor.shutdown()
    manager.executor = ThreadPoolExecutor(max_workers=1)
    manager.add_queue("secondary")
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("secondary", scan_message("second", queue="secondary"))
    old_task = manager.queues["secondary"].active_task
    assert old_task is not None

    manager.remove_queue("secondary")
    assert old_task.future.cancelled()
    manager.add_queue("secondary")
    assert "secondary" in manager.queues
    assert not second.started.is_set()
    release.set()


def test_idle_secondary_queue_expires_without_a_worker(
    monkeypatch: pytest.MonkeyPatch, make_manager: ScanFactory
) -> None:
    monkeypatch.setattr(ScanQueue, "AUTO_SHUTDOWN_TIME", 0.02)
    manager = make_manager({})
    manager.add_queue("secondary")
    timer = manager.queues["secondary"]._auto_shutdown_timer

    wait_for(lambda: "secondary" not in manager.queues)
    assert "primary" in manager.queues
    manager.shutdown()
    assert timer is not None and not timer.is_alive()


def test_pending_cancellation_restarts_idle_queue_timer(
    monkeypatch: pytest.MonkeyPatch, make_manager: ScanFactory
) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    manager.add_queue("secondary")
    queue = manager.queues["secondary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("secondary", scan_message("one", queue="secondary"))
    with manager._lock:
        original_timer = queue._cancel_auto_shutdown_timer_locked()
    assert original_timer is not None
    original_timer.join(timeout=5)
    expired_timer = threading.Timer(0, manager._remove_idle_queue, args=[queue])
    with manager._lock:
        queue._auto_shutdown_timer = expired_timer
        manager._timer_threads.add(expired_timer)
    expired_timer.start()
    expired_timer.join(timeout=5)
    assert "secondary" in manager.queues

    monkeypatch.setattr(ScanQueue, "AUTO_SHUTDOWN_TIME", 0.02)
    manager.set_abort(request_id="one", queue="secondary")
    wait_for(lambda: "secondary" not in manager.queues)
    assert not scan.started.is_set()


def test_clear_resets_empty_paused_queue(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))

    manager.set_clear(queue="primary")

    assert queue.status == ScanQueueStatus.RUNNING
    assert not queue.queue
    assert not scan.started.is_set()


def test_order_change_ignores_queue_removed_after_guard_validation(
    make_manager: ScanFactory,
) -> None:
    manager = make_manager({})
    manager.remove_queue("primary", skip_primary=False)

    manager._handle_scan_order_change(
        messages.ScanQueueOrderMessage(queue="primary", scan_id="old-scan", action="move_bottom")
    )

    assert "primary" not in manager.queues


def test_reordering_active_item_during_deferred_pause_does_not_replay_it(
    make_manager: ScanFactory,
) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))
    manager.set_deferred_pause(queue="primary")
    manager._handle_scan_order_change(
        messages.ScanQueueOrderMessage(
            queue="primary", scan_id=first.scan_info.scan_id, action="move_bottom"
        )
    )

    release.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)
    assert [item.scan_msgs[0].metadata["RID"] for item in manager.queues["primary"].queue] == [
        "second"
    ]
    manager.set_continue(queue="primary")
    assert second.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    assert not first._shutdown_event.is_set()


def test_shutdown_joins_pending_idle_timer(make_manager: ScanFactory) -> None:
    manager = make_manager({})
    manager.add_queue("secondary")
    timer = manager.queues["secondary"]._auto_shutdown_timer
    assert timer is not None and timer.is_alive()

    manager.shutdown()

    assert not timer.is_alive()


def test_queue_export_preserves_pending_order_groups_and_scan_numbers(
    make_manager: ScanFactory,
) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("first", "second", "third")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("first", group="series"))
    manager.add_to_queue("primary", scan_message("second", group="series"))
    manager.add_to_queue("primary", scan_message("third"))

    snapshot = manager.export_queue()["primary"]
    assert snapshot.status == "PAUSED"
    assert len(snapshot.info) == 2
    assert [block.RID for block in snapshot.info[0].request_blocks] == ["first", "second"]
    assert [block.RID for block in snapshot.info[1].request_blocks] == ["third"]
    assert [entry.scan_number for entry in snapshot.info] == [[1, 2], [3]]
    assert all(entry.active_request_block is None for entry in snapshot.info)
    assert all(not scan.started.is_set() for scan in scans.values())

    manager.send_queue_status()
    endpoint, message = cast(mock.Mock, manager.connector.set_and_publish).call_args.args
    assert endpoint == MessageEndpoints.scan_queue_status()
    assert message.queue["primary"] == snapshot


def test_grouped_requests_share_only_their_matching_queue_item(make_manager: ScanFactory) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("a", "b", "c")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("a", group="one"))
    manager.add_to_queue("primary", scan_message("b", group="two"))
    manager.add_to_queue("primary", scan_message("c", group="one"))

    assert len(queue.queue) == 2
    assert [[msg.metadata["RID"] for msg in item.scan_msgs] for item in queue.queue] == [
        ["a", "c"],
        ["b"],
    ]
    assert queue.get_queue_item("one") is queue.queue[0]
    assert queue.get_queue_item("missing") is None
    assert queue.get_scan("scan-c") is queue.queue[0]


@pytest.mark.parametrize(
    ("order_message", "expected"),
    [
        (messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_up"), ["a", "c", "b", "d"]),
        (
            messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_down"),
            ["a", "b", "d", "c"],
        ),
        (messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_top"), ["c", "a", "b", "d"]),
        (
            messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_bottom"),
            ["a", "b", "d", "c"],
        ),
        (
            messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_to", target_position=0),
            ["c", "a", "b", "d"],
        ),
        (
            messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_to", target_position=99),
            ["a", "b", "d", "c"],
        ),
    ],
)
def test_paused_queue_reorders_by_scan_id(
    make_manager: ScanFactory, order_message: messages.ScanQueueOrderMessage, expected: list[str]
) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in "abcd"}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    for rid in scans:
        manager.add_to_queue("primary", scan_message(rid))

    manager._handle_scan_order_change(order_message)

    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == expected


def test_reordered_pending_queue_executes_in_new_order(make_manager: ScanFactory) -> None:
    executed: list[str] = []
    scans = {
        rid: FakeScan(rid, mock.MagicMock(), lambda _scan, rid=rid: executed.append(rid))
        for rid in "abc"
    }
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    for rid in scans:
        manager.add_to_queue("primary", scan_message(rid))
    manager._handle_scan_order_change(
        messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_top")
    )

    queue.status = ScanQueueStatus.RUNNING
    wait_for(lambda: not queue.queue)
    assert executed == ["c", "a", "b"]
    assert [item.status for item in queue.history_queue] == [InstructionQueueStatus.COMPLETED] * 3


def test_pending_items_can_be_removed_by_scan_or_request_id(make_manager: ScanFactory) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("first", "second", "third")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    for rid in scans:
        manager.add_to_queue("primary", scan_message(rid))

    queue.remove_queue_item("scan-first")
    queue.remove_queue_item_by_request_id("second")

    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["third"]
    assert queue.get_scan("scan-first") is None
    assert not scans["first"].started.is_set()
    assert not scans["second"].started.is_set()


def test_abort_pending_request_publishes_cancelled_before_removing_it(
    make_manager: ScanFactory,
) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("keep", "cancel")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("keep"))
    manager.add_to_queue("primary", scan_message("cancel"))

    manager.set_abort(request_id="cancel")

    snapshots = [
        call.args[1].queue["primary"]
        for call in cast(mock.Mock, manager.connector.set_and_publish).call_args_list
    ]
    assert any(
        any(
            entry.status == "CANCELLED" and entry.request_blocks[0].RID == "cancel"
            for entry in snapshot.info
        )
        for snapshot in snapshots
    )
    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["keep"]
    assert not scans["cancel"].started.is_set()


def test_multiple_locks_restore_previous_paused_status(make_manager: ScanFactory) -> None:
    manager = make_manager({})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    first = messages.ScanQueueLock(reason="maintenance", identifier="first")
    second = messages.ScanQueueLock(reason="alignment", identifier="second")

    manager.add_queue_lock("primary", first)
    manager.add_queue_lock("primary", second)
    assert queue.status == ScanQueueStatus.LOCKED
    assert {lock.identifier for lock in manager.export_queue()["primary"].locks} == {
        "first",
        "second",
    }
    manager.remove_queue_lock("primary", first)
    assert queue.status == ScanQueueStatus.LOCKED
    manager.remove_queue_lock("primary", second)
    assert queue.status == ScanQueueStatus.PAUSED


def test_invalid_scan_request_raises_alarm_without_adding_item(make_manager: ScanFactory) -> None:
    manager = make_manager({})
    cast(mock.Mock, manager.parent.scan_assembler.is_direct_scan_message).return_value = False

    manager.add_to_queue("primary", scan_message("invalid"))

    assert not manager.queues["primary"].queue
    alarm = cast(mock.Mock, manager.connector.raise_alarm).call_args.kwargs
    assert alarm["info"].exception_type == "TypeError"
    assert alarm["metadata"]["RID"] == "invalid"


def test_non_scan_request_does_not_consume_scan_or_dataset_number(
    make_manager: ScanFactory,
) -> None:
    scan = FakeScan("rpc", mock.MagicMock())
    scan.is_scan = False
    scan.scan_info.scan_type = None
    scan.scan_info.scan_id = None
    manager = make_manager({"rpc": scan})
    manager.parent.scan_assembler.scan_manager.scan_dict["fake_v4"].is_scan = False

    manager.add_to_queue("primary", scan_message("rpc"))
    wait_for(lambda: not manager.queues["primary"].queue)

    assert scan.finished.is_set()
    assert scan.scan_info.scan_number is None
    assert scan.scan_info.dataset_number is None
    assert manager.parent.scan_number == 0
    assert manager.parent.dataset_number == 0


def test_stopped_head_buffers_and_then_flushes_inserts(make_manager: ScanFactory) -> None:
    release = threading.Event()
    scans = {
        "first": FakeScan("first", mock.MagicMock(), lambda _scan: release.wait(5)),
        "second": FakeScan("second", mock.MagicMock()),
    }
    manager = make_manager(scans)
    manager.add_to_queue("primary", scan_message("first"))
    assert scans["first"].started.wait(5)
    manager.set_abort(queue="primary")
    manager.add_to_queue("primary", scan_message("second"))
    queue = manager.queues["primary"]
    assert [msg.metadata["RID"] for msg, _ in queue._deferred_inserts] == ["second"]

    release.set()
    wait_for(lambda: queue.active_task is None)

    assert not queue._deferred_inserts
    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["second"]
    assert not scans["second"].started.is_set()
    manager.set_continue(queue="primary")
    assert scans["second"].started.wait(5)
    wait_for(lambda: not queue.queue)


def test_insert_callback_routes_request_to_named_queue(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    message = scan_message("one", queue="secondary")

    manager._scan_queue_callback(cast(Any, SimpleNamespace(value=message)))

    assert scan.started.wait(5)
    wait_for(lambda: not manager.queues["secondary"].queue)
    assert "secondary" in manager.queues
    assert manager.queues["secondary"].history_queue[-1].scan_msgs[0] is message


def test_modification_callback_dispatches_lock_and_release(make_manager: ScanFactory) -> None:
    manager = make_manager({})
    lock_message = messages.ScanQueueModificationMessage(
        scan_id=None, action="lock", parameter={"reason": "maintenance", "identifier": "hold"}
    )
    release_message = messages.ScanQueueModificationMessage(
        scan_id=None, action="release_lock", parameter={"identifier": "hold"}
    )

    manager._scan_queue_modification_callback(cast(Any, SimpleNamespace(value=lock_message)))
    assert manager.queues["primary"].status == ScanQueueStatus.LOCKED
    assert manager.export_queue()["primary"].locks[0].reason == "maintenance"
    manager._scan_queue_modification_callback(cast(Any, SimpleNamespace(value=release_message)))
    assert manager.queues["primary"].status == ScanQueueStatus.RUNNING
    assert not manager.queues["primary"].locks


@pytest.mark.parametrize(
    ("action", "parameter", "error"),
    [
        ("lock", None, "Missing parameter"),
        ("lock", {"identifier": "hold"}, "Missing lock reason"),
        ("lock", {"reason": "maintenance"}, "Missing lock identifier"),
        ("release_lock", None, "Missing parameter"),
        ("release_lock", {"reason": "maintenance"}, "Missing lock identifier"),
    ],
)
def test_lock_modifications_require_identifiers_and_reason(
    make_manager: ScanFactory, action: str, parameter: dict[str, str] | None, error: str
) -> None:
    manager = make_manager({})
    with pytest.raises(ValueError, match=error):
        if action == "lock":
            manager.set_lock(parameter=parameter)
        else:
            manager.set_release_lock(parameter=parameter)


def test_allowed_device_instructions_lock_runs_non_scan_but_holds_scan(
    make_manager: ScanFactory,
) -> None:
    rpc = FakeScan("rpc", mock.MagicMock())
    rpc.is_scan = False
    rpc.scan_info.scan_type = None
    rpc.scan_info.scan_id = None
    scan = FakeScan("scan", mock.MagicMock())
    manager = make_manager({"rpc": rpc, "scan": scan})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.parent.scan_assembler.scan_manager.scan_dict["fake_v4"].is_scan = False
    manager.add_to_queue("primary", scan_message("rpc"))
    manager.parent.scan_assembler.scan_manager.scan_dict["fake_v4"].is_scan = True
    manager.add_to_queue("primary", scan_message("scan"))
    lock = messages.ScanQueueLock(
        reason="allow RPC", identifier="rpc", allow_device_instructions=True
    )

    manager.add_queue_lock("primary", lock)
    assert rpc.finished.wait(5)
    assert not scan.started.is_set()
    manager.remove_queue_lock("primary", lock)
    assert queue.status == ScanQueueStatus.PAUSED
    assert not scan.started.is_set()
    queue.status = ScanQueueStatus.RUNNING
    assert scan.started.wait(5)
    wait_for(lambda: not queue.queue)


def test_continue_does_not_unlock_a_locked_queue(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    lock = messages.ScanQueueLock(reason="maintenance", identifier="hold")
    manager.add_queue_lock("primary", lock)
    manager.add_to_queue("primary", scan_message("one"))

    manager.set_continue(queue="primary")

    assert manager.queues["primary"].status == ScanQueueStatus.LOCKED
    assert not scan.started.is_set()
    manager.remove_queue_lock("primary", lock)
    assert scan.started.wait(5)


def test_dataset_hold_reuses_dataset_number_for_next_scan(make_manager: ScanFactory) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("first", "second")}
    manager = make_manager(scans)
    manager.add_to_queue("primary", scan_message("first"))
    assert scans["first"].finished.wait(5)
    second_message = scan_message("second")
    second_message.metadata["dataset_id_on_hold"] = True
    manager.add_to_queue("primary", second_message)
    assert scans["second"].finished.wait(5)

    assert [scans[rid].scan_info.scan_number for rid in ("first", "second")] == [1, 2]
    assert [scans[rid].scan_info.dataset_number for rid in ("first", "second")] == [1, 1]
    assert manager.parent.scan_number == 2
    assert manager.parent.dataset_number == 1


@pytest.mark.parametrize("devices", [None, [], ["motor"]])
def test_stop_devices_preserves_all_none_and_selected_device_semantics(
    make_manager: ScanFactory, devices: list[str] | None
) -> None:
    manager = make_manager({})

    manager.stop_all_devices(stop_id="scan-one", devices=devices)

    endpoint, message = cast(mock.Mock, manager.connector.send).call_args.args
    assert endpoint == MessageEndpoints.stop_devices()
    assert message.value == devices
    assert message.metadata["stop_id"] == "scan-one"


def test_pending_abort_by_scan_id_removes_only_target(make_manager: ScanFactory) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("keep", "cancel")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("keep"))
    manager.add_to_queue("primary", scan_message("cancel"))

    manager.set_abort(scan_id="scan-cancel")

    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["keep"]
    assert queue.queue[0].status == InstructionQueueStatus.PENDING
    assert not scans["cancel"].started.is_set()


def test_unknown_abort_target_leaves_pending_queue_unchanged(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))

    manager.set_abort(request_id="unknown")
    manager.set_abort(scan_id="scan-unknown")

    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["one"]
    assert queue.status == ScanQueueStatus.PAUSED
    assert not scan.started.is_set()


def test_order_change_is_ignored_while_queue_is_running(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda _scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))

    manager._handle_scan_order_change(
        messages.ScanQueueOrderMessage(scan_id="scan-second", action="move_top")
    )

    assert [item.scan_msgs[0].metadata["RID"] for item in manager.queues["primary"].queue] == [
        "first",
        "second",
    ]
    release.set()
    wait_for(lambda: not manager.queues["primary"].queue)


def test_user_completed_restores_queue_status_after_active_scan_stops(
    make_manager: ScanFactory,
) -> None:
    proceed = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_user_completed(queue="primary")
    proceed.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)

    assert manager.queues["primary"].status == ScanQueueStatus.RUNNING
    scan.actions._send_scan_status.assert_called_with("user_completed", reason="user")


def test_unexpected_future_exception_reports_alarm_and_runs_successor(
    make_manager: ScanFactory,
) -> None:
    first = FakeScan("first", mock.MagicMock())
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("first"))
    manager.add_to_queue("primary", scan_message("second"))
    original_run = DirectScanWorker.run

    def crash_first(worker: DirectScanWorker) -> ScanOutcome:
        if worker.scan.scan_info.metadata["RID"] == "first":
            raise RuntimeError("unexpected worker failure")
        return original_run(worker)

    with mock.patch.object(DirectScanWorker, "run", crash_first):
        queue.status = ScanQueueStatus.RUNNING
        wait_for(lambda: not queue.queue)

    assert [item.status for item in queue.history_queue] == [
        InstructionQueueStatus.STOPPED,
        InstructionQueueStatus.COMPLETED,
    ]
    assert second.finished.is_set()
    assert (
        cast(mock.Mock, manager.connector.raise_alarm).call_args.kwargs["info"].exception_type
        == "RuntimeError"
    )
