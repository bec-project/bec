# pylint: skip-file
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.tests.fixtures import dm_with_devices
from bec_server.scan_server.direct_scan_worker import DirectScanWorker
from bec_server.scan_server.errors import ScanAbortion
from bec_server.scan_server.generator_scan_worker import GeneratorScanWorker
from bec_server.scan_server.scan_queue import (
    DirectInstructionQueueItem,
    InstructionQueueItem,
    InstructionQueueStatus,
    QueueManager,
    ScanQueue,
)
from bec_server.scan_server.scan_stubs import ScanStubStatus
from bec_server.scan_server.scan_worker import ScanWorker


@pytest.fixture
def scan_worker_mock(dm_with_devices) -> ScanWorker:
    parent = SimpleNamespace(
        device_manager=dm_with_devices,
        connector=mock.MagicMock(),
        queue_manager=SimpleNamespace(queues={}),
    )
    scan_worker = ScanWorker(parent=parent)
    yield scan_worker


def test_get_worker_for_instruction_queue_item(scan_worker_mock):
    queue = InstructionQueueItem.__new__(InstructionQueueItem)

    worker = scan_worker_mock.get_worker_for_queue(queue)

    assert isinstance(worker, GeneratorScanWorker)


def test_get_worker_for_direct_instruction_queue_item(scan_worker_mock):
    queue = DirectInstructionQueueItem.__new__(DirectInstructionQueueItem)

    worker = scan_worker_mock.get_worker_for_queue(queue)

    assert isinstance(worker, DirectScanWorker)


def test_run_delegates_to_selected_worker(scan_worker_mock):
    queue = mock.MagicMock()
    queue.stopped = False

    delegated_worker = mock.MagicMock()

    def _process(_queue):
        scan_worker_mock.signal_event.set()

    delegated_worker.process_instructions.side_effect = _process

    with mock.patch.object(
        scan_worker_mock, "get_worker_for_queue", return_value=delegated_worker
    ) as get_worker:
        scan_worker_mock.parent.queue_manager.queues[scan_worker_mock.queue_name] = [queue]

        scan_worker_mock.run()

    get_worker.assert_called_once_with(queue)
    delegated_worker.process_instructions.assert_called_once_with(queue)
    queue.append_to_queue_history.assert_called_once()


def test_run_delegates_scan_abortion_handling_to_selected_worker(scan_worker_mock):
    queue = mock.MagicMock()
    delegated_worker = mock.MagicMock()
    delegated_worker.process_instructions.side_effect = ScanAbortion()

    def _handle(_queue, _exc):
        scan_worker_mock.signal_event.set()

    delegated_worker._handle_scan_abortion.side_effect = _handle

    with mock.patch.object(scan_worker_mock, "get_worker_for_queue", return_value=delegated_worker):
        scan_worker_mock.parent.queue_manager.queues[scan_worker_mock.queue_name] = [queue]

        scan_worker_mock.run()

    delegated_worker._handle_scan_abortion.assert_called_once()


def test_shutdown(scan_worker_mock):
    with mock.patch.object(scan_worker_mock.signal_event, "set") as set_mock:
        scan_worker_mock._started = mock.MagicMock()
        scan_worker_mock._started.is_set.return_value = True
        with mock.patch.object(scan_worker_mock, "join") as join_mock:
            scan_worker_mock.shutdown()
            set_mock.assert_called_once()
            join_mock.assert_called_once()


@pytest.mark.parametrize("queue_item_cls", [InstructionQueueItem, DirectInstructionQueueItem])
def test_shutdown_preserves_completed_item_status(scan_worker_mock, queue_item_cls):
    item = queue_item_cls(mock.MagicMock(), mock.MagicMock(), scan_worker_mock)
    scan = SimpleNamespace(_shutdown_event=threading.Event())
    if isinstance(item, DirectInstructionQueueItem):
        item.scans = [scan]
    else:
        item.queue.request_blocks = [SimpleNamespace(scan=scan)]
    item.status = InstructionQueueStatus.COMPLETED
    scan_worker_mock.current_instruction_queue_item = item

    scan_worker_mock.shutdown()
    scan_worker_mock.shutdown()

    assert scan._shutdown_event.is_set()
    assert item.status == InstructionQueueStatus.COMPLETED


@pytest.mark.parametrize("queue_item_cls", [InstructionQueueItem, DirectInstructionQueueItem])
@pytest.mark.parametrize("reorder", [False, True])
def test_shutdown_interrupts_current_scan_wait(queue_item_cls, reorder):
    parent = SimpleNamespace(device_manager=mock.MagicMock(), connector=mock.MagicMock())
    queue_manager = QueueManager(parent)
    parent.queue_manager = queue_manager
    queue_manager.send_queue_status = mock.Mock()
    queue = ScanQueue(queue_manager)
    queue_manager.queues["primary"] = queue
    worker = queue.scan_worker

    def make_item(scan_id):
        scan = SimpleNamespace(
            _shutdown_event=threading.Event(), scan_info=SimpleNamespace(scan_id=scan_id)
        )
        item = queue_item_cls(queue, mock.MagicMock(), worker)
        if isinstance(item, DirectInstructionQueueItem):
            item.scans = [scan]
        else:
            item.queue.request_blocks = [SimpleNamespace(scan=scan, scan_id=scan_id)]
        item.append_to_queue_history = mock.Mock()
        return item, scan

    active, active_scan = make_item("active-scan")
    pending, pending_scan = make_item("pending-scan")
    queue.queue.extend([active, pending])
    status = ScanStubStatus(
        queue_manager.instruction_handler, shutdown_event=active_scan._shutdown_event
    )
    waiting = threading.Event()
    shutdown_finished = threading.Event()
    errors = []

    def process(item):
        assert item is active
        item.status = InstructionQueueStatus.RUNNING
        waiting.set()
        status.wait()

    def shutdown():
        try:
            queue_manager.shutdown()
        except Exception as exc:
            errors.append(exc)
        finally:
            shutdown_finished.set()

    shutdown_thread = threading.Thread(target=shutdown)
    delegated_worker = mock.Mock()
    delegated_worker.process_instructions.side_effect = process
    with mock.patch.object(worker, "get_worker_for_queue", return_value=delegated_worker):
        worker.start()
        try:
            assert waiting.wait(timeout=2)
            assert worker.current_instruction_queue_item is active
            if reorder:
                queue_manager.set_deferred_pause(queue="primary")
                queue_manager._handle_scan_order_change(
                    messages.ScanQueueOrderMessage(
                        scan_id="pending-scan", action="move_top", queue="primary"
                    )
                )
                assert queue.queue[0] is pending

            shutdown_thread.start()
            assert shutdown_finished.wait(timeout=2), "Shutdown did not interrupt the active scan"
            assert active_scan._shutdown_event.is_set()
            # Stopping scans only signals cancellation; it does not rewrite item status.
            assert active.status == (
                InstructionQueueStatus.DEFERRED_PAUSE if reorder else InstructionQueueStatus.RUNNING
            )
            assert pending.status == InstructionQueueStatus.PENDING
        finally:
            # Release the real status wait even when testing a broken implementation.
            active_scan._shutdown_event.set()
            pending_scan._shutdown_event.set()
            queue.signal_event.set()
            worker.signal_event.set()
            worker.join(timeout=2)
            if shutdown_thread.ident is not None:
                shutdown_thread.join(timeout=2)
            queue_manager.shutdown()

    assert not worker.is_alive()
    assert not shutdown_thread.is_alive()
    assert not errors
    parent.connector.raise_alarm.assert_not_called()


def test_shutdown_prevents_processing_item_selected_during_shutdown(scan_worker_mock):
    worker = scan_worker_mock
    selecting = threading.Event()
    finish_selection = threading.Event()
    release_scan_wait = threading.Event()
    shutdown_finished = threading.Event()
    errors = []
    item = mock.MagicMock()
    item.stopped = True
    item.stop.side_effect = release_scan_wait.set

    def select_item():
        selecting.set()
        assert finish_selection.wait(timeout=2)
        return True

    def process(_item):
        release_scan_wait.wait()

    def shutdown():
        try:
            worker.shutdown()
        except Exception as exc:
            errors.append(exc)
        finally:
            shutdown_finished.set()

    item.__bool__.side_effect = select_item
    worker.parent.queue_manager.queues[worker.queue_name] = [item]
    delegated_worker = mock.Mock()
    delegated_worker.process_instructions.side_effect = process
    shutdown_thread = threading.Thread(target=shutdown)
    with mock.patch.object(worker, "get_worker_for_queue", return_value=delegated_worker):
        worker.start()
        try:
            assert selecting.wait(timeout=2)
            shutdown_thread.start()
            assert worker.signal_event.wait(timeout=2)
            finish_selection.set()
            assert shutdown_finished.wait(timeout=2), "Worker started an item after shutdown"
            delegated_worker.process_instructions.assert_not_called()
        finally:
            finish_selection.set()
            release_scan_wait.set()
            worker.signal_event.set()
            worker.join(timeout=2)
            if shutdown_thread.ident is not None:
                shutdown_thread.join(timeout=2)

    assert not worker.is_alive()
    assert not shutdown_thread.is_alive()
    assert not errors
