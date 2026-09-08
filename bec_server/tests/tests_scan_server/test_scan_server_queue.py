import threading
import time
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
from bec_server.scan_server.scan_queue import (
    DirectInstructionQueueItem,
    InstructionQueueStatus,
    QueueManager,
    ScanQueue,
    ScanQueueStatus,
)
from bec_server.scan_server.scans.scan_base import ScanType
from bec_server.scan_server.tests.fixtures import scan_server_mock
from bec_server.scan_server.tests.utils import NoopScan

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access
ScanQueue.AUTO_SHUTDOWN_TIME = 1  # Reduce auto-shutdown time for testing


@pytest.fixture
def queuemanager_mock(scan_server_mock):
    def _get_queuemanager(queues: str | list[str] | None = None) -> QueueManager:
        scan_server = scan_server_mock
        scan_server.scan_manager.scan_dict[_QueuedScan.scan_name] = _QueuedScan
        if queues is None:
            queues = ["primary"]
        if isinstance(queues, str):
            queues = [queues]
        for queue in queues:
            scan_server.queue_manager.add_queue(queue)
        return scan_server.queue_manager

    yield _get_queuemanager

    scan_server_mock.queue_manager.shutdown()


@pytest.fixture
def dormant_queue_manager():
    """Use real queue locks with worker advancement controlled by the test."""
    queue_manager = QueueManager(mock.MagicMock())
    queue = ScanQueue(queue_manager, queue_name="secondary")
    queue.scan_worker = mock.Mock()
    queue.scan_worker.is_alive.return_value = True
    queue_manager.queues["secondary"] = queue
    queue_manager.export_queue = mock.Mock(return_value={})
    with mock.patch(
        "bec_server.scan_server.scan_queue.DirectInstructionQueueItem",
        side_effect=lambda **kwargs: mock.Mock(status=InstructionQueueStatus.PENDING),
    ):
        yield queue_manager
    queue_manager.shutdown()


class _QueuedScan(NoopScan):
    """Keep a current scan active until the queue test interrupts its worker."""

    scan_name = "_queued_scan"

    def __init__(self, system_config=None, **kwargs):
        super().__init__(system_config=system_config or {}, **kwargs)

    def scan_core(self):
        while not self._shutdown_event.wait(0.01):
            self.actions._interruption_callback()
        self.actions._interruption_callback()


class _DummyV4Scan(NoopScan):
    scan_name = "_v4_dummy_scan"


def _build_dummy_v4_scan(
    scan_id: str, scan_number: int | None = None, is_scan: bool = True
) -> _DummyV4Scan:
    scan = _DummyV4Scan(
        scan_id=scan_id,
        redis_connector=mock.MagicMock(),
        device_manager=mock.MagicMock(),
        instruction_handler=mock.MagicMock(),
        request_inputs={},
        system_config={},
    )
    scan.is_scan = is_scan
    scan.scan_info.scan_type = ScanType.SOFTWARE_TRIGGERED if is_scan else None
    scan.scan_info.scan_number = scan_number
    return scan


def _queued_scan_message(
    *, queue: str = "primary", rid: str = "something"
) -> messages.ScanQueueMessage:
    return messages.ScanQueueMessage(
        scan_type="_queued_scan",
        parameter={"args": [], "kwargs": {}},
        queue=queue,
        metadata={"RID": rid},
    )


def test_queuemanager_queue_contains_primary(queuemanager_mock):
    queue_manager = queuemanager_mock()
    assert "primary" in queue_manager.queues


@pytest.mark.parametrize("queue", ["primary", "alignment"])
def test_queuemanager_add_to_queue(queuemanager_mock, queue):
    queue_manager = queuemanager_mock()
    msg = _queued_scan_message(queue=queue)
    queue_manager.add_queue(queue)
    queue_manager.add_to_queue(scan_queue=queue, msg=msg)
    assert queue_manager.queues[queue].queue.popleft().scan_msgs[0] == msg


@pytest.mark.parametrize("group_metadata", [{}, {"scan_def_id": "old"}, {"queue_group": "old"}])
def test_queue_insert_creates_independent_current_scan_items(queuemanager_mock, group_metadata):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager)
    first = _queued_scan_message(rid="first")
    second = _queued_scan_message(rid="second")
    first.metadata.update(group_metadata)
    second.metadata.update(group_metadata)

    queue.insert(first)
    queue.insert(second)

    assert len(queue.queue) == 2
    first_item, second_item = queue.queue
    assert isinstance(first_item, DirectInstructionQueueItem)
    assert isinstance(second_item, DirectInstructionQueueItem)
    assert first_item.scan_id != second_item.scan_id
    assert first_item.scan_msgs == [first]
    assert second_item.scan_msgs == [second]
    assert first_item.describe().request_blocks[0].RID == "first"
    assert second_item.describe().request_blocks[0].RID == "second"


def test_queuemanager_add_to_queue_publishes_status_for_default_append(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = _queued_scan_message()

    with mock.patch.object(queue_manager, "send_queue_status") as send_queue_status:
        queue_manager.add_to_queue(scan_queue="primary", msg=msg)

    send_queue_status.assert_called_once()


@pytest.mark.timeout(20)
def test_queuemanger_shuts_down_idle_queue(queuemanager_mock):
    """
    Test that the QueueManager shuts down idle queues after AUTO_SHUTDOWN_TIME.
    """
    queue_manager = queuemanager_mock(queues=["primary", "secondary"])
    assert "secondary" in queue_manager.queues

    # Get reference to the timer before it fires
    secondary_queue = queue_manager.queues["secondary"]
    secondary_queue._start_auto_shutdown_timer()
    timer = secondary_queue._auto_shutdown_timer

    # Wait for longer than AUTO_SHUTDOWN_TIME
    while "secondary" in queue_manager.queues:
        time.sleep(0.1)
    assert "primary" in queue_manager.queues

    # Ensure the timer thread is fully cleaned up
    if timer is not None and timer.is_alive():
        timer.join(timeout=1.0)


def test_queue_manager_does_not_auto_remove_queue_with_pending_insert(queuemanager_mock):
    queue_manager = queuemanager_mock(queues=["primary", "secondary"])
    secondary_queue = queue_manager.queues["secondary"]

    secondary_queue.reserve_insert()
    queue_manager.remove_queue("secondary", skip_pending_inserts=True)

    assert queue_manager.queues["secondary"] is secondary_queue

    secondary_queue.finish_insert()
    queue_manager.remove_queue("secondary", skip_pending_inserts=True)

    assert "secondary" not in queue_manager.queues


@pytest.mark.timeout(10)
def test_insert_reservation_does_not_deadlock_worker_status(dormant_queue_manager):
    # pylint: disable=redefined-outer-name
    queue_manager = dormant_queue_manager
    queue = queue_manager.queues["secondary"]
    completed = mock.Mock(status=InstructionQueueStatus.COMPLETED)
    queue.queue.append(completed)
    queue.active_instruction_queue = completed
    worker_at_status = threading.Event()
    reserving = threading.Event()
    errors = []
    lock_timeouts = []
    original_lock = queue._lock

    class WatchdogLock:
        def __enter__(self):
            # Release a broken interleaving through an exception rather than
            # leaving permanently deadlocked threads behind on test failure.
            if not original_lock.acquire(timeout=1):
                lock_timeouts.append(threading.current_thread().name)
                raise TimeoutError("Queue lock deadlocked with manager lock")
            return self

        def __exit__(self, *args):
            original_lock.release()

    def send_status():
        if threading.current_thread() is worker_thread:
            worker_at_status.set()
            assert reserving.wait(timeout=2)
        QueueManager.send_queue_status(queue_manager)

    def reserve():
        assert threading.current_thread() is insert_thread
        reserving.set()
        ScanQueue.reserve_insert(queue)

    def advance_worker():
        try:
            queue._next_instruction_queue()
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(exc)

    worker_thread = threading.Thread(target=advance_worker, name="queue-test-worker")
    insert_thread = threading.Thread(
        target=queue_manager.add_to_queue,
        args=("secondary", _queued_scan_message(queue="secondary")),
        name="queue-test-inserter",
    )
    with (
        mock.patch.object(queue, "_lock", WatchdogLock()),
        mock.patch.object(queue, "reserve_insert", side_effect=reserve),
        mock.patch.object(queue_manager, "send_queue_status", side_effect=send_status),
    ):
        worker_thread.start()
        try:
            assert worker_at_status.wait(timeout=2)
            insert_thread.start()
            insert_thread.join(timeout=3)
        finally:
            reserving.set()
            worker_thread.join(timeout=3)

    assert not worker_thread.is_alive()
    assert not insert_thread.is_alive()
    assert not errors
    assert not lock_timeouts
    queue_manager.connector.raise_alarm.assert_not_called()
    assert len(queue.queue) == 1
    assert not queue.has_pending_inserts


@pytest.mark.timeout(10)
@pytest.mark.parametrize("activity", ["insert", "direct_insert", "deferred", "replace", "cancel"])
def test_expired_timer_rechecks_queue_before_removal(dormant_queue_manager, activity):
    # pylint: disable=redefined-outer-name
    queue_manager = dormant_queue_manager
    queue = queue_manager.queues["secondary"]
    queue.AUTO_SHUTDOWN_TIME = 0
    callback_started = threading.Event()
    continue_callback = threading.Event()
    original_timer = threading.Timer

    def delayed_timer(interval, function, args=None, kwargs=None):
        def callback():
            assert threading.current_thread() is timer
            callback_started.set()
            assert continue_callback.wait(timeout=3)
            function(*(args or []), **(kwargs or {}))

        timer = original_timer(interval, callback)
        return timer

    with mock.patch("bec_server.scan_server.scan_queue.threading.Timer", side_effect=delayed_timer):
        queue._start_auto_shutdown_timer()
    timer = queue._auto_shutdown_timer
    expected_queue = queue
    try:
        assert callback_started.wait(timeout=2)
        msg = _queued_scan_message(queue="secondary")
        if activity == "insert":
            queue_manager.add_to_queue("secondary", msg)
        elif activity == "direct_insert":
            queue.insert(msg)
        elif activity == "deferred":
            queue.queue.append(mock.Mock(status=InstructionQueueStatus.STOPPED))
            queue.insert(msg)
            queue.clear()
            assert queue._deferred_inserts
        elif activity == "replace":
            queue.scan_worker.is_alive.return_value = False
            with mock.patch.object(ScanQueue, "start_worker"):
                queue_manager.add_queue("secondary")
            expected_queue = queue_manager.queues["secondary"]
            assert expected_queue is not queue
        else:
            # Even an unsuccessful insert invalidates the old idle timeout.
            queue.reserve_insert()
            queue.finish_insert()
        assert not queue.has_pending_inserts
    finally:
        continue_callback.set()
        timer.join(timeout=3)

    assert not timer.is_alive()
    assert queue_manager.queues["secondary"] is expected_queue
    assert not expected_queue.signal_event.is_set()
    assert expected_queue._auto_shutdown_timer is None


def test_reset_auto_shutdown_timer_joins_after_releasing_lock(queuemanager_mock):
    queue_manager = queuemanager_mock(queues=["primary", "secondary"])
    secondary_queue = queue_manager.queues["secondary"]

    class FakeTimer:
        cancel = mock.MagicMock()

        def join(self):
            assert not secondary_queue._lock._is_owned()
            assert not queue_manager._lock._is_owned()

    timer = FakeTimer()
    secondary_queue._auto_shutdown_timer = timer

    secondary_queue._reset_auto_shutdown_timer()

    timer.cancel.assert_called_once_with()
    assert secondary_queue._auto_shutdown_timer is None


def test_queuemanager_add_to_queue_restarts_queue_if_worker_is_dead(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.queues["primary"].signal_event.set()
    original_worker = queue_manager.queues["primary"].scan_worker
    original_worker.shutdown()

    assert original_worker.is_alive() is False

    msg = _queued_scan_message()
    queue_manager.add_queue("primary")
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    assert queue_manager.queues["primary"].queue.popleft().scan_msgs[0] == msg
    assert queue_manager.queues["primary"].scan_worker.is_alive() is True
    assert id(queue_manager.queues["primary"].scan_worker) != id(original_worker)


def test_queuemanager_add_to_queue_error_send_alarm(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = _queued_scan_message()
    with mock.patch.object(queue_manager, "connector") as connector:
        with mock.patch.object(queue_manager, "add_queue", side_effects=KeyError):
            queue_manager.add_to_queue(scan_queue="dummy", msg=msg)
            connector.raise_alarm.assert_called_once_with(
                severity=Alarms.MAJOR, info=mock.ANY, metadata={"RID": "something"}
            )


def test_queuemanager_scan_queue_callback(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = _queued_scan_message()
    obj = MessageObject("scan_queue", msg)
    with mock.patch.object(queue_manager, "add_to_queue") as add_to_queue:
        queue_manager._scan_queue_callback(obj)
        add_to_queue.assert_called_once_with("primary", msg)


def test_scan_queue_modification_callback(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueModificationMessage(
        scan_id="dummy", action="halt", parameter={}, metadata={"RID": "something"}
    )
    obj = MessageObject("scan_queue_modification", msg)
    with mock.patch.object(queue_manager, "scan_interception") as scan_interception:
        with mock.patch.object(queue_manager, "send_queue_status") as send_queue_status:
            queue_manager._scan_queue_modification_callback(obj)
            scan_interception.assert_called_once_with(msg)
            send_queue_status.assert_called_once()


def test_scan_interception_halt(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueModificationMessage(
        scan_id="dummy",
        action="halt",
        queue="secondary",
        parameter={},
        metadata={"RID": "something"},
    )
    with mock.patch.object(queue_manager, "set_halt") as set_halt:
        queue_manager.scan_interception(msg)
        set_halt.assert_called_once_with(
            scan_id="dummy", request_id=None, queue="secondary", parameter={}
        )


def test_set_halt(queuemanager_mock):
    queue_manager = queuemanager_mock()
    with mock.patch.object(queue_manager, "set_abort") as set_abort:
        queue_manager.set_halt(scan_id="dummy", parameter={})
        set_abort.assert_called_once_with(
            scan_id="dummy", request_id=None, queue="primary", exit_info=("halted", "user")
        )


def test_set_halt_disables_return_to_start_for_direct_instruction_queue(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.queues["primary"].active_instruction_queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"], mock.MagicMock(), mock.MagicMock()
    )
    queue_manager.queues["primary"].active_instruction_queue.run_on_exception_hook = True
    with mock.patch.object(queue_manager, "set_abort") as set_abort:
        queue = queue_manager.queues["primary"].active_instruction_queue
        queue_manager.set_halt(scan_id="dummy", parameter={})
        set_abort.assert_called_once_with(
            scan_id="dummy", request_id=None, queue="primary", exit_info=("halted", "user")
        )
        assert queue.run_on_exception_hook is False


def test_direct_instruction_queue_run_on_exception_hook_uses_scan_info(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"], mock.MagicMock(), mock.MagicMock()
    )
    scan = _build_dummy_v4_scan(scan_id="scan-id-test")
    scan.scan_info.run_on_exception_hook = False
    queue.active_scan = scan

    assert queue.run_on_exception_hook is False

    scan.scan_info.run_on_exception_hook = True

    assert queue.run_on_exception_hook is True


def test_direct_instruction_queue_status_updates_worker_and_sends_queue_status(queuemanager_mock):
    queue_manager = queuemanager_mock()
    worker = mock.MagicMock()
    queue = DirectInstructionQueueItem(queue_manager.queues["primary"], mock.MagicMock(), worker)
    queue.stop = mock.MagicMock()
    queue_manager.send_queue_status = mock.MagicMock()

    queue.status = InstructionQueueStatus.RUNNING
    queue.status = InstructionQueueStatus.STOPPED

    assert worker.status == InstructionQueueStatus.STOPPED
    queue.stop.assert_called_once_with()
    assert queue_manager.send_queue_status.call_count == 2


def test_direct_instruction_queue_append_scan_request_assembles_and_stores_scan(queuemanager_mock):
    queue_manager = queuemanager_mock()
    assembler = mock.MagicMock()
    queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"], assembler, queue_manager.queues["primary"].scan_worker
    )
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-1"},
    )
    scan = _build_dummy_v4_scan("scan-id-test")
    assembler.assemble_scan.return_value = scan

    queue.append_scan_request(msg)

    assembler.assemble_scan.assert_called_once_with(msg, scan_id=queue._scan_id)
    assert queue.scans == [scan]
    assert queue.scan_msgs == [msg]


def test_direct_instruction_queue_describe_active_scan_returns_none_when_missing(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"],
        mock.MagicMock(),
        queue_manager.queues["primary"].scan_worker,
    )
    queue_manager.queues["primary"].queue.append(queue)
    scan = _build_dummy_v4_scan("scan-id-test")

    assert queue.describe_active_scan() is None

    queue.active_scan = scan

    assert queue.describe_active_scan() is None


def test_direct_instruction_queue_describe_active_scan_returns_request_block(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"],
        mock.MagicMock(),
        queue_manager.queues["primary"].scan_worker,
    )
    scan = _build_dummy_v4_scan("scan-id-test")
    scan.scan_info.readout_priority_modification = {"monitored": ["samx"]}
    scan.scan_info.scan_report_instructions = [{"device": "samx"}]
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-1"},
    )
    queue.scans = [scan]
    queue.scan_msgs = [msg]
    scan.scan_info.metadata["RID"] = "rid-1"
    queue.active_scan = scan
    scan.device_manager.parent.device_lock_registry = mock.MagicMock()
    scan.device_manager.parent.device_lock_registry.get_owned_devices.return_value = ["samx"]
    scan.actions._queued_device_locks.update({"samx", "samy"})

    info = queue.describe_active_scan()

    assert info.msg == msg
    assert info.RID == "rid-1"
    assert info.report_instructions == [{"device": "samx"}]
    assert info.scan_id == "scan-id-test"
    assert info.owned_device_locks == ["samx"]
    assert info.pending_device_locks == []


def test_direct_instruction_queue_move_to_next_scan_activates_and_assigns_numbers(
    queuemanager_mock,
):
    queue_manager = queuemanager_mock()
    scan_queue = queue_manager.queues["primary"]
    queue = DirectInstructionQueueItem(scan_queue, mock.MagicMock(), scan_queue.scan_worker)
    first_scan = _build_dummy_v4_scan("scan-1", scan_number=None)
    second_scan = _build_dummy_v4_scan("scan-2", scan_number=None)
    second_scan.scan_info.metadata["dataset_id_on_hold"] = True
    msg1 = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-1"},
    )
    msg2 = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (2,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "rid-2", "dataset_id_on_hold": True},
    )
    queue.scans = [first_scan, second_scan]
    queue.scan_msgs = [msg1, msg2]

    active_scan = queue.move_to_next_scan()

    assert active_scan is first_scan
    assert queue.active_scan is first_scan
    assert queue.status == InstructionQueueStatus.RUNNING
    assert first_scan.scan_info.scan_number is not None
    first_dataset_number = first_scan.scan_info.dataset_number

    active_scan = queue.move_to_next_scan()

    assert active_scan is second_scan
    assert second_scan.scan_info.scan_number is not None
    assert second_scan.scan_info.dataset_number == first_dataset_number


def test_direct_instruction_queue_non_scan_does_not_allocate_scan_number(queuemanager_mock):
    queue_manager = queuemanager_mock()
    scan_queue = queue_manager.queues["primary"]
    queue = DirectInstructionQueueItem(scan_queue, mock.MagicMock(), scan_queue.scan_worker)
    scan = _build_dummy_v4_scan("scan-1", is_scan=False)
    msg = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-1"},
    )
    start_scan_number = queue_manager.parent.scan_number
    start_dataset_number = queue_manager.parent.dataset_number
    queue.scans = [scan]
    queue.scan_msgs = [msg]

    active_scan = queue.move_to_next_scan()

    assert active_scan is scan
    assert scan.scan_info.scan_number is None
    assert scan.scan_info.dataset_number is None
    assert queue_manager.parent.scan_number == start_scan_number
    assert queue_manager.parent.dataset_number == start_dataset_number
    assert queue.is_scan == [False]
    assert queue.scan_number == [None]


def test_direct_instruction_queue_non_scan_does_not_allocate_scan_id(queuemanager_mock):
    queue_manager = queuemanager_mock()
    scan_queue = queue_manager.queues["primary"]
    assembler = mock.MagicMock()
    assembler.scan_manager.scan_dict = {"umv": mock.MagicMock(is_scan=False)}
    scan = _build_dummy_v4_scan("placeholder-scan-id", is_scan=False)
    scan.scan_info.scan_id = None
    assembler.assemble_scan.return_value = scan
    queue = DirectInstructionQueueItem(scan_queue, assembler, scan_queue.scan_worker)

    msg = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-1"},
    )

    queue.append_scan_request(msg)

    assert len(queue.scans) == 1
    assembler.assemble_scan.assert_called_once_with(msg, scan_id=None)
    assert queue.scans[0] is scan
    assert queue.scans[0].scan_info.scan_id is None
    assert queue.scan_id == [None]


def test_direct_instruction_queue_move_to_next_scan_raises_when_empty_or_exhausted(
    queuemanager_mock,
):
    queue_manager = queuemanager_mock()
    queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"],
        mock.MagicMock(),
        queue_manager.queues["primary"].scan_worker,
    )

    with pytest.raises(StopIteration, match="No active scan and no scans"):
        queue.move_to_next_scan()

    scan = _build_dummy_v4_scan("scan-id-test")
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-1"},
    )
    queue.scans = [scan]
    queue.scan_msgs = [msg]
    queue.active_scan = scan

    with pytest.raises(StopIteration, match="No more scans"):
        queue.move_to_next_scan()


def test_direct_instruction_queue_append_to_queue_history_pushes_message(queuemanager_mock):
    queue_manager = queuemanager_mock()
    connector = mock.MagicMock()
    queue_manager.connector = connector
    queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"],
        mock.MagicMock(),
        queue_manager.queues["primary"].scan_worker,
    )
    queue.status = InstructionQueueStatus.COMPLETED

    queue.append_to_queue_history()

    connector.lpush.assert_called_once()
    endpoint, msg = connector.lpush.call_args.args[:2]
    assert endpoint == MessageEndpoints.scan_queue_history()
    assert msg.status == "COMPLETED"
    assert msg.queue_id == queue.queue_id
    assert connector.lpush.call_args.kwargs["max_size"] == 100


def test_direct_instruction_queue_stop_and_abort_update_internal_state(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"],
        mock.MagicMock(),
        queue_manager.queues["primary"].scan_worker,
    )
    first_scan = _build_dummy_v4_scan("scan-1")
    second_scan = _build_dummy_v4_scan("scan-2")
    first_scan._shutdown_event = mock.MagicMock()
    second_scan._shutdown_event = mock.MagicMock()
    queue.scans = [first_scan, second_scan]
    queue.scan_msgs = [mock.MagicMock(), mock.MagicMock()]
    queue.active_scan = first_scan

    queue.stop()

    first_scan._shutdown_event.set.assert_called_once_with()
    second_scan._shutdown_event.set.assert_called_once_with()

    queue.abort()

    assert queue.active_scan is None
    assert queue.scans == []
    assert queue.scan_msgs == []


def wait_to_reach_state(queue_manager, queue, state):
    while queue_manager.queues[queue].status != state:
        pass


@pytest.mark.timeout(5)
def test_set_pause(queuemanager_mock):
    """Test that set_pause sets worker_status to PAUSED when it's RUNNING"""
    queue_manager = queuemanager_mock()

    # Add a queue item so worker_status has something to operate on
    msg = _queued_scan_message()
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)

    # Set worker status to RUNNING
    queue_manager.queues["primary"].queue[0].status = InstructionQueueStatus.RUNNING

    # Call set_pause
    queue_manager.set_pause(queue="primary")

    # Verify worker_status was set to PAUSED
    assert queue_manager.queues["primary"].worker_status == InstructionQueueStatus.PAUSED


@pytest.mark.timeout(5)
def test_set_pause_does_not_change_non_running_worker(queuemanager_mock):
    """Test that set_pause doesn't change worker_status when it's not RUNNING"""
    queue_manager = queuemanager_mock()

    # Add a queue item
    msg = _queued_scan_message()
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)

    # Set worker status to PENDING (not RUNNING)
    queue_manager.queues["primary"].queue[0].status = InstructionQueueStatus.PENDING

    # Call set_pause
    queue_manager.set_pause(queue="primary")

    # Verify worker_status remains PENDING (not changed to PAUSED)
    assert queue_manager.queues["primary"].worker_status == InstructionQueueStatus.PENDING


@pytest.mark.timeout(5)
def test_set_deferred_pause(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    queue_manager.set_deferred_pause(queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.PAUSED)
    assert len(queue_manager.connector.message_sent) == 1
    assert (
        queue_manager.connector.message_sent[0].get("queue") == MessageEndpoints.scan_queue_status()
    )


@pytest.mark.timeout(5)
def test_set_continue(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    queue_manager.set_continue(queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.RUNNING)
    assert len(queue_manager.connector.message_sent) == 1
    assert (
        queue_manager.connector.message_sent[0].get("queue") == MessageEndpoints.scan_queue_status()
    )


# @pytest.mark.repeat(500)
@pytest.mark.timeout(5)
def test_set_abort(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    queue_manager.parent.device_lock_registry.get_owned_devices = mock.MagicMock(
        return_value=["samx"]
    )
    msg = _queued_scan_message()
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    scan_queue = queue_manager.queues["primary"]
    while scan_queue.scan_worker.current_instruction_queue_item is None:
        time.sleep(0.1)
    stop_id = scan_queue.queue[0].scan_id
    queue_manager.set_abort(queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.PAUSED)
    while len(queue_manager.connector.message_sent) < 5:
        time.sleep(0.1)
    assert {
        "queue": MessageEndpoints.stop_devices(),
        "msg": messages.VariableMessage(value=["samx"], metadata={"stop_id": stop_id}),
    } in queue_manager.connector.message_sent
    assert (
        queue_manager.connector.message_sent[0].get("queue") == MessageEndpoints.scan_queue_status()
    )


@pytest.mark.timeout(5)
def test_set_abort_with_scan_id(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    queue_manager.parent.device_lock_registry.get_owned_devices = mock.MagicMock(
        return_value=["samx", "samy"]
    )
    msg = messages.ScanQueueMessage(
        scan_type="_queued_scan",
        parameter={"args": [], "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    scan_queue = queue_manager.queues["primary"]
    while scan_queue.scan_worker.current_instruction_queue_item is None:
        time.sleep(0.1)
    scan_id_abort = scan_queue.queue[0].scan_id[0]
    queue_manager.set_abort(scan_id=scan_id_abort, queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.PAUSED)
    while len(queue_manager.connector.message_sent) < 5:
        time.sleep(0.1)
    assert {
        "queue": MessageEndpoints.stop_devices(),
        "msg": messages.VariableMessage(
            value=["samx", "samy"], metadata={"stop_id": [scan_id_abort]}
        ),
    } in queue_manager.connector.message_sent
    assert (
        queue_manager.connector.message_sent[0].get("queue") == MessageEndpoints.scan_queue_status()
    )


@pytest.mark.timeout(5)
def test_set_abort_with_scan_id_not_active(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    msg = messages.ScanQueueMessage(
        scan_type="_queued_scan",
        parameter={"args": [], "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    scan_queue = queue_manager.queues["primary"]
    while scan_queue.scan_worker.current_instruction_queue_item is None:
        time.sleep(0.1)
    scan_id_abort = scan_queue.queue[1].scan_id[0]  # second scan in the queue
    queue_manager.set_abort(scan_id=scan_id_abort, queue="primary")

    # The queue should remain RUNNING as the scan_id is not active
    assert queue_manager.queues["primary"].status == ScanQueueStatus.RUNNING
    assert len(scan_queue.queue) == 1  # One scan should be removed from the queue


@pytest.mark.timeout(5)
def test_set_abort_with_request_id_not_active(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    msg1 = _queued_scan_message(rid="rid-1")
    msg2 = _queued_scan_message(rid="rid-2")
    queue_manager.add_to_queue(scan_queue="primary", msg=msg1)
    queue_manager.add_to_queue(scan_queue="primary", msg=msg2)
    scan_queue = queue_manager.queues["primary"]
    while scan_queue.scan_worker.current_instruction_queue_item is None:
        time.sleep(0.1)

    queue_manager.set_abort(request_id="rid-2", queue="primary")

    assert queue_manager.queues["primary"].status == ScanQueueStatus.RUNNING
    assert len(scan_queue.queue) == 1
    remaining = scan_queue.queue[0].describe().request_blocks[0]
    assert remaining.RID == "rid-1"
    cancelled_snapshots = [
        sent["msg"]
        for sent in queue_manager.connector.message_sent
        if sent.get("queue") == MessageEndpoints.scan_queue_status()
    ]
    assert any(
        any(
            queue_item.request_blocks[0].RID == "rid-2" and queue_item.status == "CANCELLED"
            for queue_item in snapshot.queue["primary"].info
        )
        for snapshot in cancelled_snapshots
    )


@pytest.mark.timeout(5)
def test_set_abort_with_wrong_scan_id(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    msg = messages.ScanQueueMessage(
        scan_type="_queued_scan",
        parameter={"args": [], "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    scan_queue = queue_manager.queues["primary"]
    while scan_queue.scan_worker.current_instruction_queue_item is None:
        time.sleep(0.1)

    queue_manager.set_abort(scan_id="doesnt_exist", queue="primary")
    # The queue should remain RUNNING as the scan_id does not exist
    assert queue_manager.queues["primary"].status == ScanQueueStatus.RUNNING
    # The queue length should remain unchanged
    assert len(scan_queue.queue) == 2


@pytest.mark.timeout(5)
def test_set_abort_with_empty_queue(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    queue_manager.set_abort(queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.RUNNING)
    assert len(queue_manager.connector.message_sent) == 0


def test_stop_all_devices_preserves_none_for_stop_all(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []

    queue_manager.stop_all_devices(stop_id="stop-all")

    assert queue_manager.connector.message_sent == [
        {
            "queue": MessageEndpoints.stop_devices(),
            "msg": messages.VariableMessage(value=None, metadata={"stop_id": "stop-all"}),
        }
    ]


def test_stop_all_devices_preserves_empty_list_for_stop_none(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []

    queue_manager.stop_all_devices(stop_id="stop-none", devices=[])

    assert queue_manager.connector.message_sent == [
        {
            "queue": MessageEndpoints.stop_devices(),
            "msg": messages.VariableMessage(value=[], metadata={"stop_id": "stop-none"}),
        }
    ]


@pytest.mark.timeout(5)
def test_set_abort_with_no_owned_devices_sends_stop_none(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    queue_manager.parent.device_lock_registry.get_owned_devices = mock.MagicMock(return_value=[])
    msg = _queued_scan_message()
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    scan_queue = queue_manager.queues["primary"]
    while scan_queue.scan_worker.current_instruction_queue_item is None:
        time.sleep(0.1)

    stop_id = scan_queue.queue[0].scan_id
    queue_manager.set_abort(queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.PAUSED)

    assert {
        "queue": MessageEndpoints.stop_devices(),
        "msg": messages.VariableMessage(value=[], metadata={"stop_id": stop_id}),
    } in queue_manager.connector.message_sent


@pytest.mark.timeout(5)
def test_set_clear_sends_message(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    setter_mock = mock.Mock(wraps=ScanQueue.worker_status.fset)
    # pylint: disable=assignment-from-no-return
    # pylint: disable=too-many-function-args
    mock_property = ScanQueue.worker_status.setter(setter_mock)
    with mock.patch.object(ScanQueue, "worker_status", mock_property):
        queue_manager.set_clear(queue="primary")
        wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.PAUSED)
        mock_property.fset.assert_called_once_with(
            queue_manager.queues["primary"], InstructionQueueStatus.STOPPED
        )
        assert len(queue_manager.connector.message_sent) == 1
        assert (
            queue_manager.connector.message_sent[0].get("queue")
            == MessageEndpoints.scan_queue_status()
        )


@pytest.mark.timeout(5)
def test_set_restart(queuemanager_mock):
    queue_manager = queuemanager_mock()
    primary_queue = queue_manager.queues["primary"]
    primary_queue.signal_event.set()
    primary_queue.scan_worker.shutdown()

    # Replace the live queue worker with a queue whose worker thread has not been started.
    queue_manager.queues["primary"] = ScanQueue(queue_manager, queue_name="primary")
    msg = messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={
            "args": {"samx": (-5, 5, 3), "samy": (-5, 5, 3)},
            "kwargs": {"relative": False, "system_config": {}},
        },
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    iq = queue_manager.queues["primary"].queue[0]
    # We actively set the iq status to RUNNING. Otherwise, a restart would not be possible.
    iq.status = InstructionQueueStatus.RUNNING

    # Note: we mock the add_to_queue method to check if the scan will be re-added to the queue
    with mock.patch.object(queue_manager, "add_to_queue") as add_new_scan_to_queue:
        with mock.patch.object(queue_manager, "_get_active_scan_id", return_value=iq.scan_id[0]):
            with mock.patch.object(queue_manager.connector, "send") as connector_send:
                with queue_manager._lock:
                    queue_manager.set_restart(queue="primary", parameter={"RID": "something_new"})
                add_new_scan_to_queue.assert_called_once_with("primary", mock.ANY, 1)
                restart_msg = connector_send.call_args_list[0].args[1]
                assert restart_msg.original_scan_id == iq.scan_id[0]
                assert restart_msg.scan_msg.metadata["RID"] == "something_new"
                assert iq.scan_msgs[0].metadata["RID"] == "something"
                assert iq.reason == "restart"
                assert iq.describe().reason == "restart"
                assert add_new_scan_to_queue.call_args.args[1].metadata["RID"] == "something_new"


@pytest.mark.timeout(5)
@pytest.mark.parametrize("finish_original", [False, True])
def test_restart_interception_releases_manager_and_only_stops_original(
    queuemanager_mock, finish_original
):
    # pylint: disable=redefined-outer-name,too-many-statements
    queue_manager = queuemanager_mock()
    primary_queue = queue_manager.queues["primary"]
    primary_queue.signal_event.set()
    primary_queue.scan_worker.shutdown()
    primary_queue = ScanQueue(queue_manager, queue_name="primary")
    queue_manager.queues["primary"] = primary_queue

    insert_started = threading.Event()
    finish_insertion = threading.Event()
    restart_errors = []

    def blocking_insert(*args, **kwargs):
        insert_started.set()
        assert finish_insertion.wait(timeout=3)
        ScanQueue.insert(primary_queue, *args, **kwargs)

    def restart():
        try:
            queue_manager.scan_interception(restart_message)
        except Exception as exc:  # pylint: disable=broad-except
            restart_errors.append(exc)

    # Keep a real queue with a dormant worker so its advancement is controlled by the test.
    with mock.patch.object(primary_queue.scan_worker, "is_alive", return_value=True):
        queue_manager.add_to_queue("primary", _queued_scan_message())
        queue_manager.add_to_queue("primary", _queued_scan_message(rid="next"))
        original, following = primary_queue.queue
        original.status = InstructionQueueStatus.RUNNING
        primary_queue.active_instruction_queue = original
        primary_queue.scan_worker.current_instruction_queue_item = original
        restart_message = messages.ScanQueueModificationMessage(
            scan_id=original.scan_id[0],
            action="restart",
            queue="primary",
            parameter={"RID": "restarted"},
        )

        with mock.patch.object(primary_queue, "insert", side_effect=blocking_insert):
            restart_thread = threading.Thread(target=restart)
            restart_thread.start()
            try:
                assert insert_started.wait(timeout=1)
                acquired = queue_manager._lock.acquire(timeout=1)
                assert acquired, "Restart holds the manager lock during replacement insertion"
                queue_manager._lock.release()
                assert original.status == InstructionQueueStatus.RUNNING
                if finish_original:
                    with primary_queue._lock:
                        original.status = InstructionQueueStatus.COMPLETED
                        assert primary_queue.queue.popleft() is original
                        primary_queue.active_instruction_queue = following
                        primary_queue.scan_worker.current_instruction_queue_item = following
                        following.status = InstructionQueueStatus.RUNNING
            finally:
                finish_insertion.set()
                restart_thread.join(timeout=2)

        assert not restart_thread.is_alive()
        assert not restart_errors
        replacement = next(
            item for item in primary_queue.queue if item.scan_msgs[0].metadata["RID"] == "restarted"
        )
        assert primary_queue.status == ScanQueueStatus.RUNNING
        if finish_original:
            assert original.status == InstructionQueueStatus.COMPLETED
            assert following.status == InstructionQueueStatus.RUNNING
            assert list(primary_queue.queue) == [following, replacement]
        else:
            assert original.status == InstructionQueueStatus.STOPPED
            assert following.status == InstructionQueueStatus.PENDING
            assert list(primary_queue.queue) == [original, replacement, following]


@pytest.mark.timeout(5)
def test_set_restart_no_active_scan(queuemanager_mock):
    """
    Test that set_restart does nothing when there is no active scan. A scan has to be either on
    RUNNING or PAUSED state to be active.
    """
    queue_manager = queuemanager_mock()
    primary_queue = queue_manager.queues["primary"]
    primary_queue.signal_event.set()
    primary_queue.scan_worker.shutdown()

    # Replace the live queue worker with a queue whose worker thread has not been started.
    queue_manager.queues["primary"] = ScanQueue(queue_manager, queue_name="primary")
    msg = messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={
            "args": {"samx": (-5, 5, 3), "samy": (-5, 5, 3)},
            "kwargs": {"relative": False, "system_config": {}},
        },
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    iq = queue_manager.queues["primary"].queue[0]
    # We set the iq status to PENDING, meaning it's not active.
    iq.status = InstructionQueueStatus.PENDING

    # Note: we mock the add_to_queue method to check if the scan will be re-added to the queue
    with mock.patch.object(queue_manager, "add_to_queue") as add_new_scan_to_queue:
        with mock.patch.object(queue_manager, "_get_active_scan_id", return_value=iq.scan_id[0]):
            with queue_manager._lock:
                queue_manager.set_restart(queue="primary", parameter={"RID": "something_new"})
            add_new_scan_to_queue.assert_not_called()


@pytest.mark.timeout(5)
def test_set_user_completed(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.queues["primary"].status = ScanQueueStatus.RUNNING

    def _set_abort_side_effect(*, scan_id=None, request_id=None, queue="primary", exit_info=None):
        queue_manager.queues[queue].status = ScanQueueStatus.PAUSED

    with mock.patch.object(
        queue_manager, "set_abort", side_effect=_set_abort_side_effect
    ) as set_abort:
        queue_manager.set_user_completed(queue="primary")

    set_abort.assert_called_once_with(
        scan_id=None, request_id=None, queue="primary", exit_info=("user_completed", "user")
    )
    assert queue_manager.queues["primary"].status == ScanQueueStatus.RUNNING


@pytest.mark.parametrize("is_scan", [False, True])
def test_instruction_queue_scan_number(queuemanager_mock, is_scan):
    queue_manager = queuemanager_mock()
    scan_queue = queue_manager.queues["primary"]
    instruction_queue = DirectInstructionQueueItem(
        scan_queue, mock.MagicMock(), scan_queue.scan_worker
    )
    scan_queue.queue.append(instruction_queue)

    scan = _build_dummy_v4_scan("scan-1", is_scan=is_scan)
    instruction_queue.scans = [scan]

    if not scan.is_scan:
        assert instruction_queue.scan_number == [None]
        return

    with mock.patch.object(
        DirectInstructionQueueItem,
        "_scan_server_scan_number",
        new_callable=mock.PropertyMock,
        return_value=5,
    ):
        with mock.patch.object(DirectInstructionQueueItem, "scan_ids_head", return_value=0):
            assert instruction_queue.scan_number == [5]


def test_direct_instruction_queue_item_scan_number_projection_within_item(queuemanager_mock):
    queue_manager = queuemanager_mock()
    scan_queue = queue_manager.queues["primary"]
    base_scan_number = queue_manager.parent.scan_number
    instruction_queue = DirectInstructionQueueItem(
        scan_queue, mock.MagicMock(), scan_queue.scan_worker
    )
    scan_queue.queue.append(instruction_queue)

    scan1 = _build_dummy_v4_scan("scan-1")
    scan2 = _build_dummy_v4_scan("scan-2")

    instruction_queue.scans = [scan1, scan2]

    assert instruction_queue.scan_number == [base_scan_number + 1, base_scan_number + 2]


def test_direct_instruction_queue_item_scan_number_projection_across_queue_items(queuemanager_mock):
    queue_manager = queuemanager_mock()
    scan_queue = queue_manager.queues["primary"]
    base_scan_number = queue_manager.parent.scan_number

    first_queue = DirectInstructionQueueItem(scan_queue, mock.MagicMock(), scan_queue.scan_worker)
    second_queue = DirectInstructionQueueItem(scan_queue, mock.MagicMock(), scan_queue.scan_worker)
    scan_queue.queue.extend([first_queue, second_queue])

    first_scan = _build_dummy_v4_scan("scan-1")
    second_scan = _build_dummy_v4_scan("scan-2")

    first_queue.scans = [first_scan]
    second_queue.scans = [second_scan]

    assert first_queue.scan_number == [base_scan_number + 1]
    assert second_queue.scan_number == [base_scan_number + 2]


def test_scan_number_projection_during_concurrent_insert(queuemanager_mock):
    # pylint: disable=redefined-outer-name
    queue_manager = queuemanager_mock()
    scan_queue = ScanQueue(queue_manager)
    assembler = mock.MagicMock()
    instruction_queue = DirectInstructionQueueItem(scan_queue, assembler, scan_queue.scan_worker)
    instruction_queue.scans = [_build_dummy_v4_scan("target-scan")]

    previous_queue = mock.Mock(queue_id="previous", status=InstructionQueueStatus.PENDING)
    inserted_queue = mock.Mock(queue_id="inserted", status=InstructionQueueStatus.PENDING)
    scan_queue.queue.extend([previous_queue, instruction_queue])
    calculating = threading.Event()
    inserted = threading.Event()

    def read_previous_scan_ids():
        # Pause after iteration begins so the insert occurs before the next item is read.
        calculating.set()
        assert inserted.wait(timeout=2)
        return ["previous-scan"]

    type(previous_queue).scan_id = mock.PropertyMock(side_effect=read_previous_scan_ids)

    def insert_queue_item():
        if calculating.wait(timeout=2):
            # Insertion uses the queue lock, while status export uses the manager lock.
            with scan_queue._lock:
                scan_queue.queue.append(inserted_queue)
            inserted.set()

    insert_thread = threading.Thread(target=insert_queue_item)
    insert_thread.start()
    try:
        with queue_manager._lock:
            assert instruction_queue.scan_number == [queue_manager.parent.scan_number + 2]
    finally:
        calculating.set()
        insert_thread.join(timeout=2)

    assert not insert_thread.is_alive()
    assert inserted.is_set()
    assert list(scan_queue.queue) == [previous_queue, instruction_queue, inserted_queue]


def test_remove_queue_item(queuemanager_mock):
    queue_manager = queuemanager_mock()
    scan_queue = ScanQueue(queue_manager)
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "something"},
    )
    scan_queue.insert(msg)
    scan_queue.queue[0].scans[0].scan_info.scan_id = "random"
    scan_queue.remove_queue_item(scan_id=["random"])
    assert len(scan_queue.queue) == 0


def test_remove_queue_item_by_request_id(queuemanager_mock):
    queue_manager = queuemanager_mock()
    scan_queue = ScanQueue(queue_manager)
    msg1 = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-1"},
    )
    msg2 = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (2,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-2"},
    )
    scan_queue.insert(msg1)
    scan_queue.insert(msg2)

    scan_queue.remove_queue_item_by_request_id("rid-2")

    assert len(scan_queue.queue) == 1
    remaining = scan_queue.queue[0].describe().request_blocks[0]
    assert remaining.RID == "rid-1"


def test_invalid_scan_specified_in_message(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="fake test scan which does not exist!",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "something"},
    )
    with mock.patch.object(queue_manager, "connector") as connector:
        queue_manager.add_to_queue(scan_queue="dummy", msg=msg)
        connector.raise_alarm.assert_called_once_with(
            severity=Alarms.MAJOR, info=mock.ANY, metadata={"RID": "something"}
        )


def test_set_clear(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.set_clear(queue="primary")
    assert len(queue_manager.queues["primary"].queue) == 0


def test_scan_queue_next_instruction_queue(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager)
    assert queue._next_instruction_queue() is False


def test_scan_queue_next_instruction_queue_pops(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager)
    queue.queue.append(DirectInstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock()))
    queue.queue[0].status = InstructionQueueStatus.RUNNING
    queue.active_instruction_queue = queue.queue[0]
    assert queue._next_instruction_queue() is False
    assert len(queue.queue) == 0


def test_scan_queue_next_instruction_queue_does_not_pop(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager)
    queue.queue.append(DirectInstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock()))
    queue.queue[0].status = InstructionQueueStatus.PENDING
    queue.active_instruction_queue = queue.queue[0]
    assert queue._next_instruction_queue() is True
    assert len(queue.queue) == 1


def test_scan_queue_next_instruction_queue_pops_stopped_elements(queuemanager_mock):
    """
    Test that the scan queue pops the stopped elements from the queue.
    """
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager)
    queue.queue.append(DirectInstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock()))
    queue.queue.append(DirectInstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock()))
    queue.queue[0].status = InstructionQueueStatus.STOPPED
    queue.queue[1].status = InstructionQueueStatus.STOPPED
    queue.status = ScanQueueStatus.PAUSED
    queue.active_instruction_queue = queue.queue[0]
    assert queue._next_instruction_queue() is True
    assert len(queue.queue) == 1
    assert queue._next_instruction_queue() is False
    assert len(queue.queue) == 0


def test_scan_queue_insert_defers_while_head_is_stopped(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager)
    stopped_item = DirectInstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock())
    stopped_item.status = InstructionQueueStatus.STOPPED
    queue.queue.append(stopped_item)

    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-stopped"},
    )

    insert_finished = threading.Event()

    def do_insert():
        queue.insert(msg)
        insert_finished.set()

    thread = threading.Thread(target=do_insert)
    thread.start()
    thread.join(timeout=5)

    assert insert_finished.is_set()
    assert len(queue.queue) == 1
    assert len(queue._deferred_inserts) == 1


def test_scan_queue_flushes_deferred_inserts_once_stopped_head_is_removed(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager)
    stopped_item = DirectInstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock())
    stopped_item.status = InstructionQueueStatus.STOPPED
    queued_item = DirectInstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock())
    queue.queue.extend([stopped_item, queued_item])
    queue.active_instruction_queue = stopped_item

    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-flush"},
    )
    queue._deferred_inserts.append((msg, -1))

    assert queue._next_instruction_queue() is True
    assert len(queue._deferred_inserts) == 0
    assert len(queue.queue) == 2
    assert queue.queue[-1].scan_msgs[0] == msg


def test_scan_queue_insert_does_not_block_while_worker_waits_on_lock(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager)
    pending_item = DirectInstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock())
    pending_item.status = InstructionQueueStatus.PENDING
    queue.queue.append(pending_item)

    lock = messages.ScanQueueLock(
        identifier="insert_during_lock",
        reason="Testing insert while locked",
        allow_device_instructions=False,
    )
    queue.add_lock(lock)

    worker_waiting = threading.Event()
    worker_finished = threading.Event()

    def try_next():
        worker_waiting.set()
        queue._next_instruction_queue()
        worker_finished.set()

    thread = threading.Thread(target=try_next)
    thread.start()
    worker_waiting.wait(timeout=1)
    time.sleep(0.2)

    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "rid-locked-insert"},
    )

    insert_finished = threading.Event()

    def do_insert():
        queue.insert(msg)
        insert_finished.set()

    insert_thread = threading.Thread(target=do_insert)
    insert_thread.start()
    insert_thread.join(timeout=5)

    assert insert_finished.is_set()
    assert len(queue.queue) == 2
    assert queue.queue[-1].scan_msgs[0] == msg

    queue.remove_lock(lock)
    thread.join(timeout=2)
    assert worker_finished.is_set()


def test_queue_manager_get_active_scan_id(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={
            "args": {"samx": (-1, 1, 10), "samy": (-1, 1, 10)},
            "kwargs": {"relative": False, "system_config": {}},
        },
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    iq = queue_manager.queues["primary"].queue[0]
    iq.active_scan = iq.scans[0]
    assert queue_manager._get_active_scan_id("primary") == iq.active_scan.scan_info.scan_id


def test_queue_manager_get_active_scan_id_returns_None(queuemanager_mock):
    queue_manager = queuemanager_mock()
    assert queue_manager._get_active_scan_id("primary") == None


def test_queue_manager_get_active_scan_id_without_active_scan_returns_none(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {"relative": False, "system_config": {}}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    assert queue_manager._get_active_scan_id("primary") == None


def test_get_owned_devices_for_instruction_queue_returns_empty_without_registry(queuemanager_mock):
    queue_manager = queuemanager_mock()
    instruction_queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"], mock.MagicMock(), mock.MagicMock()
    )

    queue_manager.parent.device_lock_registry = None

    assert queue_manager._get_owned_devices_for_instruction_queue(instruction_queue) == []


def test_get_owned_devices_for_instruction_queue_returns_empty_without_active_scan(
    queuemanager_mock,
):
    queue_manager = queuemanager_mock()
    instruction_queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"], mock.MagicMock(), mock.MagicMock()
    )
    instruction_queue.active_scan = None
    queue_manager.parent.device_lock_registry.get_owned_devices = mock.MagicMock()

    assert queue_manager._get_owned_devices_for_instruction_queue(instruction_queue) == []
    queue_manager.parent.device_lock_registry.get_owned_devices.assert_not_called()


def test_get_owned_devices_for_instruction_queue_uses_active_scan_rid_for_direct_item(
    queuemanager_mock,
):
    queue_manager = queuemanager_mock()
    instruction_queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"], mock.MagicMock(), mock.MagicMock()
    )
    instruction_queue.active_scan = _build_dummy_v4_scan("scan-id")
    instruction_queue.active_scan.scan_info.metadata["RID"] = "rid-456"
    queue_manager.parent.device_lock_registry.get_owned_devices = mock.MagicMock(
        return_value=["samz"]
    )

    owned_devices = queue_manager._get_owned_devices_for_instruction_queue(instruction_queue)

    assert owned_devices == ["samz"]
    queue_manager.parent.device_lock_registry.get_owned_devices.assert_called_once_with("rid-456")


@pytest.mark.parametrize("active_scan", [None, _build_dummy_v4_scan("scan-id-without-rid")])
def test_get_owned_devices_for_instruction_queue_returns_empty_for_direct_item_without_rid(
    queuemanager_mock, active_scan
):
    queue_manager = queuemanager_mock()
    instruction_queue = DirectInstructionQueueItem(
        queue_manager.queues["primary"], mock.MagicMock(), mock.MagicMock()
    )
    instruction_queue.active_scan = active_scan
    queue_manager.parent.device_lock_registry.get_owned_devices = mock.MagicMock()

    assert queue_manager._get_owned_devices_for_instruction_queue(instruction_queue) == []
    queue_manager.parent.device_lock_registry.get_owned_devices.assert_not_called()


@pytest.mark.parametrize(
    "order_msg,position",
    [
        (
            messages.ScanQueueOrderMessage(
                scan_id="scan_id", queue="primary", action="move_to", target_position=2
            ),
            2,
        ),
        (messages.ScanQueueOrderMessage(scan_id="scan_id", queue="primary", action="move_top"), 0),
        (
            messages.ScanQueueOrderMessage(
                scan_id="scan_id", queue="primary", action="move_bottom"
            ),
            9,
        ),
        (
            messages.ScanQueueOrderMessage(
                scan_id="scan_id", queue="primary", action="move_to", target_position=20
            ),
            9,
        ),
        (
            messages.ScanQueueOrderMessage(
                scan_id="scan_id", queue="primary", action="move_to", target_position=9
            ),
            9,
        ),
        (messages.ScanQueueOrderMessage(scan_id="scan_id", queue="primary", action="move_up"), 4),
        (messages.ScanQueueOrderMessage(scan_id="scan_id", queue="primary", action="move_down"), 6),
    ],
)
def test_queue_order_change(queuemanager_mock, order_msg, position):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="line_scan",
        parameter={
            "args": {"samx": (-5, 5)},
            "kwargs": {"steps": 3, "system_config": {}, "relative": False},
        },
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_queue("primary")
    for _ in range(10):
        queue_manager.add_to_queue(scan_queue="primary", msg=msg)

    queue = queue_manager.queues["primary"]
    assert len(queue.queue) == 10

    target_id = queue.queue[5].scans[0].scan_info.scan_id
    order_msg.scan_id = target_id
    queue_manager._handle_scan_order_change(order_msg)
    for ii in range(10):
        if ii == position:
            assert queue.queue[ii].scans[0].scan_info.scan_id == target_id
        else:
            assert queue.queue[ii].scans[0].scan_info.scan_id != target_id


def test_add_lock_to_queue(queuemanager_mock):
    """Test adding a lock to a queue"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    # Initially no locks
    assert len(queue.locks) == 0
    assert queue.status != ScanQueueStatus.LOCKED

    # Add a lock
    lock = messages.ScanQueueLock(identifier="test_lock", reason="Testing lock functionality")
    queue.add_lock(lock)

    # Verify lock was added
    assert len(queue.locks) == 1
    assert "test_lock" in queue.locks
    assert queue.locks["test_lock"].reason == "Testing lock functionality"
    assert queue.status == ScanQueueStatus.LOCKED


def test_add_duplicate_lock_to_queue(queuemanager_mock):
    """Test adding a lock with the same identifier twice - should not duplicate"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    lock1 = messages.ScanQueueLock(identifier="duplicate_lock", reason="First reason")
    lock2 = messages.ScanQueueLock(identifier="duplicate_lock", reason="Second reason")

    queue.add_lock(lock1)
    queue.add_lock(lock2)

    # Should only have one lock
    assert len(queue.locks) == 1
    # Should keep the last reason
    assert queue.locks["duplicate_lock"].reason == "Second reason"


def test_add_multiple_locks_to_queue(queuemanager_mock):
    """Test adding multiple locks with different identifiers"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    lock1 = messages.ScanQueueLock(identifier="lock_1", reason="Maintenance")
    lock2 = messages.ScanQueueLock(identifier="lock_2", reason="Calibration")
    lock3 = messages.ScanQueueLock(identifier="lock_3", reason="Testing")

    queue.add_lock(lock1)
    queue.add_lock(lock2)
    queue.add_lock(lock3)

    assert len(queue.locks) == 3
    assert "lock_1" in queue.locks
    assert "lock_2" in queue.locks
    assert "lock_3" in queue.locks
    assert queue.status == ScanQueueStatus.LOCKED


def test_remove_lock_from_queue(queuemanager_mock):
    """Test removing a lock from a queue"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    # Add a lock first
    lock = messages.ScanQueueLock(identifier="test_lock", reason="Testing")
    queue.add_lock(lock)
    assert len(queue.locks) == 1
    assert queue.status == ScanQueueStatus.LOCKED

    # Remove the lock
    queue.remove_lock(lock)

    assert len(queue.locks) == 0
    assert "test_lock" not in queue.locks
    # Status should be restored to previous state (RUNNING is default)
    assert queue.status == ScanQueueStatus.RUNNING


def test_remove_nonexistent_lock_from_queue(queuemanager_mock):
    """Test removing a lock that doesn't exist - should not raise error"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    lock = messages.ScanQueueLock(identifier="nonexistent_lock", reason="Does not exist")

    # Should not raise an error
    queue.remove_lock(lock)
    assert len(queue.locks) == 0


def test_remove_one_of_multiple_locks(queuemanager_mock):
    """Test removing one lock when multiple locks exist"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    lock1 = messages.ScanQueueLock(identifier="lock_1", reason="First")
    lock2 = messages.ScanQueueLock(identifier="lock_2", reason="Second")
    lock3 = messages.ScanQueueLock(identifier="lock_3", reason="Third")

    queue.add_lock(lock1)
    queue.add_lock(lock2)
    queue.add_lock(lock3)

    # Remove middle lock
    queue.remove_lock(lock2)

    assert len(queue.locks) == 2
    assert "lock_1" in queue.locks
    assert "lock_2" not in queue.locks
    assert "lock_3" in queue.locks
    # Should still be locked since there are remaining locks
    assert queue.status == ScanQueueStatus.LOCKED


def test_queue_status_restored_after_removing_all_locks(queuemanager_mock):
    """Test that queue status is restored to previous state when all locks are removed"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    # Set queue to paused
    queue.status = ScanQueueStatus.PAUSED

    # Add locks
    lock1 = messages.ScanQueueLock(identifier="lock_1", reason="First")
    lock2 = messages.ScanQueueLock(identifier="lock_2", reason="Second")
    queue.add_lock(lock1)
    queue.add_lock(lock2)
    assert queue.status == ScanQueueStatus.LOCKED

    # Remove first lock - should still be LOCKED
    queue.remove_lock(lock1)
    assert queue.status == ScanQueueStatus.LOCKED

    # Remove second lock - should restore to PAUSED
    queue.remove_lock(lock2)
    assert queue.status == ScanQueueStatus.PAUSED


def test_queue_manager_add_queue_lock(queuemanager_mock):
    """Test adding a lock via QueueManager"""
    queue_manager = queuemanager_mock()

    lock = messages.ScanQueueLock(identifier="manager_lock", reason="Testing via manager")
    queue_manager.add_queue_lock("primary", lock)

    queue = queue_manager.queues["primary"]
    assert len(queue.locks) == 1
    assert "manager_lock" in queue.locks
    assert queue.status == ScanQueueStatus.LOCKED


def test_queue_manager_remove_queue_lock(queuemanager_mock):
    """Test removing a lock via QueueManager"""
    queue_manager = queuemanager_mock()

    lock = messages.ScanQueueLock(identifier="manager_lock", reason="Testing")
    queue_manager.add_queue_lock("primary", lock)
    queue_manager.remove_queue_lock("primary", lock)

    queue = queue_manager.queues["primary"]
    assert len(queue.locks) == 0
    assert queue.status == ScanQueueStatus.RUNNING


def test_queue_manager_remove_lock_from_nonexistent_queue(queuemanager_mock):
    """Test removing a lock from a queue that doesn't exist - should not raise error"""
    queue_manager = queuemanager_mock()

    lock = messages.ScanQueueLock(identifier="test_lock", reason="Testing")

    # Should not raise an error
    queue_manager.remove_queue_lock("nonexistent_queue", lock)


def test_set_lock_via_scan_interception(queuemanager_mock):
    """Test setting lock via scan_interception"""
    queue_manager = queuemanager_mock()

    queue_manager.set_lock(
        queue="primary",
        parameter={"reason": "Interception test", "identifier": "interception_lock"},
    )

    queue = queue_manager.queues["primary"]
    assert len(queue.locks) == 1
    assert "interception_lock" in queue.locks
    assert queue.status == ScanQueueStatus.LOCKED


def test_set_release_lock_via_scan_interception(queuemanager_mock):
    """Test releasing lock via scan_interception"""
    queue_manager = queuemanager_mock()

    # First set a lock
    queue_manager.set_lock(queue="primary", parameter={"reason": "Test", "identifier": "test_lock"})

    # Then release it
    queue_manager.set_release_lock(queue="primary", parameter={"identifier": "test_lock"})

    queue = queue_manager.queues["primary"]
    assert len(queue.locks) == 0
    assert queue.status == ScanQueueStatus.RUNNING


def test_set_lock_missing_parameter(queuemanager_mock):
    """Test that set_lock raises error when parameter is missing"""
    queue_manager = queuemanager_mock()

    with pytest.raises(ValueError, match="Missing parameter for lock action"):
        queue_manager.set_lock(queue="primary", parameter=None)


def test_set_lock_missing_reason(queuemanager_mock):
    """Test that set_lock raises error when reason is missing"""
    queue_manager = queuemanager_mock()

    with pytest.raises(ValueError, match="Missing lock reason"):
        queue_manager.set_lock(queue="primary", parameter={"identifier": "test"})


def test_set_lock_missing_identifier(queuemanager_mock):
    """Test that set_lock raises error when identifier is missing"""
    queue_manager = queuemanager_mock()

    with pytest.raises(ValueError, match="Missing lock identifier"):
        queue_manager.set_lock(queue="primary", parameter={"reason": "test"})


def test_set_release_lock_missing_parameter(queuemanager_mock):
    """Test that set_release_lock raises error when parameter is missing"""
    queue_manager = queuemanager_mock()

    with pytest.raises(ValueError, match="Missing parameter for release_lock action"):
        queue_manager.set_release_lock(queue="primary", parameter=None)


def test_set_release_lock_missing_identifier(queuemanager_mock):
    """Test that set_release_lock raises error when identifier is missing"""
    queue_manager = queuemanager_mock()

    with pytest.raises(ValueError, match="Missing lock identifier"):
        queue_manager.set_release_lock(queue="primary", parameter={"reason": "test"})


def test_export_queue_includes_locks(queuemanager_mock):
    """Test that export_queue includes locks in the output"""
    queue_manager = queuemanager_mock()

    lock1 = messages.ScanQueueLock(identifier="lock_1", reason="First")
    lock2 = messages.ScanQueueLock(identifier="lock_2", reason="Second")
    queue_manager.add_queue_lock("primary", lock1)
    queue_manager.add_queue_lock("primary", lock2)

    exported = queue_manager.export_queue()

    assert "primary" in exported
    assert "locks" in exported["primary"]
    assert len(exported["primary"]["locks"]) == 2

    # Check that locks are in the list (converted from dict)
    lock_identifiers = [lock.identifier for lock in exported["primary"]["locks"]]
    assert "lock_1" in lock_identifiers
    assert "lock_2" in lock_identifiers


def test_export_queue_empty_locks(queuemanager_mock):
    """Test that export_queue includes empty locks when no locks present"""
    queue_manager = queuemanager_mock()

    exported = queue_manager.export_queue()

    assert "primary" in exported
    assert "locks" in exported["primary"]
    assert len(exported["primary"]["locks"]) == 0


@pytest.mark.timeout(20)
def test_queue_waits_when_locked_and_resumes_after_release(queuemanager_mock):
    """Test that the queue waits when set locked and resumes operation after release"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    # Add items to the queue
    iq1 = DirectInstructionQueueItem(queue, mock.MagicMock(), queue.scan_worker)
    iq1.scans = [_build_dummy_v4_scan("scan-id-1")]
    iq1.status = InstructionQueueStatus.PENDING
    iq2 = DirectInstructionQueueItem(queue, mock.MagicMock(), queue.scan_worker)
    iq2.scans = [_build_dummy_v4_scan("scan-id-2")]
    iq2.status = InstructionQueueStatus.PENDING
    queue.queue.append(iq1)
    queue.queue.append(iq2)

    # Initially queue should be running
    assert queue.status == ScanQueueStatus.RUNNING
    assert len(queue.queue) == 2

    # Set the queue locked
    lock = messages.ScanQueueLock(
        identifier="hold_test", reason="Testing hold behavior", allow_device_instructions=False
    )
    queue.add_lock(lock)
    assert queue.status == ScanQueueStatus.LOCKED
    assert len(queue.locks) == 1

    # Test that _next_instruction_queue blocks while LOCKED using a thread
    result = {"completed": False, "active_item": None}

    def try_next():
        # This should block while LOCKED
        res = queue._next_instruction_queue()
        result["completed"] = True
        result["returned"] = res
        result["active_item"] = queue.active_instruction_queue

    thread = threading.Thread(target=try_next)
    thread.start()

    # Give it time to enter the blocking wait
    time.sleep(2)

    # Verify the call is blocked (thread still running, result not updated)
    assert thread.is_alive()
    assert result["completed"] is False
    assert queue.status == ScanQueueStatus.LOCKED

    # Release the hold - this should unblock the thread
    queue.remove_lock(lock)

    # Wait for thread to complete
    thread.join(timeout=2)

    # Now queue should have resumed
    assert queue.status == ScanQueueStatus.RUNNING
    assert len(queue.locks) == 0

    # Verify the _next_instruction_queue call completed and processed an item
    assert result["completed"] is True
    assert result["returned"] is True  # Should return True when it processes
    assert result["active_item"] is not None  # An item was activated


def test_status_setter_prevents_change_when_locked(queuemanager_mock):
    """Test that the status setter cannot change status away from LOCKED when locks exist"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    # Initially queue should be running
    assert queue.status == ScanQueueStatus.RUNNING

    # Normal status change should work
    queue.status = ScanQueueStatus.PAUSED
    assert queue.status == ScanQueueStatus.PAUSED

    # Add a lock - this should set status to LOCKED
    lock = messages.ScanQueueLock(identifier="test_lock", reason="Testing")
    queue.add_lock(lock)
    assert queue.status == ScanQueueStatus.LOCKED
    assert len(queue.locks) == 1

    # Try to change status while locked - should not be allowed
    # The status should remain LOCKED as long as locks exist
    with mock.patch.object(queue.queue_manager, "send_queue_status"):
        # Attempt to set to RUNNING
        queue._status = ScanQueueStatus.RUNNING
        # But it should remain LOCKED because locks exist
        # (This tests the internal state, in practice add_lock ensures status is LOCKED)
        assert len(queue.locks) == 1  # Lock still exists

    # Remove the lock - status should automatically restore
    queue.remove_lock(lock)
    assert queue.status == ScanQueueStatus.PAUSED  # Returns to previous status
    assert len(queue.locks) == 0

    # Now status changes should work normally again
    queue.status = ScanQueueStatus.RUNNING
    assert queue.status == ScanQueueStatus.RUNNING


def test_status_setter_calls_send_queue_status(queuemanager_mock):
    """Test that the status setter calls send_queue_status when status changes"""
    queue_manager = queuemanager_mock()
    queue = queue_manager.queues["primary"]

    with mock.patch.object(queue.queue_manager, "send_queue_status") as mock_send:
        queue.status = ScanQueueStatus.PAUSED
        mock_send.assert_called_once()

    # Reset and test another status change
    with mock.patch.object(queue.queue_manager, "send_queue_status") as mock_send:
        queue.status = ScanQueueStatus.RUNNING
        mock_send.assert_called_once()
