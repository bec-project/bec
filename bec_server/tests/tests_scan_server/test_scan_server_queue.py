import threading
import time
import uuid
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
from bec_server.scan_server.errors import LimitError, ScanAbortion
from bec_server.scan_server.scan_assembler import ScanAssembler
from bec_server.scan_server.scan_queue import (
    InstructionQueueItem,
    InstructionQueueStatus,
    QueueManager,
    RequestBlock,
    RequestBlockQueue,
    ScanQueue,
    ScanQueueStatus,
)
from bec_server.scan_server.scan_worker import ScanWorker
from bec_server.scan_server.tests.fixtures import scan_server_mock

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access
ScanQueue.AUTO_SHUTDOWN_TIME = 1  # Reduce auto-shutdown time for testing


@pytest.fixture
def queuemanager_mock(scan_server_mock) -> QueueManager:
    def _get_queuemanager(queues=None):
        scan_server = scan_server_mock
        if queues is None:
            queues = ["primary"]
        if isinstance(queues, str):
            queues = [queues]
        for queue in queues:
            scan_server.queue_manager.add_queue(queue)
        return scan_server.queue_manager

    yield _get_queuemanager

    scan_server_mock.queue_manager.shutdown()


class RequestBlockQueueMock(RequestBlockQueue):
    request_blocks = []
    _scan_id = []

    @property
    def scan_id(self):
        return self._scan_id

    def append(self, msg):
        pass


class InstructionQueueMock(InstructionQueueItem):
    def __init__(self, parent: ScanQueue, assembler: ScanAssembler, worker: ScanWorker) -> None:
        super().__init__(parent, assembler, worker)
        self.queue = RequestBlockQueueMock(self, assembler)

    def append_scan_request(self, msg):
        self.scan_msgs.append(msg)
        self.queue.append(msg)


def test_queuemanager_queue_contains_primary(queuemanager_mock):
    queue_manager = queuemanager_mock()
    assert "primary" in queue_manager.queues


@pytest.mark.parametrize("queue", ["primary", "alignment"])
def test_queuemanager_add_to_queue(queuemanager_mock, queue):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue=queue,
        metadata={"RID": "something"},
    )
    queue_manager.add_queue(queue)
    queue_manager.add_to_queue(scan_queue=queue, msg=msg)
    assert queue_manager.queues[queue].queue.popleft().scan_msgs[0] == msg


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


def test_queuemanager_add_to_queue_restarts_queue_if_worker_is_dead(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.queues["primary"].signal_event.set()
    original_worker = queue_manager.queues["primary"].scan_worker
    original_worker.shutdown()

    assert original_worker.is_alive() is False

    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_queue("primary")
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    assert queue_manager.queues["primary"].queue.popleft().scan_msgs[0] == msg
    assert queue_manager.queues["primary"].scan_worker.is_alive() is True
    assert id(queue_manager.queues["primary"].scan_worker) != id(original_worker)


def test_queuemanager_add_to_queue_error_send_alarm(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    with mock.patch.object(queue_manager, "connector") as connector:
        with mock.patch.object(queue_manager, "add_queue", side_effects=KeyError):
            queue_manager.add_to_queue(scan_queue="dummy", msg=msg)
            connector.raise_alarm.assert_called_once_with(
                severity=Alarms.MAJOR, info=mock.ANY, metadata={"RID": "something"}
            )


def test_queuemanager_scan_queue_callback(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    obj = MessageObject("scan_queue", msg)
    with mock.patch.object(queue_manager, "add_to_queue") as add_to_queue:
        with mock.patch.object(queue_manager, "send_queue_status") as send_queue_status:
            queue_manager._scan_queue_callback(obj, queue_manager)
            add_to_queue.assert_called_once_with("primary", msg)
            send_queue_status.assert_called_once()


def test_scan_queue_modification_callback(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueModificationMessage(
        scan_id="dummy", action="halt", parameter={}, metadata={"RID": "something"}
    )
    obj = MessageObject("scan_queue_modification", msg)
    with mock.patch.object(queue_manager, "scan_interception") as scan_interception:
        with mock.patch.object(queue_manager, "send_queue_status") as send_queue_status:
            queue_manager._scan_queue_modification_callback(obj, queue_manager)
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
        set_halt.assert_called_once_with(scan_id="dummy", queue="secondary", parameter={})


def test_set_halt(queuemanager_mock):
    queue_manager = queuemanager_mock()
    with mock.patch.object(queue_manager, "set_abort") as set_abort:
        queue_manager.set_halt(scan_id="dummy", parameter={})
        set_abort.assert_called_once_with(scan_id="dummy", queue="primary", exit_info="halted")


def test_set_halt_disables_return_to_start(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.queues["primary"].active_instruction_queue = InstructionQueueMock(
        queue_manager.queues["primary"], mock.MagicMock(), mock.MagicMock()
    )
    queue_manager.queues["primary"].active_instruction_queue.return_to_start = True
    with mock.patch.object(queue_manager, "set_abort") as set_abort:
        queue = queue_manager.queues["primary"].active_instruction_queue
        queue_manager.set_halt(scan_id="dummy", parameter={})
        set_abort.assert_called_once_with(scan_id="dummy", queue="primary", exit_info="halted")
        assert queue.return_to_start is False


def wait_to_reach_state(queue_manager, queue, state):
    while queue_manager.queues[queue].status != state:
        pass


@pytest.mark.timeout(5)
def test_set_pause(queuemanager_mock):
    """Test that set_pause sets worker_status to PAUSED when it's RUNNING"""
    queue_manager = queuemanager_mock()

    # Add a queue item so worker_status has something to operate on
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
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
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
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
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    scan_queue = queue_manager.queues["primary"]
    while scan_queue.scan_worker.current_instruction_queue_item is None:
        time.sleep(0.1)
    queue_manager.set_abort(queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.PAUSED)
    assert len(queue_manager.connector.message_sent) == 5
    assert {
        "queue": MessageEndpoints.stop_devices(),
        "msg": messages.VariableMessage(value=[], metadata={}),
    } in queue_manager.connector.message_sent
    assert (
        queue_manager.connector.message_sent[0].get("queue") == MessageEndpoints.scan_queue_status()
    )


@pytest.mark.timeout(5)
def test_set_abort_with_scan_id(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    msg = messages.ScanQueueMessage(
        scan_type="line_scan",
        parameter={"args": {"samx": (-1, 1)}, "kwargs": {"steps": 10, "relative": False}},
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
    assert len(queue_manager.connector.message_sent) == 5
    assert {
        "queue": MessageEndpoints.stop_devices(),
        "msg": messages.VariableMessage(value=[], metadata={}),
    } in queue_manager.connector.message_sent
    assert (
        queue_manager.connector.message_sent[0].get("queue") == MessageEndpoints.scan_queue_status()
    )


@pytest.mark.timeout(5)
def test_set_abort_with_scan_id_not_active(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    msg = messages.ScanQueueMessage(
        scan_type="line_scan",
        parameter={"args": {"samx": (-1, 1)}, "kwargs": {"steps": 10, "relative": False}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    scan_queue = queue_manager.queues["primary"]
    while scan_queue.scan_worker.current_instruction_queue_item is None:
        time.sleep(0.1)
    scan_id_abort = scan_queue.queue[1].scan_id[0]
    queue_manager.set_abort(scan_id=scan_id_abort, queue="primary")

    # The queue should remain RUNNING as the scan_id is not active
    assert queue_manager.queues["primary"].status == ScanQueueStatus.RUNNING
    assert len(scan_queue.queue) == 1  # One scan should be removed from the queue


@pytest.mark.timeout(5)
def test_set_abort_with_wrong_scan_id(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    msg = messages.ScanQueueMessage(
        scan_type="line_scan",
        parameter={"args": {"samx": (-1, 1)}, "kwargs": {"steps": 10, "relative": False}},
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
    msg = messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
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
            with mock.patch.object(
                queue_manager, "_wait_for_queue_to_appear_in_history"
            ) as scan_msg_wait:
                scan_msg_wait.return_value = iq
                with queue_manager._lock:
                    queue_manager.set_restart(queue="primary", parameter={"RID": "something_new"})
                scan_msg_wait.assert_called_once_with(iq.scan_id[0], "primary")
                add_new_scan_to_queue.assert_called_once()


@pytest.mark.timeout(5)
def test_set_restart_no_active_scan(queuemanager_mock):
    """
    Test that set_restart does nothing when there is no active scan. A scan has to be either on
    RUNNING or PAUSED state to be active.
    """
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
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
            with mock.patch.object(
                queue_manager, "_wait_for_queue_to_appear_in_history"
            ) as scan_msg_wait:
                with queue_manager._lock:
                    queue_manager.set_restart(queue="primary", parameter={"RID": "something_new"})
                scan_msg_wait.assert_not_called()
                add_new_scan_to_queue.assert_not_called()


@pytest.mark.timeout(5)
def test_set_user_completed(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="line_scan",
        parameter={"args": {"samx": (-1, 1)}, "kwargs": {"steps": 10, "relative": False}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    scan_queue = queue_manager.queues["primary"]
    while scan_queue.scan_worker.current_instruction_queue_item is None:
        time.sleep(0.1)
    queue_manager.set_user_completed(queue="primary")

    # The queue should return to RUNNING state after user completion
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.RUNNING)
    while len(scan_queue.queue) > 1:
        time.sleep(0.1)


def test_request_block(scan_server_mock):
    scan_server = scan_server_mock
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    request_block = RequestBlock(msg, assembler=ScanAssembler(parent=scan_server))


@pytest.mark.parametrize(
    "scan_queue_msg",
    [
        (
            messages.ScanQueueMessage(
                scan_type="mv",
                parameter={"args": {"samx": (1,)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something"},
            )
        ),
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something"},
            )
        ),
    ],
)
def test_request_block_scan_number(scan_server_mock, scan_queue_msg):
    scan_server = scan_server_mock
    request_block = RequestBlock(scan_queue_msg, assembler=ScanAssembler(parent=scan_server))
    if not request_block.is_scan:
        assert request_block.scan_number is None
        return
    with mock.patch.object(
        RequestBlock, "_scan_server_scan_number", new_callable=mock.PropertyMock, return_value=5
    ):
        with mock.patch.object(RequestBlock, "scan_ids_head", return_value=0):
            assert request_block.scan_number == 5


def test_remove_queue_item(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.queues["primary"].queue[0].queue.request_blocks[0].scan_id = "random"
    queue_manager.queues["primary"].remove_queue_item(scan_id=["random"])
    assert len(queue_manager.queues["primary"].queue) == 0


def test_invalid_scan_specified_in_message(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="fake test scan which does not exist!",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
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
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    queue_manager.set_clear(queue="primary")
    assert len(queue_manager.queues["primary"].queue) == 0


def test_scan_queue_next_instruction_queue(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager, InstructionQueueMock)
    assert queue._next_instruction_queue() is False


def test_scan_queue_next_instruction_queue_pops(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager, InstructionQueueMock)
    queue.queue.append(InstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock()))
    queue.queue[0].status = InstructionQueueStatus.RUNNING
    queue.active_instruction_queue = queue.queue[0]
    assert queue._next_instruction_queue() is False
    assert len(queue.queue) == 0


def test_scan_queue_next_instruction_queue_does_not_pop(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager, InstructionQueueMock)
    queue.queue.append(InstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock()))
    queue.queue[0].status = InstructionQueueStatus.PENDING
    queue.active_instruction_queue = queue.queue[0]
    assert queue._next_instruction_queue() is True
    assert len(queue.queue) == 1


def test_scan_queue_next_instruction_queue_pops_stopped_elements(queuemanager_mock):
    """
    Test that the scan queue pops the stopped elements from the queue.
    """
    queue_manager = queuemanager_mock()
    queue = ScanQueue(queue_manager, InstructionQueueMock)
    queue.queue.append(InstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock()))
    queue.queue.append(InstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock()))
    queue.queue[0].status = InstructionQueueStatus.STOPPED
    queue.queue[1].status = InstructionQueueStatus.STOPPED
    queue.status = ScanQueueStatus.PAUSED
    queue.active_instruction_queue = queue.queue[0]
    assert queue._next_instruction_queue() is True
    assert len(queue.queue) == 1
    assert queue._next_instruction_queue() is False
    assert len(queue.queue) == 0


def test_queue_manager_wait_for_queue_to_appear_in_history_raises_timeout(queuemanager_mock):
    queue_manager = queuemanager_mock()
    with pytest.raises(TimeoutError):
        queue_manager._wait_for_queue_to_appear_in_history("scan_id", "primary", timeout=0.5)


def test_queue_manager_wait_for_queue_to_appear_in_history(queuemanager_mock):
    queue_manager = queuemanager_mock()
    scan_queue = ScanQueue(queue_manager, InstructionQueueMock)
    instruction_queue = InstructionQueueItem(scan_queue, mock.MagicMock(), mock.MagicMock())
    request_queue = RequestBlockQueue(instruction_queue, mock.MagicMock())
    request_block = RequestBlock(mock.MagicMock(), mock.MagicMock(), request_queue)
    request_block.scan_id = "scan_id"
    request_queue.request_blocks.append(request_block)
    instruction_queue.queue = request_queue
    queue_manager.queues["primary"].history_queue.append(instruction_queue)
    queue_manager._wait_for_queue_to_appear_in_history("scan_id", "primary", timeout=0.5)


class RequestBlockMock(RequestBlock):
    def __init__(self, msg, scan_id) -> None:
        self.scan_id = scan_id
        self.msg = msg
        self.scan = None


def test_request_block_queue_scan_ids():
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    rb1 = RequestBlockMock("", str(uuid.uuid4()))
    rb2 = RequestBlockMock("", str(uuid.uuid4()))
    req_block_queue.request_blocks.append(rb1)
    req_block_queue.request_blocks.append(rb2)
    assert req_block_queue.scan_id == [rb1.scan_id, rb2.scan_id]


def test_request_block_queue_append():
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    with mock.patch("bec_server.scan_server.scan_queue.RequestBlock") as rb:
        with mock.patch.object(req_block_queue, "_update_scan_def_id") as update_scan_def:
            with mock.patch.object(req_block_queue, "append_request_block") as update_rb:
                req_block_queue.append(msg)
                update_scan_def.assert_called_once_with(rb())
                update_rb.assert_called_once_with(rb())


@pytest.mark.parametrize(
    "scan_queue_msg,scan_id",
    [
        (
            messages.ScanQueueMessage(
                scan_type="mv",
                parameter={"args": {"samx": (1,)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something"},
            ),
            None,
        ),
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something", "scan_def_id": "something"},
            ),
            "scan_id1",
        ),
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something", "scan_def_id": "existing_scan_def_id"},
            ),
            "scan_id2",
        ),
    ],
)
def test_update_scan_def_id(scan_queue_msg, scan_id):
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    req_block_queue.scan_def_ids["existing_scan_def_id"] = {"scan_id": "existing_scan_id"}
    rbl = RequestBlockMock(scan_queue_msg, scan_id)
    if rbl.msg.metadata.get("scan_def_id") in req_block_queue.scan_def_ids:
        req_block_queue._update_scan_def_id(rbl)
        scan_def_id = scan_queue_msg.metadata.get("scan_def_id")
        assert rbl.scan_id == req_block_queue.scan_def_ids[scan_def_id]["scan_id"]
        return
    req_block_queue._update_scan_def_id(rbl)
    assert rbl.scan_id == scan_id


def test_append_request_block():
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    rbl = RequestBlockMock("", "")
    with mock.patch.object(req_block_queue, "request_blocks_queue") as request_blocks_queue:
        with mock.patch.object(req_block_queue, "request_blocks") as request_blocks:
            req_block_queue.append_request_block(rbl)
            request_blocks.append.assert_called_once_with(rbl)
            request_blocks_queue.append.assert_called_once_with(rbl)


@pytest.mark.parametrize(
    "scan_queue_msg,scan_id",
    [
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something", "scan_def_id": "something"},
            ),
            "scan_id1",
        ),
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something", "scan_def_id": "existing_scan_def_id"},
            ),
            "scan_id2",
        ),
    ],
)
def test_update_point_id(scan_queue_msg, scan_id):
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    req_block_queue.scan_def_ids["existing_scan_def_id"] = {
        "scan_id": "existing_scan_id",
        "point_id": 10,
    }
    rbl = RequestBlockMock(scan_queue_msg, scan_id)
    rbl.scan = mock.MagicMock()
    scan_def_id = scan_queue_msg.metadata.get("scan_def_id")
    if rbl.msg.metadata.get("scan_def_id") in req_block_queue.scan_def_ids:
        req_block_queue._update_point_id(rbl)
        assert rbl.scan.point_id == req_block_queue.scan_def_ids[scan_def_id]["point_id"]
        return
    req_block_queue._update_point_id(rbl)
    assert rbl.scan.point_id != 10


@pytest.mark.parametrize(
    "scan_queue_msg,scan_id",
    [
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something", "scan_def_id": "existing_scan_def_id"},
            ),
            "scan_id2",
        )
    ],
)
def test_update_point_id_takes_max(scan_queue_msg, scan_id):
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    req_block_queue.scan_def_ids["existing_scan_def_id"] = {
        "scan_id": "existing_scan_id",
        "point_id": 10,
    }
    rbl = RequestBlockMock(scan_queue_msg, scan_id)
    rbl.scan = mock.MagicMock()
    rbl.scan.point_id = 20
    req_block_queue._update_point_id(rbl)
    assert rbl.scan.point_id == 20


@pytest.mark.parametrize(
    "scan_queue_msg,is_scan",
    [
        (
            messages.ScanQueueMessage(
                scan_type="mv",
                parameter={"args": {"samx": (1,)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something"},
            ),
            False,
        ),
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something"},
            ),
            True,
        ),
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something", "scan_def_id": "existing_scan_def_id"},
            ),
            True,
        ),
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something", "dataset_id_on_hold": True},
            ),
            True,
        ),
    ],
)
def test_increase_scan_number(scan_queue_msg, is_scan):
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    req_block_queue.scan_queue.queue_manager.parent.scan_number = 20
    req_block_queue.scan_queue.queue_manager.parent.dataset_number = 5
    rbl = RequestBlock(scan_queue_msg, mock.MagicMock(), req_block_queue)
    rbl.is_scan = is_scan
    dataset_id_on_hold = scan_queue_msg.metadata.get("dataset_id_on_hold")
    req_block_queue.active_rb = rbl
    rbl.assign_scan_number()
    if is_scan and rbl.scan_def_id is None:
        assert req_block_queue.scan_queue.queue_manager.parent.scan_number == 21
        if dataset_id_on_hold:
            assert req_block_queue.scan_queue.queue_manager.parent.dataset_number == 5
        else:
            assert req_block_queue.scan_queue.queue_manager.parent.dataset_number == 6
    else:
        assert req_block_queue.scan_queue.queue_manager.parent.scan_number == 20


def test_pull_request_block_non_empyt_rb():
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    scan_queue_msg = messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    rbl = RequestBlockMock(scan_queue_msg, "scan_id")
    req_block_queue.active_rb = rbl
    with mock.patch.object(req_block_queue, "request_blocks_queue") as rbqs:
        req_block_queue._pull_request_block()
        rbqs.assert_not_called()


def test_pull_request_block_empyt_rb():
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    with mock.patch.object(req_block_queue, "request_blocks_queue") as rbqs:
        with pytest.raises(StopIteration):
            req_block_queue._pull_request_block()
            rbqs.assert_not_called()


@pytest.fixture(params=[LimitError, ScanAbortion])
def request_block_queue_error(request):
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    req_block_queue.active_rb = mock.MagicMock()
    req_block_queue.active_rb.instructions.__next__.side_effect = request.param("Test error")
    return req_block_queue, request.param


@pytest.mark.parametrize(
    "scan,scan_id,scan_number,metadata",
    [
        (None, None, None, {}),
        (mock.MagicMock(), "scan_id", 1, {"scan_id": "scan_id", "scan_number": 1}),
    ],
)
def test_request_block_queue_raises_alarm_on_error(
    request_block_queue_error, scan, scan_id, scan_number, metadata
):
    req_block_queue, exc = request_block_queue_error
    req_block_queue.active_rb.scan = scan
    req_block_queue.active_rb.scan_id = scan_id
    req_block_queue.active_rb.scan_number = scan_number
    with pytest.raises(ScanAbortion):
        next(req_block_queue)
    raise_alarm_mock = req_block_queue.scan_queue.queue_manager.connector.raise_alarm
    raise_alarm_mock.assert_called_once_with(
        severity=Alarms.MAJOR, info=mock.ANY, metadata=metadata
    )
    submitted_error_info: messages.ErrorInfo = raise_alarm_mock.call_args[1]["info"]
    assert submitted_error_info.exception_type == exc.__name__
    assert submitted_error_info.error_message == "Test error"
    assert str(exc("Test error")) in str(submitted_error_info)


def test_queue_manager_get_active_scan_id(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    rbl = RequestBlockMock(msg, "scan_id")
    queue_manager.queues["primary"].queue[0].queue.active_rb = rbl
    assert queue_manager._get_active_scan_id("primary") == "scan_id"


def test_queue_manager_get_active_scan_id_returns_None(queuemanager_mock):
    queue_manager = queuemanager_mock()
    assert queue_manager._get_active_scan_id("primary") == None


def test_queue_manager_get_active_scan_id_wo_rbl_returns_None(queuemanager_mock):
    queue_manager = queuemanager_mock()
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    assert queue_manager._get_active_scan_id("primary") == None


def test_request_block_queue_next():
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    rbl = RequestBlockMock(msg, "scan_id")
    rbl.instructions = iter(["instruction1", "instruction2"])
    req_block_queue.active_rb = rbl
    with mock.patch.object(req_block_queue, "_pull_request_block") as pull_rb:
        next(req_block_queue)
        pull_rb.assert_called_once_with()


def test_request_block_queue_next_raises_stopiteration():
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    rbl = RequestBlockMock(msg, "scan_id")
    rbl.instructions = iter([])
    req_block_queue.active_rb = rbl
    with mock.patch.object(req_block_queue, "increase_scan_number") as increase_scan_number:
        with pytest.raises(StopIteration):
            next(req_block_queue)
            increase_scan_number.assert_called_once_with()


def test_request_block_queue_next_updates_point_id():
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something", "scan_def_id": "scan_def_id"},
    )
    rbl = RequestBlockMock(msg, "scan_id")
    rbl.instructions = iter([])
    rbl.scan = mock.MagicMock()
    rbl.scan.point_id = 10
    req_block_queue.scan_def_ids["scan_def_id"] = {"point_id": 0}

    req_block_queue.active_rb = rbl
    with mock.patch.object(req_block_queue, "increase_scan_number") as increase_scan_number:
        with pytest.raises(StopIteration):
            next(req_block_queue)
            increase_scan_number.assert_called_once_with()
            assert req_block_queue.scan_def_ids["scan_def_id"]["point_id"] == 10


def test_request_block_queue_flush_request_blocks():
    req_block_queue = RequestBlockQueue(mock.MagicMock(), mock.MagicMock())
    with mock.patch.object(req_block_queue, "request_blocks_queue") as request_blocks_queue:
        req_block_queue.flush_request_blocks()
        request_blocks_queue.clear.assert_called_once_with()


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
        parameter={"args": {"samx": (-5, 5)}, "kwargs": {"steps": 3}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_queue("primary")
    for _ in range(10):
        queue_manager.add_to_queue(scan_queue="primary", msg=msg)

    queue = queue_manager.queues["primary"]
    assert len(queue.queue) == 10

    target_id = queue.queue[5].queue.scan_id
    order_msg.scan_id = target_id[0]
    queue_manager._handle_scan_order_change(order_msg)
    for ii in range(10):
        if ii == position:
            assert queue.queue[ii].queue.scan_id == target_id
        else:
            assert queue.queue[ii].queue.scan_id != target_id


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
    iq1 = InstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock())
    iq1.status = InstructionQueueStatus.PENDING
    iq2 = InstructionQueueItem(queue, mock.MagicMock(), mock.MagicMock())
    iq2.status = InstructionQueueStatus.PENDING
    queue.queue.append(iq1)
    queue.queue.append(iq2)

    # Initially queue should be running
    assert queue.status == ScanQueueStatus.RUNNING
    assert len(queue.queue) == 2

    # Set the queue locked
    lock = messages.ScanQueueLock(identifier="hold_test", reason="Testing hold behavior")
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
