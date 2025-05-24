import uuid
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
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


def test_queuemanager_queue_inits_without_queues(queuemanager_mock):
    queue_manager = queuemanager_mock([])
    assert len(queue_manager.queues) == 0


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
                severity=Alarms.MAJOR,
                source=msg.content,
                msg="dummy",
                alarm_type="KeyError",
                metadata={"RID": "something"},
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
        set_abort.assert_called_once_with(scan_id="dummy", queue="primary")


def test_set_halt_disables_return_to_start(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.queues["primary"].active_instruction_queue = InstructionQueueMock(
        queue_manager.queues["primary"], mock.MagicMock(), mock.MagicMock()
    )
    queue_manager.queues["primary"].active_instruction_queue.return_to_start = True
    with mock.patch.object(queue_manager, "set_abort") as set_abort:
        queue = queue_manager.queues["primary"].active_instruction_queue
        queue_manager.set_halt(scan_id="dummy", parameter={})
        set_abort.assert_called_once_with(scan_id="dummy", queue="primary")
        assert queue.return_to_start is False


def wait_to_reach_state(queue_manager, queue, state):
    while queue_manager.queues[queue].status != state:
        pass


@pytest.mark.timeout(5)
def test_set_pause(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    queue_manager.set_pause(queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.PAUSED)
    assert len(queue_manager.connector.message_sent) == 1
    assert (
        queue_manager.connector.message_sent[0].get("queue") == MessageEndpoints.scan_queue_status()
    )


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
    queue_manager.set_abort(queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.PAUSED)
    assert len(queue_manager.connector.message_sent) == 3
    assert {
        "queue": MessageEndpoints.stop_all_devices(),
        "msg": messages.VariableMessage(value=1, metadata={}),
    } in queue_manager.connector.message_sent
    assert (
        queue_manager.connector.message_sent[0].get("queue") == MessageEndpoints.scan_queue_status()
    )


@pytest.mark.timeout(5)
def test_set_abort_with_empty_queue(queuemanager_mock):
    queue_manager = queuemanager_mock()
    queue_manager.connector.message_sent = []
    queue_manager.set_abort(queue="primary")
    wait_to_reach_state(queue_manager, "primary", ScanQueueStatus.RUNNING)
    assert len(queue_manager.connector.message_sent) == 1
    assert {
        "queue": MessageEndpoints.stop_all_devices(),
        "msg": messages.VariableMessage(value=1, metadata={}),
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
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    queue_manager.add_to_queue(scan_queue="primary", msg=msg)
    with mock.patch.object(queue_manager, "add_to_queue") as add_to_queue:
        with mock.patch.object(queue_manager, "_get_active_scan_id", return_value="new_scan_id"):
            with mock.patch.object(
                queue_manager, "_wait_for_queue_to_appear_in_history"
            ) as scan_msg_wait:
                with queue_manager._lock:
                    queue_manager.set_restart(queue="primary", parameter={"RID": "something_new"})
                scan_msg_wait.assert_called_once_with("new_scan_id", "primary")
                add_to_queue.assert_called_once_with("primary", scan_msg_wait().scan_msgs[0], 0)


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
            severity=Alarms.MAJOR,
            source=msg.content,
            msg="fake test scan which does not exist!",
            alarm_type="KeyError",
            metadata={"RID": "something"},
        )


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
            severity=Alarms.MAJOR,
            source=msg.content,
            msg="fake test scan which does not exist!",
            alarm_type="KeyError",
            metadata={"RID": "something"},
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
    rbl = RequestBlockMock(scan_queue_msg, "scan_id")
    rbl.is_scan = is_scan
    dataset_id_on_hold = scan_queue_msg.metadata.get("dataset_id_on_hold")
    req_block_queue.active_rb = rbl
    req_block_queue.increase_scan_number()
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
