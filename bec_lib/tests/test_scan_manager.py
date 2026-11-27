from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest
from typeguard import TypeCheckError

from bec_lib import messages
from bec_lib.bec_errors import ScanAbortion
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
from bec_lib.request_context import ActiveRequestContext, active_request_context
from bec_lib.scan_manager import ScanManager
from bec_lib.scan_report import ScanReport

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.redis_connector import RedisConnector


@pytest.fixture
def scan_manager():
    connector = mock.MagicMock()
    manager = ScanManager(connector=connector)
    yield manager
    manager.shutdown()


@pytest.fixture
def scan_manager_with_scan(scan_queue_status_msg):
    connector = mock.MagicMock()
    manager = ScanManager(connector=connector)
    manager.scan_storage.update_with_queue_status(scan_queue_status_msg)
    yield manager
    manager.shutdown()


@pytest.fixture
def scan_manager_with_fakeredis(connected_connector: RedisConnector):
    manager = ScanManager(connector=connected_connector)
    yield manager
    manager.shutdown()


def test_scan_manager_next_scan_number(scan_manager):
    scan_manager.connector.get.return_value = messages.VariableMessage(value=3)
    assert scan_manager.next_scan_number == 4


def test_scan_manager_next_scan_number_failed(scan_manager):
    scan_manager.connector.get.return_value = None
    assert scan_manager.next_scan_number == 0


def test_scan_manager_next_scan_number_setter(scan_manager):
    with mock.patch.object(
        scan_manager.connector, "get", return_value=messages.VariableMessage(value={"": 2})
    ):
        scan_manager.next_scan_number = 4
        scan_manager.connector.set.assert_called_once_with(
            MessageEndpoints.scan_number(), messages.VariableMessage(value={"": 3})
        )


def test_scan_manager_next_dataset_number(scan_manager):
    scan_manager.connector.get.return_value = messages.VariableMessage(value=3)
    assert scan_manager.next_dataset_number == 4


def test_scan_manager_next_dataset_number_failed(scan_manager):
    scan_manager.connector.get.return_value = None
    assert scan_manager.next_dataset_number == 0


def test_scan_manager_next_dataset_number_setter(scan_manager):
    with mock.patch.object(
        scan_manager.connector, "get", return_value=messages.VariableMessage(value={"": 2})
    ):
        scan_manager.next_dataset_number = 4
        scan_manager.connector.set.assert_called_once_with(
            MessageEndpoints.dataset_number(), messages.VariableMessage(value={"": 3})
        )


def test_scan_manager_request_scan_abortion(scan_manager):
    scan_manager.request_scan_abortion("request-id")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="abort", parameter={}
        ),
    )


@pytest.mark.parametrize("scan_id", [None, "scan_id", ["scan_id"], [None]])
def test_scan_manager_request_scan_abortion_scan_id(scan_manager, scan_id):
    class ScanStorage:
        current_scan_info = messages.QueueInfoEntry(
            queue_id="queue-id",
            scan_id=["scan_id"],
            is_scan=[True],
            request_blocks=[
                messages.RequestBlock(
                    msg=messages.ScanQueueMessage(scan_type="mv", parameter={}),
                    RID="request-id",
                    readout_priority={},
                    is_scan=True,
                    scan_number=1,
                    scan_id="scan_id",
                )
            ],
            scan_number=[1],
            status="RUNNING",
        )

    scan_manager.scan_storage = ScanStorage()
    scan_manager.request_scan_abortion(scan_id=scan_id)
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="abort", parameter={}
        ),
    )


def test_scan_manager_request_scan_abortion_request_id(scan_manager):
    scan_manager.request_scan_abortion(request_id="request-id")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="abort", parameter={}
        ),
    )


def test_scan_manager_request_scan_halt(scan_manager):
    scan_manager.request_scan_halt("request-id")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="halt", parameter={}
        ),
    )


@pytest.mark.parametrize("scan_id", [None, "scan_id", ["scan_id"], [None]])
def test_scan_manager_request_scan_halt_scan_id(scan_manager, scan_id):
    class ScanStorage:
        current_scan_info = messages.QueueInfoEntry(
            queue_id="queue-id",
            scan_id=["scan_id"],
            is_scan=[True],
            request_blocks=[
                messages.RequestBlock(
                    msg=messages.ScanQueueMessage(scan_type="mv", parameter={}),
                    RID="request-id",
                    readout_priority={},
                    is_scan=True,
                    scan_number=1,
                    scan_id="scan_id",
                )
            ],
            scan_number=[1],
            status="RUNNING",
        )

    scan_manager.scan_storage = ScanStorage()
    scan_manager.request_scan_halt()
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="halt", parameter={}
        ),
    )


def test_scan_manager_request_scan_continuation(scan_manager):
    scan_manager.request_scan_continuation("request-id")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="continue", parameter={}
        ),
    )


def test_scan_manager_request_scan_interruption_uses_request_id(scan_manager):
    class ScanStorage:
        current_scan_info = messages.QueueInfoEntry(
            queue_id="queue-id",
            scan_id=["scan_id"],
            is_scan=[True],
            request_blocks=[
                messages.RequestBlock(
                    msg=messages.ScanQueueMessage(scan_type="mv", parameter={}),
                    RID="request-id",
                    readout_priority={},
                    is_scan=True,
                    scan_number=1,
                    scan_id="scan_id",
                )
            ],
            scan_number=[1],
            status="RUNNING",
        )

        @property
        def current_scan_id(self):
            return self.current_scan_info.scan_id

    scan_manager.scan_storage = ScanStorage()
    scan_manager.request_scan_interruption(deferred_pause=False)
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="pause", parameter={}
        ),
    )


def test_scan_manager_request_scan_restart_uses_request_id(scan_manager):
    scan_manager.request_scan_restart(request_id="request-id", new_request_id="new-request-id")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None,
            request_id="request-id",
            action="restart",
            parameter={"position": "replace", "RID": "new-request-id"},
            queue="primary",
        ),
    )


def test_scan_manager_request_scan_abortion_uses_context_request_id(scan_manager):
    token = active_request_context.set(ActiveRequestContext(request_id="request-id"))
    try:
        scan_manager.request_scan_abortion()
    finally:
        active_request_context.reset(token)

    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="abort", parameter={}, queue="primary"
        ),
    )


@pytest.mark.parametrize("scan_id", [None, "scan_id", ["scan_id"], [None]])
def test_scan_manager_request_scan_continuation_scan_id(scan_manager, scan_id):
    class ScanStorage:
        current_scan_info = messages.QueueInfoEntry(
            queue_id="queue-id",
            scan_id=["scan_id"],
            is_scan=[True],
            request_blocks=[
                messages.RequestBlock(
                    msg=messages.ScanQueueMessage(scan_type="mv", parameter={}),
                    RID="request-id",
                    readout_priority={},
                    is_scan=True,
                    scan_number=1,
                    scan_id="scan_id",
                )
            ],
            scan_number=[1],
            status="RUNNING",
        )

    scan_manager.scan_storage = ScanStorage()
    scan_manager.request_scan_continuation(scan_id=scan_id)
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="continue", parameter={}
        ),
    )


@pytest.mark.parametrize(
    "action, target_position, raises_error",
    [
        ("move", 0, True),
        ("move_to", 1, False),
        ("move_up", None, False),
        ("move_down", None, False),
        ("move_top", None, False),
        ("move_bottom", None, False),
        ("move_to", None, True),
    ],
)
def test_scan_manager_request_order_change(scan_manager, action, target_position, raises_error):
    """
    Test the request order change method and ensure that the correct messages are sent
    """
    if raises_error:
        with pytest.raises((TypeCheckError, ValueError)):
            scan_manager.request_queue_order_modification(
                scan_id="scan_id", action=action, position=target_position
            )
        return
    scan_manager.request_queue_order_modification(
        scan_id="scan_id", action=action, position=target_position
    )
    assert (
        mock.call(
            MessageEndpoints.scan_queue_order_change_request(),
            messages.ScanQueueOrderMessage(
                scan_id="scan_id", action=action, target_position=target_position, queue="primary"
            ),
        )
        in scan_manager.connector.send.mock_calls
    )


def test_scan_manager_request_order_change_with_response(scan_manager_with_fakeredis):
    scan_manager = scan_manager_with_fakeredis
    response_msg = messages.RequestResponseMessage(accepted=True, message="Order change accepted")

    def send_response(msg):
        scan_manager.connector.send(
            MessageEndpoints.scan_queue_order_change_response(), response_msg
        )

    scan_manager.connector.register(
        MessageEndpoints.scan_queue_order_change_request(), cb=send_response
    )

    out = scan_manager.request_queue_order_modification(
        scan_id="scan_id", action="move_to", position=1, wait_for_response=True
    )

    assert out == response_msg


def test_scan_manager_add_scan_to_queue_schedule(scan_manager_with_fakeredis):
    """
    Test the interaction with queue schedules

    Args:
        scan_manager_with_fakeredis: The scan manager fixture with a fakeredis connection
    """
    manager: ScanManager = scan_manager_with_fakeredis
    msg = messages.ScanQueueMessage(scan_type="mv", parameter={"args": {"samx": [5], "samy": [5]}})
    manager.add_scan_to_queue_schedule("new_schedule", msg)

    with pytest.raises(TypeCheckError):
        manager.add_scan_to_queue_schedule("new_schedule", {})

    assert manager.get_scan_queue_schedule("new_schedule") == [msg]

    msg2 = messages.ScanQueueMessage(scan_type="mv", parameter={"args": {"samx": [6], "samy": [6]}})
    manager.add_scan_to_queue_schedule("new_schedule", msg2)

    assert manager.get_scan_queue_schedule("new_schedule") == [msg, msg2]

    manager.add_scan_to_queue_schedule("new_schedule2", msg)

    assert manager.get_scan_queue_schedule("new_schedule2") == [msg]

    assert manager.get_scan_queue_schedule_names() == ["new_schedule", "new_schedule2"]

    manager.clear_scan_queue_schedule("new_schedule2")

    assert manager.get_scan_queue_schedule_names() == ["new_schedule"]

    assert manager.get_scan_queue_schedule("new_schedule2") == []

    assert manager.get_scan_queue_schedule("new_schedule") == [msg, msg2]

    manager.clear_all_scan_queue_schedules()

    assert manager.get_scan_queue_schedule_names() == []


def test_scan_manager_request_set_completed(scan_manager):
    scan_manager.request_set_completed("request-id")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="user_completed", parameter={}
        ),
    )


def test_scan_manager_request_set_completed_scan_id(scan_manager):
    class ScanStorage:
        current_scan_info = messages.QueueInfoEntry(
            queue_id="queue-id",
            scan_id=["current_scan_id"],
            is_scan=[True],
            request_blocks=[
                messages.RequestBlock(
                    msg=messages.ScanQueueMessage(scan_type="mv", parameter={}),
                    RID="request-id",
                    readout_priority={},
                    is_scan=True,
                    scan_number=1,
                    scan_id="current_scan_id",
                )
            ],
            scan_number=[1],
            status="RUNNING",
        )

    scan_manager.scan_storage = ScanStorage()
    scan_manager.request_set_completed()
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None, request_id="request-id", action="user_completed", parameter={}
        ),
    )


def test_scan_manager_add_queue_lock(scan_manager):
    """
    Test adding a queue lock
    """
    scan_manager.add_queue_lock(queue="primary", reason="Testing lock", lock_id="test_lock")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None,
            request_id=None,
            action="lock",
            parameter={
                "reason": "Testing lock",
                "identifier": "test_lock",
                "allow_device_instructions": True,
            },
            queue="primary",
        ),
    )


def test_scan_manager_remove_queue_lock(scan_manager):
    """
    Test removing a queue lock
    """
    scan_manager.remove_queue_lock(queue="primary", lock_id="test_lock")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            scan_id=None,
            request_id=None,
            action="release_lock",
            parameter={"identifier": "test_lock"},
            queue="primary",
        ),
    )


def test_scan_manager_add_public_file(scan_manager_with_scan):
    """
    Test the public file callback. It should add the file info to the scan item
    of the file's scan.

    For this, we use the scan_manager_with_scan fixture, which has a scan item
    already in the queue. The queue fixture is defined in conftest.py.
    """
    msg = messages.FileMessage(
        file_path="/Users/scans/S00001_master.h5", done=True, successful=True
    )
    msg_object = MessageObject(
        topic=MessageEndpoints.public_file(
            "bfa582aa-f9cd-4258-ab5d-3e5d54d3dde5", "master"
        ).endpoint,
        value=msg,
    )
    # pylint: disable=protected-access
    scan_manager_with_scan._public_file_callback(msg=msg_object)
    assert scan_manager_with_scan.scan_storage.storage[-1].public_files == {
        msg.file_path: {"done_state": True, "successful": True}
    }
    assert (
        "File: /Users/scans/S00001_master.h5"
        in scan_manager_with_scan.scan_storage.storage[-1].describe()
    )


def test_scan_manager_add_public_file_pending(scan_manager_with_scan):
    """
    Test that the public file callback adds the message to the pending updates if
    the scan is not in the storage yet.
    """
    msg = messages.FileMessage(
        file_path="/Users/scans/S00001_master.h5", done=False, successful=False
    )
    msg_object = MessageObject(
        topic=MessageEndpoints.public_file("new_scan_id_not_yet_in_storage", "master").endpoint,
        value=msg,
    )
    # pylint: disable=protected-access
    scan_manager_with_scan._public_file_callback(msg=msg_object)

    assert scan_manager_with_scan.scan_storage.storage[-1].public_files == {}

    scan_manager_with_scan.scan_storage.add_scan_item(
        queue_id="queue_id2",
        scan_number=2,
        scan_id="new_scan_id_not_yet_in_storage",
        status="running",
    )

    inserted_scan = scan_manager_with_scan.scan_storage.find_scan_by_ID(
        "new_scan_id_not_yet_in_storage"
    )
    assert inserted_scan.public_files == {msg.file_path: {"done_state": False, "successful": False}}
    assert "File: /Users/scans/S00001_master.h5" in inserted_scan.describe()


def test_completed_scan_stays_in_recent_storage(scan_manager, scan_queue_status_msg):
    """Dequeuing a completed scan preserves the public recent-scan view."""
    primary = scan_queue_status_msg.queue["primary"]
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    scan_storage = scan_manager.scan_storage
    original_scan = scan_storage.storage[-1]

    primary.info[0] = primary.info[0].model_copy(update={"status": "COMPLETED"})
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    primary.info.clear()
    scan_manager.update_with_queue_status(scan_queue_status_msg)

    assert scan_storage.storage[-1] is original_scan
    assert scan_storage.find_scan_by_ID(original_scan.scan_id) is original_scan


@pytest.mark.parametrize("terminal_status", ["COMPLETED", "STOPPED"])
def test_live_queue_items_survive_cache_pressure(
    scan_manager, scan_queue_status_msg, terminal_status
):
    """A locked queue keeps its live data through completion and late file publication."""
    # pylint: disable=protected-access
    primary = scan_queue_status_msg.queue["primary"]
    active = primary.info[0]
    active_block = active.request_blocks[0]
    active.active_request_block = active_block
    primary.status = "LOCKED"
    primary.locks = [messages.ScanQueueLock(reason="test lock", identifier="test-lock")]

    def pending_entry(number, queue_name):
        rid = f"request-{number}"
        scan_id = f"scan-{number}"
        msg = active_block.msg.model_copy(update={"metadata": {"RID": rid}, "queue": queue_name})
        block = active_block.model_copy(update={"RID": rid, "scan_id": scan_id, "msg": msg})
        return messages.QueueInfoEntry(
            queue_id=f"queue-{number}",
            scan_id=[scan_id],
            is_scan=[True],
            request_blocks=[block],
            scan_number=[number],
            status="PENDING",
        )

    # A second queue puts pressure on the shared ScanItem cache even while primary is locked.
    scan_queue_status_msg.queue["secondary"] = messages.ScanQueueStatus(
        status="RUNNING", info=[pending_entry(number, "secondary") for number in range(101, 105)]
    )
    active_report = ScanReport.from_request(active_block.msg, client=mock.Mock(queue=scan_manager))
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    active_queue_item = active_report.queue_item
    active_scan_item = active_report.scan
    assert active_scan_item is not None
    active_scan_item._bec = mock.Mock()
    active_report.request.callbacks = mock.Mock()
    first_point = messages.ScanMessage(point_id=0, scan_id=active_block.scan_id, data={})
    scan_manager._scan_segment_callback(
        MessageObject(MessageEndpoints.scan_segment().endpoint, first_point)
    )
    assert len(active_scan_item.live_data) == 1

    pending = pending_entry(0, "primary")
    pending_block = pending.request_blocks[0]
    pending_report = ScanReport.from_request(
        pending_block.msg, client=mock.Mock(queue=scan_manager)
    )
    primary.info.append(pending)
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    pending_queue_item = pending_report.queue_item

    for number in range(1, 101):
        later = pending_entry(number, "primary")
        response = messages.RequestResponseMessage(
            accepted=True, message="", metadata={"RID": later.request_blocks[0].RID}
        )
        scan_manager._scan_queue_request_response_callback(
            MessageObject(MessageEndpoints.scan_queue_request_response().endpoint, response)
        )
        primary.info.append(later)
        scan_manager.update_with_queue_status(scan_queue_status_msg)

    assert (
        scan_manager.request_storage.find_request_by_ID(active_block.RID) is active_report.request
    )
    assert (
        scan_manager.request_storage.find_request_by_ID(pending_block.RID) is pending_report.request
    )
    assert scan_manager.queue_storage.find_queue_item_by_ID(active.queue_id) is active_queue_item
    assert scan_manager.queue_storage.find_queue_item_by_ID(pending.queue_id) is pending_queue_item
    assert active_report.scan is active_scan_item
    assert len(active_scan_item.live_data) == 1
    next_point = first_point.model_copy(update={"point_id": 1})
    scan_manager._scan_segment_callback(
        MessageObject(MessageEndpoints.scan_segment().endpoint, next_point)
    )
    assert active_report.request.callbacks.run.call_count == 2
    assert len(active_scan_item.live_data) == 2

    # The queue worker can dequeue a finished scan before the file writer finishes.
    primary.info[0] = active.model_copy(update={"status": terminal_status})
    primary.info[1] = pending.model_copy(update={"status": "CANCELLED"})
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    if terminal_status == "STOPPED":
        # Another queue can publish after the worker clears the stopped request's details.
        primary.info[0] = primary.info[0].model_copy(
            update={"active_request_block": None, "request_blocks": [], "scan_id": []}
        )
        scan_manager.update_with_queue_status(scan_queue_status_msg)
        assert active_report.scan is active_scan_item

    del primary.info[:2]
    scan_manager.update_with_queue_status(scan_queue_status_msg)

    assert (
        scan_manager.request_storage.find_request_by_ID(active_block.RID) is active_report.request
    )
    assert scan_manager.queue_storage.find_queue_item_by_ID(active.queue_id) is active_queue_item
    assert active_report.scan is active_scan_item
    assert len(active_report.scan.live_data) == 2

    assert (
        scan_manager.request_storage.find_request_by_ID(pending_block.RID) is pending_report.request
    )
    assert scan_manager.queue_storage.find_queue_item_by_ID(pending.queue_id) is pending_queue_item
    pending_report._queue_item = None
    with pytest.raises(ScanAbortion):
        pending_report.wait(timeout=0.1)

    file_message = messages.FileMessage(file_path="scan_master.h5", done=True, successful=True)
    scan_manager._public_file_callback(
        MessageObject(
            MessageEndpoints.public_file(active_block.scan_id, "master").endpoint, file_message
        )
    )
    assert active_report.scan.public_files[file_message.file_path]["done_state"]
    if terminal_status == "STOPPED":
        with pytest.raises(ScanAbortion):
            active_report.wait(timeout=0.1)
    else:
        assert active_report.wait(timeout=0.1) is active_report


def test_restart_request_lookup_prefers_replacement(scan_manager, scan_queue_status_msg):
    """A cleared stopped attempt cannot shadow its live replacement."""
    primary = scan_queue_status_msg.queue["primary"]
    original = primary.info[0]
    request = original.request_blocks[0].msg
    report = ScanReport.from_request(request, client=mock.Mock(queue=scan_manager))
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    replacement_block = original.request_blocks[0].model_copy(
        update={"scan_id": "replacement-scan"}
    )
    replacement = original.model_copy(
        update={
            "queue_id": "replacement-queue",
            "scan_id": ["replacement-scan"],
            "request_blocks": [replacement_block],
            "active_request_block": None,
            "status": "PENDING",
        }
    )
    stopped = original.model_copy(update={"status": "STOPPED"})
    primary.info = [stopped, replacement]
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    primary.info[0] = stopped.model_copy(
        update={"request_blocks": [], "scan_id": [], "active_request_block": None}
    )
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    assert report.queue_item.queue_id == replacement.queue_id
    assert report.scan.scan_id == "replacement-scan"


def test_completed_replacement_precedes_old_history(scan_manager, scan_queue_status_msg):
    """A replacement stays authoritative through completion under cache pressure."""
    from bec_lib.queue_items import QueueStorage

    scan_manager.queue_storage = QueueStorage(scan_manager, maxlen=2)
    primary = scan_queue_status_msg.queue["primary"]
    old = primary.info[0]
    block = old.request_blocks[0]
    old.active_request_block = block
    report = ScanReport.from_request(block.msg, client=mock.Mock(queue=scan_manager))
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    primary.info[0] = old.model_copy(update={"status": "STOPPED"})
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    primary.info = []
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    for number in range(2):
        primary.info.append(
            old.model_copy(
                update={
                    "queue_id": f"unrelated-{number}",
                    "request_blocks": [block.model_copy(update={"RID": f"other-{number}"})],
                    "status": "PENDING",
                }
            )
        )
        scan_manager.update_with_queue_status(scan_queue_status_msg)
    replacement = old.model_copy(update={"queue_id": "replacement", "status": "RUNNING"})
    primary.info.append(replacement)
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    primary.info[-1] = replacement.model_copy(update={"status": "COMPLETED"})
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    assert report.request.queue.queue_id == "replacement"
    primary.info.pop()
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    assert report.queue_item.queue_id == "replacement"


def test_request_before_snapshot_preserves_result_history(scan_manager, scan_queue_status_msg):
    """Prequeue responses have their own bound and keep departed requests intact."""
    from bec_lib.request_items import RequestStorage

    scan_manager.request_storage = RequestStorage(scan_manager, maxlen=1)
    primary = scan_queue_status_msg.queue["primary"]
    entry = primary.info[0]
    rid = entry.request_blocks[0].RID
    scan_manager.request_storage.update_with_request(entry.request_blocks[0].msg)
    original = scan_manager.request_storage.find_request_by_ID(rid)
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    primary.info.clear()
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    for number in range(3):
        response = messages.RequestResponseMessage(
            accepted=True, message="", metadata={"RID": f"awaiting-{number}"}
        )
        scan_manager.request_storage.update_with_response(response)
    assert scan_manager.request_storage.find_request_by_ID(rid) is original
    assert scan_manager.request_storage.find_request_by_ID("awaiting-0") is None
    assert scan_manager.request_storage.find_request_by_ID("awaiting-2") is not None
    primary.info.append(
        entry.model_copy(
            update={
                "queue_id": "new-queue",
                "status": "PENDING",
                "request_blocks": [
                    entry.request_blocks[0].model_copy(update={"RID": "awaiting-2"})
                ],
            }
        )
    )
    scan_manager.update_with_queue_status(scan_queue_status_msg)
    assert scan_manager.request_storage.find_request_by_ID(rid) is original
    assert scan_manager.request_storage.find_request_by_ID("awaiting-2") is not None
