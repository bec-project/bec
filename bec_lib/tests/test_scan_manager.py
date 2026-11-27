from __future__ import annotations

import threading
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from typeguard import TypeCheckError

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
from bec_lib.request_context import ActiveRequestContext, active_request_context
from bec_lib.scan_items import ScanItem
from bec_lib.scan_manager import ScanManager

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

    assert scan_manager_with_scan.scan_storage.storage[-1].public_files == {
        msg.file_path: {"done_state": False, "successful": False}
    }
    assert (
        "File: /Users/scans/S00001_master.h5"
        in scan_manager_with_scan.scan_storage.storage[-1].describe()
    )


def test_client_started_during_a_scan_takes_the_state_the_server_stored(
    connected_connector, scan_queue_status_msg
):
    """A client started while a scan runs has the scan, its status and its points so far."""
    scan_id = "bfa582aa-f9cd-4258-ab5d-3e5d54d3dde5"
    connector = connected_connector
    connector.set_and_publish(MessageEndpoints.scan_queue_status(), scan_queue_status_msg)
    connector.set_and_publish(
        MessageEndpoints.scan_status(),
        messages.ScanStatusMessage(scan_id=scan_id, status="open", info={"num_points": 10}),
    )
    for point_id in range(3):
        connector.set(
            MessageEndpoints.public_scan_segment(scan_id=scan_id, point_id=point_id),
            messages.ScanMessage(
                point_id=point_id,
                scan_id=scan_id,
                data={"bpm4i": {"bpm4i": {"value": float(point_id), "timestamp": 1.0}}},
                metadata={"scan_id": scan_id},
            ),
        )

    manager = ScanManager(connector=connector)  # started after all of the above
    try:
        item = manager.scan_storage.find_scan_by_ID(scan_id)
        assert item is not None
        assert item.status == "open" and item.status_message is not None
        assert sorted(item.live_data.messages) == [0, 1, 2]
        assert manager.scan_storage.current_scan is item
    finally:
        manager.shutdown()


def _segment(scan_id: str, point_id: int, value: float) -> messages.ScanMessage:
    return messages.ScanMessage(
        point_id=point_id,
        scan_id=scan_id,
        data={"bpm4i": {"bpm4i": {"value": value, "timestamp": 1.0}}},
        metadata={"scan_id": scan_id},
    )


def _pause_first_status_read(item: ScanItem, reader: threading.Thread):
    """Make the first read of ``item.status_message`` by ``reader`` wait until released."""
    reading, release = threading.Event(), threading.Event()
    values = {"status_message": item.__dict__.pop("status_message")}

    def get(_self):
        value = values["status_message"]
        if threading.current_thread() is reader and not reading.is_set():
            reading.set()  # the value is read; hold the reader before it acts on it
            release.wait(5)
        return value

    def set_(_self, value):
        values["status_message"] = value

    item.__class__ = type("PausedScanItem", (ScanItem,), {"status_message": property(get, set_)})
    return reading, release


def test_stored_status_does_not_overwrite_a_status_published_during_the_start(
    connected_connector, scan_queue_status_msg
):
    """A live "closed" that arrives while the stored "open" is being restored wins."""
    scan_id = "bfa582aa-f9cd-4258-ab5d-3e5d54d3dde5"
    connector = connected_connector
    connector.set_and_publish(MessageEndpoints.scan_queue_status(), scan_queue_status_msg)
    manager = ScanManager(connector=connector)
    try:
        item = manager.scan_storage.find_scan_by_ID(scan_id)
        assert item is not None and item.status_message is None
        stored_open = messages.ScanStatusMessage(scan_id=scan_id, status="open", info={})
        live_closed = messages.ScanStatusMessage(scan_id=scan_id, status="closed", info={})
        get = connector.get
        connector.get = lambda topic, *a, **kw: (
            stored_open if topic == MessageEndpoints.scan_status() else get(topic, *a, **kw)
        )
        restore = threading.Thread(target=manager._load_stored_state)
        reading, release = _pause_first_status_read(item, restore)
        restore.start()
        assert reading.wait(5)
        live = threading.Thread(
            target=manager.scan_storage.update_with_scan_status, args=(live_closed,)
        )
        live.start()
        live.join(0.3)  # the live status arrives while the restore checks the item
        release.set()
        restore.join(5)
        live.join(5)
        assert item.status == "closed"
    finally:
        manager.shutdown()


def test_stored_queue_and_points_do_not_replace_newer_ones(
    connected_connector, scan_queue_status_msg
):
    """The stored queue and points fill gaps only; what arrived live is kept."""
    scan_id = "bfa582aa-f9cd-4258-ab5d-3e5d54d3dde5"
    connector = connected_connector
    connector.set_and_publish(MessageEndpoints.scan_queue_status(), scan_queue_status_msg)
    connector.set_and_publish(
        MessageEndpoints.scan_status(),
        messages.ScanStatusMessage(scan_id=scan_id, status="open", info={}),
    )
    connector.set(
        MessageEndpoints.public_scan_segment(scan_id=scan_id, point_id=0), _segment(scan_id, 0, 1.0)
    )
    manager = ScanManager(connector=connector)
    try:
        item = manager.scan_storage.find_scan_by_ID(scan_id)
        assert item.queue is not None  # the queue item of the scan is restored too
        live_queue = messages.ScanQueueStatusMessage(
            queue={"primary": messages.ScanQueueStatus(info=[], status="RUNNING")}
        )
        manager.queue_storage.update_with_status(live_queue)
        manager.queue_storage.restore_status(scan_queue_status_msg)
        assert manager.queue_storage.current_scan_queue == live_queue.queue

        manager.scan_storage.add_scan_segment(_segment(scan_id, 1, 99.0))
        manager.scan_storage.restore_scan_segments(
            scan_id, [_segment(scan_id, 1, -1.0), _segment(scan_id, 2, 2.0)]
        )
        assert sorted(item.live_data.messages) == [0, 1, 2]
        assert item.live_data.messages[1].data["bpm4i"]["bpm4i"]["value"] == 99.0
    finally:
        manager.shutdown()
