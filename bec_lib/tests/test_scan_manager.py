from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest
from typeguard import TypeCheckError

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
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
    assert scan_manager.next_scan_number == 3


def test_scan_manager_next_scan_number_failed(scan_manager):
    scan_manager.connector.get.return_value = None
    assert scan_manager.next_scan_number == -1


def test_scan_manager_next_scan_number_setter(scan_manager):
    with mock.patch.object(
        scan_manager.connector, "get", return_value=messages.VariableMessage(value={"": 2})
    ):
        scan_manager.next_scan_number = 3
        scan_manager.connector.set.assert_called_once_with(
            MessageEndpoints.scan_number(), messages.VariableMessage(value={"": 3})
        )


def test_scan_manager_next_dataset_number(scan_manager):
    scan_manager.connector.get.return_value = messages.VariableMessage(value=3)
    assert scan_manager.next_dataset_number == 3


def test_scan_manager_next_dataset_number_failed(scan_manager):
    scan_manager.connector.get.return_value = None
    assert scan_manager.next_dataset_number == -1


def test_scan_manager_next_dataset_number_setter(scan_manager):
    with mock.patch.object(
        scan_manager.connector, "get", return_value=messages.VariableMessage(value={"": 2})
    ):
        scan_manager.next_dataset_number = 3
        scan_manager.connector.set.assert_called_once_with(
            MessageEndpoints.dataset_number(), messages.VariableMessage(value={"": 3})
        )


def test_scan_manager_request_scan_abortion(scan_manager):
    scan_manager.request_scan_abortion("scan_id")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(scan_id="scan_id", action="abort", parameter={}),
    )


@pytest.mark.parametrize("scan_id", [None, "scan_id", ["scan_id"], [None]])
def test_scan_manager_request_scan_abortion_scan_id(scan_manager, scan_id):

    class ScanStorage:
        current_scan_info = {"scan_id": scan_id}

        @property
        def current_scan_id(self):
            return self.current_scan_info["scan_id"]

    scan_manager.scan_storage = ScanStorage()
    scan_manager.request_scan_abortion()
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(scan_id=scan_id, action="abort", parameter={}),
    )


def test_scan_manager_request_scan_halt(scan_manager):
    scan_manager.request_scan_halt("scan_id")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(scan_id="scan_id", action="halt", parameter={}),
    )


@pytest.mark.parametrize("scan_id", [None, "scan_id", ["scan_id"], [None]])
def test_scan_manager_request_scan_halt_scan_id(scan_manager, scan_id):

    class ScanStorage:
        current_scan_info = {"scan_id": scan_id}

        @property
        def current_scan_id(self):
            return self.current_scan_info["scan_id"]

    scan_manager.scan_storage = ScanStorage()
    scan_manager.request_scan_halt()
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(scan_id=scan_id, action="halt", parameter={}),
    )


def test_scan_manager_request_scan_continuation(scan_manager):
    scan_manager.request_scan_continuation("scan_id")
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(scan_id="scan_id", action="continue", parameter={}),
    )


@pytest.mark.parametrize("scan_id", [None, "scan_id", ["scan_id"], [None]])
def test_scan_manager_request_scan_continuation_scan_id(scan_manager, scan_id):

    class ScanStorage:
        current_scan_info = {"scan_id": scan_id}

        @property
        def current_scan_id(self):
            return self.current_scan_info["scan_id"]

    scan_manager.scan_storage = ScanStorage()
    scan_manager.request_scan_continuation()
    scan_manager.connector.send.assert_called_once_with(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(scan_id=scan_id, action="continue", parameter={}),
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
