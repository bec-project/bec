from unittest import mock

import pytest

from bec_ipython_client.callbacks.ipython_live_updates import IPythonLiveUpdates
from bec_lib import messages
from bec_lib.queue_items import QueueItem


@pytest.fixture
def queue_elements(bec_client_mock):
    client = bec_client_mock
    request_msg = messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    request_block = messages.RequestBlock(
        msg=request_msg,
        RID="req_id",
        scan_motors=["samx"],
        report_instructions=[],
        readout_priority={"monitored": ["samx"]},
        is_scan=True,
        scan_number=1,
        scan_id="scan_id",
    )
    queue = QueueItem(
        scan_manager=client.queue,
        queue_id="queue_id",
        request_blocks=[request_block],
        status="PENDING",
        active_request_block={},
        scan_id=["scan_id"],
    )
    return queue, request_block, request_msg


@pytest.fixture
def sample_request_msg():
    return messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )


@pytest.fixture
def sample_request_block(sample_request_msg):
    return messages.RequestBlock(
        msg=sample_request_msg,
        RID="req_id",
        scan_motors=["samx"],
        report_instructions=[],
        readout_priority={"monitored": ["samx"]},
        is_scan=True,
        scan_number=1,
        scan_id="scan_id",
    )


@pytest.fixture
def sample_queue_info_entry(sample_request_block):
    return messages.QueueInfoEntry(
        queue_id="test_queue_id",
        scan_id=["scan_id"],
        is_scan=[True],
        request_blocks=[sample_request_block],
        scan_number=[1],
        status="RUNNING",
        active_request_block=None,
    )


@pytest.fixture
def sample_scan_queue_status(sample_queue_info_entry):
    return messages.ScanQueueStatus(info=[sample_queue_info_entry], status="RUNNING")


@pytest.mark.timeout(20)
def test_live_updates_process_queue_pending(bec_client_mock, queue_elements):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)
    queue, request_block, request_msg = queue_elements

    client.queue.queue_storage.current_scan_queue = {
        "primary": messages.ScanQueueStatus(info=[], status="RUNNING")
    }
    with mock.patch.object(queue, "_update_with_buffer"):
        with mock.patch(
            "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
        ) as queue_pos:
            queue_pos.return_value = 2
            with mock.patch.object(
                live_updates, "_available_req_blocks", return_value=[request_block]
            ):
                with mock.patch.object(live_updates, "_process_report_instructions") as process:
                    with mock.patch("builtins.print") as prt:
                        res = live_updates._process_queue(queue, request_msg, "req_id")
                        prt.assert_called_once()
                        process.assert_not_called()
                    assert res is False


@pytest.mark.timeout(20)
def test_live_updates_process_queue_running(bec_client_mock, queue_elements):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)
    _, request_block, request_msg = queue_elements
    queue = QueueItem(
        scan_manager=client.queue,
        queue_id="queue_id",
        request_blocks=[request_block],
        status="RUNNING",
        active_request_block={},
        scan_id=["scan_id"],
    )
    live_updates._active_request = request_msg
    request_block.report_instructions = [{"wait_table": 10}]
    client.queue.queue_storage.current_scan_queue = {
        "primary": messages.ScanQueueStatus(info=[], status="RUNNING")
    }
    with mock.patch.object(queue, "_update_with_buffer"):
        with mock.patch(
            "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
        ) as queue_pos:
            queue_pos.return_value = 2
            with mock.patch.object(
                live_updates, "_available_req_blocks", return_value=[request_block]
            ):
                with mock.patch.object(live_updates, "_process_instruction") as process:
                    with mock.patch("builtins.print") as prt:
                        res = live_updates._process_queue(queue, request_msg, "req_id")
                        prt.assert_not_called()
                        process.assert_called_once_with({"wait_table": 10})
                    assert res is True


@pytest.mark.timeout(20)
def test_live_updates_process_queue_without_status(bec_client_mock, queue_elements):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)
    queue, _, request_msg = queue_elements
    with mock.patch.object(queue, "_update_with_buffer"):
        assert live_updates._process_queue(queue, request_msg, "req_id") is False


@pytest.mark.timeout(20)
def test_live_updates_process_queue_without_queue_number(bec_client_mock, queue_elements):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)
    queue, _, request_msg = queue_elements

    with mock.patch(
        "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
    ) as queue_pos:
        queue = QueueItem(
            scan_manager=client.queue,
            queue_id="queue_id",
            request_blocks=[request_msg],
            status="PENDING",
            active_request_block={},
            scan_id=["scan_id"],
        )
        queue_pos.return_value = None
        with mock.patch.object(queue, "_update_with_buffer"):
            assert live_updates._process_queue(queue, request_msg, "req_id") is False


@pytest.mark.timeout(20)
def test_available_req_blocks(bec_client_mock, queue_elements):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)
    queue, request_block, request_msg = queue_elements

    # Test with matching RID
    available_blocks = live_updates._available_req_blocks(queue, request_msg)
    assert (
        len(available_blocks) == 0
    )  # request_block.RID is "req_id", request_msg.metadata["RID"] is "something"

    # Test with correct RID
    request_block.RID = "something"
    available_blocks = live_updates._available_req_blocks(queue, request_msg)
    assert len(available_blocks) == 1
    assert available_blocks[0] == request_block


@pytest.mark.timeout(20)
def test_available_req_blocks_multiple_blocks(bec_client_mock):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)

    request_msg = messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "test_rid"},
    )

    request_block1 = messages.RequestBlock(
        msg=request_msg,
        RID="test_rid",
        scan_motors=["samx"],
        report_instructions=[],
        readout_priority={"monitored": ["samx"]},
        is_scan=True,
        scan_number=1,
        scan_id="scan_id_1",
    )

    request_block2 = messages.RequestBlock(
        msg=request_msg,
        RID="test_rid",
        scan_motors=["samy"],
        report_instructions=[],
        readout_priority={"monitored": ["samy"]},
        is_scan=True,
        scan_number=2,
        scan_id="scan_id_2",
    )

    request_block3 = messages.RequestBlock(
        msg=request_msg,
        RID="different_rid",
        scan_motors=["samz"],
        report_instructions=[],
        readout_priority={"monitored": ["samz"]},
        is_scan=True,
        scan_number=3,
        scan_id="scan_id_3",
    )

    queue = QueueItem(
        scan_manager=client.queue,
        queue_id="queue_id",
        request_blocks=[request_block1, request_block2, request_block3],
        status="RUNNING",
        active_request_block={},
        scan_id=["scan_id_1", "scan_id_2", "scan_id_3"],
    )

    available_blocks = live_updates._available_req_blocks(queue, request_msg)
    assert len(available_blocks) == 2
    assert request_block1 in available_blocks
    assert request_block2 in available_blocks
    assert request_block3 not in available_blocks


@pytest.mark.timeout(20)
def test_element_in_queue_no_queue(bec_client_mock):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)

    # Test when client.queue is None
    client.queue = None
    assert live_updates._element_in_queue() is False


@pytest.mark.timeout(20)
def test_element_in_queue_no_current_scan_queue(bec_client_mock):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)

    # Test when current_scan_queue is None
    client.queue.queue_storage.current_scan_queue = None
    assert live_updates._element_in_queue() is False


@pytest.mark.timeout(20)
def test_element_in_queue_no_primary_queue(bec_client_mock):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)

    # Test when primary queue doesn't exist
    scan_queue_status = messages.ScanQueueStatus(info=[], status="RUNNING")
    client.queue.queue_storage.current_scan_queue = {"secondary": scan_queue_status}
    assert live_updates._element_in_queue() is False


@pytest.mark.timeout(20)
def test_element_in_queue_no_queue_info(bec_client_mock):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)

    # Test when queue_info is empty
    scan_queue_status = messages.ScanQueueStatus(info=[], status="RUNNING")
    client.queue.queue_storage.current_scan_queue = {"primary": scan_queue_status}
    assert live_updates._element_in_queue() is False


@pytest.mark.timeout(20)
def test_element_in_queue_no_current_queue(bec_client_mock, sample_scan_queue_status):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)

    # Test when _current_queue is None
    live_updates._current_queue = None
    client.queue.queue_storage.current_scan_queue = {"primary": sample_scan_queue_status}
    assert live_updates._element_in_queue() is False


@pytest.mark.timeout(20)
def test_element_in_queue_queue_id_not_in_info(bec_client_mock, sample_request_block):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)

    # Test when queue_id is not in info
    current_queue = mock.MagicMock()
    current_queue.queue_id = "my_queue_id"
    live_updates._current_queue = current_queue

    queue_info_entry = messages.QueueInfoEntry(
        queue_id="different_queue_id",
        scan_id=["scan_id"],
        is_scan=[True],
        request_blocks=[sample_request_block],
        scan_number=[1],
        status="RUNNING",
        active_request_block=None,
    )
    scan_queue_status = messages.ScanQueueStatus(info=[queue_info_entry], status="RUNNING")
    client.queue.queue_storage.current_scan_queue = {"primary": scan_queue_status}
    assert live_updates._element_in_queue() is False


@pytest.mark.timeout(20)
def test_element_in_queue_queue_id_in_info(bec_client_mock, sample_request_block):
    client = bec_client_mock
    live_updates = IPythonLiveUpdates(client)

    # Test when queue_id is in info (should return True)
    current_queue = mock.MagicMock()
    current_queue.queue_id = "my_queue_id"
    live_updates._current_queue = current_queue

    queue_info_entry = messages.QueueInfoEntry(
        queue_id="my_queue_id",
        scan_id=["scan_id"],
        is_scan=[True],
        request_blocks=[sample_request_block],
        scan_number=[1],
        status="RUNNING",
        active_request_block=None,
    )
    scan_queue_status = messages.ScanQueueStatus(info=[queue_info_entry], status="RUNNING")
    client.queue.queue_storage.current_scan_queue = {"primary": scan_queue_status}
    assert live_updates._element_in_queue() is True
