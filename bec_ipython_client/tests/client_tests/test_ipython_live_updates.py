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
