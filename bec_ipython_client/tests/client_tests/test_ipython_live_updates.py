from unittest import mock

import pytest

from bec_ipython_client.callbacks.ipython_live_updates import IPythonLiveUpdates
from bec_lib import messages
from bec_lib.bec_errors import ScanInterruption, ScanRestart
from bec_lib.queue_items import QueueItem


@pytest.fixture
def ipython_live_updates_with_mocked_live(bec_client_mock):
    """Create IPythonLiveUpdates instance with mocked Live display."""
    with mock.patch("bec_ipython_client.callbacks.ipython_live_updates.Live") as mock_live:
        mock_instance = mock.MagicMock()
        mock_live.return_value = mock_instance
        live_updates = IPythonLiveUpdates(bec_client_mock)
        yield live_updates, mock_live


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
def test_live_updates_process_queue_pending(ipython_live_updates_with_mocked_live, queue_elements):
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
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
                    res = live_updates._process_queue(queue, request_msg, "req_id")
                    # Verify Live panel was created for showing queue status
                    mock_live.assert_called_once()
                    mock_live.return_value.start.assert_called_once()
                    process.assert_not_called()
                    assert res is False


@pytest.mark.timeout(20)
def test_live_updates_process_queue_running(ipython_live_updates_with_mocked_live, queue_elements):
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
    queue, request_block, request_msg = queue_elements
    queue = QueueItem(
        scan_manager=client.queue,
        queue_id="queue_id",
        request_blocks=[request_block],
        status="RUNNING",
        active_request_block=None,
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
        ):
            with mock.patch.object(
                live_updates, "_available_req_blocks", return_value=[request_block]
            ):
                with mock.patch.object(live_updates, "_process_instruction") as process:
                    res = live_updates._process_queue(queue, request_msg, "req_id")
                    mock_live.assert_not_called()
                    process.assert_called_once_with({"wait_table": 10})
                    assert res is True


def test_process_request_repeats_on_ScanRestart_error(
    ipython_live_updates_with_mocked_live, queue_elements
):
    """
    Test that process_request handles ScanRestart by repeating the processing.
    During restarts, we expect _stop_status_live to be called to clean up any existing live displays.
    """
    live_updates, _ = ipython_live_updates_with_mocked_live
    client = live_updates.client
    _, _, request_msg = queue_elements
    client.queue.queue_storage.current_scan_queue = {
        "primary": messages.ScanQueueStatus(info=[], status="RUNNING")
    }
    callbacks = mock.MagicMock()
    live_updates.client._sighandler = mock.MagicMock()
    live_updates.client._sighandler.__enter__.side_effect = [
        ScanRestart(request_msg),
        ScanInterruption(),
    ]
    with mock.patch.object(live_updates, "_stop_status_live"):
        with pytest.raises(ScanInterruption):
            live_updates.process_request(request_msg, callbacks)

        # once for each _process_queue call (2 times due to ScanRestart), once per exception and once at the end
        assert live_updates._stop_status_live.call_count == 5


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


@pytest.mark.timeout(20)
def test_process_pending_queue_element_locked_queue(
    ipython_live_updates_with_mocked_live, queue_elements
):
    """Test _process_pending_queue_element when queue is LOCKED."""
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
    queue, _, _ = queue_elements

    lock = messages.ScanQueueLock(identifier="user123", reason="Manual hold for calibration")

    # Create a locked queue status with locks
    scan_queue_status = messages.ScanQueueStatus(info=[], status="LOCKED", locks=[lock])

    client.queue.queue_storage.current_scan_queue = {"primary": scan_queue_status}

    with mock.patch(
        "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
    ) as queue_pos:
        queue_pos.return_value = 0

        # Call the method
        live_updates._process_pending_queue_element(queue)

        # Verify Live panel was created with lock info
        mock_live.assert_called_once()
        call_args = mock_live.call_args[0][0]
        # Extract the renderable text from Panel
        panel_renderable = call_args.renderable
        assert "Scan is waiting for the lock to be released" in panel_renderable
        assert "user123" in panel_renderable
        assert "Manual hold for calibration" in panel_renderable

        # Verify start was called
        mock_live.return_value.start.assert_called_once()


@pytest.mark.timeout(20)
def test_process_pending_queue_element_locked_queue_update_existing_live(
    ipython_live_updates_with_mocked_live, queue_elements
):
    """Test _process_pending_queue_element updates existing Live when queue is LOCKED."""
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
    queue, _, _ = queue_elements

    # Create a lock
    lock = messages.ScanQueueLock(identifier="user123", reason="Manual hold for calibration")

    # Create a locked queue status
    scan_queue_status = messages.ScanQueueStatus(info=[], status="LOCKED", locks=[lock])
    client.queue.queue_storage.current_scan_queue = {"primary": scan_queue_status}

    with mock.patch(
        "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
    ) as queue_pos:
        queue_pos.return_value = 0

        # Set up existing _status_live
        existing_live = mock.MagicMock()
        live_updates._status_live = existing_live

        # Call the method
        live_updates._process_pending_queue_element(queue)

        # Verify Live was not created again
        mock_live.assert_not_called()

        # Verify update was called instead
        existing_live.update.assert_called_once()
        call_args = existing_live.update.call_args[0][0]
        panel_renderable = call_args.renderable
        assert "Scan is waiting for the lock to be released" in panel_renderable
        assert "user123" in panel_renderable


@pytest.mark.timeout(20)
def test_process_pending_queue_element_multiple_locks(
    ipython_live_updates_with_mocked_live, queue_elements
):
    """Test _process_pending_queue_element with multiple locks."""
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
    queue, _, _ = queue_elements

    # Create multiple locks
    lock1 = messages.ScanQueueLock(identifier="user123", reason="Calibration")

    lock2 = messages.ScanQueueLock(identifier="admin456", reason="Maintenance")

    # Create a locked queue status with multiple locks
    scan_queue_status = messages.ScanQueueStatus(info=[], status="LOCKED", locks=[lock1, lock2])
    client.queue.queue_storage.current_scan_queue = {"primary": scan_queue_status}

    with mock.patch(
        "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
    ) as queue_pos:
        queue_pos.return_value = 0

        # Call the method
        live_updates._process_pending_queue_element(queue)

        # Verify both locks are in the message
        call_args = mock_live.call_args[0][0]
        panel_renderable = call_args.renderable
        assert "user123" in panel_renderable
        assert "Calibration" in panel_renderable
        assert "admin456" in panel_renderable
        assert "Maintenance" in panel_renderable


@pytest.mark.timeout(20)
def test_process_pending_queue_element_queue_position_positive(
    ipython_live_updates_with_mocked_live, queue_elements
):
    """Test _process_pending_queue_element when queue position > 0."""
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
    queue, _, _ = queue_elements

    # Create a running queue status
    running_scan_queue_status = messages.ScanQueueStatus(info=[], status="RUNNING")
    client.queue.queue_storage.current_scan_queue = {"primary": running_scan_queue_status}

    with mock.patch(
        "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
    ) as queue_pos:
        queue_pos.return_value = 2  # Queue position > 0

        # Call the method
        live_updates._process_pending_queue_element(queue)

        # Verify Live panel was created with position info
        mock_live.assert_called_once()
        call_args = mock_live.call_args[0][0]
        panel_renderable = call_args.renderable
        assert "Scan is enqueued and is waiting for execution" in panel_renderable
        assert "position in queue" in panel_renderable
        assert "3" in panel_renderable  # Position is displayed as queue_position + 1
        assert "RUNNING" in panel_renderable

        # Verify start was called
        mock_live.return_value.start.assert_called_once()


@pytest.mark.timeout(20)
def test_process_pending_queue_element_queue_position_update_existing_live(
    ipython_live_updates_with_mocked_live, queue_elements
):
    """Test _process_pending_queue_element updates existing Live when queue position > 0."""
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
    queue, _, _ = queue_elements

    # Create a running queue status
    running_scan_queue_status = messages.ScanQueueStatus(info=[], status="RUNNING")
    client.queue.queue_storage.current_scan_queue = {"primary": running_scan_queue_status}

    with mock.patch(
        "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
    ) as queue_pos:
        queue_pos.return_value = 1

        # Set up existing _status_live
        existing_live = mock.MagicMock()
        live_updates._status_live = existing_live

        # Call the method
        live_updates._process_pending_queue_element(queue)

        # Verify Live was not created again
        mock_live.assert_not_called()

        # Verify update was called instead
        existing_live.update.assert_called_once()
        call_args = existing_live.update.call_args[0][0]
        panel_renderable = call_args.renderable
        assert "Scan is enqueued and is waiting for execution" in panel_renderable
        assert "2" in panel_renderable  # Position is displayed as queue_position + 1


@pytest.mark.timeout(20)
def test_process_pending_queue_element_no_target_queue(
    ipython_live_updates_with_mocked_live, queue_elements
):
    """Test _process_pending_queue_element when target queue is None."""
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
    queue, _, _ = queue_elements

    # Setup current_scan_queue but without the primary queue
    client.queue.queue_storage.current_scan_queue = {
        "secondary": messages.ScanQueueStatus(info=[], status="RUNNING")
    }

    with mock.patch(
        "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
    ) as queue_pos:
        queue_pos.return_value = 0

        # Call the method
        live_updates._process_pending_queue_element(queue)

        # Verify Live was never called since target_queue is None
        mock_live.assert_not_called()


@pytest.mark.timeout(20)
def test_process_pending_queue_element_queue_position_zero_not_locked(
    ipython_live_updates_with_mocked_live, queue_elements
):
    """Test _process_pending_queue_element when queue position is 0 and not locked."""
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
    queue, _, _ = queue_elements

    # Create a running queue status (not locked)
    running_scan_queue_status = messages.ScanQueueStatus(info=[], status="RUNNING")
    client.queue.queue_storage.current_scan_queue = {"primary": running_scan_queue_status}

    with mock.patch(
        "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
    ) as queue_pos:
        queue_pos.return_value = 0  # First in queue

        # Call the method
        live_updates._process_pending_queue_element(queue)

        # Verify Live was not called since queue_position is 0 and not locked
        mock_live.assert_not_called()


@pytest.mark.timeout(20)
def test_process_pending_queue_element_locked_then_position(
    ipython_live_updates_with_mocked_live, queue_elements
):
    """Test _process_pending_queue_element prioritizes LOCKED status over position."""
    live_updates, mock_live = ipython_live_updates_with_mocked_live
    client = live_updates.client
    queue, _, _ = queue_elements

    # Create mock lock
    lock = messages.ScanQueueLock(identifier="user123", reason="Manual hold for calibration")

    # Create a locked queue status
    scan_queue_status = messages.ScanQueueStatus(info=[], status="LOCKED", locks=[lock])
    client.queue.queue_storage.current_scan_queue = {"primary": scan_queue_status}

    with mock.patch(
        "bec_lib.queue_items.QueueItem.queue_position", new_callable=mock.PropertyMock
    ) as queue_pos:
        queue_pos.return_value = 3  # Also has a position in queue

        # Call the method
        live_updates._process_pending_queue_element(queue)

        # Verify Live panel shows lock info (not position info)
        call_args = mock_live.call_args[0][0]
        panel_renderable = call_args.renderable
        assert "lock" in panel_renderable.lower()
        assert "user123" in panel_renderable
        # Should not show position info
        assert "position in queue" not in panel_renderable
