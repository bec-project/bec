import time
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.callback_handler import EventType
from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_lib.scan_history import ScanHistory

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring


@pytest.fixture
def scan_history_without_thread(connected_connector, file_history_messages):
    for msg in file_history_messages:
        connected_connector.xadd(MessageEndpoints.scan_history(), {"data": msg})
    client = BECClient(connector_cls=RedisConnector)
    client.connector = connected_connector
    yield ScanHistory(client=client, load_threaded=False)


def test_scan_history_loads_messages(scan_history_without_thread, file_history_messages):
    container = scan_history_without_thread.get_by_scan_number(1)
    assert container._msg == file_history_messages[0]

    container = scan_history_without_thread.get_by_scan_number(2)
    assert container._msg == file_history_messages[1]

    container = scan_history_without_thread.get_by_scan_number(3)
    assert container._msg == file_history_messages[2]

    container = scan_history_without_thread.get_by_scan_number(4)
    assert container is None

    container = scan_history_without_thread.get_by_scan_id("scan_id_1")
    assert container._msg == file_history_messages[0]

    container = scan_history_without_thread.get_by_scan_id("scan_id_2")
    assert container._msg == file_history_messages[1]

    container = scan_history_without_thread.get_by_dataset_number(2)
    assert container[0]._msg == file_history_messages[1]
    assert container[1]._msg == file_history_messages[2]

    assert scan_history_without_thread._scan_numbers == [1, 2, 3]
    assert [
        scan_history_without_thread._scan_data[sid].scan_number
        for sid in scan_history_without_thread._scan_ids
    ] == scan_history_without_thread._scan_numbers


@pytest.mark.timeout(10)
def test_scan_history_removes_oldest_scan(scan_history_without_thread, file_history_messages):
    msg = [
        messages.ScanHistoryMessage(
            scan_id="scan_id_4",
            scan_number=4,
            dataset_number=4,
            file_path="file_path",
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
        messages.ScanHistoryMessage(
            scan_id="scan_id_5",
            scan_number=5,
            dataset_number=5,
            file_path="file_path",
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
    ]
    with mock.patch("bec_lib.scan_history.os.access", return_value=True):
        scan_history_without_thread._max_scans = 2
        for m in msg:
            scan_history_without_thread._connector.xadd(
                MessageEndpoints.scan_history(), {"data": m}
            )

        while len(scan_history_without_thread._scan_ids) > 2:
            time.sleep(0.1)

    assert scan_history_without_thread.get_by_scan_number(1) is None
    assert scan_history_without_thread.get_by_scan_number(4)._msg == msg[0]

    assert (
        len(scan_history_without_thread._scan_numbers)
        == len(scan_history_without_thread._scan_ids)
        == 2
    )
    assert scan_history_without_thread._scan_numbers == [4, 5]
    assert [
        scan_history_without_thread._scan_data[sid].scan_number
        for sid in scan_history_without_thread._scan_ids
    ] == scan_history_without_thread._scan_numbers


def test_scan_history_slices(scan_history_without_thread, file_history_messages):
    assert [scan._msg for scan in scan_history_without_thread[0:2]] == file_history_messages[:2]
    assert [scan._msg for scan in scan_history_without_thread[1:]] == file_history_messages[1:]
    assert [scan._msg for scan in scan_history_without_thread[-2:]] == file_history_messages[-2:]
    assert scan_history_without_thread[-1]._msg == file_history_messages[-1]


@pytest.mark.timeout(10)
def test_scan_history_filters_readable_files(
    scan_history_without_thread, file_history_messages, tmp_path
):
    # Create a temporary file that is not readable
    unreadable_file = tmp_path / "unreadable_file.txt"
    unreadable_file.write_text("This file is not readable")
    unreadable_file.chmod(0o000)  # Make it unreadable

    readable_file = tmp_path / "readable_file.txt"
    readable_file.write_text("This file is readable")
    readable_file.chmod(0o644)  # Make it readable

    # Add a message with the unreadable file path
    unreadable_msg = messages.ScanHistoryMessage(
        scan_id="scan_id_unreadable",
        scan_number=5,
        dataset_number=5,
        file_path=str(unreadable_file),
        exit_status="closed",
        start_time=time.time(),
        end_time=time.time(),
        scan_name="line_scan",
        num_points=10,
    )
    readable_msg = messages.ScanHistoryMessage(
        scan_id="scan_id_readable",
        scan_number=6,
        dataset_number=6,
        file_path=str(tmp_path / "readable_file.txt"),
        exit_status="closed",
        start_time=time.time(),
        end_time=time.time(),
        scan_name="line_scan",
        num_points=10,
    )
    scan_history_without_thread._connector.xadd(
        MessageEndpoints.scan_history(), {"data": unreadable_msg}
    )
    scan_history_without_thread._connector.xadd(
        MessageEndpoints.scan_history(), {"data": readable_msg}
    )

    while scan_history_without_thread.get_by_scan_id("scan_id_readable") is None:
        time.sleep(0.1)
    # Verify that the unreadable file is not included in the history
    assert scan_history_without_thread.get_by_scan_id("scan_id_unreadable") is None
    assert scan_history_without_thread.get_by_scan_id("scan_id_readable")._msg == readable_msg

    # New: ensure unreadable scan_number wasn't appended, readable was
    nums = scan_history_without_thread._scan_numbers
    assert 5 not in nums
    assert nums[-1] == 6


@pytest.mark.timeout(20)
def test_scan_history_update_callback(scan_history_without_thread, file_history_messages):
    """Test the scan history update callbacks."""
    with mock.patch.object(
        scan_history_without_thread._client.callbacks, "run"
    ) as mock_callback_run:
        for ii, msg in enumerate(file_history_messages):
            scan_history_without_thread._connector.xadd(
                MessageEndpoints.scan_history(), {"data": msg}
            )
            # Sleep is needed to ensure the message is processed
            time.sleep(0.1)
            while (
                scan_history_without_thread.get_by_scan_id(msg.scan_id) is None
            ):  # Wait for the message to be added
                time.sleep(0.1)
            mock_callback_run.assert_called_with(
                event_type=EventType.SCAN_HISTORY_UPDATE, history_msg=msg
            )


@pytest.mark.timeout(20)
def test_scan_history_multiple_scan_numbers(scan_history_without_thread, file_history_messages):
    """Test the scan history update callbacks."""
    msgs = [
        messages.ScanHistoryMessage(
            scan_id="scan_id_1_a",
            scan_number=1,
            dataset_number=4,
            file_path="file_path",
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
        messages.ScanHistoryMessage(
            scan_id="scan_id_1_b",
            scan_number=1,
            dataset_number=5,
            file_path="file_path",
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
    ]
    with mock.patch("bec_lib.scan_history.os.access", return_value=True):
        for m in msgs:
            scan_history_without_thread._connector.xadd(
                MessageEndpoints.scan_history(), {"data": m}
            )

        while len(scan_history_without_thread._scan_ids) < len(file_history_messages) + len(msgs):
            time.sleep(0.1)

    containers = scan_history_without_thread.get_by_scan_number(1)
    assert isinstance(containers, list)
    assert len(containers) == 3  # 1 from fixture + 2 new
    assert containers[0]._msg == file_history_messages[0]
    assert containers[1]._msg == msgs[0]
    assert containers[2]._msg == msgs[1]
