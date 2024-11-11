import time

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.scan_history import ScanHistory

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring


@pytest.fixture
def scan_history_without_thread(connected_connector, file_history_messages):
    for msg in file_history_messages:
        connected_connector.xadd(MessageEndpoints.scan_history(), {"data": msg})
    return ScanHistory(connector=connected_connector, load_threaded=False)


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
    scan_history_without_thread._max_scans = 2
    for m in msg:
        scan_history_without_thread._connector.xadd(MessageEndpoints.scan_history(), {"data": m})

    while len(scan_history_without_thread._scan_ids) > 2:
        time.sleep(0.1)

    assert scan_history_without_thread.get_by_scan_number(1) is None
    assert scan_history_without_thread.get_by_scan_number(4)._msg == msg[0]


def test_scan_history_slices(scan_history_without_thread, file_history_messages):
    assert [scan._msg for scan in scan_history_without_thread[0:2]] == file_history_messages[:2]
    assert [scan._msg for scan in scan_history_without_thread[1:]] == file_history_messages[1:]
    assert [scan._msg for scan in scan_history_without_thread[-2:]] == file_history_messages[-2:]
    assert scan_history_without_thread[-1]._msg == file_history_messages[-1]
