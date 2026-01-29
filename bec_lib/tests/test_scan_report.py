from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.bec_errors import ScanAbortion
from bec_lib.scan_report import ScanReport

# pylint: skip-file


@pytest.fixture
def scan_report():
    report = ScanReport()
    report._client = mock.MagicMock()
    report._queue_item = mock.MagicMock()
    report.request = mock.MagicMock()
    yield report


def test_scan_report_wait_mv(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="mv", parameter={})
    with mock.patch.object(scan_report, "_wait_move") as wait_move:
        scan_report.wait()
        wait_move.assert_called_once_with(None, 0.1)


def test_scan_report_wait_mv_timeout(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="mv", parameter={})
    with mock.patch.object(scan_report, "_wait_move") as wait_move:
        scan_report.wait(10)
        wait_move.assert_called_once_with(10, 0.1)


def test_scan_report_wait_scan(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="line_scan", parameter={})
    with mock.patch.object(scan_report, "_wait_scan") as wait_scan:
        scan_report.wait()
        wait_scan.assert_called_once_with(None, 0.1, num_points=False, file_written=False)


def test_scan_report_wait_scan_num_points(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="line_scan", parameter={})
    with mock.patch.object(scan_report, "_wait_scan") as wait_scan:
        scan_report.wait(num_points=True)
        wait_scan.assert_called_once_with(None, 0.1, num_points=True, file_written=False)


def test_scan_report_wait_scan_file_written(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="line_scan", parameter={})
    with mock.patch.object(scan_report, "_wait_scan") as wait_scan:
        scan_report.wait(file_written=True)
        wait_scan.assert_called_once_with(None, 0.1, num_points=False, file_written=True)


@pytest.mark.parametrize("timeout, elapsed_time", [(10, 0.1), (None, 0.1), (0.1, 10)])
def test_scan_report_check_timeout(timeout, elapsed_time):
    report = ScanReport()
    if timeout is None or timeout > elapsed_time:
        report._check_timeout(timeout, elapsed_time)
    else:
        with pytest.raises(TimeoutError):
            report._check_timeout(timeout, elapsed_time)


def test_scan_report_wait_move(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="mv", parameter={})
    with mock.patch.object(scan_report, "_get_mv_status") as get_mv_status:
        get_mv_status.side_effect = [False, False, True]
        scan_report._wait_move(None, 0.1)
        assert get_mv_status.call_count == 3
        assert scan_report._client.alarm_handler.raise_alarms.call_count == 2


def test_scan_report_wait_for_scan(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="mv", parameter={})
    with mock.patch.object(scan_report, "_get_mv_status") as get_mv_status:
        get_mv_status.side_effect = [False, False, True]
        scan_report.queue_item.status = "COMPLETED"
        scan_report._wait_scan(None, 0.1)


def test_scan_report_wait_for_scan_file_pending(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="mv", parameter={})
    with mock.patch.object(scan_report, "_get_mv_status") as get_mv_status:
        with mock.patch.object(scan_report, "_file_written") as file_written:
            file_written.side_effect = [False, False, True]
            get_mv_status.return_value = True
            scan_report.queue_item.status = "COMPLETED"
            scan_report._wait_scan(None, 0.1, num_points=False, file_written=True)
            assert file_written.call_count == 3


def test_scan_report_wait_for_scan_raises(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="mv", parameter={})
    with mock.patch.object(scan_report, "_get_mv_status") as get_mv_status:
        get_mv_status.side_effect = [False, False, True]
        scan_report.queue_item.status = "STOPPED"
        with pytest.raises(ScanAbortion):
            scan_report._wait_scan(None, 0.1)


def test_scan_report_aborts_on_ctrl_c(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(
        scan_type="mv", parameter={"args": {"samx": [5]}}
    )
    with mock.patch.object(scan_report, "_wait_move") as mock_wait_move:
        mock_wait_move.side_effect = [KeyboardInterrupt]
        with pytest.raises(ScanAbortion):
            scan_report.wait()
        scan_report._client.device_manager.devices.get("samx").stop.assert_called_once()


@pytest.mark.parametrize(
    "xread_return, expected",
    [
        (None, False),
        (
            [
                {
                    "data": messages.DeviceRPCMessage(
                        device="samx", return_val=5, out="done", success=True
                    )
                }
            ],
            False,
        ),
        (
            [
                {
                    "data": messages.DeviceRPCMessage(
                        device="samx", return_val=5, out="done", success=True
                    )
                },
                {
                    "data": messages.DeviceRPCMessage(
                        device="samy", return_val=5, out="done", success=True
                    )
                },
            ],
            True,
        ),
    ],
)
def test_scan_report_get_mv_status(scan_report, xread_return, expected):
    scan_report.request.request = messages.ScanQueueMessage(
        scan_type="mv", parameter={"args": {"samx": [5], "samy": [5]}}
    )
    with mock.patch.object(scan_report._client.device_manager.connector, "xread") as mock_xread:
        mock_xread.return_value = xread_return
        assert scan_report._get_mv_status() == expected


def test_scan_report_file_written(scan_report):
    with mock.patch.object(scan_report.request, "scan") as mock_scan:
        mock_scan.public_files = {"/tmp/file1_master.h5": {"done_state": True, "success": True}}
        assert scan_report._file_written() is True


def test_scan_report_file_written_no_files(scan_report):
    with mock.patch.object(scan_report.request, "scan") as mock_scan:
        mock_scan.public_files = {}
        assert scan_report._file_written() is False


def test_scan_report_file_written_no_master(scan_report):
    with mock.patch.object(scan_report.request, "scan") as mock_scan:
        mock_scan.public_files = {"/tmp/file1.h5": {"done_state": True, "success": True}}
        assert scan_report._file_written() is False


def test_scan_report_num_points_reached(scan_report):
    with mock.patch.object(scan_report.request, "scan") as mock_scan:
        mock_scan.num_points = 10
        mock_scan.live_data = {"0": "msg", "1": "msg", "2": "msg"}
        assert scan_report._num_points_reached() is False


def test_scan_report_num_points_reached_no_points(scan_report):
    with mock.patch.object(scan_report.request, "scan") as mock_scan:
        mock_scan.num_points = 0
        mock_scan.live_data = {}
        assert scan_report._num_points_reached() is True


def test_scan_report_num_points_reached_match(scan_report):
    with mock.patch.object(scan_report.request, "scan") as mock_scan:
        mock_scan.num_points = 3
        mock_scan.live_data = {"0": "msg", "1": "msg", "2": "msg"}
        assert scan_report._num_points_reached() is True
