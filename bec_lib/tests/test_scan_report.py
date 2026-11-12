import threading
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.alarm_handler import AlarmBase
from bec_lib.bec_errors import ScanAbortion
from bec_lib.callback_handler import CallbackHandler, EventType
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
        wait_scan.assert_called_once_with(None, 0.1, num_points=False, file_written=True)


def test_scan_report_wait_scan_num_points(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="line_scan", parameter={})
    with mock.patch.object(scan_report, "_wait_scan") as wait_scan:
        scan_report.wait(num_points=True)
        wait_scan.assert_called_once_with(None, 0.1, num_points=True, file_written=True)


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


@pytest.mark.parametrize("timeout", [None, 0.05])
def test_scan_report_wait_finishes_callbacks_while_another_thread_polls(scan_report, timeout):
    handler = CallbackHandler()
    scan_report.request.callbacks = handler
    scan_report.request.request = messages.ScanQueueMessage(scan_type="line_scan", parameter={})
    scan_report.queue_item.status = "COMPLETED"
    started = threading.Event()
    resume = threading.Event()
    final_poll_started = threading.Event()
    wait_finished = threading.Event()
    points = []
    errors = []

    class Recorder:
        def callback(self, point):
            if point == 0:
                started.set()
                resume.wait(timeout=5)
            points.append(point)

    recorder = Recorder()
    handler.register(EventType.SCAN_SEGMENT, recorder.callback, sync=True)
    handler.run(EventType.SCAN_SEGMENT, 0)
    scan_report._client.callbacks.poll.side_effect = lambda **_: final_poll_started.set()

    def wait_for_report():
        try:
            scan_report.wait(timeout=timeout, file_written=False)
        except TimeoutError as exc:
            errors.append(exc)
        finally:
            wait_finished.set()

    poller = threading.Thread(target=handler.poll, daemon=True)
    waiter = threading.Thread(target=wait_for_report, daemon=True)
    poller.start()
    try:
        assert started.wait(timeout=5)
        handler.run(EventType.SCAN_SEGMENT, 1)
        waiter.start()
        assert final_poll_started.wait(timeout=5)
        if timeout is None:
            assert not wait_finished.wait(timeout=0.1)
        else:
            assert wait_finished.wait(timeout=2)
            assert len(errors) == 1
            assert points == []
    finally:
        resume.set()
        poller.join(timeout=5)
        if waiter.ident is not None:
            waiter.join(timeout=5)

    assert not poller.is_alive()
    assert wait_finished.is_set()
    if timeout is None:
        assert not errors
        assert points == [0, 1]
    else:
        handler.poll()
        assert points == [0, 1]


def test_scan_report_wait_with_zero_timeout_accepts_completed_scan_without_callbacks(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="line_scan", parameter={})
    scan_report.queue_item.status = "COMPLETED"
    scan_report.request.callbacks = CallbackHandler()
    scan_report._client.callbacks = CallbackHandler()

    assert scan_report.wait(timeout=0, file_written=False) is scan_report


@pytest.mark.parametrize("callback_scope", ["request", "client"])
def test_scan_report_wait_checks_timeout_after_running_callback(scan_report, callback_scope):
    handler = CallbackHandler()
    owner = scan_report.request if callback_scope == "request" else scan_report._client
    owner.callbacks = handler
    scan_report.request.request = messages.ScanQueueMessage(scan_type="line_scan", parameter={})
    scan_report.queue_item.status = "COMPLETED"
    points = []

    class Recorder:
        def callback(self, point):
            points.append(point)
            clock.return_value = 1

    recorder = Recorder()
    handler.register(EventType.SCAN_SEGMENT, recorder.callback, sync=True)
    handler.run(EventType.SCAN_SEGMENT, 0)
    handler.run(EventType.SCAN_SEGMENT, 1)

    with mock.patch("time.monotonic", return_value=0) as clock:
        with pytest.raises(TimeoutError, match="draining callbacks"):
            scan_report.wait(timeout=0, file_written=False)
        assert points == []
        with pytest.raises(TimeoutError, match="draining callbacks"):
            scan_report.wait(timeout=0.02, file_written=False)

    assert points == [0]
    handler.poll()
    assert points == [0, 1]


def test_scan_report_wait_for_scan_file_pending(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="mv", parameter={})
    with mock.patch.object(scan_report, "_get_mv_status") as get_mv_status:
        with mock.patch.object(scan_report, "_file_written") as file_written:
            file_written.side_effect = [False, False, True]
            get_mv_status.return_value = True
            scan_report.queue_item.status = "COMPLETED"
            scan_report._wait_scan(None, 0.1, num_points=False, file_written=True)
            assert file_written.call_count == 3


def test_scan_report_wait_for_non_scan_without_scan_item(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="umv", parameter={})
    scan_report.request.requestID = "request-id"
    scan_report.queue_item.status = "COMPLETED"
    scan_report.request.queue.request_blocks = [mock.MagicMock(RID="request-id", is_scan=False)]

    with (
        mock.patch.object(ScanReport, "scan", new_callable=mock.PropertyMock) as scan,
        mock.patch("time.sleep") as sleep,
    ):
        scan.return_value = None
        scan_report._wait_scan(None, 0.1, num_points=False, file_written=True)

    sleep.assert_not_called()


def test_scan_report_wait_for_non_scan_uses_cached_queue_item(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="umv", parameter={})
    scan_report.request.requestID = "request-id"
    scan_report.queue_item.status = "COMPLETED"
    scan_report.queue_item.request_blocks = [mock.MagicMock(RID="request-id", is_scan=False)]
    scan_report.request.queue = None

    with (
        mock.patch.object(ScanReport, "scan", new_callable=mock.PropertyMock) as scan,
        mock.patch("time.sleep") as sleep,
    ):
        scan.return_value = None
        scan_report._wait_scan(None, 0.1, num_points=False, file_written=True)

    sleep.assert_not_called()


def test_scan_report_wait_for_scan_num_points_waits_for_scan_item(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="line_scan", parameter={})
    scan_report.queue_item.status = "COMPLETED"

    missing_scan = None
    incomplete_scan = mock.MagicMock(num_monitored_readouts=3, live_data={"0": "msg"})
    complete_scan = mock.MagicMock(
        num_monitored_readouts=3, live_data={"0": "msg", "1": "msg", "2": "msg"}
    )

    with (
        mock.patch.object(ScanReport, "scan", new_callable=mock.PropertyMock) as scan,
        mock.patch("time.sleep"),
    ):
        scan.side_effect = [missing_scan, *([incomplete_scan] * 4), *([complete_scan] * 4)]
        scan_report._wait_scan(None, 0.1, num_points=True, file_written=False)

    assert scan_report._client.callbacks.poll.call_count == 3


def test_scan_report_wait_for_scan_raises(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="mv", parameter={})
    with mock.patch.object(scan_report, "_get_mv_status") as get_mv_status:
        get_mv_status.side_effect = [False, False, True]
        scan_report.queue_item.status = "STOPPED"
        with pytest.raises(ScanAbortion):
            scan_report._wait_scan(None, 0.1)


def test_scan_report_wait_for_scan_raises_when_request_is_cancelled(scan_report):
    scan_report.request.request = messages.ScanQueueMessage(scan_type="line_scan", parameter={})
    scan_report.queue_item.status = "CANCELLED"
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


def test_scan_report_get_mv_status_raises_alarm_on_failed_move(scan_report):
    error_info = messages.ErrorInfo(
        error_message="Motor move failed",
        compact_error_message="RuntimeError: Motor move failed",
        exception_type="RuntimeError",
        device="samx",
    )
    scan_report.request.request = messages.ScanQueueMessage(
        scan_type="mv", parameter={"args": {"samx": [5]}}
    )
    with mock.patch.object(scan_report._client.device_manager.connector, "xread") as mock_xread:
        mock_xread.return_value = [
            {
                "data": messages.DeviceReqStatusMessage(
                    device="samx",
                    success=False,
                    request_id="request-id",
                    metadata={"error_info": error_info},
                )
            }
        ]
        with pytest.raises(AlarmBase) as exc_info:
            scan_report._get_mv_status()

    assert exc_info.value.severity.name == "MAJOR"
    assert exc_info.value.alarm.info == error_info


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
        mock_scan.num_monitored_readouts = 10
        mock_scan.live_data = {"0": "msg", "1": "msg", "2": "msg"}
        assert scan_report._num_points_reached() is False


def test_scan_report_num_points_reached_no_points(scan_report):
    with mock.patch.object(scan_report.request, "scan") as mock_scan:
        mock_scan.num_monitored_readouts = 0
        mock_scan.live_data = {}
        assert scan_report._num_points_reached() is True


def test_scan_report_num_points_reached_match(scan_report):
    with mock.patch.object(scan_report.request, "scan") as mock_scan:
        mock_scan.num_monitored_readouts = 3
        mock_scan.live_data = {"0": "msg", "1": "msg", "2": "msg"}
        assert scan_report._num_points_reached() is True
