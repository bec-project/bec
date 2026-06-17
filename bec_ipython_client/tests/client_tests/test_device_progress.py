from unittest import mock

import pytest

from bec_ipython_client.callbacks.device_progress import LiveUpdatesDeviceProgress
from bec_ipython_client.callbacks.utils import ScanState
from bec_lib import messages
from bec_lib.bec_errors import ScanInterruption, ScanRestart


def test_update_progressbar_continues_without_device_data():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()

    bec.connector.get.return_value = None
    res = live_update._update_progressbar(progressbar, ["async_dev1"])
    assert res is False


def test_update_progressbar_continues_when_scan_id_doesnt_match():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()
    live_update.scan_item = mock.MagicMock()
    live_update.scan_item.scan_id = "scan_id2"
    live_update.scan_item.restarted_msg = None
    live_update.scan_item.status = "open"
    live_update.scan_item.status_message = None

    bec.connector.get.return_value = messages.ProgressMessage(
        value=1, max_value=10, done=False, metadata={"scan_id": "scan_id"}
    )
    res = live_update._update_progressbar(progressbar, ["async_dev1"])
    assert res is False


def test_update_progressbar_updates_max_value():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()
    live_update.scan_item = mock.MagicMock()
    live_update.scan_item.scan_id = "scan_id"
    live_update.scan_item.restarted_msg = None
    live_update.scan_item.status = "open"
    live_update.scan_item.status_message = None

    bec.connector.get.return_value = messages.ProgressMessage(
        value=10, max_value=20, done=False, metadata={"scan_id": "scan_id"}
    )
    res = live_update._update_progressbar(progressbar, ["async_dev1"])
    assert res is False
    assert progressbar.max_points == 20
    progressbar.update.assert_called_once_with(10)


def test_update_progressbar_returns_true_when_max_value_is_reached():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()
    live_update.scan_item = mock.MagicMock()
    live_update.scan_item.scan_id = "scan_id"
    live_update.scan_item.restarted_msg = None
    live_update.scan_item.status = "open"
    live_update.scan_item.status_message = None

    bec.connector.get.return_value = messages.ProgressMessage(
        value=10, max_value=10, done=True, metadata={"scan_id": "scan_id"}
    )
    res = live_update._update_progressbar(progressbar, ["async_dev1"])
    assert res is True


def test_update_progressbar_raises_scan_restart_when_scan_restarted():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()
    restart_msg = messages.ScanQueueMessage(scan_type="grid_scan", parameter={"args": {}})
    live_update.scan_item = mock.MagicMock(
        scan_id="scan_id", restarted_msg=restart_msg, status="open", status_message=None
    )

    with mock.patch("bec_ipython_client.callbacks.device_progress.print") as mock_print:
        with pytest.raises(ScanRestart) as exc_info:
            live_update._update_progressbar(progressbar, ["async_dev1"])

    assert exc_info.value.new_scan_msg == restart_msg
    mock_print.assert_not_called()


def test_check_scan_state_returns_wait_for_restart_reason():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    live_update.scan_item = mock.MagicMock(
        status="open",
        restarted_msg=None,
        queue=mock.MagicMock(status="STOPPED", reason="restart", scans=[mock.MagicMock()]),
    )

    assert live_update._check_scan_state() == ScanState.WAIT


def test_update_progressbar_returns_true_when_scan_completed_by_user():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()
    live_update.scan_item = mock.MagicMock(
        scan_id="scan_id", restarted_msg=None, status="user_completed", status_message=None
    )

    with mock.patch("bec_ipython_client.callbacks.device_progress.print") as mock_print:
        res = live_update._update_progressbar(progressbar, ["async_dev1"])

    assert res is True
    mock_print.assert_called_once_with("Scan was set to 'completed' by user.")


def test_update_progressbar_raises_scan_interruption_when_aborted_by_user():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()
    live_update.scan_item = mock.MagicMock(
        scan_id="scan_id",
        scan_number=5,
        restarted_msg=None,
        status="open",
        status_message=mock.MagicMock(reason="user"),
    )

    with pytest.raises(ScanInterruption, match="Scan 5 was aborted by user."):
        live_update._update_progressbar(progressbar, ["async_dev1"])


def test_update_progressbar_waits_for_restart_message_before_finishing():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()
    restart_msg = messages.ScanQueueMessage(scan_type="grid_scan", parameter={"args": {}})
    queue = mock.MagicMock(status="STOPPED", reason="restart")
    scan_item = mock.MagicMock(
        scan_id="scan_id",
        scan_number=5,
        restarted_msg=None,
        status="open",
        status_message=None,
        queue=queue,
    )
    queue.scans = [scan_item]
    live_update.scan_item = scan_item

    bec.connector.get.return_value = messages.ProgressMessage(
        value=10, max_value=10, done=True, metadata={"scan_id": "scan_id"}
    )

    with mock.patch("bec_ipython_client.callbacks.device_progress.time.sleep") as sleep:

        def trigger_restart(*_args, **_kwargs):
            scan_item.restarted_msg = restart_msg

        sleep.side_effect = trigger_restart

        first = live_update._update_progressbar(progressbar, ["async_dev1"])
        assert first is False
        with pytest.raises(ScanRestart) as exc_info:
            live_update._update_progressbar(progressbar, ["async_dev1"])

    assert exc_info.value.new_scan_msg == restart_msg
