from unittest import mock

from bec_ipython_client.callbacks.device_progress import LiveUpdatesDeviceProgress
from bec_messages import messages


def test_update_progressbar_continues_without_device_data():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()

    bec.connector.get.return_value = None
    res = live_update._update_progressbar(progressbar, "async_dev1")
    assert res is False


def test_update_progressbar_continues_when_scan_id_doesnt_match():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()
    live_update.scan_item = mock.MagicMock()
    live_update.scan_item.scan_id = "scan_id2"

    bec.connector.get.return_value = messages.ProgressMessage(
        value=1, max_value=10, done=False, metadata={"scan_id": "scan_id"}
    )
    res = live_update._update_progressbar(progressbar, "async_dev1")
    assert res is False


def test_update_progressbar_updates_max_value():
    bec = mock.MagicMock()
    request = mock.MagicMock()
    live_update = LiveUpdatesDeviceProgress(bec=bec, report_instruction={}, request=request)
    progressbar = mock.MagicMock()
    live_update.scan_item = mock.MagicMock()
    live_update.scan_item.scan_id = "scan_id"

    bec.connector.get.return_value = messages.ProgressMessage(
        value=10, max_value=20, done=False, metadata={"scan_id": "scan_id"}
    )
    res = live_update._update_progressbar(progressbar, "async_dev1")
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

    bec.connector.get.return_value = messages.ProgressMessage(
        value=10, max_value=10, done=True, metadata={"scan_id": "scan_id"}
    )
    res = live_update._update_progressbar(progressbar, "async_dev1")
    assert res is True
