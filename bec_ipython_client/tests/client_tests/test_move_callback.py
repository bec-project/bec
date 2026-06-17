import collections
import time
from unittest import mock

import pytest

from bec_ipython_client.callbacks.move_device import (
    LiveUpdatesReadbackProgressbar,
    ReadbackDataHandler,
)
from bec_ipython_client.callbacks.utils import ScanState
from bec_lib import messages
from bec_lib.bec_errors import ScanInterruption, ScanRestart
from bec_lib.endpoints import MessageEndpoints


@pytest.fixture
def readback_data_handler(bec_client_mock, connected_connector):
    with mock.patch.object(bec_client_mock.device_manager, "connector", connected_connector):
        yield ReadbackDataHandler(
            bec_client_mock.device_manager, ["samx", "samy"], request_id="something"
        )


def test_move_callback(bec_client_mock):
    client = bec_client_mock
    request = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "something"},
    )
    readback = collections.deque()
    readback.extend([[-10], [0], [10]])

    def mock_readback(*args, **kwargs):
        if len(readback) > 1:
            return readback.popleft()
        return readback[0]

    req_done = collections.deque()
    req_done.extend([{"samx": (False, False)}, {"samx": (False, False)}, {"samx": (True, True)}])

    def mock_req_msg(*args):
        if len(req_done) > 1:
            return req_done.popleft()
        return req_done[0]

    with mock.patch("bec_ipython_client.callbacks.move_device.check_alarms"):
        with mock.patch.object(LiveUpdatesReadbackProgressbar, "wait_for_request_acceptance"):
            with mock.patch.object(
                LiveUpdatesReadbackProgressbar, "_print_client_msgs_asap"
            ) as mock_client_msgs:
                with mock.patch.object(
                    LiveUpdatesReadbackProgressbar, "_print_client_msgs_all"
                ) as mock_client_msgs_all:
                    with mock.patch.object(ReadbackDataHandler, "get_device_values", mock_readback):
                        with mock.patch.object(ReadbackDataHandler, "device_states", mock_req_msg):
                            with mock.patch.object(
                                ReadbackDataHandler, "done", side_effect=[False, False, True]
                            ):
                                LiveUpdatesReadbackProgressbar(bec=client, request=request).run()
                                assert mock_client_msgs.called is True
                                assert mock_client_msgs_all.called is True


def test_move_callback_with_report_instruction(bec_client_mock):
    client = bec_client_mock
    request = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "something"},
    )
    readback = collections.deque()
    readback.extend([[-10], [0], [10]])
    report_instruction = {
        "readback": {"RID": "something", "devices": ["samx"], "start": [0], "end": [10]}
    }

    def mock_readback(*args, **kwargs):
        if len(readback) > 1:
            return readback.popleft()
        return readback[0]

    req_done = collections.deque()
    req_done.extend([{"samx": (False, False)}, {"samx": (False, False)}, {"samx": (True, True)}])

    def mock_req_msg(*args):
        if len(req_done) > 1:
            return req_done.popleft()
        return req_done[0]

    with mock.patch("bec_ipython_client.callbacks.move_device.check_alarms"):
        with mock.patch.object(LiveUpdatesReadbackProgressbar, "wait_for_request_acceptance"):
            with mock.patch.object(LiveUpdatesReadbackProgressbar, "_print_client_msgs_asap"):
                with mock.patch.object(LiveUpdatesReadbackProgressbar, "_print_client_msgs_all"):
                    with mock.patch.object(ReadbackDataHandler, "get_device_values", mock_readback):
                        with mock.patch.object(ReadbackDataHandler, "device_states", mock_req_msg):
                            with mock.patch.object(
                                ReadbackDataHandler, "done", side_effect=[False, False, False, True]
                            ):
                                LiveUpdatesReadbackProgressbar(
                                    bec=client,
                                    report_instruction=report_instruction,
                                    request=request,
                                ).run()


def test_move_callback_check_scan_state_raises_user_interruption(bec_client_mock):
    request = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "something"},
    )
    live_update = LiveUpdatesReadbackProgressbar(bec=bec_client_mock, request=request)
    live_update.scan_queue_request = mock.MagicMock(
        queue=mock.MagicMock(
            status="STOPPED",
            scans=[
                mock.MagicMock(
                    scan_number=5, restarted_msg=None, status_message=mock.MagicMock(reason="user")
                )
            ],
        )
    )

    with pytest.raises(ScanInterruption, match="Scan 5 was aborted by user."):
        live_update._check_scan_state()


def test_move_callback_check_scan_state_raises_scan_restart(bec_client_mock):
    request = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "something"},
    )
    restart_msg = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "restart"},
    )
    live_update = LiveUpdatesReadbackProgressbar(bec=bec_client_mock, request=request)
    live_update.scan_queue_request = mock.MagicMock(
        queue=mock.MagicMock(
            status="STOPPED",
            scans=[
                mock.MagicMock(
                    scan_number=5,
                    restarted_msg=restart_msg,
                    status_message=mock.MagicMock(reason="alarm"),
                )
            ],
        )
    )

    with pytest.raises(ScanRestart) as exc_info:
        live_update._check_scan_state()

    assert exc_info.value.new_scan_msg == restart_msg


def test_move_callback_check_scan_state_raises_alarm_interruption(bec_client_mock):
    request = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "something"},
        allow_restart=False,
    )
    live_update = LiveUpdatesReadbackProgressbar(bec=bec_client_mock, request=request)
    live_update.scan_queue_request = mock.MagicMock(
        queue=mock.MagicMock(
            status="STOPPED",
            scans=[
                mock.MagicMock(
                    scan_number=7, restarted_msg=None, status_message=mock.MagicMock(reason="alarm")
                )
            ],
        )
    )

    with pytest.raises(ScanInterruption, match="Scan 7 was interrupted."):
        live_update._check_scan_state()


def test_move_callback_check_scan_state_raises_cancelled_interruption(bec_client_mock):
    request = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "something"},
    )
    live_update = LiveUpdatesReadbackProgressbar(bec=bec_client_mock, request=request)
    live_update.scan_queue_request = mock.MagicMock(
        queue=mock.MagicMock(status="CANCELLED", scans=[None])
    )

    with pytest.raises(ScanInterruption, match="Scan was cancelled."):
        live_update._check_scan_state()


def test_move_callback_check_scan_state_restart_without_restart_message_is_nonblocking(
    bec_client_mock,
):
    request = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "something"},
    )
    live_update = LiveUpdatesReadbackProgressbar(bec=bec_client_mock, request=request)
    live_update.scan_queue_request = mock.MagicMock(
        queue=mock.MagicMock(
            status="STOPPED",
            reason="restart",
            scans=[
                mock.MagicMock(restarted_msg=None, status_message=mock.MagicMock(reason="alarm"))
            ],
        )
    )

    assert live_update._check_scan_state() == ScanState.WAIT


def test_move_callback_run_waits_for_restart_message_before_exiting(bec_client_mock):
    client = bec_client_mock
    request = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "something"},
    )
    report_instruction = {
        "readback": {"RID": "something", "devices": ["samx"], "start": [0], "end": [10]}
    }
    restart_msg = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "restart"},
    )
    live_update = LiveUpdatesReadbackProgressbar(
        bec=client, report_instruction=report_instruction, request=request
    )
    queue = mock.MagicMock(
        status="STOPPED",
        reason="restart",
        scans=[mock.MagicMock(restarted_msg=None, status_message=mock.MagicMock(reason="alarm"))],
    )
    live_update.scan_queue_request = mock.MagicMock(queue=queue)

    with (
        mock.patch("bec_ipython_client.callbacks.move_device.check_alarms"),
        mock.patch.object(LiveUpdatesReadbackProgressbar, "wait_for_request_acceptance"),
        mock.patch.object(LiveUpdatesReadbackProgressbar, "_print_client_msgs_asap"),
        mock.patch.object(LiveUpdatesReadbackProgressbar, "_print_client_msgs_all"),
        mock.patch.object(ReadbackDataHandler, "get_device_values", side_effect=[[0], [10]]),
        mock.patch.object(
            ReadbackDataHandler, "device_states", return_value={"samx": (True, True)}
        ),
        mock.patch.object(ReadbackDataHandler, "done", return_value=True),
        mock.patch("bec_ipython_client.callbacks.move_device.time.sleep") as sleep,
    ):

        def trigger_restart(*_args, **_kwargs):
            queue.scans[-1].restarted_msg = restart_msg

        sleep.side_effect = trigger_restart

        with pytest.raises(ScanRestart) as exc_info:
            live_update.core()

    assert exc_info.value.new_scan_msg == restart_msg


def test_readback_data_handler(readback_data_handler):
    readback_data_handler.data = {
        "samx": messages.DeviceMessage(
            signals={"samx": {"value": 10}, "samx_setpoint": {"value": 20}},
            metadata={"device": "samx"},
        ),
        "samy": messages.DeviceMessage(
            signals={"samy": {"value": 10}, "samy_setpoint": {"value": 20}},
            metadata={"device": "samy"},
        ),
    }

    res = readback_data_handler.get_device_values()
    assert res == [10, 10]


def test_readback_data_handler_multiple_hints(readback_data_handler):
    readback_data_handler.device_manager.devices.samx._info["hints"]["fields"] = [
        "samx_setpoint",
        "samx",
    ]
    readback_data_handler.data = {
        "samx": messages.DeviceMessage(
            signals={"samx": {"value": 10}, "samx_setpoint": {"value": 20}},
            metadata={"device": "samx"},
        ),
        "samy": messages.DeviceMessage(
            signals={"samy": {"value": 10}, "samy_setpoint": {"value": 20}},
            metadata={"device": "samy"},
        ),
    }
    res = readback_data_handler.get_device_values()
    assert res == [20, 10]


def test_readback_data_handler_multiple_no_hints(readback_data_handler):
    readback_data_handler.device_manager.devices.samx._info["hints"]["fields"] = []
    readback_data_handler.data = {
        "samx": messages.DeviceMessage(
            signals={"samx": {"value": 10}, "samx_setpoint": {"value": 20}},
            metadata={"device": "samx"},
        ),
        "samy": messages.DeviceMessage(
            signals={"samy": {"value": 10}, "samy_setpoint": {"value": 20}},
            metadata={"device": "samy"},
        ),
    }
    res = readback_data_handler.get_device_values()
    assert res == [10, 10]


def test_readback_data_handler_init(readback_data_handler):
    """
    Test that the ReadbackDataHandler is initialized correctly.
    """

    # Initial state
    assert readback_data_handler._devices_done_state == {
        "samx": (False, False),
        "samy": (False, False),
    }
    assert readback_data_handler._devices_received == {"samx": False, "samy": False}
    assert readback_data_handler.data == {}


def test_readback_data_handler_readback_callbacks(readback_data_handler):
    """
    Test that the readback callback properly updates the readback data.
    """

    # Submit readback for samx
    msg = messages.DeviceMessage(
        signals={"samx": {"value": 15}}, metadata={"device": "samx", "RID": "something"}
    )
    readback_data_handler.connector.set_and_publish(MessageEndpoints.device_readback("samx"), msg)

    msg_old = messages.DeviceMessage(
        signals={"samy": {"value": 10}}, metadata={"device": "samx", "RID": "something_else"}
    )
    readback_data_handler.connector.set_and_publish(
        MessageEndpoints.device_readback("samy"), msg_old
    )
    while (
        not readback_data_handler._devices_received["samx"]
        or "samx" not in readback_data_handler.data
    ):
        time.sleep(0.01)
    assert readback_data_handler.data["samx"].signals["samx"]["value"] == 15
    dev_data = readback_data_handler.get_device_values()
    assert dev_data[0] == 15
    assert dev_data[1] == 10  # samy remains unchanged


def test_readback_data_handler_request_done_callbacks(readback_data_handler):
    """
    Test that the request done callback properly updates the device done state.
    """

    # Submit request done for samx
    msg = messages.DeviceReqStatusMessage(device="samx", success=True, request_id="something")
    readback_data_handler.connector.xadd(
        MessageEndpoints.device_req_status("something"), {"data": msg}
    )
    while not readback_data_handler._devices_done_state["samx"][0]:
        time.sleep(0.01)
    assert readback_data_handler._devices_done_state["samx"] == (True, True)

    assert readback_data_handler.done() is False

    # Submit request done for samy
    msg = messages.DeviceReqStatusMessage(device="samy", success=False, request_id="something")
    readback_data_handler.connector.xadd(
        MessageEndpoints.device_req_status("something"), {"data": msg}
    )
    while not readback_data_handler._devices_done_state["samy"][0]:
        time.sleep(0.01)
    assert readback_data_handler._devices_done_state["samy"] == (True, False)
    assert readback_data_handler.done() is True
