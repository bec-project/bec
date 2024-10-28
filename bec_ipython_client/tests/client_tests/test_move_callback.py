import collections
from unittest import mock

import pytest

from bec_ipython_client.callbacks.move_device import (
    LiveUpdatesReadbackProgressbar,
    ReadbackDataMixin,
)
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints


@pytest.fixture
def readback_data_mixin(bec_client_mock):
    with mock.patch.object(bec_client_mock.device_manager, "connector"):
        yield ReadbackDataMixin(bec_client_mock.device_manager, ["samx", "samy"])


def test_move_callback(bec_client_mock):
    client = bec_client_mock
    request = messages.ScanQueueMessage(
        scan_type="umv",
        parameter={"args": {"samx": [10]}, "kwargs": {"relative": True}},
        metadata={"RID": "something"},
    )
    readback = collections.deque()
    readback.extend([[-10], [0], [10]])

    def mock_readback(*args):
        if len(readback) > 1:
            return readback.popleft()
        return readback[0]

    req_done = collections.deque()
    msg_acc = messages.DeviceReqStatusMessage(
        device="samx", success=True, metadata={"RID": "something"}
    )
    req_done.extend([[None], [None], [None], [msg_acc]])

    def mock_req_msg(*args):
        if len(req_done) > 1:
            return req_done.popleft()
        return req_done[0]

    with mock.patch("bec_ipython_client.callbacks.move_device.check_alarms") as check_alarms_mock:
        with mock.patch.object(ReadbackDataMixin, "wait_for_RID"):
            with mock.patch.object(LiveUpdatesReadbackProgressbar, "wait_for_request_acceptance"):
                with mock.patch.object(
                    LiveUpdatesReadbackProgressbar, "_print_client_msgs_asap"
                ) as mock_client_msgs:
                    with mock.patch.object(
                        LiveUpdatesReadbackProgressbar, "_print_client_msgs_all"
                    ) as mock_client_msgs_all:
                        with mock.patch.object(
                            ReadbackDataMixin, "get_device_values", mock_readback
                        ):
                            with mock.patch.object(
                                ReadbackDataMixin, "get_request_done_msgs", mock_req_msg
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

    def mock_readback(*args):
        if len(readback) > 1:
            return readback.popleft()
        return readback[0]

    req_done = collections.deque()
    msg_acc = messages.DeviceReqStatusMessage(
        device="samx", success=True, metadata={"RID": "something"}
    )
    req_done.extend([[None], [None], [None], [msg_acc]])

    def mock_req_msg(*args):
        if len(req_done) > 1:
            return req_done.popleft()
        return req_done[0]

    with mock.patch("bec_ipython_client.callbacks.move_device.check_alarms") as check_alarms_mock:
        with mock.patch.object(ReadbackDataMixin, "wait_for_RID"):
            with mock.patch.object(LiveUpdatesReadbackProgressbar, "wait_for_request_acceptance"):
                with mock.patch.object(LiveUpdatesReadbackProgressbar, "_print_client_msgs_asap"):
                    with mock.patch.object(
                        LiveUpdatesReadbackProgressbar, "_print_client_msgs_all"
                    ):
                        with mock.patch.object(
                            ReadbackDataMixin, "get_device_values", mock_readback
                        ):
                            with mock.patch.object(
                                ReadbackDataMixin, "get_request_done_msgs", mock_req_msg
                            ):
                                LiveUpdatesReadbackProgressbar(
                                    bec=client,
                                    report_instruction=report_instruction,
                                    request=request,
                                ).run()


def test_readback_data_mixin(readback_data_mixin):
    readback_data_mixin.device_manager.connector.get.side_effect = [
        messages.DeviceMessage(
            signals={"samx": {"value": 10}, "samx_setpoint": {"value": 20}},
            metadata={"device": "samx"},
        ),
        messages.DeviceMessage(
            signals={"samy": {"value": 10}, "samy_setpoint": {"value": 20}},
            metadata={"device": "samy"},
        ),
    ]
    res = readback_data_mixin.get_device_values()
    assert res == [10, 10]


def test_readback_data_mixin_multiple_hints(readback_data_mixin):
    readback_data_mixin.device_manager.devices.samx._info["hints"]["fields"] = [
        "samx_setpoint",
        "samx",
    ]
    readback_data_mixin.device_manager.connector.get.side_effect = [
        messages.DeviceMessage(
            signals={"samx": {"value": 10}, "samx_setpoint": {"value": 20}},
            metadata={"device": "samx"},
        ),
        messages.DeviceMessage(
            signals={"samy": {"value": 10}, "samy_setpoint": {"value": 20}},
            metadata={"device": "samy"},
        ),
    ]
    res = readback_data_mixin.get_device_values()
    assert res == [20, 10]


def test_readback_data_mixin_multiple_no_hints(readback_data_mixin):
    readback_data_mixin.device_manager.devices.samx._info["hints"]["fields"] = []
    readback_data_mixin.device_manager.connector.get.side_effect = [
        messages.DeviceMessage(
            signals={"samx": {"value": 10}, "samx_setpoint": {"value": 20}},
            metadata={"device": "samx"},
        ),
        messages.DeviceMessage(
            signals={"samy": {"value": 10}, "samy_setpoint": {"value": 20}},
            metadata={"device": "samy"},
        ),
    ]
    res = readback_data_mixin.get_device_values()
    assert res == [10, 10]


def test_get_request_done_msgs(readback_data_mixin):
    res = readback_data_mixin.get_request_done_msgs()
    readback_data_mixin.device_manager.connector.pipeline.assert_called_once()
    assert (
        mock.call(
            MessageEndpoints.device_req_status("samx"),
            readback_data_mixin.device_manager.connector.pipeline.return_value,
        )
        in readback_data_mixin.device_manager.connector.get.call_args_list
    )
    assert (
        mock.call(
            MessageEndpoints.device_req_status("samy"),
            readback_data_mixin.device_manager.connector.pipeline.return_value,
        )
        in readback_data_mixin.device_manager.connector.get.call_args_list
    )
