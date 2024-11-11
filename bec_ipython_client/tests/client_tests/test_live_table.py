# pylint: disable=missing-function-docstring
import threading
import time
from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

import numpy as np
import pytest

from bec_ipython_client.callbacks.live_table import LiveUpdatesTable, sort_devices
from bec_ipython_client.callbacks.utils import ScanRequestMixin
from bec_lib import messages


@pytest.fixture
def client_with_grid_scan(bec_client_mock):
    client = bec_client_mock
    request_msg = messages.ScanQueueMessage(
        scan_type="grid_scan",
        parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )
    yield client, request_msg


@pytest.mark.timeout(20)
def test_scan_request_mixin(client_with_grid_scan):
    client, request_msg = client_with_grid_scan
    response_msg = messages.RequestResponseMessage(
        accepted=True, message={"msg": ""}, metadata={"RID": "something"}
    )
    request_mixin = ScanRequestMixin(client, "something")

    def update_with_response(request_msg):
        time.sleep(1)
        client.queue.request_storage.update_with_response(response_msg)

    client.queue.request_storage.update_with_request(request_msg)
    update_thread = threading.Thread(target=update_with_response, args=(response_msg,))
    update_thread.start()
    with mock.patch.object(client.queue.queue_storage, "find_queue_item_by_requestID"):
        request_mixin.wait()
    update_thread.join()


def test_sort_devices():
    devices = sort_devices(["samx", "bpm4i", "samy", "bpm4s"], ["samx", "samy"])
    assert devices == ["samx", "samy", "bpm4i", "bpm4s"]


@pytest.mark.parametrize(
    "request_msg,scan_report_devices",
    [
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "something"},
            ),
            ["samx"],
        ),
        (
            messages.ScanQueueMessage(
                scan_type="round_scan",
                parameter={"args": {"samx": ["samy", 0, 25, 5, 3]}},
                queue="primary",
                metadata={"RID": "something"},
            ),
            ["samx", "samy"],
        ),
    ],
)
def test_get_devices_from_scan_data(bec_client_mock, request_msg, scan_report_devices):
    client = bec_client_mock
    client.start()
    data = messages.ScanMessage(
        point_id=0, scan_id="", data={}, metadata={"scan_report_devices": scan_report_devices}
    )
    live_update = LiveUpdatesTable(client, {"scan_progress": 10}, request_msg)
    devices = live_update.get_devices_from_scan_data(data)
    assert devices[0 : len(scan_report_devices)] == scan_report_devices


@pytest.mark.timeout(20)
def test_wait_for_request_acceptance(client_with_grid_scan):
    client, request_msg = client_with_grid_scan
    response_msg = messages.RequestResponseMessage(
        accepted=True, message={"msg": ""}, metadata={"RID": "something"}
    )
    client.queue.request_storage.update_with_request(request_msg)
    client.queue.request_storage.update_with_response(response_msg)
    live_update = LiveUpdatesTable(client, {"scan_progress": 10}, request_msg)
    with mock.patch.object(client.queue.queue_storage, "find_queue_item_by_requestID"):
        live_update.wait_for_request_acceptance()


class ScanItemMock:
    def __init__(self, live_data):
        self.live_data = live_data
        self.metadata = {}


def test_print_table_data(client_with_grid_scan):
    client, request_msg = client_with_grid_scan
    response_msg = messages.RequestResponseMessage(
        accepted=True, message={"msg": ""}, metadata={"RID": "something"}
    )
    client.queue.request_storage.update_with_request(request_msg)
    client.queue.request_storage.update_with_response(response_msg)
    live_update = LiveUpdatesTable(client, {"scan_progress": 10}, request_msg)
    live_update.point_data = messages.ScanMessage(
        point_id=0,
        scan_id="",
        data={"samx": {"samx": {"value": 0}}},
        metadata={"scan_report_devices": ["samx"], "scan_type": "step"},
    )
    live_update.scan_item = ScanItemMock(live_data=[live_update.point_data])
    with mock.patch.object(live_update, "_print_client_msgs_asap") as mock_client_msgs:
        live_update.print_table_data()
        assert mock_client_msgs.called


def test_print_table_data_lamni_flyer(client_with_grid_scan):
    client, request_msg = client_with_grid_scan
    response_msg = messages.RequestResponseMessage(
        accepted=True, message={"msg": ""}, metadata={"RID": "something"}
    )
    client.queue.request_storage.update_with_request(request_msg)
    client.queue.request_storage.update_with_response(response_msg)
    live_update = LiveUpdatesTable(client, {"scan_progress": 10}, request_msg)
    live_update.point_data = messages.ScanMessage(
        point_id=0,
        scan_id="",
        data={"lamni_flyer_1": {"value": 0}},
        metadata={"scan_report_devices": ["samx"], "scan_type": "fly"},
    )
    live_update.scan_item = ScanItemMock(live_data=[live_update.point_data])
    with mock.patch.object(live_update, "_print_client_msgs_asap") as mock_client_msgs:
        live_update.print_table_data()
        assert mock_client_msgs.called


def test_print_table_data_hinted_value(client_with_grid_scan):
    client, request_msg = client_with_grid_scan
    response_msg = messages.RequestResponseMessage(
        accepted=True, message={"msg": ""}, metadata={"RID": "something"}
    )
    client.queue.request_storage.update_with_request(request_msg)
    client.queue.request_storage.update_with_response(response_msg)
    live_update = LiveUpdatesTable(client, {"scan_progress": 10}, request_msg)
    client.device_manager.devices["samx"]._info["hints"] = {"fields": ["samx_hint"]}
    client.device_manager.devices["samx"].precision = 3
    live_update.point_data = messages.ScanMessage(
        point_id=0,
        scan_id="",
        data={"samx": {"samx_hint": {"value": 0}}},
        metadata={"scan_report_devices": ["samx"], "scan_type": "fly"},
    )
    live_update.scan_item = ScanItemMock(live_data=[live_update.point_data])

    with (
        mock.patch.object(live_update, "table") as mocked_table,
        mock.patch.object(live_update, "_print_client_msgs_asap") as mock_client_msgs,
    ):
        live_update.dev_values = (len(live_update._get_header()) - 1) * [0]
        live_update.print_table_data()
        mocked_table.get_row.assert_called_with("0", "0.000")
        assert mock_client_msgs.called


def test_print_table_data_hinted_value_with_precision(client_with_grid_scan):
    client, request_msg = client_with_grid_scan
    response_msg = messages.RequestResponseMessage(
        accepted=True, message={"msg": ""}, metadata={"RID": "something"}
    )
    client.queue.request_storage.update_with_request(request_msg)
    client.queue.request_storage.update_with_response(response_msg)
    live_update = LiveUpdatesTable(client, {"scan_progress": 10}, request_msg)
    client.device_manager.devices["samx"]._info["hints"] = {"fields": ["samx_hint"]}
    client.device_manager.devices["samx"].precision = 2
    live_update.point_data = messages.ScanMessage(
        point_id=0,
        scan_id="",
        data={"samx": {"samx_hint": {"value": 0}}},
        metadata={"scan_report_devices": ["samx"], "scan_type": "fly"},
    )
    live_update.scan_item = ScanItemMock(live_data=[live_update.point_data])

    with (
        mock.patch.object(live_update, "table") as mocked_table,
        mock.patch.object(live_update, "_print_client_msgs_asap") as mock_client_msgs,
    ):
        live_update.dev_values = (len(live_update._get_header()) - 1) * [0]
        live_update.print_table_data()
        mocked_table.get_row.assert_called_with("0", f"{0:.2f}")


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, "0.00"),
        (1, "1.00"),
        (0.000, "0.00"),
        (True, "1.00"),
        (False, "0.00"),
        ("True", "True"),
        ("False", "False"),
        ("0", "0"),
        ("1", "1"),
        ((0, 1), "(0, 1)"),
        ({"value": 0}, "{'value': 0}"),
        (np.array([0, 1]), "[0 1]"),
        ({1, 2}, "{1, 2}"),
    ],
)
def test_print_table_data_variants(client_with_grid_scan, value, expected):
    client, request_msg = client_with_grid_scan
    response_msg = messages.RequestResponseMessage(
        accepted=True, message={"msg": ""}, metadata={"RID": "something"}
    )
    client.queue.request_storage.update_with_request(request_msg)
    client.queue.request_storage.update_with_response(response_msg)
    live_update = LiveUpdatesTable(client, {"scan_progress": 10}, request_msg)
    live_update.point_data = messages.ScanMessage(
        point_id=0,
        scan_id="",
        data={"lamni_flyer_1": {"value": value}},
        metadata={"scan_report_devices": ["samx"], "scan_type": "fly"},
    )
    live_update.scan_item = ScanItemMock(live_data=[live_update.point_data])

    with mock.patch.object(live_update, "_print_client_msgs_asap") as mock_client_msgs:
        live_update.print_table_data()
        with mock.patch.object(live_update, "table") as mocked_table:
            live_update.dev_values = (len(live_update._get_header()) - 1) * [value]
            live_update.print_table_data()
            mocked_table.get_row.assert_called_with("0", expected)


def test_print_client_msgs(client_with_grid_scan):
    client, request_msg = client_with_grid_scan
    response_msg = messages.RequestResponseMessage(
        accepted=True, message={"msg": ""}, metadata={"RID": "something"}
    )
    client.queue.request_storage.update_with_request(request_msg)
    client.queue.request_storage.update_with_response(response_msg)
    client_msg = messages.ClientInfoMessage(
        message="message", RID="something", show_asap=True, source="scan_server"
    )
    client.queue.request_storage.update_with_client_message(client_msg)
    with mock.patch.object(
        client.queue.request_storage.scan_manager.queue_storage, "find_queue_item_by_requestID"
    ):
        live_update = LiveUpdatesTable(client, {"scan_progress": 10}, request_msg)
        live_update.wait_for_request_acceptance()
        result = StringIO()
        with redirect_stdout(result):
            live_update._print_client_msgs_asap()
            rtr1 = "Client info (scan_server) : message" + "\n"
            assert result.getvalue() == rtr1
            # second time should not add anything
            live_update._print_client_msgs_asap()
            assert result.getvalue() == rtr1
            # live_update._print_client_msgs_all()
            # rtr2 = (
            #     "------------------------"
            #     + "\n"
            #     + "Summary of client messages"
            #     + "\n"
            #     + "------------------------"
            #     + "\n"
            #     + "Client info (scan_server) : message"
            #     + "\n"
            #     + "------------------------"
            #     + "\n"
            # )
            # assert result.getvalue() == rtr1 + rtr2
