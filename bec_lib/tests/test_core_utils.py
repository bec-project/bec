import csv
import os
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.live_scan_data import LiveScanData
from bec_lib.scan_items import ScanItem
from bec_lib.scan_report import ScanReport
from bec_lib.utils.rpc_utils import user_access
from bec_lib.utils.scan_utils import _extract_scan_data, _write_csv, scan_to_csv, scan_to_dict


class ScanReportMock(ScanReport):
    def __init__(self, scan_id: str) -> None:
        super().__init__()
        self.request = mock.MagicMock()
        self.request.scan.scan_id = scan_id


@pytest.fixture
def scan_data():
    scan_data = LiveScanData()
    for ii in range(10):
        msg = messages.ScanMessage(
            point_id=ii,
            scan_id="scan_id",
            data={
                "samx": {
                    "setpoint": {"value": ii, "timestamp": ii},
                    "samx": {"value": ii, "timestamp": ii},
                }
            },
        )
        scan_data.set(ii, msg)
    yield scan_data


@pytest.fixture
def scanitem():
    scanitem = ScanItem(
        queue_id="queue_id",
        scan_id="scan_id",
        scan_number=1,
        status="closed",
        scan_manager=mock.MagicMock(),
    )
    scanitem.status_message = mock.MagicMock()
    yield scanitem


class class_mock:
    USER_ACCESS = []

    @user_access
    def _func_decorated_not_in_user_access(self, *args, **kwargs):
        return None

    @user_access
    def _func_decorated_in_user_access(self, *args, **kwargs):
        return None

    def _func_not_decorated_not_in_user_access(self, *args, **kwargs):
        return None


@pytest.fixture(scope="class")
def class_factory():
    yield class_mock()


def test_user_access(class_factory):
    """Test user_access function."""
    assert class_factory.USER_ACCESS == [
        "_func_decorated_not_in_user_access",
        "_func_decorated_in_user_access",
    ]


def test__write_csv():
    """Test _write_csv function."""

    output = [["#samx", "bpm4i"], ["2.056", "100.1234"], ["0.0", "-0.12345"]]

    _write_csv(output_name="test.csv", delimiter=",", dialect=None, output=output)
    with open("test.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile)
        for row, row_value in zip(csvreader, output):
            assert row == row_value

    os.remove("test.csv")


def test_scan_to_dict(scanitem, scan_data):
    """Test scan_to_dict function."""
    scanitem.live_data = scan_data
    return_dict = scan_to_dict(scanitem, flat=True)
    assert return_dict.keys() == {"timestamp", "value"}
    assert return_dict["value"].keys() == {"samx", "setpoint"}
    assert len(return_dict["value"]["samx"]) == 10


def test_extract_scan_data(scanitem, scan_data):
    """Test _extract_scan_data function"""
    datasetnumber = 5
    scanitem.start_time = 1620000000
    scanitem.end_time = 1620000000 + 3.5
    scanitem.status_message.return_value = messages.ScanStatusMessage(
        scan_id=scanitem.scan_id,
        status=scanitem.status,
        info={"scan_number": scanitem.scan_number, "dataset_number": datasetnumber},
    )
    scanitem.live_data = scan_data
    header, body = _extract_scan_data(scanitem, header=None, write_metadata=True)
    assert len(header) == 7
    assert len(body) == 10


def test_scan_to_csv():
    """Test scan_to_csv function."""
    scanreport_mock = mock.MagicMock(spec=ScanReport)
    with pytest.raises(Exception):
        scan_to_csv(
            scan_report=scanreport_mock,
            output_name=1234,
            delimiter=",",
            dialect=None,
            header=None,
            write_metadata=True,
        )
    with pytest.raises(Exception):
        scan_to_csv(
            scan_report=[scanreport_mock, scanreport_mock, scanreport_mock],
            output_name="test.csv",
            delimiter=",",
            dialect=None,
            header=None,
            write_metadata=True,
        )
    with pytest.raises(Exception):
        scan_to_csv(
            scan_report=[scanreport_mock, scanreport_mock, scanreport_mock],
            output_name="test.csv",
            delimiter=123,
            dialect=None,
            header=None,
            write_metadata=True,
        )
