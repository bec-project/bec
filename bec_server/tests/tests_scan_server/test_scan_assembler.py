from unittest import mock

import pytest

from bec_messages import messages
from bec_server.scan_server.scan_assembler import ScanAssembler
from bec_server.scan_server.scans import FermatSpiralScan, LineScan, RequestBase


@pytest.fixture
def scan_assembler():
    return ScanAssembler(parent=mock.MagicMock())


class CustomScan(RequestBase):
    scan_name = "custom_scan"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        pass


class CustomScan2(RequestBase):
    scan_name = "custom_scan2"

    def __init__(self, arg1, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        pass


@pytest.mark.parametrize(
    "msg, request_inputs_expected",
    [
        (
            # Fermat scan with args and kwargs, matching the FermatSpiralScan signature
            messages.ScanQueueMessage(
                scan_type="fermat_scan",
                parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"steps": 3}},
                queue="primary",
            ),
            {
                "arg_bundle": [],
                "inputs": {
                    "motor1": "samx",
                    "start_motor1": -5,
                    "stop_motor1": 5,
                    "motor2": "samy",
                    "start_motor2": -5,
                    "stop_motor2": 5,
                },
                "kwargs": {"steps": 3},
            },
        ),
        (
            # Fermat scan with no args; everything is in kwargs
            messages.ScanQueueMessage(
                scan_type="fermat_scan",
                parameter={
                    "args": [],
                    "kwargs": {
                        "motor1": "samx",
                        "start_motor1": -5,
                        "stop_motor1": 5,
                        "motor2": "samy",
                        "start_motor2": -5,
                        "stop_motor2": 5,
                        "steps": 3,
                    },
                },
                queue="primary",
            ),
            {
                "arg_bundle": [],
                "inputs": {
                    "motor1": "samx",
                    "start_motor1": -5,
                    "stop_motor1": 5,
                    "motor2": "samy",
                    "start_motor2": -5,
                    "stop_motor2": 5,
                },
                "kwargs": {"steps": 3},
            },
        ),
        (
            # Fermat scan with mixed args and kwargs
            messages.ScanQueueMessage(
                scan_type="fermat_scan",
                parameter={
                    "args": ["samx"],
                    "kwargs": {
                        "start_motor1": -5,
                        "stop_motor1": 5,
                        "motor2": "samy",
                        "start_motor2": -5,
                        "stop_motor2": 5,
                        "steps": 3,
                    },
                },
                queue="primary",
            ),
            {
                "arg_bundle": [],
                "inputs": {
                    "motor1": "samx",
                    "start_motor1": -5,
                    "stop_motor1": 5,
                    "motor2": "samy",
                    "start_motor2": -5,
                    "stop_motor2": 5,
                },
                "kwargs": {"steps": 3},
            },
        ),
        (
            # Line scan with arg bundle
            messages.ScanQueueMessage(
                scan_type="line_scan",
                parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"steps": 3}},
                queue="primary",
            ),
            {"arg_bundle": ["samx", -5, 5, "samy", -5, 5], "inputs": {}, "kwargs": {"steps": 3}},
        ),
        (
            # Custom scan with args
            messages.ScanQueueMessage(
                scan_type="custom_scan",
                parameter={"args": ["samx", -5, 5], "kwargs": {}},
                queue="primary",
            ),
            {"arg_bundle": [], "inputs": {"args": ["samx", -5, 5]}, "kwargs": {}},
        ),
        (
            # Custom scan with args
            messages.ScanQueueMessage(
                scan_type="custom_scan2",
                parameter={"args": [True, "samx", -5, 5], "kwargs": {}},
                queue="primary",
            ),
            {"arg_bundle": [], "inputs": {"arg1": True, "args": ["samx", -5, 5]}, "kwargs": {}},
        ),
    ],
)
def test_scan_assembler_request_inputs(msg, request_inputs_expected, scan_assembler):

    class MockScanManager:
        available_scans = {
            "fermat_scan": {"class": "FermatSpiralScan"},
            "line_scan": {"class": "LineScan"},
            "custom_scan": {"class": "CustomScan"},
            "custom_scan2": {"class": "CustomScan2"},
        }
        scan_dict = {
            "FermatSpiralScan": FermatSpiralScan,
            "LineScan": LineScan,
            "CustomScan": CustomScan,
            "CustomScan2": CustomScan2,
        }

    with mock.patch.object(scan_assembler, "scan_manager", MockScanManager()):
        request = scan_assembler.assemble_device_instructions(msg, "scan_id")
        assert request.request_inputs == request_inputs_expected
