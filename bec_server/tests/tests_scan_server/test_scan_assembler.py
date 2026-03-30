from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.tests.fixtures import dm_with_devices
from bec_server.scan_server.legacy_scans import FermatSpiralScan, LineScan, RequestBase, ScanArgType
from bec_server.scan_server.scan_assembler import ScanAssembler
from bec_server.scan_server.scans import ScanBase as ScanBaseV4


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


class CustomDirectScan(ScanBaseV4):
    scan_name = "custom_direct_scan"
    arg_input = {"device": ScanArgType.DEVICE, "target": ScanArgType.FLOAT}
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": None}
    is_scan = False

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.received_args = args


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
            "fermat_scan": FermatSpiralScan,
            "line_scan": LineScan,
            "custom_scan": CustomScan,
            "custom_scan2": CustomScan2,
        }

    with mock.patch.object(scan_assembler, "scan_manager", MockScanManager()):
        request = scan_assembler.assemble_device_instructions(msg, "scan_id")
        assert request.request_inputs == request_inputs_expected


def test_scan_assembler_assemble_direct_scan_resolves_device_args(dm_with_devices):
    parent = mock.MagicMock()
    parent.device_manager = dm_with_devices
    parent.connector = mock.MagicMock()
    parent.queue_manager.instruction_handler = mock.MagicMock()
    assembler = ScanAssembler(parent=parent)

    class MockScanManager:
        scan_dict = {"custom_direct_scan": CustomDirectScan}

    msg = messages.ScanQueueMessage(
        scan_type="custom_direct_scan",
        parameter={
            "args": {"samx": (1,), "samy": (2,)},
            "kwargs": {"system_config": {"file_directory": "/tmp/data"}},
        },
        queue="primary",
    )

    with mock.patch.object(assembler, "scan_manager", MockScanManager()):
        request = assembler.assemble_direct_scan(msg, "scan_id")

    assert request.received_args == (
        dm_with_devices.devices["samx"],
        1,
        dm_with_devices.devices["samy"],
        2,
    )
    assert request.scan_info.request_inputs["arg_bundle"] == ["samx", 1, "samy", 2]
