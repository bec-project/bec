from typing import Annotated
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.bec_errors import ScanInputValidationError
from bec_lib.device import DeviceBase
from bec_lib.scan_args import ScanArgument
from bec_lib.tests.fixtures import dm_with_devices
from bec_server.scan_server.scan_assembler import ScanAssembler
from bec_server.scan_server.scans import ScanArgType
from bec_server.scan_server.scans.legacy_scans import FermatSpiralScan, LineScan, RequestBase
from bec_server.scan_server.scans.scan_base import ScanBase as ScanBaseV4


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


class CustomFixedDirectScan(ScanBaseV4):
    scan_name = "custom_fixed_direct_scan"
    is_scan = False

    def __init__(self, device: DeviceBase, target: float, **kwargs):
        super().__init__(**kwargs)
        self.device = device
        self.target = target


class CustomBoundedDirectScan(ScanBaseV4):
    scan_name = "custom_bounded_direct_scan"
    is_scan = False

    def __init__(
        self,
        value: Annotated[float, ScanArgument(display_name="Value", gt=0, ge=1, lt=10, le=9)],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.value = value


class CustomBundledBoundedDirectScan(ScanBaseV4):
    scan_name = "custom_bundled_bounded_direct_scan"
    arg_input = {
        "device": ScanArgType.DEVICE,
        "target": Annotated[int, ScanArgument(display_name="Target", ge=1)],
    }
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": None}
    is_scan = False

    def __init__(
        self,
        *args,
        scale: Annotated[float, ScanArgument(display_name="Scale", le=10)] = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.received_args = args
        self.scale = scale


class CustomBundledScanWithDeviceKwarg(ScanBaseV4):
    scan_name = "custom_bundled_scan_with_device_kwarg"
    arg_input = {"device": ScanArgType.DEVICE, "target": ScanArgType.FLOAT}
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": None}
    is_scan = False

    def __init__(self, *args, monitor: DeviceBase, **kwargs):
        super().__init__(**kwargs)
        self.received_args = args
        self.monitor = monitor


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


def test_scan_assembler_assemble_direct_scan_resolves_annotated_device_args(dm_with_devices):
    parent = mock.MagicMock()
    parent.device_manager = dm_with_devices
    parent.connector = mock.MagicMock()
    parent.queue_manager.instruction_handler = mock.MagicMock()
    assembler = ScanAssembler(parent=parent)

    class MockScanManager:
        scan_dict = {"custom_fixed_direct_scan": CustomFixedDirectScan}

    msg = messages.ScanQueueMessage(
        scan_type="custom_fixed_direct_scan",
        parameter={
            "args": ["samx", 1],
            "kwargs": {"system_config": {"file_directory": "/tmp/data"}},
        },
        queue="primary",
    )

    with mock.patch.object(assembler, "scan_manager", MockScanManager()):
        request = assembler.assemble_direct_scan(msg, "scan_id")

    assert request.device is dm_with_devices.devices["samx"]
    assert request.target == 1
    assert request.scan_info.request_inputs["inputs"] == {"device": "samx", "target": 1}


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (0, "greater than"),
        (0.5, "greater than or equal to"),
        (10, "less than"),
        (9.5, "less than or equal to"),
    ],
)
def test_scan_assembler_validates_fixed_direct_scan_input_bounds(dm_with_devices, value, message):
    parent = mock.MagicMock()
    parent.device_manager = dm_with_devices
    parent.connector = mock.MagicMock()
    parent.queue_manager.instruction_handler = mock.MagicMock()
    assembler = ScanAssembler(parent=parent)

    class MockScanManager:
        scan_dict = {"custom_bounded_direct_scan": CustomBoundedDirectScan}

    msg = messages.ScanQueueMessage(
        scan_type="custom_bounded_direct_scan",
        parameter={"args": [value], "kwargs": {"system_config": {"file_directory": "/tmp/data"}}},
        queue="primary",
    )

    with mock.patch.object(assembler, "scan_manager", MockScanManager()):
        with pytest.raises(ScanInputValidationError, match=message):
            assembler.assemble_direct_scan(msg, "scan_id")


def test_scan_assembler_validates_fixed_direct_scan_input_type(dm_with_devices):
    parent = mock.MagicMock()
    parent.device_manager = dm_with_devices
    parent.connector = mock.MagicMock()
    parent.queue_manager.instruction_handler = mock.MagicMock()
    assembler = ScanAssembler(parent=parent)

    class MockScanManager:
        scan_dict = {"custom_fixed_direct_scan": CustomFixedDirectScan}

    msg = messages.ScanQueueMessage(
        scan_type="custom_fixed_direct_scan",
        parameter={
            "args": ["samx", "invalid"],
            "kwargs": {"system_config": {"file_directory": "/tmp/data"}},
        },
        queue="primary",
    )

    with mock.patch.object(assembler, "scan_manager", MockScanManager()):
        with pytest.raises(ScanInputValidationError, match="target': .*float or int") as exc:
            assembler.assemble_direct_scan(msg, "scan_id")

    assert exc.value.error_info is not None
    assert exc.value.error_info.exception_type == "ScanInputValidationError"


def test_scan_assembler_validates_bundled_direct_scan_input_bounds(dm_with_devices):
    parent = mock.MagicMock()
    parent.device_manager = dm_with_devices
    parent.connector = mock.MagicMock()
    parent.queue_manager.instruction_handler = mock.MagicMock()
    assembler = ScanAssembler(parent=parent)

    class MockScanManager:
        scan_dict = {"custom_bundled_bounded_direct_scan": CustomBundledBoundedDirectScan}

    msg = messages.ScanQueueMessage(
        scan_type="custom_bundled_bounded_direct_scan",
        parameter={
            "args": {"samx": (0,)},
            "kwargs": {"system_config": {"file_directory": "/tmp/data"}},
        },
        queue="primary",
    )

    with mock.patch.object(assembler, "scan_manager", MockScanManager()):
        with pytest.raises(ScanInputValidationError, match="target.*greater than or equal to"):
            assembler.assemble_direct_scan(msg, "scan_id")


def test_scan_assembler_validates_bundled_direct_scan_input_type(dm_with_devices):
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
            "args": {"samx": ("invalid",)},
            "kwargs": {"system_config": {"file_directory": "/tmp/data"}},
        },
        queue="primary",
    )

    with mock.patch.object(assembler, "scan_manager", MockScanManager()):
        with pytest.raises(ScanInputValidationError, match="target'.*expected float"):
            assembler.assemble_direct_scan(msg, "scan_id")


def test_scan_assembler_validates_signature_kwargs_for_arg_input_scan(dm_with_devices):
    parent = mock.MagicMock()
    parent.device_manager = dm_with_devices
    parent.connector = mock.MagicMock()
    parent.queue_manager.instruction_handler = mock.MagicMock()
    assembler = ScanAssembler(parent=parent)

    class MockScanManager:
        scan_dict = {"custom_bundled_bounded_direct_scan": CustomBundledBoundedDirectScan}

    msg = messages.ScanQueueMessage(
        scan_type="custom_bundled_bounded_direct_scan",
        parameter={
            "args": {"samx": (1,)},
            "kwargs": {"scale": 11, "system_config": {"file_directory": "/tmp/data"}},
        },
        queue="primary",
    )

    with mock.patch.object(assembler, "scan_manager", MockScanManager()):
        with pytest.raises(ScanInputValidationError, match="scale.*less than or equal to"):
            assembler.assemble_direct_scan(msg, "scan_id")


def test_scan_assembler_validates_signature_kwargs_type_for_arg_input_scan(dm_with_devices):
    parent = mock.MagicMock()
    parent.device_manager = dm_with_devices
    parent.connector = mock.MagicMock()
    parent.queue_manager.instruction_handler = mock.MagicMock()
    assembler = ScanAssembler(parent=parent)

    class MockScanManager:
        scan_dict = {"custom_bundled_bounded_direct_scan": CustomBundledBoundedDirectScan}

    msg = messages.ScanQueueMessage(
        scan_type="custom_bundled_bounded_direct_scan",
        parameter={
            "args": {"samx": (1,)},
            "kwargs": {"scale": "invalid", "system_config": {"file_directory": "/tmp/data"}},
        },
        queue="primary",
    )

    with mock.patch.object(assembler, "scan_manager", MockScanManager()):
        with pytest.raises(ScanInputValidationError, match="scale': .*float or int"):
            assembler.assemble_direct_scan(msg, "scan_id")


def test_scan_assembler_resolves_signature_device_kwargs_for_arg_input_scan(dm_with_devices):
    parent = mock.MagicMock()
    parent.device_manager = dm_with_devices
    parent.connector = mock.MagicMock()
    parent.queue_manager.instruction_handler = mock.MagicMock()
    assembler = ScanAssembler(parent=parent)

    class MockScanManager:
        scan_dict = {"custom_bundled_scan_with_device_kwarg": CustomBundledScanWithDeviceKwarg}

    msg = messages.ScanQueueMessage(
        scan_type="custom_bundled_scan_with_device_kwarg",
        parameter={
            "args": {"samx": (1,)},
            "kwargs": {"monitor": "samy", "system_config": {"file_directory": "/tmp/data"}},
        },
        queue="primary",
    )

    with mock.patch.object(assembler, "scan_manager", MockScanManager()):
        request = assembler.assemble_direct_scan(msg, "scan_id")

    assert request.received_args == (dm_with_devices.devices["samx"], 1)
    assert request.monitor is dm_with_devices.devices["samy"]
