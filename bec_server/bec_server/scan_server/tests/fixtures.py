from functools import partial
from unittest import mock

import pytest

from bec_lib.logger import bec_logger
from bec_lib.tests.fixtures import dm_with_devices
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.tests.utils import DeviceMockType, DMMock
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.tests.utils import ScanServerMock


# pylint: disable=missing-function-docstring
# pylint: disable=protected-access
@pytest.fixture
def scan_server_mock(dm_with_devices):
    server = ScanServerMock(dm_with_devices)
    yield server
    bec_logger.logger.remove()


@pytest.fixture
def connector_mock():
    connector = ConnectorMock("")
    yield connector


@pytest.fixture
def device_manager_mock():
    device_manager = DMMock()
    device_manager.add_device("rtx")
    device_manager.add_device("samx")
    device_manager.add_device("samy")
    device_manager.add_device("samz")
    device_manager.add_device(
        "eiger", dev_type=DeviceMockType.SIGNAL, readout_priority="monitored", software_trigger=True
    )
    device_manager.add_device("bpm4i", dev_type=DeviceMockType.SIGNAL, readout_priority="monitored")
    yield device_manager


@pytest.fixture
def instruction_handler_mock(connector_mock):
    instruction_handler = InstructionHandler(connector_mock)
    with mock.patch("bec_server.scan_server.scan_stubs.ScanStubStatus.wait", return_value=None):
        yield instruction_handler


class _ScanStubStatusMock:
    def __init__(self, done_func) -> None:
        self._done = done_func()

    @property
    def done(self):
        return next(self._done)

    def wait(self):
        return


@pytest.fixture
def ScanStubStatusMock():
    return _ScanStubStatusMock


@pytest.fixture
def scan_assembler(instruction_handler_mock, device_manager_mock):
    def _assemble_scan(scan_class, *args, **kwargs):
        return scan_class(*args, **kwargs)

    return partial(
        _assemble_scan,
        instruction_handler=instruction_handler_mock,
        device_manager=device_manager_mock,
    )
