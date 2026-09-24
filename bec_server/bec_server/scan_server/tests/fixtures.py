import pytest

from bec_lib.logger import bec_logger
from bec_lib.tests.fixtures import dm_with_devices
from bec_server.scan_server.tests.utils import ScanServerMock


# pylint: disable=missing-function-docstring
# pylint: disable=protected-access
@pytest.fixture
def scan_server_mock(dm_with_devices):
    server = ScanServerMock(dm_with_devices)
    yield server
    server.shutdown()
    bec_logger.logger.remove()
