import os
from unittest import mock

import pytest
import yaml

import bec_lib
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.logger import bec_logger
from bec_messages.messages import BECStatus
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock
from bec_server.scan_bundler import ScanBundler

# overwrite threads_check fixture from bec_lib,
# to have it in autouse


@pytest.fixture(autouse=True)
def threads_check(threads_check):
    yield
    bec_logger.logger.remove()


class ScanBundlerMock(ScanBundler):
    def __init__(self, device_manager, connector_cls) -> None:
        super().__init__(
            ServiceConfig(redis={"host": "dummy", "port": 6379}), connector_cls=ConnectorMock
        )
        self.device_manager = device_manager

    def _start_device_manager(self):
        pass

    def _start_metrics_emitter(self):
        pass

    def _start_update_service_info(self):
        pass

    def wait_for_service(self, name, status=BECStatus.RUNNING):
        pass


dir_path = os.path.dirname(bec_lib.__file__)


@pytest.fixture
def scan_bundler_mock(dm_with_devices):
    device_manager = dm_with_devices
    scan_bundler_mock = ScanBundlerMock(device_manager, device_manager.connector)
    yield scan_bundler_mock
    scan_bundler_mock.shutdown()
