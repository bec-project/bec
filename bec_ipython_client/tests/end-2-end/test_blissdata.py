import time

import numpy as np
import pytest
import yaml

from bec_lib.alarm_handler import AlarmBase
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.logger import bec_logger

logger = bec_logger.logger


@pytest.mark.timeout(100)
def test_line_scan(capsys, bec_client_lib):
    bec = bec_client_lib
    scans = bec.scans
    bec.metadata.update({"unit_test": "test_line_scan"})
    dev = bec.device_manager.devices
    status = scans.line_scan(dev.samx, -5, 5, steps=10, exp_time=0.01, relative=True)
    status.wait()
    assert len(status.scan.data) == 10
    assert status.scan.num_points == 10
