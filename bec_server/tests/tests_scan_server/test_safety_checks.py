from typing import DefaultDict
from unittest.mock import MagicMock

import pytest

import bec_server.scan_server.safety_check
from bec_server.scan_server.safety_check import (
    SafetyCheck,
    SafetyCheckFailed,
    run_safety_check,
    safety_check,
)


@pytest.fixture
def add_samx_safety_check():
    @safety_check("samx")
    def check_samx_le_samy(devices, position):
        return position <= devices.samy.get()

    yield
    bec_server.scan_server.safety_check._SAFETY_CHECKS = DefaultDict(set)


def test_check_registered(add_samx_safety_check):
    assert bec_server.scan_server.safety_check._SAFETY_CHECKS.get("samx") is not None


def test_samx_func(add_samx_safety_check):
    mock_devices = MagicMock()
    mock_samx = MagicMock(dotted_name="samx")
    mock_devices.samy.get.return_value = 10
    assert run_safety_check(mock_devices, mock_samx, 5) is None
    with pytest.raises(SafetyCheckFailed) as e:
        assert run_safety_check(mock_devices, mock_samx, 11)
    assert e.match("failed for device samx and position 11")
