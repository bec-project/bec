from unittest import mock

import pytest

from bec_lib.device import Device, DeviceBase, Positioner
from bec_lib.messages import ScanArgType
from bec_server.scan_server.scan_manager import ScanManager


@pytest.fixture
def scan_manager():
    parent = mock.MagicMock()
    yield ScanManager(parent=parent)


@pytest.mark.parametrize(
    "arg_input, arg_output",
    [
        ({"a": float}, {"a": "float"}),
        ({"a": ScanArgType.FLOAT}, {"a": "float"}),
        ({"a": ScanArgType.DEVICE}, {"a": "DeviceBase"}),
        ({"a": ScanArgType.INT}, {"a": "int"}),
        ({"a": ScanArgType.BOOL}, {"a": "bool"}),
        ({"a": ScanArgType.LIST}, {"a": "list"}),
        ({"a": ScanArgType.DICT}, {"a": "dict"}),
        ({"a": str}, {"a": "str"}),
        ({"a": int}, {"a": "int"}),
        ({"a": bool}, {"a": "bool"}),
        ({"a": list}, {"a": "list"}),
        ({"a": dict}, {"a": "dict"}),
        ({"a": DeviceBase}, {"a": "DeviceBase"}),
        ({"a": Device}, {"a": "DeviceBase"}),
        ({"a": Positioner}, {"a": "DeviceBase"}),
        ({"a": DeviceBase | str}, {"a": ["DeviceBase", "str"]}),
    ],
)
def test_scan_manager_convert_arg_input(scan_manager, arg_input, arg_output):
    assert scan_manager.convert_arg_input(arg_input) == arg_output


def test_scan_manager_convert_arg_input_does_not_mutate(scan_manager):
    arg_input = {"a": ScanArgType.FLOAT}

    scan_manager.convert_arg_input(arg_input)

    assert arg_input == {"a": ScanArgType.FLOAT}
