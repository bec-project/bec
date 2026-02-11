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
        ({"a": ScanArgType.DEVICE}, {"a": "device"}),
        ({"a": ScanArgType.INT}, {"a": "int"}),
        ({"a": ScanArgType.BOOL}, {"a": "boolean"}),
        ({"a": ScanArgType.LIST}, {"a": "list"}),
        ({"a": ScanArgType.DICT}, {"a": "dict"}),
        ({"a": str}, {"a": "str"}),
        ({"a": int}, {"a": "int"}),
        ({"a": bool}, {"a": "boolean"}),
        ({"a": list}, {"a": "list"}),
        ({"a": dict}, {"a": "dict"}),
        ({"a": DeviceBase}, {"a": "device"}),
        ({"a": Device}, {"a": "device"}),
        ({"a": Positioner}, {"a": "device"}),
    ],
)
def test_scan_manager_convert_arg_input(scan_manager, arg_input, arg_output):
    assert scan_manager.convert_arg_input(arg_input) == arg_output
