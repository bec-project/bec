from typing import Annotated
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.device import Device, DeviceBase, Positioner
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
from bec_server.scan_server.scan_manager import ScanManager
from bec_server.scan_server.tests.utils import NoopScan


@pytest.fixture
def scan_manager():
    parent = mock.MagicMock()
    yield ScanManager(parent=parent)


@pytest.mark.parametrize(
    "arg_input, arg_output",
    [
        ({"a": float}, {"a": "float"}),
        ({"a": str}, {"a": "str"}),
        ({"a": int}, {"a": "int"}),
        ({"a": bool}, {"a": "bool"}),
        ({"a": list}, {"a": "list"}),
        ({"a": dict}, {"a": "dict"}),
        ({"a": DeviceBase}, {"a": "DeviceBase"}),
        ({"a": Device}, {"a": "DeviceBase"}),
        ({"a": Positioner}, {"a": "DeviceBase"}),
        ({"a": DeviceBase | str}, {"a": ["DeviceBase", "str"]}),
        ({"a": Annotated[float, "device"]}, {"a": "float"}),
        ({"a": list[float]}, {"a": {"Generic": {"origin": "list", "args": ["float"]}}}),
    ],
)
def test_scan_manager_convert_arg_input(scan_manager, arg_input, arg_output):
    assert scan_manager.convert_arg_input(arg_input) == arg_output


class _GuiConfigScan(NoopScan):
    scan_name = "gui_config_scan"
    arg_input = {}
    arg_bundle_size = {"bundle": 0, "min": None, "max": None}
    gui_config = {"Timing": ["exp_time"]}

    def __init__(self, *, exp_time: float = 0.1, **kwargs):
        """
        Dummy scan used for GUI config override tests.

        Args:
            exp_time (float): exposure time

        Returns:
            ScanReport
        """
        super().__init__(**kwargs)


def test_scan_manager_update_available_scans_resets_existing_entries(scan_manager):
    with mock.patch.object(
        ScanManager, "get_available_scans", return_value=[("gui", _GuiConfigScan)]
    ):
        scan_manager.update_available_scans()
        first_result = scan_manager.available_scans.copy()

        scan_manager.update_available_scans()

    assert scan_manager.available_scans == first_result
    assert scan_manager.scan_dict == {_GuiConfigScan.scan_name: _GuiConfigScan}


def test_scan_manager_update_available_scans_reload_forces_discovery_refresh(scan_manager):
    with (
        mock.patch.object(ScanManager, "_reload_scan_discovery") as reload_scan_discovery,
        mock.patch.object(ScanManager, "get_available_scans", return_value=[]),
    ):
        scan_manager.update_available_scans(reload=True)

    reload_scan_discovery.assert_called_once_with()


def test_scan_manager_handle_reload_scans_request_forces_reload(scan_manager):
    msg = messages.ServiceRequestMessage(action="reload_scans")
    msg_obj = MessageObject(topic="test", value=msg)

    with (
        mock.patch.object(scan_manager, "update_available_scans") as update_available_scans,
        mock.patch.object(scan_manager, "publish_available_scans") as publish_available_scans,
    ):
        scan_manager.handle_reload_scans_request(msg_obj)

    update_available_scans.assert_called_once_with(reload=True)
    publish_available_scans.assert_called_once_with()


def test_scan_manager_publish_available_scans_uses_set_and_publish(scan_manager):
    scan_manager.parent.connector.set_and_publish.reset_mock()
    scan_manager.available_scans = {"test_scan": {"doc": "test"}}

    scan_manager.publish_available_scans()

    scan_manager.parent.connector.set_and_publish.assert_called_once()
    endpoint, message = scan_manager.parent.connector.set_and_publish.call_args.args
    assert endpoint == MessageEndpoints.available_scans()
    assert message.resource == scan_manager.available_scans


def test_scan_manager_reload_scan_discovery_reloads_plugin_scan_modules():
    with (
        mock.patch(
            "bec_server.scan_server.scan_manager.plugin_helper.reload_plugin_modules"
        ) as reload_plugin_modules,
        mock.patch(
            "bec_server.scan_server.scan_manager.get_scan_modifier.cache_clear"
        ) as clear_modifier_cache,
    ):
        ScanManager._reload_scan_discovery()

    clear_modifier_cache.assert_called_once_with()
    reload_plugin_modules.assert_called_once_with()
