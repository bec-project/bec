from typing import Annotated
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.device import Device, DeviceBase, Positioner
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
from bec_lib.scan_args import ScanArgument
from bec_server.scan_server.scan_manager import ScanManager
from bec_server.scan_server.scans import ScanArgType
from bec_server.scan_server.tests.utils import NoopScan


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
        ({"a": Annotated[float, "device"]}, {"a": "float"}),
        ({"a": list[float]}, {"a": {"Generic": {"origin": "list", "args": ["float"]}}}),
    ],
)
def test_scan_manager_convert_arg_input(scan_manager, arg_input, arg_output):
    assert scan_manager.convert_arg_input(arg_input) == arg_output


def test_scan_manager_convert_arg_input_does_not_mutate(scan_manager):
    arg_input = {"a": ScanArgType.FLOAT}

    scan_manager.convert_arg_input(arg_input)

    assert arg_input == {"a": ScanArgType.FLOAT}


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


class _GuiConfigModifier:
    @staticmethod
    def scan_argument_overrides(scan_name, arguments, defaults):
        arguments["integ_time"] = Annotated[float, ScanArgument(description="integration time")]
        arguments["frames_per_trigger"] = Annotated[
            int, ScanArgument(description="frames per trigger")
        ]
        defaults["integ_time"] = 0.5
        defaults["frames_per_trigger"] = 1
        return arguments, defaults

    @staticmethod
    def gui_config_overrides(scan_name, gui_config):
        gui_config["Timing"].append("integ_time")
        gui_config["Advanced"] = ["frames_per_trigger"]
        return gui_config


def test_scan_manager_does_not_apply_gui_config_overrides_to_legacy_gui_config(scan_manager):
    with (
        mock.patch.object(
            ScanManager, "get_available_scans", return_value=[("gui", _GuiConfigScan)]
        ),
        mock.patch(
            "bec_server.scan_server.scans.scan_argument_modifier.get_scan_modifier",
            return_value=_GuiConfigModifier,
        ),
    ):
        scan_manager.available_scans = {}
        scan_manager.scan_dict = {}
        scan_manager.update_available_scans()

    gui_config = scan_manager.available_scans[_GuiConfigScan.scan_name]["gui_config"]
    groups = {
        group["name"]: [entry["name"] for entry in group["inputs"]]
        for group in gui_config["kwarg_groups"]
    }
    assert groups["Timing"] == ["exp_time"]
    assert "Advanced" not in groups


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
