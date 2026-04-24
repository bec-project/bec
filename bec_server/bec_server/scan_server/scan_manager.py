"""
Scan Manager loads the available scans and publishes them to redis.
"""

from __future__ import annotations

import functools
import importlib
import inspect
import pkgutil
from typing import TYPE_CHECKING, Type

from bec_lib import plugin_helper
from bec_lib.alarm_handler import Alarms
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import AvailableResourceMessage, ErrorInfo
from bec_lib.signature_serializer import serialize_dtype, signature_to_dict
from bec_server.scan_server.scan_gui_models import GUIConfig

from . import scans as scans_v4_module
from .scans import legacy_scans as scans_module
from .scans.scan_base import ScanBase as ScanBaseV4

if TYPE_CHECKING:
    from bec_server.scan_server.scan_server import ScanServer

logger = bec_logger.logger

_SCAN_ARG_TYPE_TO_DTYPE = {
    scans_module.ScanArgType.DEVICE: DeviceBase,
    scans_module.ScanArgType.FLOAT: float,
    scans_module.ScanArgType.INT: int,
    scans_module.ScanArgType.BOOL: bool,
    scans_module.ScanArgType.STR: str,
    scans_module.ScanArgType.LIST: list,
    scans_module.ScanArgType.DICT: dict,
}


class ScanManager:
    """
    Scan Manager loads the available scans and publishes them to redis.
    """

    def __init__(self, *, parent: ScanServer):
        """
        Scan Manager loads and manages the available scans.
        """
        self.parent = parent
        self.available_scans = {}
        self.scan_dict: dict[str, type[scans_module.RequestBase] | type[ScanBaseV4]] = {}
        self._plugins = {}
        self.update_available_scans()
        self.publish_available_scans()

    @functools.lru_cache(maxsize=1)
    @staticmethod
    def get_available_scans() -> list[tuple[str, Type]]:
        """
        Get all available scans, including legacy scans, v4 scans and plugin scans.

        Returns:
            list[tuple[str, Type]]: list of scan name and scan class tuples
        """
        # internal, legacy scans
        members: list[tuple[str, Type]] = inspect.getmembers(
            scans_module, predicate=inspect.isclass
        )

        # internal, v4 scans
        members.extend(ScanManager._get_v4_scan_members())

        # plugin scans
        members.extend((name, cls) for name, cls in ScanManager._get_scan_plugins().items())

        to_remove = []
        for name, scan_cls in members:
            is_scan = issubclass(scan_cls, (scans_module.RequestBase, ScanBaseV4))
            if not is_scan or not scan_cls.scan_name:
                logger.debug(f"Ignoring {name}")
                to_remove.append((name, scan_cls))
        for item in to_remove:
            members.remove(item)
        return members

    def update_available_scans(self):
        """load all scans and plugin scans"""
        members = ScanManager.get_available_scans()

        for name, scan_cls in members:

            if not scan_cls.scan_name.isidentifier():
                logger.error(
                    f"Invalid scan_name '{scan_cls.scan_name}' for scan class {name}. scan_name must be a valid Python identifier, that is, it can only contain letters, numbers, and underscores, and must not start with a number. Skipping."
                )
                self.parent.connector.raise_alarm(
                    severity=Alarms.WARNING,
                    info=ErrorInfo(
                        error_message=f"Invalid scan_name '{scan_cls.scan_name}' for scan class {name}. scan_name must be a valid Python identifier, that is, it can only contain letters, numbers, and underscores, and must not start with a number. Skipping.",
                        compact_error_message=f"Invalid scan_name '{scan_cls.scan_name}' for scan class {name}.",
                        exception_type="InvalidScanName",
                        device=None,
                    ),
                )
                continue

            if scan_cls.scan_name in self.available_scans:
                logger.error(f"{scan_cls.scan_name} already exists. Skipping.")
                self.parent.connector.raise_alarm(
                    severity=Alarms.WARNING,
                    info=ErrorInfo(
                        error_message=f"Scan name '{scan_cls.scan_name}' for scan class {name} already exists. Skipping.",
                        compact_error_message=f"Scan name '{scan_cls.scan_name}' for scan class {name} already exists.",
                        exception_type="DuplicateScanName",
                        device=None,
                    ),
                )
                continue

            report_classes = [
                scans_module.ScanBase,
                scans_module.AsyncFlyScanBase,
                scans_module.SyncFlyScanBase,
                scans_module.ScanStubs,
                scans_module.ScanComponent,
            ]
            base_cls = scans_module.RequestBase.__name__
            for report_cls in report_classes:
                if issubclass(scan_cls, report_cls):
                    base_cls = report_cls.__name__
            if issubclass(scan_cls, ScanBaseV4):
                base_cls = "ScanBaseV4"

            self.scan_dict[scan_cls.scan_name] = scan_cls
            gui_config = self.validate_gui_config(scan_cls)
            gui_visibility = {}
            if hasattr(scan_cls, "gui_visibility"):
                gui_visibility = scan_cls.gui_visibility  # type: ignore
            elif hasattr(scan_cls, "gui_config"):  # type: ignore
                gui_visibility = scan_cls.gui_config  # type: ignore

            self.available_scans[scan_cls.scan_name] = {
                "class": scan_cls.__name__,
                "base_class": base_cls,
                "arg_input": self.convert_arg_input(scan_cls.arg_input),
                "required_kwargs": getattr(scan_cls, "required_kwargs", []),
                "arg_bundle_size": scan_cls.arg_bundle_size,
                "doc": scan_cls.__doc__ or scan_cls.__init__.__doc__,
                "signature": signature_to_dict(scan_cls.__init__),
                "gui_visibility": gui_visibility,
                "gui_config": gui_config,  # deprecated! - should be removed
            }

    def validate_gui_config(self, scan_cls) -> dict:
        """
        Validate the gui_config of the scan class

        Args:
            scan_cls: class

        Returns:
            dict: gui_config
        """

        if not hasattr(scan_cls, "gui_config"):
            return {}
        if not isinstance(scan_cls.gui_config, GUIConfig) and not isinstance(
            scan_cls.gui_config, dict
        ):
            logger.error(
                f"Invalid gui_config for {scan_cls.scan_name}. gui_config must be of type GUIConfig or dict."
            )
            return {}
        gui_config = (
            GUIConfig.from_dict(scan_cls)
            if isinstance(scan_cls.gui_config, dict)
            else scan_cls.gui_config
        )
        return gui_config.model_dump()

    def convert_arg_input(self, arg_input) -> dict:
        """
        Convert the arg_input to supported data types

        Args:
            arg_input: dict

        Returns:
            dict: converted arg_input
        """
        converted_arg_input = {}
        for key, value in arg_input.items():
            dtype = _SCAN_ARG_TYPE_TO_DTYPE.get(value, value)
            if inspect.isclass(dtype) and issubclass(dtype, DeviceBase):
                dtype = DeviceBase
            converted_arg_input[key] = serialize_dtype(dtype)
        return converted_arg_input

    @staticmethod
    def _get_scan_plugins() -> dict[str, type]:
        verified_plugins = {}
        plugins = plugin_helper.get_scan_plugins()
        if not plugins:
            return verified_plugins
        for name, cls in plugins.items():
            if not issubclass(cls, (scans_module.RequestBase, ScanBaseV4)):
                continue
            verified_plugins[name] = cls
            logger.info(f"Loading scan plugin {name}")

        return verified_plugins

    @staticmethod
    def _get_v4_scan_members() -> list[tuple[str, Type[ScanBaseV4]]]:
        """Collect classes from all modules in the scans package."""
        members: list[tuple[str, Type[ScanBaseV4]]] = []
        for module_info in pkgutil.iter_modules(
            scans_v4_module.__path__, prefix=f"{scans_v4_module.__name__}."
        ):
            if module_info.name == f"{scans_v4_module.__name__}.legacy_scans":
                continue
            module = importlib.import_module(module_info.name)
            members.extend(
                (name, cls)
                for name, cls in inspect.getmembers(module, predicate=inspect.isclass)
                if cls.__module__ == module.__name__
            )
        return members

    def publish_available_scans(self):
        """send all available scans to the broker"""
        self.parent.connector.set(
            MessageEndpoints.available_scans(),
            AvailableResourceMessage(resource=self.available_scans),
        )
