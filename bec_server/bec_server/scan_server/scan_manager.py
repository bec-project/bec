"""
Scan Manager loads the available scans and publishes them to redis.
"""

import importlib
import inspect
import pkgutil

from bec_lib import plugin_helper
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import AvailableResourceMessage
from bec_lib.signature_serializer import signature_to_dict
from bec_server.scan_server.scan_gui_models import GUIConfig, GUIInput

from . import scans as scans_module
from . import scans_v4 as scans_v4_module

logger = bec_logger.logger


class ScanManager:
    """
    Scan Manager loads the available scans and publishes them to redis.
    """

    def __init__(self, *, parent):
        """
        Scan Manager loads and manages the available scans.
        """
        self.parent = parent
        self.available_scans = {}
        self.scan_dict: dict[
            str, type[scans_module.RequestBase] | type[scans_v4_module.ScanBase]
        ] = {}
        self._plugins = {}
        self.load_plugins()
        self.update_available_scans()
        self.publish_available_scans()

    def load_plugins(self):
        """load scan plugins"""
        plugins = plugin_helper.get_scan_plugins()
        if not plugins:
            return
        for name, cls in plugins.items():
            if not issubclass(cls, scans_module.RequestBase):
                logger.error(
                    f"Plugin {name} is not a valid scan plugin as it does not inherit from RequestBase. Skipping."
                )
                continue
            self._plugins[name] = cls
            logger.info(f"Loading scan plugin {name}")

    def update_available_scans(self):
        """load all scans and plugin scans"""
        members: list[tuple[str, type]] = inspect.getmembers(
            scans_module, predicate=inspect.isclass
        )
        members.extend(self._get_v4_scan_members())
        members.extend((name, cls) for name, cls in self._plugins.items() if inspect.isclass(cls))

        for name, scan_cls in members:
            is_scan = issubclass(scan_cls, (scans_module.RequestBase, scans_v4_module.ScanBase))
            if not is_scan or not scan_cls.scan_name:
                logger.debug(f"Ignoring {name}")
                continue

            if scan_cls.scan_name in self.available_scans:
                logger.error(f"{scan_cls.scan_name} already exists. Skipping.")
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

            self.scan_dict[scan_cls.__name__] = scan_cls
            gui_config = self.validate_gui_config(scan_cls)
            self.available_scans[scan_cls.scan_name] = {
                "class": scan_cls.__name__,
                "base_class": base_cls,
                "arg_input": self.convert_arg_input(scan_cls.arg_input),
                "gui_config": gui_config,
                "required_kwargs": scan_cls.required_kwargs,
                "arg_bundle_size": scan_cls.arg_bundle_size,
                "doc": scan_cls.__doc__ or scan_cls.__init__.__doc__,
                "signature": signature_to_dict(scan_cls.__init__),
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
        converted = arg_input.copy()
        for key, value in converted.items():
            value = GUIInput.convert_to_legacy_scan_arg_type(value)
            if isinstance(value, scans_module.ScanArgType):
                converted[key] = value.value
                continue
            if isinstance(value, str):
                converted[key] = value
                continue
            if issubclass(value, DeviceBase):
                # once we have generalized the device types, this should be removed
                converted[key] = "device"
            elif issubclass(value, bool):
                # should be unified with the ScanArgType.BOOL
                converted[key] = "boolean"
            else:
                converted[key] = value.__name__
        return converted

    def _get_v4_scan_members(self) -> list[tuple[str, type]]:
        """Collect classes from all modules in the scans_v4 package."""
        members: list[tuple[str, type]] = []
        for module_info in pkgutil.iter_modules(
            scans_v4_module.__path__, prefix=f"{scans_v4_module.__name__}."
        ):
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
