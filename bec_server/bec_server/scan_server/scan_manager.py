"""Discover built-in and plugin scans and publish their definitions."""

from __future__ import annotations

import functools
import importlib
import inspect
import pkgutil
from typing import TYPE_CHECKING, Any, cast

from bec_lib import messages, plugin_helper
from bec_lib.alarm_handler import Alarms
from bec_lib.connector import MessageObject
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import AvailableResourceMessage, ErrorInfo
from bec_lib.signature_serializer import serialize_dtype
from bec_server.scan_server.scans.scan_argument_modifier import (
    get_scan_modifier,
    scan_doc_with_modifiers,
    scan_signature_with_modifiers,
)

from . import scans as scans_module
from .scans.scan_base import ScanBase

if TYPE_CHECKING:
    from bec_server.scan_server.scan_server import ScanServer

logger = bec_logger.logger

INTERNAL_SCAN_CLASSES = {"ScanBase", "DeviceRpc"}


class ScanManager:
    """Discover scan classes and publish their definitions."""

    def __init__(self, *, parent: ScanServer) -> None:
        """Initialize scan discovery and its request subscriptions.

        Args:
            parent (ScanServer): Owning scan server.
        """
        self.parent = parent
        self.available_scans = {}
        self.scan_dict: dict[str, type[ScanBase]] = {}
        self._plugins = {}
        self.parent.connector.register(
            MessageEndpoints.service_request(), cb=self.handle_reload_scans_request
        )
        self.update_available_scans()
        self.publish_available_scans()

    @functools.lru_cache(maxsize=2)
    @staticmethod
    def get_available_scans(allow_duplicates: bool = False) -> list[tuple[str, type[ScanBase]]]:
        """Get all available built-in scans and plugin scans.

        Args:
            allow_duplicates (bool): If True, allow duplicate scan names. Default is False.

        Returns:
            list[tuple[str, type[ScanBase]]]: scan name and scan class tuples
        """

        def _append_new_scan_members(
            members: list[tuple[str, type[ScanBase]]],
            candidates: list[tuple[str, type[ScanBase]]],
            skip_duplicates: bool = False,
        ) -> None:
            seen_scan_names = {
                scan_cls.scan_name
                for _, scan_cls in members
                if hasattr(scan_cls, "scan_name") and scan_cls.scan_name
            }
            for name, scan_cls in candidates:
                scan_name = getattr(scan_cls, "scan_name", None)
                if skip_duplicates and scan_name and scan_name in seen_scan_names:
                    continue
                members.append((name, scan_cls))
                if scan_name:
                    seen_scan_names.add(scan_name)

        members: list[tuple[str, type[ScanBase]]] = ScanManager._get_scan_members()

        # plugin scans
        _append_new_scan_members(
            members,
            list((name, cls) for name, cls in ScanManager._get_scan_plugins().items()),
            skip_duplicates=not allow_duplicates,
        )

        to_remove = []
        for name, scan_cls in members:
            is_scan = issubclass(scan_cls, ScanBase)
            if not is_scan or not scan_cls.scan_name or scan_cls is ScanBase:
                logger.debug(f"Ignoring {name}")
                to_remove.append((name, scan_cls))
        for item in to_remove:
            members.remove(item)

        return members

    def update_available_scans(self, reload: bool = False) -> None:
        """Load built-in and plugin scan definitions.

        Args:
            reload (bool): Whether to invalidate cached discovery and reload plugin modules.
        """
        if reload:
            self._reload_scan_discovery()

        self.available_scans = {}
        self.scan_dict = {}
        members = ScanManager.get_available_scans(allow_duplicates=True)

        for name, scan_cls in members:

            if not scan_cls.scan_name.isidentifier():
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

            self.scan_dict[scan_cls.scan_name] = scan_cls
            gui_visibility = {}
            if hasattr(scan_cls, "gui_visibility"):
                gui_visibility = scan_cls.gui_visibility  # type: ignore
            elif hasattr(scan_cls, "gui_config"):  # type: ignore
                gui_visibility = scan_cls.gui_config  # type: ignore

            self.available_scans[scan_cls.scan_name] = {
                "class": scan_cls.__name__,
                # Preserve the identifier consumed by existing clients.
                "base_class": "ScanBaseV4",
                "is_scan": scan_cls.is_scan if hasattr(scan_cls, "is_scan") else False,
                "is_internal": self.scan_is_internal(scan_cls),
                "arg_input": self.convert_arg_input(scan_cls.arg_input),
                "required_kwargs": getattr(scan_cls, "required_kwargs", []),
                "arg_bundle_size": scan_cls.arg_bundle_size,
                "doc": scan_doc_with_modifiers(scan_cls),
                "signature": scan_signature_with_modifiers(scan_cls),
                "gui_visibility": gui_visibility,
            }

    @staticmethod
    def scan_is_internal(scan_cls: type[ScanBase]) -> bool:
        """Determine whether a scan definition is internal.

        Args:
            scan_cls (type[ScanBase]): Scan class to inspect.

        Returns:
            bool: Whether the class is marked internal or has a reserved internal class name.
        """
        if scan_cls.__name__ in INTERNAL_SCAN_CLASSES:
            return True
        return getattr(scan_cls, "is_internal", False)

    def convert_arg_input(self, arg_input: dict[str, Any]) -> dict[str, Any]:
        """Serialize declared scan argument types.

        Args:
            arg_input (dict[str, Any]): Argument names and their declared types.

        Returns:
            dict[str, Any]: Argument names mapped to serialized type descriptions.
        """
        converted_arg_input = {}
        for key, value in arg_input.items():
            dtype = value
            if inspect.isclass(dtype) and issubclass(dtype, DeviceBase):
                dtype = DeviceBase
            converted_arg_input[key] = serialize_dtype(dtype)
        return converted_arg_input

    def handle_reload_scans_request(
        self, msg: MessageObject[messages.ServiceRequestMessage]
    ) -> None:
        """Reload and publish scan definitions when requested.

        Args:
            msg (MessageObject[messages.ServiceRequestMessage]): Service request to handle.
        """
        message = cast(messages.ServiceRequestMessage, msg.value)
        if message.action == "reload_scans":
            self.update_available_scans(reload=True)
            self.publish_available_scans()

    def publish_available_scans(self) -> None:
        """Publish the discovered scan definitions to Redis."""
        self.parent.connector.set_and_publish(
            MessageEndpoints.available_scans(),
            AvailableResourceMessage(resource=self.available_scans),
        )

    #############################################
    ############### Helper Methods ##############
    #############################################

    @classmethod
    def _reload_scan_discovery(cls) -> None:
        """Invalidate cached discovery and reload plugin modules."""
        get_scan_modifier.cache_clear()
        cls.get_available_scans.cache_clear()
        plugin_helper.reload_plugin_modules()

    @staticmethod
    def _get_scan_plugins() -> dict[str, type[ScanBase]]:
        verified_plugins: dict[str, type[ScanBase]] = {}
        plugins = plugin_helper.get_scan_plugins()
        if not plugins:
            return verified_plugins
        for name, cls in plugins.items():
            if not inspect.isclass(cls) or not issubclass(cls, ScanBase):
                continue
            verified_plugins[name] = cls
            logger.info(f"Loading scan plugin {name}")

        return verified_plugins

    @staticmethod
    def _get_scan_members() -> list[tuple[str, type[ScanBase]]]:
        """Collect classes from all modules in the scans package."""
        members: list[tuple[str, type[ScanBase]]] = []
        for module_info in pkgutil.iter_modules(
            scans_module.__path__, prefix=f"{scans_module.__name__}."
        ):
            module = importlib.import_module(module_info.name)
            members.extend(
                (name, cls)
                for name, cls in inspect.getmembers(module, predicate=inspect.isclass)
                if cls.__module__ == module.__name__ and issubclass(cls, ScanBase)
            )
        return members
