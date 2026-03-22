"""
This module provides the DAPPlugins class, which is used to access all available DAP plugins.
"""

from __future__ import annotations

import importlib

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.signature_serializer import dict_to_signature

logger = bec_logger.logger


class DAPPlugins:
    """
    DAPPlugins is a class that provides access to all available DAP plugins.
    """

    def __init__(self, parent):
        self._parent = parent
        self._available_dap_plugins = {}
        self._import_dap_plugins()
        self._selected_model = None
        self._auto_run = False
        self._selected_device = None

    def refresh(self):
        """
        Refresh the list of available DAP plugins. This is useful if new plugins have been added after
        the client has been initialized. This method is called automatically when the client is initialized.
        A call to this method is idempotent, meaning it can be called multiple times without side effects.
        """
        self._import_dap_plugins()

    def _import_dap_plugins(self):
        available_services = self._parent.service_status
        if not available_services:
            # not sure how we got here...
            return
        dap_services = [
            service for service in available_services if service.startswith("DAPServer/")
        ]
        for service in dap_services:
            available_plugins = self._parent.connector.get(
                MessageEndpoints.dap_available_plugins(service)
            )
            if available_plugins is None:
                logger.warning("No plugins available. Are redis and the BEC server running?")
                return
            for plugin_name, plugin_info in available_plugins.content["resource"].items():
                try:
                    if plugin_name in self._available_dap_plugins:
                        continue
                    name = plugin_info["user_friendly_name"]
                    auto_run_supported = plugin_info.get("auto_run_supported", False)
                    plugin_cls = self._get_plugin_class(plugin_info)
                    self._available_dap_plugins[name] = plugin_cls(
                        name,
                        plugin_info,
                        client=self._parent,
                        auto_run_supported=auto_run_supported,
                        service_info=available_services[service].content,
                    )
                    self._set_plugin(
                        name,
                        plugin_info.get("class_doc"),
                        plugin_info.get("run_doc"),
                        plugin_info.get("run_name"),
                        plugin_info.get("signature"),
                    )
                # pylint: disable=broad-except
                except Exception as e:
                    logger.error(f"Error importing plugin {plugin_name}: {e}")

    def _get_plugin_class(self, plugin_info):
        dap_plugin_objects = importlib.import_module("bec_lib.dap_plugin_objects")
        if hasattr(dap_plugin_objects, plugin_info["class"]):
            return getattr(dap_plugin_objects, plugin_info["class"])
        if plugin_info.get("auto_run_supported"):
            return dap_plugin_objects.DAPPluginObjectAutoRun
        return dap_plugin_objects.DAPPluginObject

    def _set_plugin(
        self,
        plugin_name: str,
        class_doc_string: str,
        run_doc_string: str,
        run_name: str,
        signature: dict,
    ):
        # pylint disable=protected-access
        setattr(self, plugin_name, self._available_dap_plugins[plugin_name])
        plugin = getattr(self, plugin_name)
        setattr(plugin, "__doc__", class_doc_string)
        setattr(plugin, run_name, plugin._user_run)
        setattr(plugin._user_run, "__doc__", run_doc_string)
        setattr(plugin._user_run, "__signature__", dict_to_signature(signature))
