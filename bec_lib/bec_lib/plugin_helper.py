from __future__ import annotations

import importlib
import importlib.metadata
import inspect
import json
from functools import cache, lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from bec_lib.logger import bec_logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.metadata_schema import BasicScanMetadata

logger = bec_logger.logger


def get_plugin_class(class_spec: str, additional_modules=None) -> type:
    """
    Load a class from a plugin module.
    The class specification should follow the syntax <package>.<module>.<class> and is thus
    equivalent to the import statement `from <package>.<module> import <class>`. Imports along
    the lines of `from <package> import <class>` are also supported, assuming that the class is
    residing in the `__init__.py` file of the package.

    Args:
        class_spec (str): The class specification.
        additional_modules (list): Additional modules to search for the class.

    Returns:
        class: The class object.

    Raises:
        ValueError: If the class specification is invalid.
        ModuleNotFoundError: If the module could not be found.
    """
    class_specs = class_spec.split(".")
    if len(class_specs) == 1:
        raise ValueError(
            f"Invalid specification {class_spec}: The class spec should follow the syntax <package>.<module>.<class>"
        )

    parent_module = class_specs[0]
    full_module = ".".join(class_specs[:-1])
    class_name = class_specs[-1]

    if parent_module not in [
        plugins.__name__ for plugins in _get_available_modules("bec", additional_modules)
    ]:
        raise ModuleNotFoundError(f"Could not find module {parent_module}.")

    module = _import_module(full_module)
    return getattr(module, class_name)


def get_scan_plugins() -> dict:
    """
    Load all scan plugins.

    Returns:
        dict: A dictionary with the plugin names as keys and the plugin classes as values.
    """
    modules = _get_available_plugins("bec.scans")
    loaded_plugins = {}
    for module in modules:
        mods = inspect.getmembers(module, predicate=_filter_plugins)
        for name, mod_cls in mods:
            if name in loaded_plugins:
                logger.warning(f"Duplicated scan plugin {name}.")
            loaded_plugins[name] = mod_cls
    return loaded_plugins


def get_file_writer_plugins() -> dict:
    """
    Load all file writer plugins.

    Returns:
        dict: A dictionary with the plugin names as keys and the plugin classes as values.
    """
    modules = _get_available_plugins("bec.file_writer")
    loaded_plugins = {}
    for module in modules:
        mods = inspect.getmembers(module, predicate=_filter_plugins)
        for name, mod_cls in mods:
            if name in loaded_plugins:
                logger.warning(f"Duplicated file writer plugin {name}.")
            loaded_plugins[name] = mod_cls
    return loaded_plugins


@cache
def get_metadata_schema_registry() -> tuple[dict, type[BasicScanMetadata]]:
    module = _get_available_plugins("bec.scans.metadata_schema")
    if len(module) == 0:
        return {}, None
    try:
        registry_module = importlib.import_module(module[0].__name__ + ".metadata_schema_registry")
        return (
            registry_module.METADATA_SCHEMA_REGISTRY,
            getattr(registry_module, "DEFAULT_SCHEMA", None),
        )
    except Exception as e:
        logger.error(f"Error while loading metadata schema registry from plugins: {e}")
        return {}, None


def get_ipython_client_startup_plugins(state: Literal["pre", "post"]) -> dict:
    """
    Load all IPython client startup plugins.

    Args:
        state (str): The state of the plugin. Either "pre" or "post".

    Returns:
        dict: A dictionary with the plugin names as keys and the plugin module and source name as values.
    """
    group = "bec.ipython_client_startup"
    target = f"plugin_ipython_client_{state}"
    entry_points = importlib.metadata.entry_points(group=group)
    modules = {
        entry_point.name: {"source": entry_point.dist.name, "module": entry_point.load()}
        for entry_point in entry_points
        if entry_point.name == target
    }
    return modules


def plugins_installed() -> int:
    return len(importlib.metadata.entry_points(group="bec"))


def plugin_package_name():
    """Get the package and module name of the installed plugin repository. Raises ValueError if no
    plugin is installed or more than one plugin is installed."""
    plugins = importlib.metadata.entry_points(group="bec")
    if len(plugins) != 1:
        raise ValueError(
            "You must have one and only one BEC plugin repository installed for this to work"
        )
    return list(plugins)[0].value


def module_dist_info(name: str) -> dict[str, Any]:
    """Get the 'direct_url' distribution info for the given package name"""
    dist = importlib.metadata.Distribution.from_name(name)
    return json.loads(dist.read_text("direct_url.json") or "{}")


def plugin_repo_path() -> str:
    """Get the path on disk of the installed plugin repository. Raises ValueError if no plugin is
    installed or more than one plugin is installed. Raises ValueError if the installed plugin is not
    installed in editable mode."""
    dist_info = module_dist_info(plugin_package_name())
    if not dist_info.get("dir_info", {}).get("editable", False):
        raise ValueError("Plugin repo must be installed in editable mode")
    return dist_info.get("url")[5:]  # cut off "file:" prefix # type: ignore # this must exist


def _filter_plugins(module) -> bool:
    """
    Filter out classes that are not plugins.

    Args:
        module: The module to filter.

    Returns:
        bool: True if the module is a plugin, False otherwise.
    """
    return inspect.isclass(module) and not module.__name__.startswith("__")


@lru_cache(maxsize=10)
def _get_available_plugins(group) -> list:
    """
    Retrieve all available plugins for a given plugin group.

    Args:
        group (str): The name of the group.

    Returns:
        list: A list of modules.
    """

    plugins = importlib.metadata.entry_points(group=group)
    modules = []
    for plugin in plugins:
        try:
            module = plugin.load()
            modules.append(module)
        except Exception as e:
            logger.error(f"Error loading plugin {plugin.name}: {e}")
            continue
    return modules


def _get_available_modules(plugin: str, additional_modules=None) -> list:
    """
    Load all available modules for a given plugin.

    Args:
        plugin (str): The name of the plugin.
        additional_modules (list): Additional modules to append to the list of available modules.

    Returns:
        list: A list of modules.
    """

    modules = _get_available_plugins(plugin)
    if additional_modules:
        modules.extend(additional_modules)
    return modules


@lru_cache(maxsize=20)
def _import_module(module_name: str):
    """
    Import a module by name.
    """
    module = importlib.import_module(module_name)
    return module
