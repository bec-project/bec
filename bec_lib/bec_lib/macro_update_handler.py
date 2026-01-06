from __future__ import annotations

import ast
import builtins
import glob
import importlib
import importlib.metadata
import importlib.util
import inspect
import os
from typing import TYPE_CHECKING, Any, Callable, Literal, TypedDict

import slugify

from bec_lib import messages
from bec_lib.callback_handler import EventType
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

if TYPE_CHECKING:
    from bec_lib.user_macros import UserMacros


logger = bec_logger.logger


def has_executable_code(content: str) -> tuple[bool, int | None]:
    """
    Check if the given Python code contains executable code at module level using AST.

    Args:
        content (str): The Python code to check.

    Returns:
        tuple[bool, int | None]: A tuple where the first element is True if executable code is found, False otherwise.
                                The second element is the line number of the first executable statement found, or None if none found.
    """
    try:
        tree = ast.parse(content)
    except (OSError, SyntaxError) as e:
        logger.error(f"Error parsing macro: {e}")
        return True, None  # Assume unsafe if we can't parse

    # Check for unsafe statements at module level
    for node in tree.body:  # Only check top-level nodes
        # Allow imports, function definitions, class definitions, and simple assignments
        if isinstance(
            node,
            (
                ast.Import,
                ast.ImportFrom,
                ast.FunctionDef,
                ast.AsyncFunctionDef,
                ast.ClassDef,
                ast.AnnAssign,
            ),
        ):
            continue

        # Block any other executable statements
        if isinstance(
            node,
            (
                ast.Expr,
                ast.Assign,
                ast.AugAssign,
                ast.For,
                ast.While,
                ast.If,
                ast.With,
                ast.Try,
                ast.Assert,
                ast.Delete,
                ast.Global,
                ast.Nonlocal,
                ast.Return,
                ast.Yield,
                ast.YieldFrom,
                ast.Raise,
                ast.Break,
                ast.Continue,
            ),
        ):
            # Special case: allow docstrings (string literals as expressions)
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                continue
            return True, node.lineno

    return False, None


class MacroUpdateHandler:
    def __init__(self, macros: UserMacros):
        """
        MacroUpdateHandler is responsible for loading, unloading, and managing user macros.

        Args:
            macros (UserMacros): The UserMacros instance to manage.
        """
        self.client = macros._client
        self._macro_path = self.client._service_config.model.user_macros.base_path
        self.client.connector.register(
            MessageEndpoints.macro_update(), cb=self._macro_update_callback, parent=self
        )

    @property
    def macros(self) -> dict[str, dict[str, Any]]:
        """Get the currently loaded macros.

        Returns:
            dict: A dictionary of macro names and their corresponding class objects and source code.
        """
        return builtins.__dict__.get("_user_macros", {})

    def _add_macro(self, name: str, macro: dict[str, Any]) -> None:
        """Add a macro to the builtins.

        Args:
            name (str): The name of the macro.
            macro (dict[str, Any]): The macro details including class object and source code.
        """
        if "_user_macros" not in builtins.__dict__:
            builtins.__dict__["_user_macros"] = {}
        builtins.__dict__["_user_macros"][name] = macro
        builtins.__dict__[name] = macro["cls"]

    def _remove_macro(self, name: str) -> None:
        """Remove a macro from the builtins.

        Args:
            name (str): The name of the macro to remove.
        """
        if "_user_macros" in builtins.__dict__ and name in builtins.__dict__["_user_macros"]:
            builtins.__dict__["_user_macros"].pop(name)
        if name in builtins.__dict__:
            builtins.__dict__.pop(name)

    def _remove_all_macros(self) -> None:
        """Remove all macros from the builtins."""
        if "_user_macros" in builtins.__dict__:
            for name in list(builtins.__dict__["_user_macros"].keys()):
                self._remove_macro(name)
            builtins.__dict__.pop("_user_macros")

    def load_all_user_macros(self) -> None:
        """Load all macros from the `macros` directory.

        Runs a callback of type `EventType.NAMESPACE_UPDATE`
        to inform clients about added objects in the namesapce.
        """
        self.forget_all_user_macros()
        macro_files = []

        # load all macros from the user's macro directory in the home directory
        user_macro_dir = os.path.join(os.path.expanduser("~"), "bec", "macros")
        if os.path.exists(user_macro_dir):
            macro_files.extend(glob.glob(os.path.abspath(os.path.join(user_macro_dir, "*.py"))))

        config_macro_dir = os.path.expanduser(self._macro_path)
        if os.path.exists(config_macro_dir):
            macro_files.extend(glob.glob(os.path.abspath(os.path.join(config_macro_dir, "*.py"))))

        # load macros from the plugins
        plugins = importlib.metadata.entry_points(group="bec")
        for plugin in plugins:
            if plugin.name == "plugin_bec":
                plugin = plugin.load()
                plugin_macros_dir = os.path.join(plugin.__path__[0], "macros")
                if os.path.exists(plugin_macros_dir):
                    macro_files.extend(
                        glob.glob(os.path.abspath(os.path.join(plugin_macros_dir, "*.py")))
                    )

        for file in macro_files:
            self.load_user_macro(file)

    def load_macro_module(self, file) -> list:
        """Load a macro module safely by checking for executable code using AST.

        This method uses AST parsing to detect and prevent loading of files that
        contain executable statements at the module level (other than function/class definitions).

        Args:
            file: Path to the macro file to load

        Returns:
            List of (name, object) tuples for callables found in the module
        """
        # First, check if the file contains executable code
        with open(file, "r", encoding="utf-8") as f:
            content = f.read()

        exec_code, line_no = has_executable_code(content)
        if exec_code:
            if line_no is not None:
                logger.warning(
                    f"Macro file {file} contains executable code at module level (line {line_no}) and will not be loaded for security reasons."
                )
            else:
                logger.warning(
                    f"Macro file {file} contains executable code at module level and will not be loaded for security reasons."
                )
            return []

        # If safe, load the module
        module_suffix = os.path.basename(file)
        module_suffix = slugify.slugify(module_suffix, separator="_")
        module_spec = importlib.util.spec_from_file_location(f"macros_{module_suffix}", file)
        if module_spec is None or module_spec.loader is None:
            logger.error(f"Failed to create module spec for {file}")
            return []

        plugin_module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(plugin_module)
        module_members = inspect.getmembers(plugin_module)
        return module_members

    def forget_all_user_macros(self) -> None:
        """unload / remove loaded user macros from builtins. Files will remain untouched.

        Runs a callback of type `EventType.NAMESPACE_UPDATE`
        to inform clients about removing objects from the namesapce.

        """
        for name, obj in self.macros.items():
            self.client.callbacks.run(
                EventType.NAMESPACE_UPDATE, action="remove", ns_objects={name: obj["cls"]}
            )
        self._remove_all_macros()

    def load_user_macro(self, file: str, ignore_existing: bool = False) -> None:
        """load a user macro file and import all its definitions

        Args:
            file (str): Full path to the macro file.
            ignore_existing (bool, optional): If True, existing macros will be ignored and no warning will be logged. Defaults to False.
        """
        macros_in_file = self.get_macros_from_file(file)
        for name, macro in macros_in_file.items():
            if name in self.macros:
                if ignore_existing:
                    continue
                logger.warning(f"Conflicting definitions for {name}.")
            if name not in self.macros and name in builtins.__dict__:
                logger.warning(f"Attempting to overwrite existing built-in {name}. Skipping.")
                continue

            logger.info(f"Importing {name}")
            self._add_macro(name, macro)
            self.client.callbacks.run(
                EventType.NAMESPACE_UPDATE, action="add", ns_objects={name: macro["cls"]}
            )

    def forget_user_macro(self, name: str) -> None:
        """unload / remove a user macros. The file will remain on disk."""
        if name not in self.macros:
            logger.error(f"{name} is not a known user macro.")
            return
        self.client.callbacks.run(
            EventType.NAMESPACE_UPDATE, action="remove", ns_objects={name: self.macros[name]["cls"]}
        )
        self._remove_macro(name)

    def reload_user_macro(self, name: str, file: str) -> None:
        """reload a user macro from file.

        Args:
            name (str): Name of the macro to reload.
            file (str): Full path to the macro file.
        """
        self.forget_user_macro(name)
        self.load_user_macro(file, ignore_existing=True)

    @staticmethod
    def _macro_update_callback(msg, parent):
        """Callback to handle macro update messages.

        Args:
            msg: The message containing the macro update information.
            parent: The UserMacros instance.
        """
        msg = msg.value
        if not isinstance(msg, messages.MacroUpdateMessage):
            logger.error(f"Received invalid message type: {type(msg)}")
            return

        parent.on_macro_update(msg)

    def get_macros_from_file(self, file: str) -> dict[str, dict[str, Any]]:
        """
        Get all macros defined in a specific file.

        Args:
            file (str): The path to the macro file.

        Returns:
            dict: A dictionary of macro names and their corresponding class objects and source code.
        """

        module_members = self.load_macro_module(file)
        macros_in_file = {}
        for name, cls in module_members:
            if not callable(cls):
                continue
            # ignore imported classes
            if not cls.__module__.startswith("macros_"):
                continue
            macros_in_file[name] = {
                "cls": cls,
                "fname": file,
                "source": (inspect.getsource(cls) if inspect.isfunction(cls) else None),
            }
        return macros_in_file

    def get_existing_macros(self, file: str) -> dict:
        """
        Get all macros that were loaded from a specific file.

        Args:
            file (str): The path to the macro file.
        Returns:
            dict: A dictionary of macro names and their corresponding class objects and source code.
        """
        return {name: v for name, v in self.macros.items() if v.get("fname") == file}

    def broadcast(
        self,
        action: Literal["add", "remove", "reload", "reload_all"],
        name: str | None = None,
        file_path: str | None = None,
    ) -> None:
        """Broadcast a change in the macros to all clients.

        Args:
            action (Literal["add", "remove", "reload", "reload_all"]): The action performed.
            name (str | None, optional): The name of the macro. Required for "add", "remove", and "reload". Defaults to None.
            file_path (str | None, optional): The file path of the macro. Required for "add" and "reload". Defaults to None.
        """
        msg = messages.MacroUpdateMessage(update_type=action, macro_name=name, file_path=file_path)
        self.client.connector.send(MessageEndpoints.macro_update(), msg)

    def on_macro_update(self, msg: messages.MacroUpdateMessage) -> None:
        """Handle macro update messages.

        Args:
            msg (messages.MacroUpdateMessage): The message containing the macro update information.
        """
        match msg.update_type:
            case "add":
                # A new macro file has been added, it may contain multiple macros
                self.load_user_macro(msg.file_path, ignore_existing=True)  # type: ignore - checked in the pydantic model
            case "remove":
                # A single macro is to be removed
                self.forget_user_macro(msg.macro_name)  # type: ignore - checked in the pydantic model
            case "reload":
                # A single macro is to be reloaded
                self.reload_user_macro(msg.macro_name, msg.file_path)  # type: ignore - checked in the pydantic model
            case "reload_all":
                self.load_all_user_macros()
            case _:
                logger.error(f"Unknown macro update type: {msg.update_type}")
