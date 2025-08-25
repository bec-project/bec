"""
This module provides a mixin class for the BEC class that allows the user to load and unload macros from the `macros` directory.
"""

from __future__ import annotations

import ast
import builtins
import glob
import importlib
import importlib.metadata
import importlib.util
import inspect
import os
import pathlib
import traceback
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

from bec_lib.callback_handler import EventType
from bec_lib.logger import bec_logger
from bec_lib.utils.import_utils import lazy_import, lazy_import_from

if TYPE_CHECKING:  # pragma: no cover
    from pylint.message import Message

    from bec_lib.client import BECClient

logger = bec_logger.logger
pylint = lazy_import("pylint")
CollectingReporter = lazy_import_from("pylint.reporters", ("CollectingReporter",))


class UserMacros:
    def __init__(self, client: BECClient) -> None:
        self._macros = {}
        self._client = client
        self._macro_path = client._service_config.model.user_macros.base_path

    def load_all_user_macros(self) -> None:
        try:
            self._load_all_user_macros()
        except Exception:
            content = traceback.format_exc()
            logger.error(f"Error while loading user macros: \n {content}")

    def _load_all_user_macros(self) -> None:
        """Load all macros from the `macros` directory.

        Runs a callback of type `EventType.NAMESPACE_UPDATE`
        to inform clients about added objects in the namesapce.
        """
        self.forget_all_user_macros()

        # load all macros from the macros directory
        current_path = pathlib.Path(__file__).parent.resolve()
        macro_files = glob.glob(os.path.abspath(os.path.join(current_path, "../macros/*.py")))

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
        builtins.__dict__.update({name: v["cls"] for name, v in self._macros.items()})

    def forget_all_user_macros(self) -> None:
        """unload / remove loaded user macros from builtins. Files will remain untouched.

        Runs a callback of type `EventType.NAMESPACE_UPDATE`
        to inform clients about removing objects from the namesapce.

        """
        for name, obj in self._macros.items():
            builtins.__dict__.pop(name)
            self._client.callbacks.run(
                EventType.NAMESPACE_UPDATE, action="remove", ns_objects={name: obj["cls"]}
            )
        self._macros.clear()

    def load_user_macro(self, file: str) -> None:
        """load a user macro file and import all its definitions

        Args:
            file (str): Full path to the macro file.
        """
        # TODO: re-enable linter
        # self._run_linter_on_file(file)
        module_members = self._load_macro_module(file)
        for name, cls in module_members:
            if not callable(cls):
                continue
            # ignore imported classes
            if cls.__module__ != "macros":
                continue
            if name in self._macros:
                logger.warning(f"Conflicting definitions for {name}.")
            logger.info(f"Importing {name}")
            self._macros[name] = {"cls": cls, "fname": file}
            self._client.callbacks.run(
                EventType.NAMESPACE_UPDATE, action="add", ns_objects={name: cls}
            )

    def forget_user_macro(self, name: str) -> None:
        """unload / remove a user macros. The file will remain on disk."""
        if name not in self._macros:
            logger.error(f"{name} is not a known user macro.")
            return
        self._client.callbacks.run(
            EventType.NAMESPACE_UPDATE,
            action="remove",
            ns_objects={name: self._macros[name]["cls"]},
        )
        builtins.__dict__.pop(name)
        self._macros.pop(name)

    def list_user_macros(self):
        """display all currently loaded user macros"""
        console = Console()
        table = Table(title="User macros")
        table.add_column("Name", justify="center")
        table.add_column("Location", justify="center", overflow="fold")

        for name, content in self._macros.items():
            table.add_row(name, content.get("fname"))
        console.print(table)

    @staticmethod
    def _has_executable_code(file: str) -> bool:
        """Check if a Python file contains executable code at module level using AST.

        This method parses the file and checks for statements that would be executed
        when the module is imported, excluding function/class definitions and imports.

        Args:
            file: Path to the Python file to check

        Returns:
            True if the file contains executable code at module level, False otherwise
        """
        try:
            with open(file, "r", encoding="utf-8") as f:
                source = f.read()

            tree = ast.parse(source, filename=file)
        except (OSError, SyntaxError) as e:
            logger.error(f"Error parsing macro file {file}: {e}")
            return True  # Assume unsafe if we can't parse

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

            # Allow simple constant assignments (like MY_CONSTANT = "value")
            if isinstance(node, ast.Assign):
                # Check if it's a simple assignment to a constant value
                if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                    # Allow assignments of constants, but not function calls or complex expressions
                    if isinstance(
                        node.value, (ast.Constant, ast.List, ast.Tuple, ast.Dict, ast.Set)
                    ) or (isinstance(node.value, ast.Name) and node.value.id.isupper()):
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
                if isinstance(node, ast.Expr) and isinstance(node.value, (ast.Constant, ast.Str)):
                    continue
                return True

        return False

    def _load_macro_module(self, file) -> list:
        """Load a macro module safely by checking for executable code using AST.

        This method uses AST parsing to detect and prevent loading of files that
        contain executable statements at the module level (other than function/class definitions).

        Args:
            file: Path to the macro file to load

        Returns:
            List of (name, object) tuples for callables found in the module
        """
        # First, check if the file contains executable code
        if self._has_executable_code(file):
            logger.warning(
                f"Macro file {file} contains executable code at module level and will not be loaded for security reasons."
            )
            return []

        # If safe, load the module
        module_spec = importlib.util.spec_from_file_location("macros", file)
        if module_spec is None or module_spec.loader is None:
            logger.error(f"Failed to create module spec for {file}")
            return []

        plugin_module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(plugin_module)
        module_members = inspect.getmembers(plugin_module)
        return module_members

    def _run_linter_on_file(self, file) -> None:
        accepted_vars = ",".join([key for key in builtins.__dict__ if not key.startswith("_")])
        reporter = CollectingReporter()
        print(f"{accepted_vars}")
        pylint.lint.Run(
            [file, "--errors-only", f"--additional-builtins={accepted_vars}"],
            exit=False,
            reporter=reporter,
        )
        if not reporter.messages:
            return

        def _format_pylint_output(msg: Message):
            return f"Line {msg.line}, column {msg.column}: {msg.msg}."

        for msg in reporter.messages:
            logger.error(
                f"During the import of {file}, the following error was detected: \n{_format_pylint_output(msg)}.\nThe script was imported but may not work as expected."
            )
