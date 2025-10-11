"""
This module provides a mixin class for the BEC class that allows the user to load and unload macros from the `macros` directory.
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

from bec_lib.logger import bec_logger
from bec_lib.macro_update_handler import MacroUpdateHandler
from bec_lib.utils.import_utils import lazy_import, lazy_import_from

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.client import BECClient

logger = bec_logger.logger
pylint = lazy_import("pylint")
CollectingReporter = lazy_import_from("pylint.reporters", ("CollectingReporter",))


class UserMacros:
    """
    UserMacros is a class that exposes user methods to load and unload macros from the `macros` directory.
    """

    def __init__(self, client: BECClient) -> None:

        self._client = client

        self._update_handler = MacroUpdateHandler(self)

    def load_all_user_macros(self) -> None:
        """Load all user macros from the `macros` directory."""
        try:
            self._update_handler.load_all_user_macros()
        except Exception:
            content = traceback.format_exc()
            logger.error(f"Error while loading user macros: \n {content}")

    def forget_all_user_macros(self) -> None:
        """unload / remove loaded user macros from builtins. Files will remain untouched.
        Note: This does not delete the macro files, it only removes them from the current session.
        """
        self._update_handler.forget_all_user_macros()

    def forget_user_macro(self, name: str) -> None:
        """unload / remove a specific user macro from builtins. File will remain untouched.
        Note: This does not delete the macro file, it only removes it from the current session.

        Args:
            name (str): Name of the macro to unload.
        """
        self._update_handler.forget_user_macro(name)

    def load_user_macro(self, file: str) -> None:
        """Load a user macro from a file.

        Args:
            file (str): Full path to the macro file.
        """
        self._update_handler.load_user_macro(file)

    def list_user_macros(self):
        """display all currently loaded user macros"""
        console = Console()
        table = Table(title="User macros")
        table.add_column("Name", justify="center")
        table.add_column("Location", justify="center", overflow="fold")

        for name, content in self._update_handler.macros.items():
            table.add_row(name, content.get("fname"))
        console.print(table)
