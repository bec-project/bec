"""
This module contains the custom exceptions used in the BEC library.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages


class BECError(Exception):
    """Base class for all BEC exceptions"""

    def __init__(self, message: str, error_info: messages.ErrorInfo) -> None:
        super().__init__(message)
        self.error_info = error_info

    def pretty_print(self) -> None:
        """
        Use Rich to pretty print the alarm message,
        following the same logic used in __str__().
        """
        console = Console()

        msg = self.error_info.compact_error_message or self.error_info.error_message

        text = Text()
        text.append(f"{self.error_info.exception_type}", style="bold")
        if self.error_info.context:
            text.append(f" | {self.error_info.context}", style="bold")

        if self.error_info.device:
            text.append(f" | Device {self.error_info.device}\n", style="bold")
        text.append("\n")

        # Format message inside a syntax box if it looks like traceback
        if "Traceback (most recent call last):" in msg:
            body = Syntax(msg.strip(), "python", word_wrap=True)
        else:
            body = Text(msg.strip())

        if self.error_info.device:
            body.append(
                f"\n\nThe error is likely unrelated to BEC. Please check the device '{self.error_info.device}'.",
                style="bold",
            )

        console.print(Panel(body, title=text, border_style="red", expand=True))

    def print_details(self) -> None:
        console = Console()

        # --- HEADER ---
        header = Text()
        header.append("Error Occurred\n", style="bold red")
        header.append(f"Type: {self.error_info.exception_type}\n", style="bold")
        if self.error_info.context:
            header.append(f"Context: {self.error_info.context}\n", style="bold")
        if self.error_info.device:
            header.append(f"Device: {self.error_info.device}\n", style="bold")

        console.print(Panel(header, title="Error Info", border_style="red", expand=False))

        # --- SHOW SUMMARY
        if self.error_info.compact_error_message:
            console.print(
                Panel(
                    Text(self.error_info.compact_error_message),
                    title="Error Summary",
                    border_style="red",
                    expand=False,
                )
            )

        # --- SHOW FULL TRACEBACK
        tb_str = self.error_info.error_message
        if tb_str:
            try:
                console.print(tb_str)
            except Exception as e:
                console.print(f"Error printing traceback: {e}", style="bold red")


class ScanAbortion(Exception):
    """Scan abortion exception"""


class ScanInterruption(Exception):
    """Scan interruption exception"""


class ServiceConfigError(Exception):
    """Service config error"""


class DeviceConfigError(Exception):
    """Device config error"""
