"""
Shared Rich pretty-print helpers for BEC errors and alarms.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages
    from bec_lib.alarm_handler import Alarms


def _lazy_import_rich():
    global Console, Group, Panel, Syntax, Text
    from rich.console import Console, Group
    from rich.panel import Panel
    from rich.syntax import Syntax
    from rich.text import Text


class ErrorInfoPrettyPrinter:
    """Reusable Rich formatter for an ``ErrorInfo`` payload."""

    def __init__(self, error_info: messages.ErrorInfo):
        self.error_info = error_info

    def pretty_print_title(self):
        _lazy_import_rich()

        title = Text()
        title.append(f"{self.error_info.exception_type}", style="bold")
        if self.error_info.context:
            title.append(f" | {self.error_info.context}", style="bold")
        if self.error_info.device:
            title.append(f" | Device {self.error_info.device}", style="bold")
        title.append("\n")
        return title

    def pretty_print_renderables(self, msg: str) -> list:
        _lazy_import_rich()

        renderables = []
        if "Traceback (most recent call last):" in msg:
            renderables.append(Syntax(msg.strip(), "python", word_wrap=True))
        else:
            renderables.append(Text(msg.strip()))

        if self.error_info.device:
            renderables.append(
                Text(
                    f"\n\nThe error is likely unrelated to BEC. Please check the device '{self.error_info.device}'.",
                    style="bold",
                )
            )
        return renderables

    def details_header(self):
        _lazy_import_rich()

        header = Text()
        header.append("Error Occurred\n", style="bold red")
        header.append(f"Type: {self.error_info.exception_type}\n", style="bold")
        if self.error_info.context:
            header.append(f"Context: {self.error_info.context}\n", style="bold")
        if self.error_info.device:
            header.append(f"Device: {self.error_info.device}\n", style="bold")
        return header

    def details_title(self) -> str:
        return "Error Info"

    def summary_text_style(self) -> str | None:
        return None

    def pretty_print(self) -> None:
        """
        Use Rich to pretty print the compact error message when available.
        """
        _lazy_import_rich()

        console = Console()
        msg = self.error_info.compact_error_message or self.error_info.error_message
        body = Group(*self.pretty_print_renderables(msg))
        console.print(Panel(body, title=self.pretty_print_title(), border_style="red", expand=True))

    def print_details(self) -> None:
        """
        Use Rich to pretty print the full error details, including the full error message.
        """
        _lazy_import_rich()

        console = Console()
        console.print(
            Panel(
                self.details_header(), title=self.details_title(), border_style="red", expand=False
            )
        )

        if self.error_info.compact_error_message:
            console.print(
                Panel(
                    Text(self.error_info.compact_error_message, style=self.summary_text_style()),
                    title="Summary",
                    border_style="yellow",
                    expand=False,
                )
            )

        tb_str = self.error_info.error_message
        if tb_str:
            try:
                console.print(tb_str)
            except Exception:
                console.print(Panel(tb_str, title="Message", border_style="cyan"))


class AlarmPrettyPrinter(ErrorInfoPrettyPrinter):
    """Alarm-specific formatter that adds severity-focused headers."""

    def __init__(self, error_info: messages.ErrorInfo, severity: Alarms):
        super().__init__(error_info)
        self.severity = severity

    def pretty_print_title(self):
        _lazy_import_rich()

        title = Text()
        title.append(f"{self.error_info.exception_type} | ", style="bold")
        title.append(f"Severity {self.severity.name}", style="bold yellow")
        if self.error_info.device:
            title.append(f" | Device {self.error_info.device}\n", style="bold")
        title.append("\n")
        return title

    def details_header(self):
        _lazy_import_rich()

        header = Text()
        header.append("Alarm Raised\n", style="bold red")
        header.append(f"Severity: {self.severity.name}\n", style="bold")
        header.append(f"Type: {self.error_info.exception_type}\n", style="bold")
        if self.error_info.device:
            header.append(f"Device: {self.error_info.device}\n", style="bold")
        return header

    def details_title(self) -> str:
        return "Alarm Info"

    def summary_text_style(self) -> str | None:
        return "yellow"
