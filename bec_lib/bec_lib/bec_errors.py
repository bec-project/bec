"""
This module contains the custom exceptions used in the BEC library.
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bec_lib import messages


class ScanAbortion(Exception):
    """Scan abortion exception"""


class ScanInterruption(Exception):
    """Scan interruption exception"""


class ScanRestart(Exception):
    """Exception to indicate that a scan has been restarted."""

    def __init__(self, new_scan_msg: messages.ScanQueueMessage):
        super().__init__("Scan has been restarted.")
        self.new_scan_msg = new_scan_msg


class ServiceConfigError(Exception):
    """Service config error"""


class DeviceConfigError(Exception):
    """Device config error"""


class ExceptionWithErrorInfo(Exception):
    """Base exception class for exceptions that contain error info"""

    def __init__(self, error_info: messages.ErrorInfo):
        super().__init__(error_info.error_message)
        self.error_info = error_info

    def __str__(self) -> str:
        msg = self.error_info.compact_error_message
        return f"{self.__class__.__name__}: {msg}" if msg else super().__str__()

    def pretty_print(self) -> None:
        """
        Use Rich to pretty print the error message,
        following the same logic used in __str__().
        """
        from rich.console import Console, Group
        from rich.panel import Panel
        from rich.syntax import Syntax
        from rich.text import Text

        console = Console()
        msg = self.error_info.compact_error_message or self.error_info.error_message

        text = Text()
        text.append(f"{self.__class__.__name__}", style="bold")
        text.append("\n")

        renderables = []
        # Format message inside a syntax box if it looks like traceback
        if "Traceback (most recent call last):" in msg:
            renderables.append(Syntax(msg.strip(), "python", word_wrap=True))
        else:
            renderables.append(Text(msg.strip()))

        body = Group(*renderables)

        console.print(Panel(body, title=text, border_style="red", expand=True))


class ScanInputValidationError(ExceptionWithErrorInfo):
    """Scan input validation error"""

    def __init__(self, error_info: messages.ErrorInfo):
        super().__init__(error_info)
        self.error_info = error_info

    @classmethod
    def with_error_info(cls, message: str) -> ScanInputValidationError:
        from bec_lib.messages import ErrorInfo

        stack = "".join(traceback.format_stack()[:-1])
        error_info = ErrorInfo(
            error_message=f"{stack}{cls.__name__}: {message}",
            compact_error_message=message,
            exception_type=cls.__name__,
        )
        return cls(error_info=error_info)
