"""
This module contains the custom exceptions used in the BEC library.
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

from bec_lib.utils.error_pretty_print import ErrorInfoPrettyPrinter

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages


class BECError(Exception):
    """Base class for all BEC exceptions"""

    def __init__(self, message: str, error_info: messages.ErrorInfo) -> None:
        super().__init__(message)
        self.error_info = error_info
        self._pretty_printer = ErrorInfoPrettyPrinter(error_info)

    def pretty_print(self) -> None:
        self._pretty_printer.pretty_print()

    def print_details(self) -> None:
        self._pretty_printer.print_details()


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
        self._pretty_printer = ErrorInfoPrettyPrinter(error_info)

    def __str__(self) -> str:
        msg = self.error_info.compact_error_message
        return f"{self.__class__.__name__}: {msg}" if msg else super().__str__()

    def pretty_print(self) -> None:
        self._pretty_printer.pretty_print()

    def print_details(self) -> None:
        self._pretty_printer.print_details()


class ScanInputValidationError(ExceptionWithErrorInfo):
    """Scan input validation error"""

    def __init__(self, error_info: messages.ErrorInfo):
        super().__init__(error_info)

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
