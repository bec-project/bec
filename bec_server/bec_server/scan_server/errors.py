from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bec_lib.messages import ErrorInfo


class ScanAbortion(Exception):
    pass


class LimitError(Exception):
    pass


class DeviceMessageError(Exception):
    pass


class DeviceInstructionError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message
        self.traceback = None
        self.compact_message = None
        self.exception_type = None
        self.device = None

    def set_info(self, error_info: ErrorInfo | dict):
        self.traceback = error_info.get("error_message") if error_info else None
        self.compact_message = error_info.get("compact_error_message") if error_info else None
        self.exception_type = error_info.get("exception_type") if error_info else None
        self.device = error_info.get("device") if error_info else None
