from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bec_lib.messages import ErrorInfo


class ScanAbortion(Exception):
    pass


class LimitError(Exception):
    def __init__(self, message, device: str | None = None):
        super().__init__(message)
        self.message = message
        self.device = device


class DeviceMessageError(Exception):
    pass


class DeviceInstructionError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message
        self.error_info: ErrorInfo | None = None

    def set_info(self, error_info: ErrorInfo):
        self.error_info = error_info
