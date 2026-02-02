from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from bec_lib.messages import ErrorInfo


class ScanAbortion(Exception):
    pass


class UserScanInterruption(ScanAbortion):
    def __init__(self, exit_info: Literal["aborted", "halted", "user_completed"]):
        super().__init__()
        self.exit_info: Literal["aborted", "halted", "user_completed"] = exit_info


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
