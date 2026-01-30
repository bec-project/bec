"""
This module contains the custom exceptions used in the BEC library.
"""

from __future__ import annotations

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
