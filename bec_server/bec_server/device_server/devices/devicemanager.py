"""Compatibility imports for the Ophyd device manager implementation."""

from bec_lib.bec_errors import DeviceConfigError
from bec_server.device_server.ophyd.device_manager import DeviceManagerDS, DeviceProgress, DSDevice

__all__ = ["DSDevice", "DeviceManagerDS", "DeviceProgress", "DeviceConfigError"]
