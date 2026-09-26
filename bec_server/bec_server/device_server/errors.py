"""Hardware-independent device instruction errors."""


class DisabledDeviceError(Exception):
    """Raised when a disabled or read-only device is accessed."""


class InvalidDeviceError(Exception):
    """Raised when an invalid device or operation result is accessed."""
