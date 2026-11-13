"""Shared device serialization utilities."""

from enum import Enum
from typing import Any

import msgpack

from bec_lib.numpy_encoder import numpy_encode


class OwnershipMode(str, Enum):
    """
    Ownership policy used in serialized device info.

    FREE: The device is free to be used by any scan or procedure without acquiring a lock.
        This is typically used for read-only signals.
    CLAIMABLE: The device can be locked by a scan or procedure. It can be used without a lock,
        but if a scan or procedure wants to use it, it should acquire a lock first. Typical
        examples are motors that are normally on readout priority "baseline". When a scan wants
        to use the motor, it should acquire a lock first. If the motor is already locked by
        another scan or procedure, the scan will wait until the lock is released.
    PINNED: When enabled, the device has to be locked by a scan before it can be used. This is
        typically used for devices that are stateful during a scan, such as detectors or flyers.
    """

    FREE = "free"
    CLAIMABLE = "claimable"
    PINNED = "pinned"


def is_serializable(var: Any) -> bool:
    """
    Check if a variable is serializable

    Args:
        var (Any): variable to check

    Returns:
        bool: True if the variable is serializable, False otherwise
    """
    try:
        msgpack.dumps(var, default=numpy_encode)
        return True
    except (TypeError, OverflowError):
        return False
