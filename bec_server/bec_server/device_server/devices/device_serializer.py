"""Compatibility imports for classic device inspection and serialization."""

from typing import Any

from bec_server.device_server.device_providers.ophyd.device_serializer import (
    disable_lazy_wait_for_connection,
    get_custom_user_access_info,
    get_device_base_class,
    get_lazy_wait_for_connection,
    get_protected_class_methods,
    serialize_device_info,
)
from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
    OphydDeviceProvider,
)
from bec_server.device_server.devices import OwnershipMode, is_serializable

__all__ = [
    "OwnershipMode",
    "disable_lazy_wait_for_connection",
    "get_custom_user_access_info",
    "get_device_base_class",
    "get_device_info",
    "get_lazy_wait_for_connection",
    "get_ownership_mode",
    "get_protected_class_methods",
    "is_serializable",
]


get_ownership_mode = OphydDeviceProvider.get_ownership_mode


def get_device_info(obj: Any, connect: bool = True) -> dict:
    """
    Serialize a classic device for legacy callers.

    Args:
        obj (Any): Native device or signal to inspect.
        connect (bool): Include information that requires a connected device.

    Returns:
        dict: Serialized device interface and metadata.
    """
    return serialize_device_info(obj, connect, get_ownership_mode=get_ownership_mode).to_wire()
