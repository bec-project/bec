"""Compatibility imports for the Ophyd serialization implementation."""

from bec_server.device_server.ophyd.serialization import (
    OwnershipMode,
    disable_lazy_wait_for_connection,
    get_custom_user_access_info,
    get_device_base_class,
    get_device_info,
    get_lazy_wait_for_connection,
    get_ownership_mode,
    get_protected_class_methods,
    is_serializable,
)

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
