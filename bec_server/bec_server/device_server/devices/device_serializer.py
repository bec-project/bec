"""
This module contains functions to get the device info from an object. The device info 
is used to create the device interface for proxy objects on other services.
"""

import functools
from typing import Any

import msgpack
from ophyd import Device, PositionerBase, Signal
from ophyd_devices import BECDeviceBase, ComputedSignal

from bec_lib.bec_errors import DeviceConfigError
from bec_lib.device import DeviceBase
from bec_lib.numpy_encoder import numpy_encode


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


def get_custom_user_access_info(obj: Any, obj_interface: dict) -> dict:
    """
    Get the custom user access info

    Args:
        obj (Any): object to get the user access info from
        obj_interface (dict): object interface

    Returns:
        dict: updated object interface
    """
    # user_funcs = get_user_functions(obj)
    if hasattr(obj, "USER_ACCESS"):
        for var in [func for func in dir(obj) if func in obj.USER_ACCESS]:
            obj_member = getattr(obj, var)
            if not callable(obj_member):
                if is_serializable(obj_member):
                    obj_interface[var] = {"type": type(obj_member).__name__}
                elif get_device_base_class(obj_member) == "unknown":
                    obj_interface[var] = get_custom_user_access_info(obj_member, {})
                else:
                    continue
            else:
                obj_interface[var] = {"type": "func", "doc": obj_member.__doc__}
    return obj_interface


@functools.lru_cache(maxsize=2)
def get_protected_class_methods():
    """get protected methods of the DeviceBase class"""
    return [func for func in dir(DeviceBase) if not func.startswith("__")]


def get_device_base_class(obj: Any) -> str:
    """
    Get the base class of the object

    Args:
        obj (Any): object to get the base class from

    Returns:
        str: base class of the object
    """
    if isinstance(obj, PositionerBase):
        return "positioner"
    if isinstance(obj, ComputedSignal):
        return "computed_signal"
    if isinstance(obj, Signal):
        return "signal"
    if isinstance(obj, Device):
        return "device"
    if isinstance(obj, BECDeviceBase):
        return "device"

    return "unknown"


def get_device_info(obj: PositionerBase | ComputedSignal | Signal | Device | BECDeviceBase) -> dict:
    """
    Get the device info from the object

    Args:
        obj (PositionerBase | ComputedSignal | Signal | Device | BECDeviceBase): object to get the device info from
        device_info (dict): device info

    Returns:
        dict: updated device info
    """
    protected_names = get_protected_class_methods()

    user_access = get_custom_user_access_info(obj, {})
    if set(user_access.keys()) & set(protected_names):
        raise DeviceConfigError(
            f"User access method name {set(user_access.keys()) & set(protected_names)} is protected and cannot be used. Please rename the method."
        )

    signals = []
    if hasattr(obj, "component_names"):
        for component_name in obj.component_names:
            if get_device_base_class(getattr(obj, component_name)) == "signal":
                if component_name in protected_names:
                    raise DeviceConfigError(
                        f"Signal name {component_name} is protected and cannot be used. Please rename the signal."
                    )
                signals.append(
                    {
                        "component_name": component_name,
                        "obj_name": getattr(obj, component_name).name,
                        "kind_int": getattr(obj, component_name).kind,
                        "kind_str": str(getattr(obj, component_name).kind),
                    }
                )
    sub_devices = []

    if hasattr(obj, "walk_subdevices"):
        for _, dev in obj.walk_subdevices():
            sub_devices.append(get_device_info(dev))
    if obj.name in protected_names or getattr(obj, "dotted_name", None) in protected_names:
        raise DeviceConfigError(
            f"Device name {obj.name} is protected and cannot be used. Please rename the device."
        )
    return {
        "device_name": obj.name,
        "device_info": {
            "device_attr_name": getattr(obj, "attr_name", ""),
            "device_dotted_name": getattr(obj, "dotted_name", ""),
            "device_base_class": get_device_base_class(obj),
            "signals": signals,
            "hints": obj.hints,
            "describe": obj.describe(),
            "describe_configuration": obj.describe_configuration(),
            "sub_devices": sub_devices,
            "custom_user_access": user_access,
        },
    }