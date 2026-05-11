from typing import DefaultDict, Protocol

from bec_lib.device import DeviceBase
from bec_lib.devicemanager import DeviceContainer
from bec_lib.logger import bec_logger

logger = bec_logger.logger


class SafetyCheckFailed(Exception):
    """A device has failed a safety check"""


class SafetyCheck(Protocol):
    def __call__(self, devices: DeviceContainer, position: int | float) -> bool: ...


_SAFETY_CHECKS: dict[str, set[SafetyCheck]] = DefaultDict(set)


def safety_check(device_dotted_name: str):
    def decorator(func: SafetyCheck):
        _SAFETY_CHECKS[device_dotted_name].add(func)
        return func

    return decorator


def run_safety_check(devices: DeviceContainer, device: DeviceBase, position: int | float) -> None:
    if checks := _SAFETY_CHECKS.get(device.dotted_name):
        for check in checks:
            try:
                if not check(devices, position):
                    raise SafetyCheckFailed(
                        f"Safety check '{check.__qualname__}' failed for device {device.dotted_name} and position {position}"
                    )
            except Exception as e:
                raise SafetyCheckFailed(
                    f"Safety check '{check.__qualname__}' encountered an error for device {device.dotted_name} and position {position}: {e}"
                )
