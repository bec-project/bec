from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bec_lib.device import DeviceBase
    from bec_server.scan_server.scans_v4.scans_v4 import ScanBase


def scan_hook(func):
    """
    Decorator for scan hooks. It registers the decorated method as a scan hook and thus allows
    scan modifiers to override or augment the scan logic.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        return func(self, *args, **kwargs)

    # pylint: disable=protected-access
    wrapper._scan_hook_info = {"method_name": func.__name__}  # type: ignore

    return wrapper


def scan_hook_impl(hook_type: str):
    """
    Decorator for scan hook implementations. It registers the decorated method as an implementation of the specified scan hook type.
    The hook_type should be one of the following: "before", "after" or "replace".
    This allows the scan modifier to specify whether the decorated method should be executed before, after or instead of the original scan hook method.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        # pylint: disable=protected-access
        wrapper._scan_hook_impl_info = {"hook_type": hook_type}  # type: ignore

        return wrapper

    return decorator


def get_scan_hooks(cls) -> dict[str, str]:
    """
    Get the scan hooks defined in the given class. It returns a dictionary mapping the hook method names to their corresponding scan hook types.
    """
    hooks = {}
    for attr_name in dir(cls):
        attr = getattr(cls, attr_name)
        if callable(attr) and hasattr(attr, "_scan_hook_info"):
            hook_info = attr._scan_hook_info  # type: ignore
            hooks[hook_info["method_name"]] = attr_name
    return hooks


def prepare_eiger(scan: ScanBase):
    if "eiger" not in scan.dev:
        return
    print("Preparing Eiger for the scan...")
    eiger = scan.dev["eiger"]
    num_frames = scan.scan_info.frames_per_trigger * scan.scan_info.num_points
    eiger.num_frames.set(num_frames).wait()


def prepare_falcon(scan: ScanBase):
    if "falcon" not in scan.dev:
        return
    print("Preparing Falcon for the scan...")
    falcon = scan.dev["falcon"]
    falcon.num_frames.set(100).wait()


class ScanModifier:

    @scan_hook_impl("after")
    def stage(self, scan: ScanBase):
        """
        Stage the devices for the upcoming scan. The stage logic is typically
        implemented on the device itself (i.e. by the device's stage method).
        However, if there are any additional steps that need to be executed before
        staging the devices, they can be implemented here.
        """
        prepare_eiger(scan)
        prepare_falcon(scan)
