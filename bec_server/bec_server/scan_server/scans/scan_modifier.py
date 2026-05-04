from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypeAlias, get_args

from bec_lib.scan_args import ScanArgument, Units

if TYPE_CHECKING:
    from bec_lib.device import DeviceBase
    from bec_server.scan_server.scans.scan_base import ScanBase


ScanHookName: TypeAlias = Literal[
    "prepare_scan",
    "open_scan",
    "stage",
    "pre_scan",
    "scan_core",
    "at_each_point",
    "post_scan",
    "unstage",
    "close_scan",
    "on_exception",
]

# somehow, pylance doesn't like it when we define scan hooks and create the
# literals out of it, so we do it the other way around
VALID_SCAN_HOOKS = set(get_args(ScanHookName))


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


def scan_hook_impl(
    hook_name: ScanHookName, hook_type: Literal["before", "after", "replace"] = "before"
):
    """
    Decorator for scan hook implementations. It registers the decorated method as an implementation of the specified scan hook.
    The hook_name must refer to an existing scan hook.
    The hook_type should be one of the following: "before", "after" or "replace".
    This allows the scan modifier to specify whether the decorated method should be executed before, after or instead of the original scan hook method.
    """
    if hook_name not in VALID_SCAN_HOOKS:
        raise ValueError(f"Invalid scan hook: {hook_name}")
    if hook_type not in {"before", "after", "replace"}:
        raise ValueError(f"Invalid scan hook type: {hook_type}")

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        # pylint: disable=protected-access
        wrapper._scan_hook_impl_info = {"hook_name": hook_name, "hook_type": hook_type}  # type: ignore

        return wrapper

    return decorator


def get_scan_hooks_impl(cls) -> dict[str, str]:
    """
    Get the scan hooks implemented by the given class. It returns
    a dictionary mapping the original hook method name to the scan modifier's method name that implements it.
    """
    hooks = {}
    for attr_name in dir(cls):
        attr = getattr(cls, attr_name)
        if callable(attr) and hasattr(attr, "_scan_hook_impl_info"):
            hook_info = attr._scan_hook_impl_info  # type: ignore
            hooks[hook_info["hook_name"]] = attr_name
    return hooks


class ScanModifier:

    def __init__(self, scan: ScanBase):
        self.scan = scan
        self.dev = scan.dev
        self.actions = scan.actions
        self.components = scan.components
        self.scan_info = scan.scan_info

    @staticmethod
    def scan_signature_overrides(
        scan_name: str,
        arguments: dict[str, Annotated[Any, ScanArgument] | None],
        defaults: dict[str, Any],
    ) -> tuple[dict, dict]:
        """
        Define scan signature overrides for the scan modifier. This allows the scan modifier to modify the scan arguments for specific scans.
        The method receives the original scan signature (arguments and defaults) and should return the modified signature as a tuple (arguments, defaults).
        The scan_name can be used to specify different signature overrides for different scans.

        Args:
            scan_name (str): The name of the scan for which the signature should be overridden.
            arguments (dict): The original scan arguments as a dictionary mapping argument names to their types and ScanArgument metadata.
            defaults (dict): The original scan argument defaults as a dictionary mapping argument names to their default values.

        Returns:
            tuple: A tuple containing the modified arguments and defaults dictionaries.

        Examples:
            >>> # Set the default exposure time to 1 second for all scans that have an exp_time argument
            >>> def scan_signature_overrides(scan_name, arguments, defaults):
            >>>     if "exp_time" in arguments:
            >>>         defaults["exp_time"] = 1.0
            >>>     return arguments, defaults

            >>> # Add a new argument called integ_time to all scans and set its default value to 0.5 seconds
            >>> # Note that additional args are automatically added to additional_scan_parameters
            >>> def scan_signature_overrides(scan_name, arguments, defaults):
            >>>     arguments["integ_time"] = Annotated[
            >>>         float,
            >>>         ScanArgument(description="Integration time for the scan", units=Units.s, ge=0),
            >>>     ]
            >>>     defaults["integ_time"] = 0.5
            >>>     return arguments, defaults

        """
        return arguments, defaults

    @staticmethod
    def gui_config_overrides(
        scan_name: str, gui_config: dict[str, list[str]]
    ) -> dict[str, list[str]]:
        """
        Define GUI configuration overrides for the scan modifier. This allows the scan modifier to modify the GUI configuration for specific scans.
        The method receives the original GUI configuration and should return the modified configuration as a dictionary.
        The scan_name can be used to specify different GUI configuration overrides for different scans.

        Args:
            scan_name (str): The name of the scan for which the GUI configuration should be overridden.
            gui_config (dict): The original GUI configuration as a dictionary mapping section names to lists of argument names.

        Returns:
            dict: The modified GUI configuration as a dictionary mapping section names to lists of argument names.

        Examples:
            >>> # Add the integ_time argument to the "Scan Parameters" section for all scans
            >>> def gui_config_overrides(scan_name, gui_config):
            >>>     if "Scan Parameters" in gui_config:
            >>>         gui_config["Scan Parameters"].append("integ_time")
            >>>     else:
            >>>         gui_config["Scan Parameters"] = ["integ_time"]
            >>>     return gui_config

            >>> # Remove the exp_time argument from the "Scan Parameters" section for the line_scan
            >>> def gui_config_overrides(scan_name, gui_config):
            >>>     if scan_name == "line_scan":
            >>>         for section, args in gui_config.items():
            >>>             if "exp_time" in args:
            >>>                 args.remove("exp_time")
            >>>     return gui_config

        """
        return gui_config

    def device_is_available(self, device: list[str] | str, check_enabled: bool = True) -> bool:
        """
        Check if the specified device(s) are available. This can be used to conditionally enable or disable the scan modifier based on the availability of certain devices.
        The device(s) can be specified as a string (for a single device) or a list of strings (for multiple devices).

        Args:
            device (str or list of str): The name(s) of the device(s) to check for availability.
            check_enabled (bool): If True, also check if the device is enabled.

        Returns:
            bool: True if all specified devices are available, False otherwise.

        """
        if isinstance(device, str):
            device = [device]
        for dev_name in device:
            if dev_name not in self.dev:
                return False
            if check_enabled and not self.dev[dev_name].enabled:
                return False
        return True
