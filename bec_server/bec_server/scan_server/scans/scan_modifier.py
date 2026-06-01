from __future__ import annotations

from fnmatch import fnmatchcase
from functools import wraps
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypeAlias, TypedDict, get_args

from bec_lib.scan_args import ScanArgument

if TYPE_CHECKING:
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
HookType: TypeAlias = Literal["before", "after", "replace"]


class FilteredHookConfig(TypedDict):
    method_name: str
    scan_names: list[str]


HookConfig: TypeAlias = str | FilteredHookConfig
HookLifecycleConfig: TypeAlias = HookConfig | list[HookConfig]
ScanHookConfigMap: TypeAlias = dict[HookType, HookLifecycleConfig]


def _matches_scan_name(scan_name: str | None, patterns: list[str] | None) -> bool:
    if not patterns:
        return True
    if scan_name is None:
        return False
    return any(fnmatchcase(scan_name, pattern) for pattern in patterns)


def _get_hook_method_name(
    hook_name: str, hook_info: ScanHookConfigMap, hook_type: HookType, scan_name: str | None
) -> str | None:
    """
    Resolve the scan modifier method name for a hook lifecycle.

    Args:
        hook_name (str): Name of the scan hook being resolved, such as ``"post_scan"``.
        hook_info (ScanHookConfigMap):
            Hook implementation metadata produced by :func:`get_scan_hooks_impl`.
        hook_type (HookType): Lifecycle stage to resolve within the hook metadata.
        scan_name (str | None): Scan name used to evaluate optional ``scan_names`` filters.

    Returns:
        str | None: The matching modifier method name, or ``None`` if no implementation applies.

    Raises:
        ValueError: If more than one implementation matches the same hook lifecycle for the given
            ``scan_name``.
    """
    hook_config = hook_info.get(hook_type)
    if hook_config is None:
        return None
    if isinstance(hook_config, list):
        matched_method_names = []
        for config in hook_config:
            if isinstance(config, str):
                matched_method_names.append(config)
                continue
            if _matches_scan_name(scan_name, config.get("scan_names")):
                matched_method_names.append(config["method_name"])
        if len(matched_method_names) > 1:
            raise ValueError(
                f"Multiple scan modifier implementations matched hook '{hook_name}' "
                f"for lifecycle '{hook_type}' and scan '{scan_name}'"
            )
        return matched_method_names[0] if matched_method_names else None
    if isinstance(hook_config, str):
        return hook_config
    if not _matches_scan_name(scan_name, hook_config.get("scan_names")):
        return None
    return hook_config["method_name"]


def scan_hook(func):
    """
    Decorator for scan hooks. It registers the decorated method as a scan hook and thus allows
    scan modifiers to override or augment the scan logic.
    When a method is decorated with @scan_hook, it will check if the scan modifier has an
    implementation for this hook (by looking for the _scan_modifier_hooks attribute).
    If an implementation is found, it will execute the before, replace, and after methods
    in the appropriate order.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if func.__name__ not in self._scan_modifier_hooks:
            return func(self, *args, **kwargs)

        if self._scan_modifier is None:
            return func(self, *args, **kwargs)

        hook_info = self._scan_modifier_hooks[func.__name__]
        scan_name = getattr(self, "scan_name", None)

        before_method_name = _get_hook_method_name(func.__name__, hook_info, "before", scan_name)
        if before_method_name is not None:
            before_method = getattr(self._scan_modifier, before_method_name)
            before_method(*args, **kwargs)

        replace_method_name = _get_hook_method_name(func.__name__, hook_info, "replace", scan_name)
        if replace_method_name is not None:
            replace_method = getattr(self._scan_modifier, replace_method_name)
            replace_method(*args, **kwargs)
        else:
            func(self, *args, **kwargs)

        after_method_name = _get_hook_method_name(func.__name__, hook_info, "after", scan_name)
        if after_method_name is not None:
            after_method = getattr(self._scan_modifier, after_method_name)
            after_method(*args, **kwargs)

        return

    # pylint: disable=protected-access
    wrapper._scan_hook_info = {"method_name": func.__name__}  # type: ignore
    wrapper._scan_hook_original = func  # type: ignore[attr-defined]

    return wrapper


def scan_hook_impl(
    hook_name: ScanHookName, hook_type: HookType = "before", scan_names: list[str] | None = None
):
    """
    Register a scan modifier method as an implementation of a scan hook lifecycle.

    Args:
        hook_name (ScanHookName): Name of the scan hook to attach to.
        hook_type (HookType): Lifecycle stage in which the
            modifier method should run. ``"before"`` runs ahead of the original hook,
            ``"after"`` runs after it, and ``"replace"`` runs instead of it.
        scan_names (list[str] | None): Optional list of scan-name patterns that restrict when
            this implementation applies. Patterns use shell-style wildcards such as
            ``"*_line_scan"``. If ``None``, the implementation applies to all scan names.

    Returns:
        Callable: A decorator that annotates the wrapped method with scan hook metadata.

    Raises:
        ValueError: If ``hook_name`` is not a supported hook, if ``hook_type`` is invalid, or if
            ``scan_names`` is not a list of strings.
    """
    if hook_name not in VALID_SCAN_HOOKS:
        raise ValueError(f"Invalid scan hook: {hook_name}")
    if hook_type not in {"before", "after", "replace"}:
        raise ValueError(f"Invalid scan hook type: {hook_type}")
    if scan_names is not None and not isinstance(scan_names, list):
        raise ValueError("scan_names must be a list of scan name patterns")
    if scan_names is not None and any(not isinstance(pattern, str) for pattern in scan_names):
        raise ValueError("scan_names must contain only string scan name patterns")

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        # pylint: disable=protected-access
        wrapper._scan_hook_impl_info = {
            "hook_name": hook_name,
            "hook_type": hook_type,
            "scan_names": scan_names,
        }  # type: ignore

        return wrapper

    return decorator


def get_scan_hooks_impl(cls) -> dict[str, ScanHookConfigMap]:
    """
    Get the scan hooks implemented by the given class. It returns
    a dictionary mapping the original scan hook names to the corresponding method names and hook types in the scan modifier.

    """
    hooks: dict[str, ScanHookConfigMap] = {}
    for attr_name in dir(cls):
        attr = getattr(cls, attr_name)
        if callable(attr) and hasattr(attr, "_scan_hook_impl_info"):
            info = attr._scan_hook_impl_info
            hook_name = info["hook_name"]
            hook_type = info["hook_type"]
            if hook_name not in hooks:
                hooks[hook_name] = {}
            scan_names = info.get("scan_names")
            hook_config: HookConfig
            if scan_names is None:
                hook_config = attr_name
            else:
                hook_config = {"method_name": attr_name, "scan_names": scan_names}
            existing_hook_config = hooks[hook_name].get(hook_type)
            if existing_hook_config is None:
                hooks[hook_name][hook_type] = hook_config
            elif isinstance(existing_hook_config, list):
                existing_hook_config.append(hook_config)
            else:
                hooks[hook_name][hook_type] = [existing_hook_config, hook_config]
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

    def call_original(self, hook_name: ScanHookName, *args, **kwargs):
        """
        Call the scan's original hook implementation directly, bypassing scan modifier dispatch.

        Args:
            hook_name (ScanHookName): Name of the original scan hook to call.
            *args: Positional arguments forwarded to the original hook.
            **kwargs: Keyword arguments forwarded to the original hook.

        Returns:
            Any: The return value of the original hook implementation.

        Raises:
            AttributeError: If the scan does not expose an original implementation for the hook.
        """
        original_hooks = getattr(self.scan, "_scan_original_hooks", {})
        try:
            original_hook = original_hooks[hook_name]
        except KeyError as exc:
            raise AttributeError(
                f"Scan {type(self.scan).__name__!r} does not expose an original hook for {hook_name!r}"
            ) from exc
        return original_hook(*args, **kwargs)
