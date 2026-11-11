"""
Helpers for extracting device constructor kwargs from BEC device config.

BEC device config serves two different phases of device setup:

* constructor kwargs, used immediately to instantiate the ophyd object
* post-init config, applied later by the device manager through ``update_config``

Most constructor kwargs can be identified from the device class signature. A few
device classes accept additional constructor-only options through ``**kwargs``;
those are forwarded unless the key names an exposed signal/property or another
config key that the device manager is expected to apply after construction.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any

import ophyd
from ophyd.signal import EpicsSignalBase
from ophyd_devices import PSIDeviceBase

POST_INIT_CONFIG_KEYS = {"device_access", "device_mapping", "labels", "limits"}


@dataclass(frozen=True)
class DeviceConfigExtraction:
    """Constructor kwargs plus config that should remain for post-init updates."""

    init_kwargs: dict[str, Any]
    post_init_config: dict[str, Any]


@dataclass(frozen=True)
class _DeviceConfigSplit:
    """Internal split result that still carries device-manager injection hints."""

    init_kwargs: dict[str, Any]
    post_init_config: dict[str, Any]
    device_access: Any
    has_device_mapping: bool


def extract_device_config(
    dev: dict, dev_cls: type, device_manager: Any | None
) -> DeviceConfigExtraction:
    """
    Extract constructor kwargs and post-init config for one device.

    This is the public entry point used by device construction. It first splits
    the static ``deviceConfig`` values into constructor kwargs and post-init
    config, then injects runtime-only dependencies:

    * ``device_manager`` is injected for ``PSIDeviceBase`` subclasses, when
      explicitly requested by the class signature, or for legacy device configs
      that signal device-manager access through ``device_access`` /
      ``device_mapping``.
    * ``scan_info`` is injected for ``PSIDeviceBase`` subclasses or when
      explicitly requested by the class signature.
    """
    config_split = _split_device_config(dev, dev_cls)
    init_kwargs = config_split.init_kwargs.copy()

    is_psi_device = _is_psi_device_class(dev_cls)
    if (
        is_psi_device
        or config_split.device_access
        or (config_split.device_access is None and config_split.has_device_mapping)
    ):
        init_kwargs["device_manager"] = device_manager

    signature = inspect.signature(dev_cls)
    if "device_manager" in signature.parameters:
        init_kwargs["device_manager"] = device_manager
    if is_psi_device or "scan_info" in signature.parameters:
        # static_device_test calls construction with device_manager=None.
        init_kwargs["scan_info"] = device_manager.scan_info if device_manager else None

    return DeviceConfigExtraction(
        init_kwargs=init_kwargs, post_init_config=config_split.post_init_config
    )


def _split_device_config(dev: dict, dev_cls: type) -> _DeviceConfigSplit:
    """
    Split static config values into constructor kwargs and post-init config.

    The split has three steps:

    1. Copy explicit constructor parameters from the device config into
       ``init_kwargs``. For ophyd subclasses, include selected ophyd base-class
       signatures because some devices inherit constructor parameters from
       those bases.
    2. If the device accepts arbitrary ``**kwargs``, forward remaining keys that
       are not known post-init config attributes. This supports devices that
       intentionally hide constructor-only options behind ``**kwargs``.
    3. Return the leftover config for ``initialize_device`` / ``update_config``.
       ``device_access`` itself is consumed here, but ``device_mapping`` is
       preserved because proxy registration uses it after construction.

    Classes implementing ``_update_device_config`` receive their full remaining
    config after construction, so unknown keys are not forced into their
    constructor even if they accept ``**kwargs``.
    """
    config = {**(dev.get("deviceConfig") or {}), "name": dev.get("name")}
    init_param_names = _get_constructor_param_names(dev_cls)

    init_kwargs = {key: value for key, value in config.items() if key in init_param_names}
    remaining_config = {key: value for key, value in config.items() if key not in init_param_names}
    device_access = remaining_config.get("device_access")
    has_device_mapping = bool(remaining_config.get("device_mapping"))

    # Some device classes hide constructor-only options behind **kwargs. Forward
    # those, but leave attributes that update_config can apply after init.
    if _device_class_accepts_kwargs(dev_cls) and not _device_class_handles_config_update(dev_cls):
        config_attrs = _get_device_class_config_attrs(dev_cls)
        init_kwargs.update(
            {key: value for key, value in remaining_config.items() if key not in config_attrs}
        )
        remaining_config = {
            key: value for key, value in remaining_config.items() if key in config_attrs
        }

    post_init_config = {
        key: value for key, value in remaining_config.items() if key != "device_access"
    }
    return _DeviceConfigSplit(
        init_kwargs=init_kwargs,
        post_init_config=post_init_config,
        device_access=device_access,
        has_device_mapping=has_device_mapping,
    )


def _get_constructor_param_names(dev_cls: type) -> set[str]:
    """Return constructor parameter names considered safe to pass explicitly."""
    device_classes = [dev_cls]
    if issubclass(dev_cls, ophyd.Signal):
        device_classes.append(ophyd.Signal)
    if issubclass(dev_cls, EpicsSignalBase):
        device_classes.append(EpicsSignalBase)
    if issubclass(dev_cls, ophyd.OphydObject):
        device_classes.append(ophyd.OphydObject)

    return {
        param
        for device_class in device_classes
        for param in inspect.signature(device_class).parameters
    }


def _device_class_accepts_kwargs(dev_cls: type) -> bool:
    """Return whether the class constructor accepts arbitrary keyword arguments."""
    return any(
        param.kind is inspect.Parameter.VAR_KEYWORD
        for param in inspect.signature(dev_cls).parameters.values()
    )


def _is_psi_device_class(dev_cls: type) -> bool:
    """Return whether the device class derives from ``PSIDeviceBase``."""
    return issubclass(dev_cls, PSIDeviceBase)


def _get_device_class_config_attrs(dev_cls: type) -> set[str]:
    """
    Return attribute names that should stay in post-init config.

    These include ophyd components, Python properties, cached ophyd signal
    attributes, and a few device-manager config keys that are interpreted after
    object construction.
    """
    config_attrs = set(POST_INIT_CONFIG_KEYS)
    for cls in reversed(dev_cls.mro()):
        for name, attr in vars(cls).items():
            if isinstance(attr, ophyd.Component) or isinstance(attr, property):
                config_attrs.add(name)
    config_attrs.update((getattr(dev_cls, "_sig_attrs", None) or {}).keys())
    return config_attrs


def _device_class_handles_config_update(dev_cls: type) -> bool:
    """Return whether the class wants remaining config via ``_update_device_config``."""
    return callable(getattr(dev_cls, "_update_device_config", None))
