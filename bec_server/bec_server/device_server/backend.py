"""Load a device-library provider at the explicit service construction boundary."""

from __future__ import annotations

from importlib import import_module

from bec_lib.device_backend import DeviceProvider


def create_device_provider() -> DeviceProvider:
    """Create the coordinated device-library implementation lazily.

    Importing server dispatch or neutral contracts must not import a hardware HAL.
    Tests and embedding services can instead inject any DeviceProvider.
    """
    try:
        module = import_module("ophyd_devices.bec_adapters")
    except ModuleNotFoundError as error:
        if error.name not in ("ophyd_devices", "ophyd_devices.bec_adapters"):
            raise
        raise RuntimeError(
            "This BEC device server requires the matching ophyd_devices release "
            "with bec_adapters support. Upgrade both packages together."
        ) from error
    return module.OphydProvider()


def legacy_convert_value(device: object, value: object) -> object:
    """Retain the old service helper without exposing native signal metadata in core dispatch."""
    module = import_module("ophyd_devices.bec_adapters")
    return module.convert_value(device, value)
