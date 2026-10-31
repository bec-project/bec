"""Static producer/consumer conformance; never constructs hardware."""

from ophyd_devices.bec_adapters import OphydProvider

from bec_lib.device_backend import Completion, DeviceHandler, DeviceProvider, Operation, Readings


def conformance(provider: OphydProvider) -> tuple[DeviceProvider, DeviceHandler, Completion[None]]:
    handler: DeviceHandler = provider.wrap(object())
    completion: Completion[None] = Operation[None]()
    readings: Readings = {"signal": {"value": 1, "timestamp": 0.0}}
    return provider, handler, completion
