"""These four assignments must be rejected by the boundary checker."""

from bec_lib.device_backend import Completion, DeviceHandler, DeviceProvider, Readings


class InvalidCompletion:
    done: bool = False

    def result(self, timeout: float | None = None) -> str:
        return "wrong type"


completion: Completion[None] = InvalidCompletion()
handler: DeviceHandler = object()
provider: DeviceProvider = object()
readings: Readings = {"signal": {"native_alarm": 1}}
