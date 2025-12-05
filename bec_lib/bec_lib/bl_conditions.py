from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib.client import BECClient
    from bec_lib.redis_connector import MessageObject, RedisConnector


class BeamlineConditionConfig:
    """Manager for beamline conditions."""

    def __init__(self, client: BECClient) -> None:
        self._client = client
        self._connector = client.connector
        self._conditions: list[messages.BeamlineConditionUpdateEntry] = []
        self._connector.register(
            MessageEndpoints.available_beamline_conditions(),
            cb=self._on_condition_update,
            parent=self,
            from_start=True,
        )

    @staticmethod
    def _on_condition_update(msg_dict: dict, *, parent: BeamlineConditionConfig, **_kwargs) -> None:
        msg: messages.BeamlineConditionUpdate = msg_dict["data"]  # type: ignore ; we know it's a BeamlineConditionUpdateMessage
        parent._conditions = msg.conditions

    def add(self, condition: BeamlineCondition) -> None:
        """
        Add a new beamline condition to the manager.
        Args:
            condition (BeamlineCondition): The beamline condition to add.
        """

        if any(cond.name == condition.name for cond in self._conditions):
            return  # condition already exists

        info: messages.BeamlineConditionUpdateEntry = messages.BeamlineConditionUpdateEntry(
            name=condition.name,
            title=condition.title,
            condition_type=condition.__class__.__name__,
            parameters=condition.parameters(),
        )
        cls = condition.__class__

        try:
            condi = cls(name=condition.name, redis_connector=self._connector)
            condi.configure(**condition.parameters())
        except Exception as e:
            raise RuntimeError(f"Failed to add condition {condition.name}: {e}") from e

        if isinstance(condition, DeviceBeamlineCondition):
            self._verify_signal_exists(condition)

        self._conditions.append(info)
        msg = messages.AvailableBeamlineConditionsMessage(conditions=self._conditions)
        self._connector.xadd(
            MessageEndpoints.available_beamline_conditions(), {"data": msg}, max_size=1
        )

    def _verify_signal_exists(self, condition: DeviceBeamlineCondition) -> None:
        """
        Verify that the device and signal exist in the device manager.

        Args:
            condition (DeviceBeamlineCondition): The condition to verify.

        Raises: RuntimeError if the device or signal does not exist.
        """
        device = condition.parameters().get("device")
        signal = condition.parameters().get("signal")
        if isinstance(device, DeviceBase):
            device = device.name

        if not self._client.device_manager.devices.get(device):
            raise RuntimeError(
                f"Device {device} not found in device manager. Cannot add condition {condition.name}."
            )
        if signal is not None:
            if signal not in self._client.device_manager.devices[device].read():
                raise RuntimeError(
                    f"Signal {signal} not found in device {device}. Cannot add condition {condition.name}."
                )
        else:
            hinted_signals = self._client.device_manager.devices[device]._hints
            if hinted_signals:
                signal = hinted_signals[0]
            else:
                signal = device
        condition.parameters().update({"device": device, "signal": signal})

    def remove(self, condition_name: str) -> None:
        """
        Remove a beamline condition by name.

        Args:
            condition_name (str): The name of the condition to remove.
        """
        if not any(cond.name == condition_name for cond in self._conditions):
            return  # condition does not exist
        self._conditions = [cond for cond in self._conditions if cond.name != condition_name]
        msg = messages.AvailableBeamlineConditionsMessage(conditions=self._conditions)
        self._connector.xadd(
            MessageEndpoints.available_beamline_conditions(), {"data": msg}, max_size=1
        )

    def show_all(self):
        """
        Pretty print all beamline conditions using rich.
        """
        console = Console()
        table = Table(title="Beamline Conditions")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Type", style="magenta")
        table.add_column("Parameters", style="green")

        for cond in self._conditions:
            params = cond.parameters if cond.parameters else "-"
            table.add_row(str(cond.name), str(cond.condition_type), str(params))

        console.print(table)


class BeamlineCondition(ABC):
    """Abstract base class for beamline conditions."""

    def __init__(
        self, name: str, redis_connector: RedisConnector | None = None, title: str | None = None
    ) -> None:
        self.name = name
        self.connector = redis_connector
        self.title = title if title is not None else name
        self._configured = False
        self._last_state: messages.BeamlineConditionMessage | None = None

    def configure(self, **kwargs) -> None:
        """Configure the condition with given parameters."""
        self._configured = True

    def parameters(self) -> dict:
        """Return the configuration parameters of the condition."""
        return {}

    @abstractmethod
    def evaluate(self, *args, **kwargs) -> messages.BeamlineConditionMessage | None:
        """Evaluate the condition and return its state."""

    def start(self) -> None:
        """Start monitoring the condition if needed."""

    def stop(self) -> None:
        """Stop monitoring the condition if needed."""


class DeviceBeamlineCondition(BeamlineCondition):
    """A beamline condition that depends on a device reading."""

    def configure(self, device: str | DeviceBase, signal: str | None = None, **kwargs) -> None:
        self.device = device if isinstance(device, str) else device.name
        self.signal = signal
        super().configure(**kwargs)

    def parameters(self) -> dict:
        params = super().parameters()
        params.update({"device": self.device, "signal": self.signal})
        return params

    def start(self) -> None:
        if not self._configured:
            raise RuntimeError("Condition must be configured before starting.")
        if self.connector is None:
            raise RuntimeError("Redis connector is not set.")
        self.connector.register(
            MessageEndpoints.device_readback(self.device), cb=self._update_device_state, parent=self
        )

    def stop(self) -> None:
        if not self._configured:
            return
        if self.connector is None:
            return
        self.connector.unregister(
            MessageEndpoints.device_readback(self.device), cb=self._update_device_state
        )

    @staticmethod
    def _update_device_state(msg_obj: MessageObject, parent: DeviceBeamlineCondition) -> None:

        # Since this is called from the Redis connector, we
        assert parent.connector is not None

        msg: messages.DeviceMessage = msg_obj.value  # type: ignore ; we know it's a DeviceMessage
        out = parent.evaluate(msg)
        if out is not None and out != parent._last_state:
            parent._last_state = out
            parent.connector.xadd(
                MessageEndpoints.beamline_condition(parent.name), {"data": out}, max_size=1
            )


class ShutterCondition(DeviceBeamlineCondition):
    """
    A condition that checks if the shutter is open.

    Example:
        shutter_condition = ShutterCondition(name="shutter_open")
        shutter_condition.configure(device="shutter1")
        bec.beamline_conditions.add(shutter_condition)
    """

    def evaluate(self, msg: messages.DeviceMessage, **kwargs) -> messages.BeamlineConditionMessage:
        val = msg.signals.get(self.signal, {}).get("value", "").lower()
        if val == "open":
            return messages.BeamlineConditionMessage(
                name=self.name, status="normal", message="Shutter is open."
            )
        return messages.BeamlineConditionMessage(
            name=self.name, status="alarm", message="Shutter is closed."
        )


class DeviceWithinLimitsCondition(DeviceBeamlineCondition):
    """
    A condition that checks if a positioner is within limits.

    Example:
        device_condition = DeviceWithinLimitsCondition(name="sample_x_within_limits")
        device_condition.configure(device="sample_x", signal="sample_x_signal_name", min_limit=0.0, max_limit=10.0)
        bec.beamline_conditions.add(device_condition)

    """

    def configure(
        self,
        device: str,
        min_limit: float,
        max_limit: float,
        tolerance: float = 0.1,
        signal: str | None = None,
        **kwargs,
    ) -> None:
        """
        Configure the positioner condition.

        Args:
            device (str): The name of the positioner device.
            min_limit (float): The minimum limit for the positioner.
            max_limit (float): The maximum limit for the positioner.
            tolerance (float): The tolerance for warning conditions (default is 0.1). When the positioner is within
                               10% of the limits, a warning condition will be issued.
            signal (str, optional): The name of the signal to monitor. If not provided, defaults to the device name.
        """
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.tolerance = tolerance
        super().configure(device=device, signal=signal, **kwargs)

    def parameters(self) -> dict:
        params = super().parameters()
        params.update(
            {
                "device": self.device,
                "min_limit": self.min_limit,
                "max_limit": self.max_limit,
                "tolerance": self.tolerance,
                "signal": self.signal,
            }
        )
        return params

    def evaluate(self, msg: messages.DeviceMessage, **kwargs) -> messages.BeamlineConditionMessage:
        """
        Evaluate if the positioner is within the defined limits. If it is outside the limits,
        return an alarm condition. Otherwise, return a normal condition. If it is within 10% of the limits,
        return a warning condition.
        """

        val = msg.signals.get(self.device, {}).get("value", None)
        if val is None:
            return messages.BeamlineConditionMessage(
                name=self.name, status="alarm", message=f"Positioner {self.device} value not found."
            )

        if val < self.min_limit or val > self.max_limit:

            return messages.BeamlineConditionMessage(
                name=self.name, status="alarm", message=f"Positioner {self.device} out of limits"
            )
        if val < self.min_limit + self.tolerance * (
            self.max_limit - self.min_limit
        ) or val > self.max_limit - self.tolerance * (self.max_limit - self.min_limit):
            return messages.BeamlineConditionMessage(
                name=self.name, status="warning", message=f"Positioner {self.device} near limits"
            )

        return messages.BeamlineConditionMessage(
            name=self.name, status="normal", message=f"Positioner {self.device} within limits"
        )
