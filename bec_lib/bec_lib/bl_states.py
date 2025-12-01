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


class BeamlineStateManager:
    """Manager for beamline states."""

    def __init__(self, client: BECClient) -> None:
        self._client = client
        self._connector = client.connector
        self._states: list[messages.BeamlineStateConfig] = []
        self._connector.register(
            MessageEndpoints.available_beamline_states(),
            cb=self._on_state_update,
            parent=self,
            from_start=True,
        )

    @staticmethod
    def _on_state_update(msg_dict: dict, *, parent: BeamlineStateManager, **_kwargs) -> None:
        # type: ignore ; we know it's an AvailableBeamlineStatesMessage
        msg: messages.AvailableBeamlineStatesMessage = msg_dict["data"]
        parent._states = msg.states

    def add(self, state: BeamlineState) -> None:
        """
        Add a new beamline state to the manager.
        Args:
            state (BeamlineState): The beamline state to add.
        """

        if any(state.name == existing_state.name for existing_state in self._states):
            return  # state already exists
        info: messages.BeamlineStateConfig = messages.BeamlineStateConfig(
            name=state.name,
            title=state.title,
            state_type=state.__class__.__name__,
            parameters=state.parameters(),
        )
        cls = state.__class__

        try:
            condi = cls(name=state.name, redis_connector=self._connector)
            condi.configure(**state.parameters())
        except Exception as e:
            raise RuntimeError(f"Failed to add state {state.name}: {e}") from e

        if isinstance(state, DeviceBeamlineState):
            self._verify_signal_exists(state)

        self._states.append(info)
        msg = messages.AvailableBeamlineStatesMessage(states=self._states)
        self._connector.xadd(
            MessageEndpoints.available_beamline_states(), {"data": msg}, max_size=1
        )

    def _verify_signal_exists(self, state: DeviceBeamlineState) -> None:
        """
        Verify that the device and signal exist in the device manager.

        Args:
            state (DeviceBeamlineState): The state to verify.

        Raises: RuntimeError if the device or signal does not exist.
        """
        device = state.parameters().get("device")
        signal = state.parameters().get("signal")
        if isinstance(device, DeviceBase):
            device = device.name

        if not self._client.device_manager.devices.get(device):
            raise RuntimeError(
                f"Device {device} not found in device manager. Cannot add state {state.name}."
            )
        if signal is not None:
            if signal not in self._client.device_manager.devices[device].read():
                raise RuntimeError(
                    f"Signal {signal} not found in device {device}. Cannot add state {state.name}."
                )
        else:
            hinted_signals = self._client.device_manager.devices[device]._hints
            if hinted_signals:
                signal = hinted_signals[0]
            else:
                signal = device
        state.update_parameters(device=device, signal=signal)

    def remove(self, state_name: str) -> None:
        """
        Remove a beamline state by name.
        Args:
            state_name (str): The name of the state to remove.
        """
        if not any(state.name == state_name for state in self._states):
            return  # state does not exist
        self._states = [state for state in self._states if state.name != state_name]
        msg = messages.AvailableBeamlineStatesMessage(states=self._states)
        self._connector.xadd(
            MessageEndpoints.available_beamline_states(), {"data": msg}, max_size=1
        )

    def show_all(self):
        """
        Pretty print all beamline states using rich.
        """
        console = Console()
        table = Table(title="Beamline States")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Type", style="magenta")
        table.add_column("Parameters", style="green")

        for state in self._states:
            params = state.parameters if state.parameters else "-"
            table.add_row(str(state.name), str(state.state_type), str(params))

        console.print(table)


class BeamlineState(ABC):
    """Abstract base class for beamline states."""

    def __init__(
        self, name: str, redis_connector: RedisConnector | None = None, title: str | None = None
    ) -> None:
        self.name = name
        self.connector = redis_connector
        self.title = title if title is not None else name
        self._configured = False
        self._last_state: messages.BeamlineStateMessage | None = None

    def configure(self, **kwargs) -> None:
        """Configure the state with given parameters."""
        self._configured = True

    def parameters(self) -> dict:
        """Return the configuration parameters of the state."""
        return {}

    def update_parameters(self, **kwargs) -> None:
        """Update the configuration parameters of the state."""
        pass

    @abstractmethod
    def evaluate(self, *args, **kwargs) -> messages.BeamlineStateMessage | None:
        """Evaluate the state and return its state."""

    def start(self) -> None:
        """Start monitoring the state if needed."""

    def stop(self) -> None:
        """Stop monitoring the state if needed."""


class DeviceBeamlineState(BeamlineState):
    """A beamline state that depends on a device reading."""

    def configure(self, device: str | DeviceBase, signal: str | None = None, **kwargs) -> None:
        self.device = device if isinstance(device, str) else device.name
        self.signal = signal
        super().configure(**kwargs)

    def parameters(self) -> dict:
        params = super().parameters()
        params.update({"device": self.device, "signal": self.signal})
        return params

    def update_parameters(self, **kwargs) -> None:
        if "device" in kwargs:
            device = kwargs.pop("device")
            self.device = device if isinstance(device, str) else device.name
        if "signal" in kwargs:
            self.signal = kwargs.pop("signal")
        super().update_parameters(**kwargs)

    def start(self) -> None:
        if not self._configured:
            raise RuntimeError("State must be configured before starting.")
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
    def _update_device_state(msg_obj: MessageObject, parent: DeviceBeamlineState) -> None:

        # Since this is called from the Redis connector, we
        assert parent.connector is not None

        msg: messages.DeviceMessage = msg_obj.value  # type: ignore ; we know it's a DeviceMessage
        out = parent.evaluate(msg)
        if out is not None and out != parent._last_state:
            parent._last_state = out
            parent.connector.xadd(
                MessageEndpoints.beamline_state(parent.name), {"data": out}, max_size=1
            )


class ShutterState(DeviceBeamlineState):
    """
    A state that checks if the shutter is open.

    Example:
        shutter_state = ShutterState(name="shutter_open")
        shutter_state.configure(device="shutter1")
        bec.beamline_states.add(shutter_state)
    """

    def evaluate(self, msg: messages.DeviceMessage, **kwargs) -> messages.BeamlineStateMessage:
        val = msg.signals.get(self.signal, {}).get("value", "").lower()
        if val == "open":
            return messages.BeamlineStateMessage(
                name=self.name, status="valid", label="Shutter is open."
            )
        return messages.BeamlineStateMessage(
            name=self.name, status="invalid", label="Shutter is closed."
        )


class DeviceWithinLimitsState(DeviceBeamlineState):
    """
    A state that checks if a positioner is within limits.

    Example:
        device_state = DeviceWithinLimitsState(name="sample_x_within_limits")
        device_state.configure(device="sample_x", signal="sample_x_signal_name", min_limit=0.0, max_limit=10.0)
        bec.beamline_states.add(device_state)

    """

    def configure(
        self,
        device: str,
        min_limit: float | None = None,
        max_limit: float | None = None,
        tolerance: float = 0.1,
        signal: str | None = None,
        **kwargs,
    ) -> None:
        """
        Configure the positioner condition.

        Args:
            device (str): The name of the positioner device.
            min_limit (float | None): The minimum limit for the positioner. If None, no minimum limit is enforced.
            max_limit (float | None): The maximum limit for the positioner. If None, no maximum limit is enforced.
            tolerance (float): The tolerance for warning conditions (default is 0.1). When the positioner is within
                               10% of the limits, a warning condition will be issued. Note that the tolerance is ignored
                               if one of the limits is None.
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

    def update_parameters(self, **kwargs) -> None:
        if "min_limit" in kwargs:
            self.min_limit = kwargs.pop("min_limit")
        if "max_limit" in kwargs:
            self.max_limit = kwargs.pop("max_limit")
        if "tolerance" in kwargs:
            self.tolerance = kwargs.pop("tolerance")
        super().update_parameters(**kwargs)

    def evaluate(self, msg: messages.DeviceMessage, **kwargs) -> messages.BeamlineStateMessage:
        """
        Evaluate if the positioner is within the defined limits. If it is outside the limits,
        return an invalid state. Otherwise, return a valid state. If it is within 10% of the limits,
        return a warning state.
        """

        if self.min_limit is None:
            self.min_limit = float("-inf")
        if self.max_limit is None:
            self.max_limit = float("inf")

        signal_name = self.signal if self.signal is not None else self.device

        val = msg.signals.get(signal_name, {}).get("value", None)
        if val is None:
            return messages.BeamlineStateMessage(
                name=self.name, status="invalid", label=f"Positioner {self.device} value not found."
            )

        if val < self.min_limit or val > self.max_limit:
            return messages.BeamlineStateMessage(
                name=self.name, status="invalid", label=f"Positioner {self.device} out of limits"
            )

        if self.min_limit == float("-inf") or self.max_limit == float("inf"):
            self.tolerance = 0

        min_warning_threshold = self.min_limit + self.tolerance * (self.max_limit - self.min_limit)
        max_warning_threshold = self.max_limit - self.tolerance * (self.max_limit - self.min_limit)
        if val < min_warning_threshold or val > max_warning_threshold:
            return messages.BeamlineStateMessage(
                name=self.name, status="warning", label=f"Positioner {self.device} near limits"
            )

        return messages.BeamlineStateMessage(
            name=self.name, status="valid", label=f"Positioner {self.device} within limits"
        )
