from __future__ import annotations

import keyword
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Generic, Type, TypeVar

from pydantic import BaseModel, field_validator

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector

if TYPE_CHECKING:
    from bec_lib.redis_connector import MessageObject, RedisConnector


class BeamlineStateConfig(BaseModel):
    """
    Base Configuration for a beamline state.
    """

    state_type: ClassVar[str] = "BeamlineState"

    name: str
    title: str | None = None

    model_config = {"extra": "forbid", "arbitrary_types_allowed": True}

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """
        Validate that the state name is a valid Python identifier and does not conflict with reserved method names.
        """
        if not v.isidentifier():
            raise ValueError(f"State name '{v}' must be a valid Python identifier.")
        if keyword.iskeyword(v):
            raise ValueError(f"State name '{v}' cannot be a reserved Python keyword.")
        if v in {"add", "remove", "show_all"}:
            raise ValueError(f"State name '{v}' is reserved and cannot be used.")
        return v


class DeviceStateConfig(BeamlineStateConfig):
    """
    Configuration for a device-based beamline state.
    """

    state_type: ClassVar[str] = "DeviceState"

    device: DeviceBase | str
    signal: DeviceBase | str | None = None

    @field_validator("device", "signal", mode="before")
    @classmethod
    def validate_device(cls, v: DeviceBase | str) -> str:
        """
        Validate that the device is either a string or a DeviceBase instance. If it's a DeviceBase instance, return its name.
        """
        if isinstance(v, DeviceBase):
            return v.dotted_name
        return v


class DeviceWithinLimitsStateConfig(DeviceStateConfig):
    """
    Configuration for a device within limits beamline state.
    """

    state_type: ClassVar[str] = "DeviceWithinLimitsState"

    min_limit: float | None = None
    max_limit: float | None = None
    tolerance: float = 0.1


C = TypeVar("C", bound=BeamlineStateConfig)
D = TypeVar("D", bound=DeviceStateConfig)


class BeamlineState(ABC, Generic[C]):
    """Abstract base class for beamline states."""

    CONFIG_CLASS: Type[C]

    def __init__(
        self, config: C | None = None, redis_connector: RedisConnector | None = None, **kwargs
    ) -> None:
        self.config = config or self.CONFIG_CLASS(**kwargs)
        self.connector = redis_connector
        self._last_state: messages.BeamlineStateMessage | None = None

    def update_parameters(self, **kwargs) -> None:
        """Update the configuration parameters of the state."""
        self.config = self.CONFIG_CLASS(**{**self.config.model_dump(), **kwargs})

    @abstractmethod
    def evaluate(self, *args, **kwargs) -> messages.BeamlineStateMessage | None:
        """Evaluate the state and return its state."""

    def start(self) -> None:
        """Start monitoring the state if needed."""

    def stop(self) -> None:
        """Stop monitoring the state if needed."""

    def restart(self) -> None:
        """Restart the state monitoring."""
        self.stop()
        self.start()

    def _emit_state(self, state_msg: messages.BeamlineStateMessage) -> None:
        if self.connector is None:
            return
        is_different = (
            state_msg.model_dump(exclude={"timestamp"})
            != self._last_state.model_dump(exclude={"timestamp"})
            if self._last_state
            else True
        )
        if self._last_state is None:
            is_different = True
        if is_different:
            self._last_state = state_msg
            self.connector.xadd(
                MessageEndpoints.beamline_state(self.config.name),
                {"data": state_msg},
                max_size=1,
                approximate=False,
            )


class DeviceBeamlineState(BeamlineState[D], Generic[D]):
    """A beamline state that depends on a device reading."""

    CONFIG_CLASS: Type[D]

    def __init__(
        self, config: D | None = None, redis_connector: RedisConnector | None = None, **kwargs
    ) -> None:
        super().__init__(config, redis_connector, **kwargs)

    def start(self) -> None:
        if self.connector is None:
            raise RuntimeError("Redis connector is not set.")
        self.connector.register(
            MessageEndpoints.device_readback(self.config.device),
            cb=self._update_device_state,
            parent=self,
        )

    def stop(self) -> None:
        if self.connector is None:
            return
        self.connector.unregister(
            MessageEndpoints.device_readback(self.config.device), cb=self._update_device_state
        )

    @staticmethod
    def _update_device_state(msg_obj: MessageObject, parent: DeviceBeamlineState) -> None:

        # Since this is called from the Redis connector, we
        assert parent.connector is not None

        msg: messages.DeviceMessage = msg_obj.value  # type: ignore ; we know it's a DeviceMessage
        out = parent.evaluate(msg)
        if out is None:
            return
        parent._emit_state(out)


class ShutterState(DeviceBeamlineState[DeviceStateConfig]):
    """
    A state that checks if the shutter is open.

    Example:
        shutter_state = ShutterState(name="shutter_open")
        shutter_state.configure(device="shutter1")
        bec.beamline_states.add(shutter_state)
    """

    CONFIG_CLASS = DeviceStateConfig

    def evaluate(
        self, msg: messages.DeviceMessage, *args, **kwargs
    ) -> messages.BeamlineStateMessage:
        val = msg.signals.get(self.config.signal, {}).get("value", "").lower()
        if val == "open":
            return messages.BeamlineStateMessage(
                name=self.config.name, status="valid", label="Shutter is open."
            )
        return messages.BeamlineStateMessage(
            name=self.config.name, status="invalid", label="Shutter is closed."
        )


class DeviceWithinLimitsState(DeviceBeamlineState[DeviceWithinLimitsStateConfig]):
    """
    A state that checks if a positioner is within limits.

    Example:
        device_state = DeviceWithinLimitsState(name="sample_x_within_limits")
        device_state.configure(device="sample_x", signal="sample_x_signal_name", min_limit=0.0, max_limit=10.0)
        bec.beamline_states.add(device_state)

    """

    CONFIG_CLASS = DeviceWithinLimitsStateConfig

    def evaluate(
        self, msg: messages.DeviceMessage, *args, **kwargs
    ) -> messages.BeamlineStateMessage:
        """
        Evaluate if the positioner is within the defined limits. If it is outside the limits,
        return an invalid state. Otherwise, return a valid state. If it is within 10% of the limits,
        return a warning state.
        """

        if self.config.min_limit is None:
            self.config.min_limit = float("-inf")
        if self.config.max_limit is None:
            self.config.max_limit = float("inf")

        signal_name = self.config.signal if self.config.signal is not None else self.config.device

        val = msg.signals.get(signal_name, {}).get("value", None)
        if val is None:
            return messages.BeamlineStateMessage(
                name=self.config.name,
                status="invalid",
                label=f"Positioner {self.config.device} value not found.",
            )

        if val < self.config.min_limit or val > self.config.max_limit:
            return messages.BeamlineStateMessage(
                name=self.config.name,
                status="invalid",
                label=f"Positioner {self.config.device} out of limits",
            )

        min_warning_threshold = self.config.min_limit + self.config.tolerance
        max_warning_threshold = self.config.max_limit - self.config.tolerance

        if val < min_warning_threshold or val > max_warning_threshold:
            return messages.BeamlineStateMessage(
                name=self.config.name,
                status="warning",
                label=f"Positioner {self.config.device} near limits",
            )

        return messages.BeamlineStateMessage(
            name=self.config.name,
            status="valid",
            label=f"Positioner {self.config.device} within limits",
        )
