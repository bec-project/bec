"""Module defining beamline states and their evaluation logic."""

from __future__ import annotations

import functools
import keyword
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Generic, Literal, Type, TypeVar, cast

import yaml
from pydantic import BaseModel, field_validator, model_validator

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.device import DeviceBase, Signal
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject, RedisConnector


def with_state_error_handling(func: Callable) -> Callable:
    """
    Decorator for handling exceptions in state evaluation methods.

    This decorator:
    1. Calls update_device_signal_info() on the parent object before executing the function
    2. If update_device_signal_info() fails, emits an "unknown" state and raises an alarm
    3. If the decorated function fails, emits an "unknown" state and raises an alarm

    The decorated function should expect a 'parent' parameter of type DeviceBeamlineState.
    This could be the 'self' parameter of an instance method on a DeviceBeamlineState subclass.
    """

    @functools.wraps(func)
    def wrapper(parent: "DeviceBeamlineState", msg_obj: MessageObject) -> None:
        assert parent.connector is not None

        try:
            parent.update_device_signal_info()
        except Exception as exc:
            parent._handle_state_exception(exc)
            return

        try:
            result = func(parent, msg_obj)
            if result is not None:
                parent._emit_state(result)
        except Exception as exc:
            parent._handle_state_exception(exc)

    return wrapper


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

    state_type: ClassVar[str] = "DeviceBeamlineState"

    device: DeviceBase | str
    signal: DeviceBase | str | None = None

    @model_validator(mode="after")
    def validate_signal(self) -> DeviceStateConfig:
        """
        Validate that the signal is either None, a string, or a DeviceBase instance. If it's a DeviceBase instance, return its name.
        """
        if self.signal is None:
            return self
        if isinstance(self.signal, DeviceBase) and not isinstance(self.signal, Signal):
            raise ValueError(
                f"Signal must be a string or a Signal instance, got {type(self.signal)}"
            )
        if isinstance(self.device, DeviceBase) and isinstance(self.signal, DeviceBase):
            if self.signal.parent != self.device:
                raise ValueError(
                    f"Signal '{self.signal.dotted_name}' does not belong to device '{self.device.dotted_name}'"
                )
        if isinstance(self.device, DeviceBase):
            self.device = self.device.dotted_name
        if isinstance(self.signal, DeviceBase):
            self.signal = self.signal.dotted_name
        return self


class DeviceWithinLimitsStateConfig(DeviceStateConfig):
    """
    Configuration for a device within limits beamline state.
    """

    state_type: ClassVar[str] = "DeviceWithinLimitsState"

    low_limit: float | None = None
    high_limit: float | None = None
    tolerance: float = 0.1


class SignalConfig(BaseModel):
    """Target value for a signal inside a named machine state."""

    value: float | int | str | bool
    abs_tol: float = 0.0


class DeviceConfig(BaseModel):
    """Configuration for a device inside a named machine state."""

    abs_tol: float = 0.0
    value: float | int | str | bool | None = None
    low_limit: SignalConfig | None = None
    high_limit: SignalConfig | None = None
    signals: dict[str, SignalConfig] | None = None

    @model_validator(mode="after")
    def validate_config(self) -> DeviceConfig:
        """
        Validate that either value, low_limit, high_limit, or signals are provided.
        """
        if (
            self.value is None
            and self.low_limit is None
            and self.high_limit is None
            and self.signals is None
        ):
            raise ValueError(
                "At least one of value, low_limit, high_limit, or signals must be provided."
            )
        return self


class SubDeviceStateConfig(BaseModel):
    """
    Configuration for a sub-state with a specific label.
    This is a device/signal mappping to either a DeviceConfig or SignalConfig.
    """

    devices: dict[str, DeviceConfig | SignalConfig]
    transition_metadata: dict[str, Any] | None = None


class AggregatedStateConfig(BeamlineStateConfig):
    """
    Configuration for a state machine driven by multiple device signals.

    Keys of the states dictionary are the labels of the different states.
    """

    state_type: ClassVar[str] = "AggregatedState"

    states: dict[str, SubDeviceStateConfig]


C = TypeVar("C", bound=BeamlineStateConfig)
D = TypeVar("D", bound=DeviceStateConfig)


class BeamlineState(ABC, Generic[C]):
    """Abstract base class for beamline states."""

    CONFIG_CLASS: Type[C]

    def __init__(
        self,
        config: C | None = None,
        redis_connector: RedisConnector | None = None,
        device_manager: DeviceManagerBase | None = None,
        **kwargs,
    ) -> None:
        self.config = config or self.CONFIG_CLASS(**kwargs)
        self.connector = redis_connector
        self.device_manager = device_manager
        self.raised_warning = False
        self.started = False
        self._last_state: messages.BeamlineStateMessage | None = None
        self._error_prefix = f"[BL State {self.config.name}]:"

    def update_parameters(self, **kwargs) -> None:
        """Update the configuration parameters of the state."""
        self.config = self.CONFIG_CLASS(**{**self.config.model_dump(), **kwargs})

    @abstractmethod
    def evaluate(self, *args, **kwargs) -> messages.BeamlineStateMessage | None:
        """Evaluate the state and return its state."""

    def start(self) -> None:
        """Start monitoring the state if needed."""
        self.started = True
        self.raised_warning = False

    def stop(self) -> None:
        """Stop monitoring the state if needed."""
        self.started = False

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

    def _handle_state_exception(self, exc: Exception) -> None:
        """
        Handle exceptions that occur during state evaluation by emitting an "unknown" state and raising an alarm.

        Args:
            exc (Exception): The exception that occurred.
        """
        traceback_content = traceback.format_exc()
        info = exc.args[0] if exc.args else traceback_content

        if self.connector is not None and not self.raised_warning:
            error_info = messages.ErrorInfo(
                exception_type=type(exc).__name__,
                error_message=traceback_content,
                compact_error_message=info,
            )
            self.connector.raise_alarm(severity=Alarms.WARNING, info=error_info)

        out = messages.BeamlineStateMessage(name=self.config.name, status="unknown", label=info)
        self._emit_state(out)
        self.raised_warning = True


class DeviceBeamlineState(BeamlineState[D], Generic[D]):
    """A beamline state that depends on a device reading."""

    CONFIG_CLASS: Type[D]

    def update_device_signal_info(self) -> None:
        if self.device_manager is None:
            from bec_lib.client import BECClient

            bec = BECClient()  # fetch the singleton instance of the BECClient
            dev = bec.device_manager.devices
        else:
            dev = self.device_manager.devices

        try:
            self.device_obj: DeviceBase = dev[self.config.device]
        except KeyError:
            # pylint: disable=raise-missing-from
            raise ValueError(f"{self._error_prefix} Device '{self.config.device}' not found.")

        if self.config.signal is not None:
            signal = cast(str, self.config.signal)
            # We support two options here:
            # 1) The signal is the dotted name
            # 2) The signal is the obj_name of the signal, i.e. the entry in the device's read dictionary
            # We can distinguish these two cases by checking if the signal name contains a dot or not.
            if "." in signal:
                try:
                    signal_obj = dev[signal]
                except AttributeError:
                    # pylint: disable=raise-missing-from
                    raise ValueError(
                        f"{self._error_prefix} Signal '{signal}' not found for device '{self.config.device}'."
                    )
                if signal_obj.parent != self.device_obj:
                    raise ValueError(
                        f"{self._error_prefix} Signal '{signal}' does not belong to device '{self.config.device}'"
                    )

                signal_component = ".".join(signal.split(".")[1:])
                self.signal_name = self.device_obj.root._info["signals"][signal_component][
                    "obj_name"
                ]
            else:
                # The signal is the obj_name, so we need to find the corresponding signal
                self.signal_name = self.config.signal
                for sig_info in self.device_obj.root._info["signals"].values():
                    if sig_info["obj_name"] == self.signal_name:
                        break
                else:
                    raise ValueError(
                        f"{self._error_prefix} Signal '{self.signal_name}' not found for device '{self.config.device}'. "
                        f"Make sure to specify the correct signal name as seen in the device's read "
                        f"dictionary or use the full dotted name of the signal."
                    )

        else:
            # Take the hinted signal of the device
            if self.device_obj._hints:
                self.signal_name = self.device_obj._hints[0]
            else:
                raise ValueError(
                    f"[BL State {self.config.name}] No signal specified for device '{self.config.device}' and no hints available."
                )

    def start(self) -> None:
        if self.started:
            return
        super().start()

        if self.connector is None:
            raise RuntimeError("Redis connector is not set.")
        try:
            self.update_device_signal_info()
        except Exception as exc:
            self._handle_state_exception(exc)
            return

        msg = self.connector.get(MessageEndpoints.device_readback(self.device_obj.root.name))
        if msg is not None:
            self._update_device_state(
                MessageObject(
                    topic=MessageEndpoints.device_readback(self.device_obj.root.name).endpoint,
                    value=msg,
                )
            )
        self.connector.register(
            MessageEndpoints.device_readback(self.device_obj.root.name),
            cb=self._update_device_state,
        )

    def stop(self) -> None:
        if not self.started:
            return
        if self.connector is None:
            return
        self.connector.unregister(
            MessageEndpoints.device_readback(self.device_obj.root.name),
            cb=self._update_device_state,
        )

        super().stop()

    @with_state_error_handling
    def _update_device_state(self, msg_obj: MessageObject) -> messages.BeamlineStateMessage | None:
        """
        Update the device state based on the received message.
        """
        msg: messages.DeviceMessage = msg_obj.value  # type: ignore ; we know it's a DeviceMessage
        return self.evaluate(msg)


SignalSource = TypeVar("SignalSource", bound=Literal["readback", "configuration", "limits"])


@dataclass(frozen=True)
class ResolvedStateSignal:
    label: str
    device_name: str
    signal_name: str
    expected_value: float | int | str | bool
    abs_tolerance: float | int
    source: SignalSource


class AggregatedState(BeamlineState[AggregatedStateConfig]):
    """Beamline state that infers the current named state from multiple device signals."""

    CONFIG_CLASS = AggregatedStateConfig

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Mapping from signal updates to affected state labels, used for efficient evaluation when a signal update is received
        self._signal_info_to_labels: dict[tuple[str, SignalSource, str], set[str]] = {}
        # Mapping from state labels to the list of signal requirements that define that state
        self._requirements_for_label: dict[str, list[ResolvedStateSignal]] = {}
        # Set of subscriptions to signal updates
        self._subscriptions: set[tuple[str, SignalSource]] = set()
        # Cache of the latest signal values
        self._signal_value_cache: dict[tuple[str, SignalSource, str], Any] = {}
        # List of currently active state labels
        self._current_labels: list[str] = []

    @staticmethod
    def _endpoint(device: str, source: SignalSource):
        """Static method to get the appropriate message endpoint based on the signal source."""
        if source == "readback":
            return MessageEndpoints.device_readback(device)
        if source == "configuration":
            return MessageEndpoints.device_read_configuration(device)
        if source == "limits":
            return MessageEndpoints.device_limits(device)
        raise ValueError(
            f"Invalid signal source '{source}', please use 'readback', 'configuration', or 'limits'."
        )

    def _get_device_manager(self):
        if self.device_manager is None:
            # pylint: disable=import-outside-toplevel
            from bec_lib.client import BECClient

            bec = BECClient()
            return bec.device_manager
        return self.device_manager

    @staticmethod
    def _get_signal_source(signal_info: dict[str, Any], error_prefix: str) -> SignalSource:
        kind_str = str(signal_info.get("kind_str", "")).lower()
        if "hinted" in kind_str or "normal" in kind_str:
            return "readback"
        if "config" in kind_str:
            return "configuration"
        raise ValueError(
            f"{error_prefix} Unsupported kind: '{kind_str}' for signal : \n {yaml.dump(signal_info, indent=4)}"
        )

    @staticmethod
    def _resolve_signal(
        device_name: str, signal_name: str, device_manager: DeviceManagerBase, error_prefix: str
    ) -> tuple[str, SignalSource]:
        devices = device_manager.devices
        try:
            if not isinstance(device_name, str):
                raise ValueError(
                    f"{error_prefix} Device name must be a string, got {type(device_name)}"
                )
            device_obj: DeviceBase = devices[device_name]
        except KeyError:
            raise ValueError(f"{error_prefix} Device '{device_name}' not found.") from None

        # Special handling for limits, as they are not regular signals.
        if signal_name in ["low_limit", "low_limit_travel"]:
            return "low", "limits"
        if signal_name in ["high_limit", "high_limit_travel"]:
            return "high", "limits"

        signal_info = None
        # This case is relevant if we are looking at a Signal directly
        if device_name == signal_name and len(device_obj.root._info["signals"]) == 0:
            signal_info = {"obj_name": signal_name, "kind_str": "hinted"}
        # Case where we have a signal specified as a dotted name, e.g.
        elif "." in signal_name:
            try:
                signal_obj = devices[signal_name]
            except AttributeError:
                raise ValueError(
                    f"{error_prefix} Signal '{signal_name}' not found for device '{device_name}'."
                ) from None
            if signal_obj.parent != device_obj:
                raise ValueError(
                    f"{error_prefix} Signal '{signal_name}' does not belong to device '{device_name}'."
                )
            signal_component = ".".join(signal_name.split(".")[1:])
            signal_info = device_obj.root._info["signals"].get(signal_component)
        # Case where the signal is specified as the signal
        else:
            signal_info = device_obj.root._info["signals"].get(signal_name)
            if signal_info is None:
                for candidate in device_obj.root._info["signals"].values():
                    if candidate.get("obj_name") == signal_name:
                        signal_info = candidate
                        break

        if signal_info is None:
            raise ValueError(
                f"{error_prefix} Signal '{signal_name}' not found for device '{device_name}'."
            )

        obj_name = signal_info.get("obj_name")
        signal_source = AggregatedState._get_signal_source(signal_info, error_prefix)
        return obj_name, signal_source

    @staticmethod
    def get_state_requirements(
        label: str,
        state_config: SubDeviceStateConfig,
        device_manager: DeviceManagerBase,
        error_prefix: str,
    ) -> list[ResolvedStateSignal]:
        state_requirements: list[ResolvedStateSignal] = []
        for device_name, config in state_config.devices.items():
            if isinstance(config, SignalConfig):
                state_requirements.append(
                    AggregatedState._build_requirement_for_signal(
                        device_name,
                        device_name,
                        config.value,
                        config.abs_tol,
                        label,
                        device_manager,
                        error_prefix,
                    )
                )
            elif isinstance(config, DeviceConfig):
                # If a value is specified for the device, add it as a requirement
                if config.value is not None:
                    state_requirements.append(
                        AggregatedState._build_requirement_for_signal(
                            device_name,
                            device_name,
                            config.value,
                            config.abs_tol,
                            label,
                            device_manager,
                            error_prefix,
                        )
                    )
                if config.low_limit is not None:
                    state_requirements.append(
                        AggregatedState._build_requirement_for_signal(
                            device_name,
                            "low_limit",
                            config.low_limit.value,
                            config.low_limit.abs_tol,
                            label,
                            device_manager,
                            error_prefix,
                        )
                    )
                if config.high_limit is not None:
                    state_requirements.append(
                        AggregatedState._build_requirement_for_signal(
                            device_name,
                            "high_limit",
                            config.high_limit.value,
                            config.high_limit.abs_tol,
                            label,
                            device_manager,
                            error_prefix,
                        )
                    )
                for signal_name, signal_config in (config.signals or {}).items():
                    state_requirements.append(
                        AggregatedState._build_requirement_for_signal(
                            device_name,
                            signal_name,
                            signal_config.value,
                            signal_config.abs_tol,
                            label,
                            device_manager,
                            error_prefix,
                        )
                    )
        return state_requirements

    def _build_rules(self) -> None:
        self._signal_info_to_labels.clear()
        self._requirements_for_label.clear()
        self._subscriptions.clear()
        for label, device_configs in self.config.states.items():
            state_requirements: list[ResolvedStateSignal] = AggregatedState.get_state_requirements(
                label, device_configs, self._get_device_manager(), self._error_prefix
            )
            for requirement in state_requirements:
                device_name = requirement.device_name
                signal_name = requirement.signal_name
                source = requirement.source
                self._subscriptions.add((device_name, source))
                self._signal_info_to_labels.setdefault(
                    (device_name, source, signal_name), set()
                ).add(label)
            self._requirements_for_label[label] = state_requirements

    @staticmethod
    def _build_requirement_for_signal(
        device_name: str,
        signal_name: str,
        value: Any,
        abs_tol: float,
        label: str,
        device_manager: DeviceManagerBase,
        error_prefix: str,
    ) -> ResolvedStateSignal:
        resolved_signal_name, source = AggregatedState._resolve_signal(
            device_name, signal_name, device_manager, error_prefix
        )

        return ResolvedStateSignal(
            label=label,
            device_name=device_name,
            signal_name=resolved_signal_name,
            expected_value=value,
            abs_tolerance=abs_tol,
            source=source,
        )

    def start(self) -> None:
        if self.started:
            return

        if self.connector is None:
            raise RuntimeError("Redis connector is not set.")

        try:
            msg = None
            self._build_rules()
            affected_labels = self._fill_cache()
            msg = self.evaluate(affected_labels=affected_labels)
        except Exception as exc:
            self._handle_state_exception(exc)

        if msg is not None:
            self._emit_state(msg)
        for device, source in self._subscriptions:
            self.connector.register(
                self._endpoint(device, source),
                cb=self._update_aggregated_state,
                device=device,
                source=source,
            )
        super().start()

    def _fill_cache(self) -> set[str]:
        affected_labels: set[str] = set()
        for device, source in self._subscriptions:
            endpoint = self._endpoint(device, source)
            msg = self.connector.get(endpoint)
            if msg is not None:
                affected_labels.update(self._cache_message(device, source, msg))
        return affected_labels

    def _cache_message(
        self, device: str, source: SignalSource, msg: messages.DeviceMessage
    ) -> set[str]:
        affected_labels: set[str] = set()
        for signal_name, signal_data in msg.signals.items():
            key = (device, source, signal_name)
            labels = self._signal_info_to_labels.get(key)
            if labels is None:  # signal not relevant for any state
                continue
            self._signal_value_cache[key] = signal_data.get("value")
            affected_labels.update(labels)
        return affected_labels

    def stop(self) -> None:
        if not self.started:
            return
        if self.connector is not None:
            for device, source in self._subscriptions:
                self.connector.unregister(
                    self._endpoint(device, source), cb=self._update_aggregated_state
                )
        super().stop()

    def _update_aggregated_state(
        self, msg_obj: MessageObject, device: str, source: SignalSource, **_kwargs
    ) -> None:
        try:
            msg: messages.DeviceMessage = msg_obj.value  # type: ignore ; we know it's a DeviceMessage
            affected_labels = self._cache_message(device, source, msg)
            if affected_labels:
                msg = self.evaluate(affected_labels=affected_labels)
                if msg is not None:
                    self._emit_state(msg)
        except Exception as exc:
            self._handle_state_exception(exc)

    def evaluate(
        self, affected_labels: set[str] | None = None
    ) -> messages.BeamlineStateMessage | None:
        if affected_labels is None:
            return None
        # We need to always extend the affected labels with the current labels,
        # as the signal that updated might be not relevant for the currently active state,
        # but the state should still be checked for validity.
        affected_labels.update(self._current_labels)
        matching_labels = [label for label in affected_labels if self._label_matches(label)]
        if matching_labels:
            self._current_labels = matching_labels
            state_msg = messages.BeamlineStateMessage(
                name=self.config.name, status="valid", label="|".join(matching_labels)
            )
            return state_msg

        self._current_labels = []
        state_msg = messages.BeamlineStateMessage(
            name=self.config.name, status="invalid", label="No matching state"
        )
        return state_msg

    def _label_matches(self, label: str) -> bool:
        requirements = self._requirements_for_label.get(label, [])
        return bool(requirements) and all(
            self._requirement_matches(requirement) for requirement in requirements
        )

    def _requirement_matches(self, requirement: ResolvedStateSignal) -> bool:
        key = (requirement.device_name, requirement.source, requirement.signal_name)
        cached_value = self._signal_value_cache.get(key, None)
        if cached_value is None:
            return False

        try:
            # Cast to float to make sure comparison with abs works as expected.
            value = float(cached_value)
            expected_value = float(requirement.expected_value)
            return abs(value - expected_value) <= requirement.abs_tolerance
        # Catch TypeError and ValueError in case the value is not a number or cannot be cast to float,
        # in that case we fall back to exact equality.
        except (TypeError, ValueError):
            try:
                result = cached_value == requirement.expected_value
            except (TypeError, ValueError):
                return False
            # In case this comparison runs on comparing two arrays.
            # We do not consider this comparsion as valid currently.
            try:
                return bool(result)
            except (TypeError, ValueError):
                return False


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
        val = msg.signals.get(self.signal_name, {}).get("value", "").lower()
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
        device_state.configure(device="sample_x", signal="sample_x_signal_name", low_limit=0.0, high_limit=10.0)
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

        if self.config.low_limit is None:
            self.config.low_limit = float("-inf")
        if self.config.high_limit is None:
            self.config.high_limit = float("inf")

        val = msg.signals.get(self.signal_name, {}).get("value", None)
        if val is None:
            return messages.BeamlineStateMessage(
                name=self.config.name,
                status="invalid",
                label=f"Positioner {self.device_obj.name}: Value {self.signal_name} not found.",
            )

        if val < self.config.low_limit or val > self.config.high_limit:
            return messages.BeamlineStateMessage(
                name=self.config.name,
                status="invalid",
                label=f"Positioner {self.device_obj.dotted_name} out of limits",
            )

        min_warning_threshold = self.config.low_limit + self.config.tolerance
        max_warning_threshold = self.config.high_limit - self.config.tolerance

        if val < min_warning_threshold or val > max_warning_threshold:
            return messages.BeamlineStateMessage(
                name=self.config.name,
                status="warning",
                label=f"Positioner {self.device_obj.dotted_name} near limits",
            )

        return messages.BeamlineStateMessage(
            name=self.config.name,
            status="valid",
            label=f"Positioner {self.device_obj.dotted_name} within limits",
        )
