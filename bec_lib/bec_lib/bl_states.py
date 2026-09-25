"""Module defining beamline states and their evaluation logic."""

from __future__ import annotations

import functools
import keyword
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Annotated, Any, Callable, ClassVar, Generic, Literal, Type, TypeVar, cast

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.device import DeviceBase, Signal
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject, RedisConnector
from bec_lib.scan_args import ScanArgument


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

    name: Annotated[
        str,
        ScanArgument(
            display_name="State name",
            description=(
                "Unique name for the beamline state. Must be a valid Python identifier and cannot be a reserved keyword. This name is used to identify the state in the system and should be descriptive of the state being monitored."
            ),
        ),
    ]
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

    device: Annotated[
        DeviceBase | str,
        ScanArgument(
            display_name="Device",
            description=(
                "The device this state depends on. Can be specified as the device's dotted name or as the Device object itself. If the device has hints configured, the state will use the first hinted signal of the device by default. Otherwise, a signal must be specified explicitly for the state to function."
            ),
        ),
    ]

    signal: Annotated[
        Signal | str | None,
        ScanArgument(
            display_name="Signal",
            description=(
                "The signal of the device to monitor for this state. Can be specified as the signal's dotted name, the signal object itself, or the obj_name of the signal as defined in the device's read dictionary. If not specified, the state will attempt to use the first hinted signal of the device. If the device has no hints and no signal is specified, the state will raise an error."
            ),
        ),
    ] = None

    @model_validator(mode="after")
    def validate_signal(self) -> DeviceStateConfig:
        """
        Validate that the signal is either None, a string, or a Signal instance. If it's a Signal instance, return its name.
        """
        if isinstance(self.device, Signal):
            # Signals don't have sub-signals, so if the device
            # itself is a signal, we ignore the signal field and use
            # the device name as the signal.
            # However, validator has to also count in scenario when gui provides both device/signal field for just signal.
            signal_name = self.device.dotted_name
            if isinstance(self.signal, Signal) and self.signal != self.device:
                raise ValueError(
                    f"Signal '{self.signal.dotted_name}' does not match signal device '{signal_name}'"
                )
            if isinstance(self.signal, str) and self.signal not in {self.device.name, signal_name}:
                raise ValueError(
                    f"Signal '{self.signal}' does not match signal device '{signal_name}'"
                )
            self.device = signal_name
            self.signal = signal_name
            return self
        if self.signal is None:
            if isinstance(self.device, DeviceBase):
                self.device = self.device.dotted_name
            return self
        if isinstance(self.device, DeviceBase) and isinstance(self.signal, Signal):
            if self.signal.parent != self.device:
                raise ValueError(
                    f"Signal '{self.signal.dotted_name}' does not belong to device '{self.device.dotted_name}'"
                )
        if isinstance(self.device, DeviceBase):
            self.device = self.device.dotted_name
        if isinstance(self.signal, Signal):
            self.signal = self.signal.dotted_name
        return self


class DeviceWithinLimitsStateConfig(DeviceStateConfig):
    """
    Configuration for a device within limits beamline state.
    """

    state_type: ClassVar[str] = "DeviceWithinLimitsState"

    low_limit: Annotated[
        float | None,
        ScanArgument(
            display_name="Low limit",
            description="Optional lower allowed value. Leave disabled for no lower limit.",
            reference_units="device",
        ),
    ] = None

    high_limit: Annotated[
        float | None,
        ScanArgument(
            display_name="High limit",
            description="Optional upper allowed value. Leave disabled for no upper limit.",
            reference_units="device",
        ),
    ] = None

    tolerance: Annotated[
        float,
        ScanArgument(
            display_name="Tolerance",
            description="Warning margin applied inside the configured limits.",
            reference_units="device",
        ),
    ] = 0.1

    @model_validator(mode="after")
    def validate_limits(self) -> DeviceWithinLimitsStateConfig:
        """Ensure configured limits are logically ordered when both are provided."""
        if (
            self.low_limit is not None
            and self.high_limit is not None
            and self.low_limit >= self.high_limit
        ):
            raise ValueError("low_limit must be smaller than high_limit.")
        return self


class SignalConfig(BaseModel):
    """
    Config describing a signal requirement for a device-based beamline state.
    Either 'value' or 'at' must be provided, but not both. If 'at' is provided, the expected value
    will be fetched on-demand from the user parameter of the device the signal belongs to.

    Args:
        value (float | int | str | bool | None): The expected value of the signal. Optional if 'at' is provided.
        at (str | None): Optional user parameter name to use as a dynamic input for the expected value. Cannot be used with 'value'.
        abs_tol (float): The absolute tolerance for the signal value. Must be non-negative.
    """

    value: float | int | str | bool | None = None
    abs_tol: float = Field(default=0.0, ge=0.0)
    at: str | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def validate_config(self) -> SignalConfig:
        """Validate configuration to ensure either 'value' or 'at' is provided, but not both."""
        if self.value is not None and self.at is not None:
            raise ValueError("Cannot specify both 'value' and 'at' for a signal configuration.")
        if self.value is None and self.at is None:
            raise ValueError("Either 'value' or 'at' must be specified for a signal configuration.")
        return self


class DeviceConfig(BaseModel):
    """
    Config describing a device requirement for a device-based beamline state.
    Either 'value', 'at', 'low_limit', 'high_limit', or 'signals' must be provided. The parameters
    'value' and 'at' are mutually exclusive. If 'at' is provided, the expected value will be fetched
    on-demand from the user parameter of the device.

    Args:
        value (float | int | str | bool | None): The expected value of the device. Optional if 'at' is provided.
        at (str | None): Optional user parameter name to use as a dynamic input for the expected value. Cannot be used with 'value'.
        abs_tol (float): The absolute tolerance for the device value. Must be non-negative.
        low_limit (SignalConfig | scalar | None): Optional lower limit configuration for the
            device. A scalar is shorthand for ``SignalConfig(value=scalar)``.
        high_limit (SignalConfig | scalar | None): Optional upper limit configuration for the
            device. A scalar is shorthand for ``SignalConfig(value=scalar)``.
        signals (dict[str, SignalConfig] | None): Optional dictionary of signal configurations for the device.
    """

    abs_tol: float = Field(default=0.0, ge=0.0)
    value: float | int | str | bool | None = None
    at: str | None = None  # Optional input for user parameter
    low_limit: SignalConfig | int | float | None = None
    high_limit: SignalConfig | int | float | None = None
    signals: dict[str, SignalConfig] | None = None
    model_config = {"extra": "forbid"}

    @field_validator("low_limit", "high_limit", mode="before")
    @classmethod
    def normalize_limit_config(cls, value: Any) -> Any:
        """Expand scalar limit targets to their SignalConfig shorthand."""
        if value is None or isinstance(value, (dict, SignalConfig)):
            return value
        elif isinstance(value, (int, float)):
            return {"value": value}
        else:
            raise ValueError(
                f"Invalid limit configuration: {value}. Must be a SignalConfig, dict, or scalar."
            )

    @model_validator(mode="after")
    def validate_config(self) -> DeviceConfig:
        """
        Validate that either value, at, low_limit, high_limit, or signals are provided. In addition,
        ensure that 'value' and 'at' are not both specified.
        """
        if self.value is not None and self.at is not None:
            raise ValueError("Cannot specify both 'value' and 'at' for a device configuration.")
        if (
            self.value is None
            and self.at is None
            and self.low_limit is None
            and self.high_limit is None
            and self.signals is None
        ):
            raise ValueError(
                "At least one of value, at, low_limit, high_limit, or signals must be provided."
            )
        return self


class SubDeviceStateConfig(BaseModel):
    """Config for a sub-state of an aggregated beamline state"""

    devices: dict[str, DeviceConfig | SignalConfig]
    transition_metadata: dict[str, Any] | None = None


class AggregatedStateConfig(BeamlineStateConfig):
    """
    AggregatedState configuration that defines a set of sub-states, each of which is defined by a set
    of device/signal requirements. The aggregated state evaluates the current state based on the evaluation_method
    and current device/signal values. The evaluation_method can be 'any', 'all', or 'exclusive' to determine how the
    sub-states are evaluated. The description of each sub-state is provided in the states dictionary.

    Args:
        evaluation_method (Literal["any", "all", "exclusive"] | None): How matching labels determine validity.
                            Use None to disable validation and always report valid after successful initialization.
        states (dict[str, SubDeviceStateConfig]): A dictionary of sub-state configurations, where each key is a
                            label and the value is a SubDeviceStateConfig defining the device/signal requirements for that label.
    """

    state_type: ClassVar[str] = "AggregatedState"

    evaluation_method: Literal["any", "all", "exclusive"] | None = Field(
        default="any",
        description=(
            "How matching labels determine validity. Use null to disable validation and always "
            "report valid after successful initialization."
        ),
    )
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

    @staticmethod
    def format_config_summary(config: BeamlineStateConfig, max_text_length: int = 60) -> str:
        """Return a compact, single-line configuration summary for overview displays."""
        parameters = config.model_dump(exclude={"name"}, exclude_none=True)
        if not parameters:
            return "-"

        summary_parts = []
        for name, value in parameters.items():
            value_text = str(value).replace("\n", " ")
            if len(value_text) > max_text_length:
                value_text = f"{value_text[:max_text_length - 1]}…"
            summary_parts.append(f"{name}={value_text}")
        return ", ".join(summary_parts)

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

        if isinstance(self.device_obj, Signal):
            if self.config.signal is None:
                self.signal_name = self.device_obj.name
                return
            signal = cast(str, self.config.signal)
            if signal in {self.device_obj.name, self.device_obj.dotted_name}:
                self.signal_name = self.device_obj.name
                return
            raise ValueError(
                f"{self._error_prefix} Signal '{signal}' does not match signal device '{self.config.device}'."
            )

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
            self.started = False
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


# Source of a signal in Redis; can be either "readback", "configuration", or "limits".
SignalSource = Literal["readback", "configuration", "limits"]


@dataclass(frozen=True)
class ResolvedStateSignal:
    """
    Data class representing a resolved signal requirement for the AggregatedState evaluation.
    Please note that 'expected_value' and 'at' are mutually exclusive and only one should be given.
    Methods that use this data class may implement logic to handle in case both or neither are provided.
    """

    label: str
    device_name: str
    signal_name: str
    expected_value: float | int | str | bool | None
    at: str | None
    abs_tolerance: float | int
    source: SignalSource


class AggregatedState(BeamlineState[AggregatedStateConfig]):
    """
    A beamline state that depends on multiple sub-states, each defined by a set of device/signal requirements.
    Sub-states are evaluated individually, and the overall valid/invalid state is determined based on the
    evaluation_method specified in the configuration. The evaluation_method can be 'any', 'all', or 'exclusive' to
    determine how the overall state is evaluated.
    """

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

    #########################
    ## Static Methods
    #########################
    # These static methods are used to resolve signal information, formatting configuration summaries etc.
    # They are static because they may be used from outside the class context, e.g. when parsing configuration
    # into information.

    @staticmethod
    def get_expected_value(
        requirement: ResolvedStateSignal, device_manager: DeviceManagerBase
    ) -> Any:
        """
        Get a value from a requirement, either from the expected_value or from the user parameter of the device.
        If both are provided, a ValueError is raised. If neither is provided, a ValueError is raised.
        If `at` is provided, the value is fetched from the user parameter of the device. If either the device
        is not present in the device_manager or the parameter not available in user_parameters, a ValueError is raised.
        """
        if (requirement.expected_value is not None and requirement.at is not None) or (
            requirement.expected_value is None and requirement.at is None
        ):
            raise ValueError(
                f"Requirement for device '{requirement.device_name}' and signal '{requirement.signal_name}' "
                f"cannot have both 'expected_value' {requirement.expected_value} and 'at' {requirement.at} specified."
            )
        if requirement.expected_value is not None:
            return requirement.expected_value
        if requirement.device_name not in device_manager.devices:
            raise ValueError(
                f"Device '{requirement.device_name}' not found in device manager for requirement."
            )
        val = device_manager.devices[requirement.device_name].user_parameter.get(
            requirement.at, None
        )
        if val is None:
            raise ValueError(
                f"User parameter '{requirement.at}' not found for device '{requirement.device_name}' in requirement."
            )
        return val

    @staticmethod
    def get_state_requirements(
        label: str,
        state_config: SubDeviceStateConfig,
        device_manager: DeviceManagerBase,
        error_prefix: str,
    ) -> list[ResolvedStateSignal]:
        """
        Get a list of state requirements for a given label and state configuration.

        Args:
            label (str): The label for the state.
            state_config (SubDeviceStateConfig): The state configuration.
            device_manager (DeviceManagerBase): The device manager instance.
            error_prefix (str): A prefix to use in error messages for better context.

        Returns:
            list[ResolvedStateSignal]: A list of resolved state signals.
        """
        state_requirements: list[ResolvedStateSignal] = []
        for device_name, config in state_config.devices.items():
            if isinstance(config, SignalConfig):
                state_requirements.append(
                    AggregatedState._build_requirement_from_config(
                        device_name, device_name, config, label, device_manager, error_prefix
                    )
                )
            elif isinstance(config, DeviceConfig):
                if config.value is not None or config.at is not None:
                    state_requirements.append(
                        AggregatedState._build_requirement_from_config(
                            device_name, device_name, config, label, device_manager, error_prefix
                        )
                    )
                if config.low_limit is not None:
                    state_requirements.append(
                        AggregatedState._build_requirement_from_config(
                            device_name,
                            "low_limit",
                            config.low_limit,
                            label,
                            device_manager,
                            error_prefix,
                        )
                    )
                if config.high_limit is not None:
                    state_requirements.append(
                        AggregatedState._build_requirement_from_config(
                            device_name,
                            "high_limit",
                            config.high_limit,
                            label,
                            device_manager,
                            error_prefix,
                        )
                    )
                for signal_name, signal_config in (config.signals or {}).items():
                    state_requirements.append(
                        AggregatedState._build_requirement_from_config(
                            device_name,
                            signal_name,
                            signal_config,
                            label,
                            device_manager,
                            error_prefix,
                        )
                    )

        return state_requirements

    @staticmethod
    def format_config_summary(config: BeamlineStateConfig, max_text_length: int = 60) -> str:
        """Custom formatting for AggregatedStateConfig to provide a compact summary of the configuration."""
        if not isinstance(config, AggregatedStateConfig):  # Fallback
            return BeamlineState.format_config_summary(config, max_text_length)

        device_names = {
            device_name for state in config.states.values() for device_name in state.devices
        }
        transition_count = sum(
            state.transition_metadata is not None for state in config.states.values()
        )
        evaluation_method = config.evaluation_method or "null"

        def count_phrase(count: int, noun: str) -> str:
            suffix = "" if count == 1 else "s"
            return f"{count} {noun}{suffix}"

        summary = (
            f"{evaluation_method} · {count_phrase(len(config.states), 'label')} · "
            f"{count_phrase(len(device_names), 'device')} · "
            f"{count_phrase(AggregatedState._count_config_requirements(config), 'requirement')}"
        )
        if transition_count:
            summary += f" · {count_phrase(transition_count, 'transition')}"
        return summary

    #########################
    ## Private static helpers
    #########################

    @staticmethod
    def _endpoint(device: str, source: SignalSource):
        """Retrieve the appropriate message endpoint for a given device and signal source."""
        if source == "readback":
            return MessageEndpoints.device_readback(device)
        if source == "configuration":
            return MessageEndpoints.device_read_configuration(device)
        if source == "limits":
            return MessageEndpoints.device_limits(device)
        raise ValueError(
            f"Invalid signal source '{source}', please use 'readback', 'configuration', or 'limits'."
        )

    @staticmethod
    def _count_config_requirements(config: AggregatedStateConfig) -> int:
        """Count individual signal requirements without resolving devices."""
        requirement_count = 0
        for state in config.states.values():
            for device_config in state.devices.values():
                if isinstance(device_config, SignalConfig):
                    requirement_count += 1
                    continue
                requirement_count += int(
                    device_config.value is not None or device_config.at is not None
                )
                requirement_count += int(device_config.low_limit is not None)
                requirement_count += int(device_config.high_limit is not None)
                requirement_count += len(device_config.signals or {})
        return requirement_count

    @staticmethod
    def _get_signal_source(signal_info: dict[str, Any], error_prefix: str) -> SignalSource:
        """Retrieve the signal source based on the serialized signal_info.

        Args:
            signal_info (dict[str, Any]): The signal information dictionary containing at least the "kind_str" key.
            error_prefix (str): A prefix to use in error messages for better context.

        Returns:
            SignalSource: A string literal indicating the signal source, one of "readback", "configuration", or "limits".
        """
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
        """
        Resolve the signal information for a given device and signal name.

        Args:
            device_name (str): The name of the device.
            signal_name (str): The name of the signal.
            device_manager (DeviceManagerBase): The device manager instance.
            error_prefix (str): A prefix to use in error messages for better context.

        Returns:
            tuple[str, SignalSource]: A tuple containing the object name and the signal source.
        """
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
            # Attribute error because DeviceContainer is not a dict, it uses __getattr__ to resolve dotted names
            except AttributeError:
                raise ValueError(
                    f"{error_prefix} Signal '{signal_name}' not found for device '{device_name}'."
                ) from None
            if signal_obj.root != device_obj.root:
                raise ValueError(
                    f"{error_prefix} Signal '{signal_name}' does not belong to device '{device_name}'."
                )
            signal_component = ".".join(signal_name.split(".")[1:])
            signal_info = device_obj.root._info["signals"].get(signal_component)
        # Regular device signals
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
    def _validate_user_parameter(
        device_name: str,
        config: DeviceConfig | SignalConfig,
        device_manager: DeviceManagerBase,
        error_prefix: str,
    ) -> None:
        """
        Validate a user parameter referenced by a signal or device configuration.

        Args:
            device_name (str): The name of the device.
            config (DeviceConfig | SignalConfig): The configuration containing the reference.
            device_manager (DeviceManagerBase): The device manager instance.
            error_prefix (str): A prefix to use in error messages for better context.
        """
        if config.at is None:
            return

        dev_obj = device_manager.devices.get(device_name, None)
        if dev_obj is None:
            raise ValueError(f"{error_prefix} Device '{device_name}' not found in device manager.")
        if dev_obj.user_parameter.get(config.at) is None:
            raise ValueError(
                f"{error_prefix} User parameter '{config.at}' for device '{device_name}' not found."
            )

    @staticmethod
    def _build_requirement_from_config(
        device_name: str,
        signal_name: str,
        config: DeviceConfig | SignalConfig,
        label: str,
        device_manager: DeviceManagerBase,
        error_prefix: str,
    ) -> ResolvedStateSignal:
        """Build a ResolvedStateSignal from a device/signal configuration, validating user parameters if necessary."""
        AggregatedState._validate_user_parameter(device_name, config, device_manager, error_prefix)
        return AggregatedState._build_requirement_for_signal(
            device_name=device_name,
            signal_name=signal_name,
            value=config.value,
            at=config.at,
            abs_tol=config.abs_tol,
            label=label,
            device_manager=device_manager,
            error_prefix=error_prefix,
        )

    @staticmethod
    def _build_requirement_for_signal(
        device_name: str,
        signal_name: str,
        value: Any | None,
        at: str | None,
        abs_tol: float,
        label: str,
        device_manager: DeviceManagerBase,
        error_prefix: str,
    ) -> ResolvedStateSignal:
        """
        Build a ResolvedStateSignal for a given device, signal, and expected value.

        Args:
            device_name (str): The name of the device.
            signal_name (str): The name of the signal.
            value (Any | None): The expected value for the signal.
            abs_tol (float): The absolute tolerance for comparing the signal value.
            label (str): The label of the state that this requirement belongs to.
            device_manager (DeviceManagerBase): The device manager instance.
            error_prefix (str): A prefix to use in error messages for better context.
            at (str | None): The user parameter for the expected value, if applicable.

        Returns:
            ResolvedStateSignal: The resolved state signal requirement.
        """
        if value is None and at is None:
            raise ValueError(
                f"{error_prefix} For device '{device_name}' and signal '{signal_name}', "
                f"either 'value' or 'at' must be specified for state label '{label}'."
            )
        resolved_signal_name, source = AggregatedState._resolve_signal(
            device_name, signal_name, device_manager, error_prefix
        )
        cfg = {
            "device_name": device_name,
            "signal_name": resolved_signal_name,
            "expected_value": value,
            "abs_tolerance": abs_tol,
            "label": label,
            "source": source,
            "at": at,
        }
        return ResolvedStateSignal(**cfg)

    ##########################
    ## Normal Methods
    ##########################

    def _requirement_matches(self, requirement: ResolvedStateSignal) -> bool:
        """Check if the given requirement matches the current signal values."""
        key = (requirement.device_name, requirement.source, requirement.signal_name)
        cached_value = self._signal_value_cache.get(key, None)
        if cached_value is None:
            return False

        expected_value = self.get_expected_value(requirement, self._get_device_manager())
        try:
            # Cast to float to make sure comparison with abs works as expected.
            value = float(cached_value)
            comparison_value = float(expected_value)
            return abs(value - comparison_value) <= requirement.abs_tolerance
        # Catch TypeError and ValueError in case the value is not a number or cannot be cast to float,
        # in that case we fall back to exact equality.
        except (TypeError, ValueError):
            try:
                result = cached_value == expected_value
            except (TypeError, ValueError):
                return False
            # In case this comparison runs on comparing two arrays.
            # We do not consider this comparsion as valid currently.
            try:
                return bool(result)
            except (TypeError, ValueError):
                return False

    def _get_device_manager(self):
        """Utility method to retrieve the device manager."""
        if self.device_manager is None:
            # pylint: disable=import-outside-toplevel
            from bec_lib.client import BECClient

            bec = BECClient()
            return bec.device_manager
        return self.device_manager

    def _build_rules(self) -> None:
        """Build the internal rules and mappings for state evaluation based on the configuration."""
        signal_info_to_labels = {}
        requirements_for_label = {}
        subscriptions = set()

        for label, device_config in self.config.states.items():
            requirements = self.get_state_requirements(
                label, device_config, self._get_device_manager(), self._error_prefix
            )

            for requirement in requirements:
                key = (requirement.device_name, requirement.source, requirement.signal_name)
                subscriptions.add((requirement.device_name, requirement.source))
                signal_info_to_labels.setdefault(key, set()).add(label)

            requirements_for_label[label] = requirements

        # Commit only after every label has resolved successfully.
        self._signal_info_to_labels = signal_info_to_labels
        self._requirements_for_label = requirements_for_label
        self._subscriptions = subscriptions

    def start(self) -> None:
        if self.started:
            return

        if self.connector is None:
            raise RuntimeError("Redis connector is not set.")
        msg = None
        try:
            self._build_rules()
            self._signal_value_cache.clear()
            self._current_labels.clear()
            affected_labels = self._fill_cache()
            msg = self.evaluate(affected_labels=affected_labels)
        except Exception as exc:
            self._signal_info_to_labels.clear()
            self._requirements_for_label.clear()
            self._subscriptions.clear()
            self._signal_value_cache.clear()
            self._current_labels.clear()
            self._handle_state_exception(exc)
            self.started = False
            return  # Do not proceed if there was an exception during rule building or cache filling

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
        """Fill the signal value cache with the current values and return the set of affected state labels."""
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
        """Cache the signal values from a device message and return the set of affected state labels."""
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
        """Stop the state manager and unregister all subscriptions."""
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
        """Update the aggregated state based on a new device message."""
        try:
            msg: messages.DeviceMessage = msg_obj.value  # type: ignore ; we know it's a DeviceMessage
            affected_labels = self._cache_message(device, source, msg)
            if affected_labels:
                state_msg = self.evaluate(affected_labels=affected_labels)
                if state_msg is not None:
                    self._emit_state(state_msg)
        except Exception as exc:
            self._handle_state_exception(exc)

    def evaluate(
        self, affected_labels: set[str] | None = None
    ) -> messages.BeamlineStateMessage | None:
        """
        Evaluate affected and currently matching labels using cached signal values.

        All matching labels are retained and reported. The configured
        ``evaluation_method`` determines whether the set of matching labels is valid.

        Args:
            affected_labels (set[str] | None): The set of state labels that are affected by
            the latest signal update. If None is provided, the evaluation will not proceed and None will be returned.

        Returns:
            messages.BeamlineStateMessage | None: The resulting state message after evaluation, or None
            if no state could be evaluated.
        """
        if affected_labels is None:
            return None
        # We need to always extend the affected labels with the current labels,
        # as the signal that updated might be not relevant for the currently active state,
        # but the state should still be checked for validity.
        affected_labels.update(self._current_labels)
        matching_labels = sorted(label for label in affected_labels if self._label_matches(label))
        self._current_labels = matching_labels

        status = "valid" if self._matching_labels_are_valid(matching_labels) else "invalid"
        return messages.BeamlineStateMessage(
            name=self.config.name,
            status=status,
            label="|".join(matching_labels) if matching_labels else "No matching state",
        )

    def _matching_labels_are_valid(self, matching_labels: list[str]) -> bool:
        """Evaluate the matching-label set according to the configured method."""
        evaluation_method = self.config.evaluation_method
        if evaluation_method is None:
            return True
        if evaluation_method == "any":
            return bool(matching_labels)
        if evaluation_method == "all":
            return bool(self.config.states) and len(matching_labels) == len(self.config.states)
        if evaluation_method == "exclusive":
            return len(matching_labels) == 1
        raise ValueError(f"Unsupported evaluation method: {evaluation_method!r}")

    def _label_matches(self, label: str) -> bool:
        """Check if the given label matches the current signal values based on the defined requirements."""
        requirements = self._requirements_for_label.get(label, [])
        return bool(requirements) and all(
            self._requirement_matches(requirement) for requirement in requirements
        )


class DeviceWithinLimitsState(DeviceBeamlineState[DeviceWithinLimitsStateConfig]):
    """
    A state that checks if a device signal is within limits.

    Example:
        device_state = DeviceWithinLimitsStateConfig(
            name="samx_within_limits",
            device="samx",
            signal="samx",
            low_limit=0.0,
            high_limit=10.0,
        )
        bec.beamline_states.add(device_state)

    """

    CONFIG_CLASS = DeviceWithinLimitsStateConfig

    @staticmethod
    def format_config_summary(config: BeamlineStateConfig, max_text_length: int = 60) -> str:
        """Return a readable summary of the monitored signal and its limits."""
        if not isinstance(config, DeviceWithinLimitsStateConfig):
            return BeamlineState.format_config_summary(config, max_text_length)
        signal = config.signal or config.device or ""
        low_limit = config.low_limit if config.low_limit is not None else "-∞"
        high_limit = config.high_limit if config.high_limit is not None else "∞"
        return (
            f"{config.device} · signal={signal} · limits=[{low_limit}, {high_limit}] · "
            f"tolerance={config.tolerance}"
        )

    def evaluate(
        self, msg: messages.DeviceMessage, *args, **kwargs
    ) -> messages.BeamlineStateMessage:
        """
        Evaluate if the device signal is within the defined limits. If it is outside the limits,
        return an invalid state. Otherwise, return a valid state. If it is close to the limits,
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
                label=f"Device {self.device_obj.name}: Value {self.signal_name} not found.",
            )

        if val < self.config.low_limit or val > self.config.high_limit:
            return messages.BeamlineStateMessage(
                name=self.config.name,
                status="invalid",
                label=f"Device {self.device_obj.dotted_name} out of limits",
            )

        min_warning_threshold = self.config.low_limit + self.config.tolerance
        max_warning_threshold = self.config.high_limit - self.config.tolerance

        if val < min_warning_threshold or val > max_warning_threshold:
            return messages.BeamlineStateMessage(
                name=self.config.name,
                status="warning",
                label=f"Device {self.device_obj.dotted_name} near limits",
            )

        return messages.BeamlineStateMessage(
            name=self.config.name,
            status="valid",
            label=f"Device {self.device_obj.dotted_name} within limits",
        )
