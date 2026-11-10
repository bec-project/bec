"""Provider metadata before conversion to the device-info wire payload."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from bec_server.device_server.devices import OwnershipMode


class DeviceSignalInfo(BaseModel):
    """
    Describe one entry in a device's signal mapping.

    A multi-channel DynamicSignal has one entry per channel. For example, the
    `spectra.left` and `spectra.right` entries both refer to the `spectra` component
    and share the `det_spectra` Redis stream for device `det`. A single-channel
    DynamicSignal uses the component name as the mapping key, like an ordinary signal.
    """

    model_config = ConfigDict(extra="allow", strict=True)

    component_name: str = Field(
        description="Attribute name on the device. Channels of a dynamic signal share this name.",
        examples=["gain", "spectra"],
    )
    signal_class: str = Field(
        description="Class name of the underlying signal object.",
        examples=["Signal", "EpicsSignal", "DynamicSignal"],
    )
    obj_name: str = Field(
        description="Native signal name. For a multi-channel dynamic signal, the channel name "
        "is appended to the native name with an underscore.",
        examples=["det_gain", "det_spectra_left", "det_spectra_right"],
    )
    storage_name: str | None = Field(
        default=None,
        description="Native message signal name. The file writer uses it to locate the async "
        "Redis stream, which is shared by all channels. Omitted for ordinary signals.",
        examples=["det_spectra"],
    )
    kind_int: int | None = Field(
        default=None,
        description="Numeric Ophyd Kind value, taken from the channel for a message signal.",
        examples=[1, 2, 5],
    )
    kind_str: str | None = Field(
        default=None,
        description="Ophyd Kind name corresponding to kind_int.",
        examples=["normal", "config", "hinted"],
    )
    doc: str = Field(
        default="",
        description="Explicit documentation from the component declaration, or an empty string.",
        examples=["Detector gain."],
    )
    describe: dict[str, Any] = Field(
        default_factory=dict,
        description="Entry from the native signal's describe() result. It includes dtype, shape, "
        "and source, plus units, control limits, and precision when provided by the backend. "
        "Channels of a dynamic signal share the parent descriptor and its signal_info.",
        examples=[
            {"dtype": "integer", "shape": [], "source": "SIM:det_gain"},
            {"dtype": "number", "shape": [], "source": "PV:DET:EXPOSURE", "units": "s"},
            {
                "dtype": "DeviceMessage",
                "shape": [],
                "source": "BECMessageSignal:det_spectra",
                "signal_info": {"ndim": 1, "saved": True, "signals": [["left", 1], ["right", 1]]},
            },
        ],
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Snapshot of the native signal metadata at inspection, including connection, "
        "access, timestamp, alarm status, severity, and precision when available.",
        examples=[{"connected": True, "read_access": True, "write_access": True, "severity": None}],
    )
    labels: list[str] = Field(
        default_factory=list,
        description="Sorted Ophyd labels assigned to the native signal, or an empty list.",
    )


class DeviceInfo(BaseModel):
    """
    Describe a device interface, including nested devices and exposed signals.
    """

    model_config = ConfigDict(extra="allow", strict=True)

    device_base_class: Literal["device", "positioner", "signal", "computed_signal", "unknown"]
    device_class: str
    device_attr_name: str | None = None
    ownership_mode: OwnershipMode | None = Field(default=None, strict=False)
    read_access: bool | None = None
    write_access: bool | None = None
    signals: dict[str, DeviceSignalInfo] = Field(default_factory=dict)
    hints: dict[str, Any] = Field(default_factory=dict)
    describe: dict[str, Any] = Field(default_factory=dict)
    describe_configuration: dict[str, Any] = Field(default_factory=dict)
    sub_devices: list[DeviceInfo] = Field(default_factory=list)
    custom_user_access: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @field_serializer("ownership_mode", when_used="unless-none")
    def _serialize_ownership_mode(self, mode: OwnershipMode) -> str:
        return mode.value

    def to_wire(self) -> dict[str, Any]:
        """
        Convert to the existing dictionary payload, retaining explicitly set fields.

        Returns:
            dict[str, Any]: Device metadata with native values left for the Redis encoder.
        """
        return self.model_dump(exclude_unset=True)
