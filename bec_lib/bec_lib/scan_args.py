from __future__ import annotations

from typing import Annotated, Any, Literal, TypeAlias

import pint
from pint.facets.plain import PlainQuantity
from pydantic import ConfigDict, Field, TypeAdapter, field_validator, model_validator
from pydantic.dataclasses import dataclass

Units = pint.UnitRegistry()


@dataclass(frozen=True, config=ConfigDict(arbitrary_types_allowed=True))
class ScanArgument:
    """
    UI and validation metadata for a scan or configuration argument.

    ScanArgument is attached as ``Annotated`` metadata, both in plain function signatures
    (scan definitions) and in Pydantic model fields (e.g. beamline state configs)::

        exp_time: Annotated[float, ScanArgument(display_name="Exposure Time", units=Units.s)]

    It is a frozen Pydantic dataclass rather than a ``BaseModel`` on purpose: ``BaseModel``
    instances used as ``Annotated`` metadata of model fields participate in Pydantic's schema
    generation through ``__get_pydantic_core_schema__``, which is deprecated since Pydantic
    2.11 and removed in V3. A dataclass is inert metadata (like ``annotated_types``) and is
    preserved as-is in ``FieldInfo.metadata``.
    """

    display_name: Annotated[str | None, Field(description="Display name for the argument")] = None
    description: Annotated[str | None, Field(description="Description of the argument")] = None
    tooltip: Annotated[str | None, Field(description="Tooltip for the argument")] = None
    expert: Annotated[bool, Field(description="Whether the argument is for expert users only")] = (
        False
    )
    hidden: Annotated[
        bool, Field(description="Whether the argument should be hidden in the UI")
    ] = False
    example: Annotated[Any | None, Field(description="Example value for the argument")] = None
    units: Annotated[
        str | pint.Unit | PlainQuantity[Any] | None, Field(description="Units of the argument")
    ] = None
    reference_units: Annotated[
        str | None,
        Field(
            description=(
                "Reference for the units. Use it when the units depend on another argument, e.g. "
                "the units of a step size might depend on the units of the specified motor. The "
                "reference_units should be set to the name of the argument that serves as reference."
            )
        ),
    ] = None
    gt: Annotated[float | None, Field(description="Value must be greater than this")] = None
    ge: Annotated[
        float | None, Field(description="Value must be greater than or equal to this")
    ] = None
    lt: Annotated[float | None, Field(description="Value must be less than this")] = None
    le: Annotated[float | None, Field(description="Value must be less than or equal to this")] = (
        None
    )
    reference_limits: Annotated[
        str | None,
        Field(
            description=(
                "Reference for the limits. Use it when the limits depend on another argument, e.g. "
                "the limits of start / stop positions might depend on the range of the specified motor. "
                "The reference_limits should be set to the name of the argument that serves as reference."
            )
        ),
    ] = None
    precision: Annotated[int | None, Field(description="Number of decimal places for display")] = (
        None
    )
    alternative_group: Annotated[
        str | None,
        Field(
            description=(
                "Identifier for arguments that are alternative parameterizations of the same "
                "concept and should not be supplied together, e.g. step_size and steps."
            )
        ),
    ] = None

    def model_dump(self, *, exclude_none: bool = False) -> dict[str, Any]:
        """
        Return the scan argument metadata as a plain dictionary.

        Delegates to a pydantic ``TypeAdapter`` so serialization semantics match those of
        a ``BaseModel`` (field serializers, nested types).

        Args:
            exclude_none (bool): Whether to omit entries whose value is None.

        Returns:
            dict: The scan argument metadata.
        """
        data = _SCAN_ARGUMENT_ADAPTER.dump_python(self)
        if exclude_none:
            return {key: value for key, value in data.items() if value is not None}
        return data

    @classmethod
    def model_validate(cls, data: dict[str, Any] | ScanArgument) -> ScanArgument:
        """
        Construct a validated ScanArgument from a dictionary or instance.

        Delegates to a pydantic ``TypeAdapter`` so validation semantics match those of a
        ``BaseModel``: unknown keys are ignored (forward compatibility across versions)
        and all field/model validators run.

        Args:
            data (dict | ScanArgument): Scan argument metadata, e.g. from :meth:`model_dump`.

        Returns:
            ScanArgument: The validated scan argument.
        """
        return _SCAN_ARGUMENT_ADAPTER.validate_python(data)

    @field_validator("units", mode="before")
    @classmethod
    def _serialize_units(cls, value: object) -> object:
        """
        Serialize Pint units to compact unit strings before model validation.

        Args:
            value (object): Units value supplied for the model field.

        Returns:
            The compact string form for Pint units, otherwise the original value.
        """
        if isinstance(value, pint.Unit):
            return f"{value:~P}"
        if isinstance(value, PlainQuantity):
            return f"{value.units:~P}"
        return value

    @model_validator(mode="after")
    def _validate_unit_fields_are_exclusive(self) -> ScanArgument:
        """
        Validate that only one unit source is specified.

        Returns:
            ScanArgument: The validated scan argument model.
        """
        if self.units is not None and self.reference_units is not None:
            raise ValueError("units and reference_units are mutually exclusive")
        return self


# Reusable adapter providing BaseModel-equivalent (de)serialization for the dataclass.
_SCAN_ARGUMENT_ADAPTER = TypeAdapter(ScanArgument)


################################################
######### Frequently used ScanArguments ########
################################################
class DefaultArgType:
    """Namespace for reusable scan argument type aliases."""

    Relative: TypeAlias = Annotated[
        bool,
        ScanArgument(
            display_name="Relative",
            description="Whether the positions are relative to the current position",
        ),
    ]
    Snaked: TypeAlias = Annotated[
        bool,
        ScanArgument(
            display_name="Snaked",
            description="Whether to snake the scan, i.e. reverse the direction of every other line",
        ),
    ]
    ExposureTime: TypeAlias = Annotated[
        float,
        ScanArgument(
            display_name="Exposure Time",
            description="Exposure time in seconds",
            units=Units.s,
            ge=0,
        ),
    ]
    FramesPerTrigger: TypeAlias = Annotated[
        int,
        ScanArgument(
            display_name="Frames per Trigger",
            description="Number of frames per trigger for devices that support configurable frame counts per trigger.",
            ge=1,
        ),
    ]
    SettlingTime: TypeAlias = Annotated[
        float,
        ScanArgument(
            display_name="Settling Time",
            description="Time to settle before trigger and readout.",
            units=Units.s,
            ge=0,
        ),
    ]
    SettlingTimeAfterTrigger: TypeAlias = Annotated[
        float,
        ScanArgument(
            display_name="Settling Time After Trigger",
            description="Time to settle after the trigger but before the readout.",
            units=Units.s,
            ge=0,
        ),
    ]
    ReadoutTime: TypeAlias = Annotated[
        float,
        ScanArgument(
            display_name="Readout Time",
            description="Configuration for devices that support configurable readout times.",
            units=Units.s,
            ge=0,
        ),
    ]
    BurstAtEachPoint: TypeAlias = Annotated[
        int,
        ScanArgument(
            display_name="Burst at Each Point",
            description="Number of triggers and readouts at each point.",
            ge=1,
        ),
    ]
    OptimizeTrajectory: TypeAlias = Annotated[
        Literal["corridor", "shell", "nearest", None],
        ScanArgument(
            display_name="Optimize Trajectory",
            description="Method for optimizing the scan trajectory.",
        ),
    ]


Relative = DefaultArgType.Relative
Snaked = DefaultArgType.Snaked
ExposureTime = DefaultArgType.ExposureTime
FramesPerTrigger = DefaultArgType.FramesPerTrigger
SettlingTime = DefaultArgType.SettlingTime
SettlingTimeAfterTrigger = DefaultArgType.SettlingTimeAfterTrigger
ReadoutTime = DefaultArgType.ReadoutTime
BurstAtEachPoint = DefaultArgType.BurstAtEachPoint
OptimizeTrajectory = DefaultArgType.OptimizeTrajectory
