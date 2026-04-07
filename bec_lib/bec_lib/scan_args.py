from __future__ import annotations

from typing import Annotated, Any

import pint
from pint.facets.plain import PlainQuantity
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Units = pint.UnitRegistry()


class ScanArgument(BaseModel):

    display_name: Annotated[str | None, Field(description="Display name for the argument")] = None
    description: Annotated[str | None, Field(description="Description of the argument")] = None
    tooltip: Annotated[str | None, Field(description="Tooltip for the argument")] = None
    expert: Annotated[bool, Field(description="Whether the argument is for expert users only")] = (
        False
    )
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
    alternative_group: Annotated[
        str | None,
        Field(
            description=(
                "Identifier for arguments that are alternative parameterizations of the same "
                "concept and should not be supplied together, e.g. step_size and steps."
            )
        ),
    ] = None

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

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
