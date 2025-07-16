"""
This module provides the models for the BEC Atlas API.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, Optional, Type, TypeVar

from pydantic import BaseModel, Field, create_model, model_validator
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

BM = TypeVar("BM", bound=BaseModel)


def make_all_fields_optional(model: Type[BM], model_name: str) -> Type[BM]:
    """Convert all fields in a Pydantic model to Optional."""

    def optional_field(field: FieldInfo):
        # pylint: disable=protected-access
        fields_info = field._attributes_set
        fields_info.pop("annotation", None)
        if field.default_factory is not None:
            fields_info["default_factory"] = field.default_factory
        else:
            fields_info["default"] = (
                field.default if field.default is not PydanticUndefined else None
            )
        return (Optional[field.annotation], Field(**fields_info))

    fields = {name: optional_field(field) for name, field in model.model_fields.items()}
    return create_model(model_name, **fields, __config__=model.model_config)


class _DeviceModelCore(BaseModel):
    """Represents the internal config values for a device"""

    enabled: bool
    deviceClass: str
    deviceConfig: Annotated[
        dict | None,
        Field(
            default=None,
            deprecated="Please use the initParameters, attributeSettings, and signalSettings fields instead!",
        ),
    ]
    initParameters: dict[str, Any] = Field(
        default_factory=dict, description="parameters to pass to the device class' __init__ method."
    )
    attributeSettings: dict[str, Any] = Field(
        default_factory=dict, description="attributes to set on the device after initialisation."
    )
    signalSettings: dict[str, Any] = Field(
        default_factory=dict, description="signals to set on the device after initialisation."
    )
    readoutPriority: Literal["monitored", "baseline", "async", "on_request", "continuous"]
    description: str | None = None
    readOnly: bool = False
    softwareTrigger: bool = False
    deviceTags: set[str] = set()
    userParameter: dict = {}

    @model_validator(mode="after")
    def verify_params(self):
        if self.deviceConfig is not None and (
            [self.initParameters, self.attributeSettings, self.signalSettings] != [{}, {}, {}]
        ):
            raise ValueError(
                "Do not use both the deprecated 'deviceConfig' alongside the replacements 'initParameters', 'attributeSettings', and 'signalSettings'."
            )
        return self


class Device(_DeviceModelCore):
    """
    Represents a device in the BEC Atlas API. This model is also used by the SciHub service to
    validate updates to the device configuration.
    """

    name: str


DevicePartial = make_all_fields_optional(Device, "DevicePartial")
