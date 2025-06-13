"""
This module provides the models for the BEC Atlas API.
"""

from __future__ import annotations

from typing import Literal, Optional, Type, TypeVar

from pydantic import BaseModel, Field, create_model
from pydantic_core import PydanticUndefined

BM = TypeVar("BM", bound=BaseModel)


def make_all_fields_optional(model: Type[BM], model_name: str) -> Type[BM]:
    """Convert all fields in a Pydantic model to Optional."""

    fields = {}

    for name, field in model.model_fields.items():
        default = field.default if field.default is not PydanticUndefined else None
        # pylint: disable=protected-access
        fields_info = field._attributes_set
        fields_info["annotation"] = Optional[field.annotation]
        fields_info["default"] = default
        fields[name] = (Optional[field.annotation], Field(**fields_info))

    return create_model(model_name, **fields, __config__=model.model_config)


class _DeviceModelCore(BaseModel):
    """Represents the internal config values for a device"""

    enabled: bool
    deviceClass: str
    deviceConfig: dict | None = None
    readoutPriority: Literal["monitored", "baseline", "async", "on_request", "continuous"]
    description: str | None = None
    readOnly: bool = False
    softwareTrigger: bool = False
    deviceTags: set[str] = set()
    userParameter: dict = {}


class Device(_DeviceModelCore):
    """
    Represents a device in the BEC Atlas API. This model is also used by the SciHub service to
    validate updates to the device configuration.
    """

    name: str


DevicePartial = make_all_fields_optional(Device, "DevicePartial")
