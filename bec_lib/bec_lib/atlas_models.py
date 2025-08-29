"""
This module provides the models for the BEC Atlas API.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from textwrap import dedent
from typing import Literal, Optional, Type, TypeVar

from pydantic import BaseModel, Field, PrivateAttr, create_model, model_validator
from pydantic_core import PydanticUndefined

from bec_lib.utils.json import ExtendedEncoder

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


class HashInclusion(str, Enum):
    INCLUDE = "INCLUDE"
    EXCLUDE = "EXCLUDE"
    VARIANT = "VARIANT"


class DictHashInclusion(BaseModel, frozen=True):
    field_inclusion: HashInclusion = HashInclusion.EXCLUDE
    inclusion_keys: set[str] | None = None
    remainder_inclusion: HashInclusion | None = None

    @model_validator(mode="after")
    def _check_compat(self) -> DictHashInclusion:
        if self.field_inclusion != HashInclusion.INCLUDE and self.inclusion_keys is not None:
            raise ValueError("You may only select specific keys if the field is included.")
        if self.remainder_inclusion == HashInclusion.INCLUDE:
            raise ValueError("Only EXCLUDE and VARIANT are valid for remainders.")
        if (self.inclusion_keys is None and self.remainder_inclusion is not None) or (
            self.inclusion_keys is not None and self.remainder_inclusion is None
        ):
            raise ValueError(
                "You may only choose what to do with the remainder if some keys are included, in which case you must specify what to do with the remainder."
            )
        if self.inclusion_keys == set():
            raise ValueError(
                "Don't pass an empty list for inclusion keys. "
                "If you want to include all keys, use inclusion_keys = None. "
                "If you want to include no keys, use field_inclusion = HashInclusion.EXCLUDE. "
            )
        return self


class DeviceHashModel(BaseModel, frozen=True):
    """Model for which fields to include in a device hash.

    For plain HashInclusion fields:
        - If fields are HashInclusion.INCLUDE, they are used to calculate the hash
        - If fields are HashInclusion.EXCLUDE, they are ignored
        - If fields are HashInclusion.VARIANT, they are ignored for the hash calculation, but considered for device variants.

    For DictHashInclusion fields:
        - If field_inclusion is HashInclusion.EXCLUDE: The entire field is ignored
        - If field_inclusion is HashInclusion.VARIANT: The entire field is used for variant devices
        - If field_inclusion is HashInclusion.INCLUDE:
            - If inclusion_keys is None, the entire field is included
            - If inclusion_keys is present, those keys are included, and:
                - If remainder_inclusion is HashInclusion.EXCLUDE: the rest of the keys are ignored
                - If remainder_inclusion is HashInclusion.VARIANT: the rest of the keys are considered for device variants.
        All other possibilities should be excluded in the validator.


    """

    name: HashInclusion = HashInclusion.INCLUDE
    enabled: HashInclusion = HashInclusion.EXCLUDE
    deviceClass: HashInclusion = HashInclusion.INCLUDE
    deviceConfig: DictHashInclusion = DictHashInclusion(field_inclusion=HashInclusion.VARIANT)
    deviceTags: HashInclusion = HashInclusion.EXCLUDE
    readoutPriority: HashInclusion = HashInclusion.EXCLUDE
    description: HashInclusion = HashInclusion.EXCLUDE
    readOnly: HashInclusion = HashInclusion.EXCLUDE
    softwareTrigger: HashInclusion = HashInclusion.EXCLUDE
    userParameter: DictHashInclusion = DictHashInclusion()

    def shallow_dump(self) -> dict[str, HashInclusion | DictHashInclusion]:
        return {k: getattr(self, k) for k in self.__class__.model_fields}


class Device(_DeviceModelCore):
    """
    Represents a device in the BEC Atlas API. This model is also used by the SciHub service to
    validate updates to the device configuration.
    """

    name: str


class HashableDevice(Device):
    hash_model: DeviceHashModel = DeviceHashModel()

    names: set[str] = Field(default_factory=set, exclude=True)
    variants: set[Device] = Field(default_factory=set, exclude=True)
    _source_files: set[str] = PrivateAttr(default_factory=set)

    @model_validator(mode="after")
    def add_name(self) -> HashableDevice:
        self.names.add(self.name)
        return self

    def as_normal_device(self):
        return Device.model_validate(self)

    def _hash_input(self):
        data = self.model_dump(exclude_defaults=True)
        hash_keys: dict[str, HashInclusion | DictHashInclusion] = self.hash_model.shallow_dump()
        for field_name, hash_inclusion in hash_keys.items():
            if field_name in data:
                if hash_inclusion in [HashInclusion.EXCLUDE, HashInclusion.VARIANT]:
                    del data[field_name]
                elif isinstance(hash_inclusion, DictHashInclusion):
                    if hash_inclusion.field_inclusion in [
                        HashInclusion.EXCLUDE,
                        HashInclusion.VARIANT,
                    ]:
                        del data[field_name]
                    elif hash_inclusion.inclusion_keys is not None:
                        data[field_name] = {
                            k: v
                            for k, v in data[field_name].items()
                            if k in hash_inclusion.inclusion_keys
                        }
        return json.dumps(data, sort_keys=True, cls=ExtendedEncoder)

    def __hash__(self) -> int:
        return int(hashlib.md5(self._hash_input().encode()).hexdigest(), 16)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, self.__class__):
            return False
        if hash(self) == hash(value):
            return True
        return False

    def _variant_info(self) -> dict:
        """Returns the content of this model instance relevant for device variants"""
        data = self.model_dump(exclude=["hash_model"])
        for field_name, hash_inclusion in self.hash_model.shallow_dump().items():
            # Keep everything with HashInclusion.VARIANT but don't delete DictHashInclusion
            if hash_inclusion in (HashInclusion.INCLUDE, HashInclusion.EXCLUDE):
                del data[field_name]
            elif isinstance(hash_inclusion, DictHashInclusion):
                # Get rid of it if we include or exclude the whole field or some combination thereof
                if hash_inclusion.field_inclusion == HashInclusion.EXCLUDE:
                    del data[field_name]
                elif hash_inclusion.field_inclusion == HashInclusion.INCLUDE and (
                    # Including the whole field:
                    hash_inclusion.inclusion_keys is None
                    # Including some and excluding the rest:
                    or hash_inclusion.remainder_inclusion == HashInclusion.EXCLUDE
                ):
                    del data[field_name]
                # If the remainder policy is set, strip the the keys which are included
                elif hash_inclusion.remainder_inclusion == HashInclusion.VARIANT:
                    # inclusion_keys must be specified if remainder_inclusion is not None
                    data[field_name] = {
                        k: v
                        for k, v in data[field_name].items()
                        if k not in hash_inclusion.inclusion_keys
                    }
                # ignore the case where field_inclusion is VARIANT, keep the whole field
        return data

    def is_variant(self, other: HashableDevice) -> bool:
        """Check if other is a variant of self."""
        if self != other:
            return False  # always includes the hash model
        if self._variant_info() == other._variant_info():
            return False  # devices are completely identical
        return True

    def add_sources(self, other: HashableDevice):
        self._source_files.update(other._source_files)

    def add_tags(self, other: HashableDevice):
        self.deviceTags.update(other.deviceTags)

    def add_names(self, other: HashableDevice):
        self.names.update(other.names)

    def add_variants(self, other: HashableDevice):
        self.variants.update(other.variants)


DevicePartial = make_all_fields_optional(Device, "DevicePartial")
