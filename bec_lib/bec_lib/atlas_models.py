"""
This module provides the models for the BEC Atlas API.
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import AbstractSet, Any, Literal, TypeVar

from pydantic import BaseModel, Field, PrivateAttr, create_model, model_validator
from pydantic_core import PydanticUndefined

from bec_lib.utils.json import ExtendedEncoder

_BM = TypeVar("BM", bound=BaseModel)


def make_all_fields_optional(model: type[_BM], model_name: str) -> type[_BM]:
    """Convert all fields in a Pydantic model to Optional."""
    fields = {}
    for name, field in model.model_fields.items():
        field_info = field._attributes_set
        field_info.pop("annotation", None)
        if "default_factory" not in field_info:
            # default_factory and default are mutually exclusive
            field_info["default"] = (
                field.default if field.default is not PydanticUndefined else None
            )
        fields[name] = (field.annotation | None, Field(**field_info))
    new_model = create_model(model_name, **fields, __config__=model.model_config)
    return new_model


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
    def check_compatibility(self) -> DictHashInclusion:
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

    def shallow_dump(self) -> dict[str, _InclusionT]:
        return {k: getattr(self, k) for k in self.__class__.model_fields}


class Device(_DeviceModelCore):
    """
    Represents a device in the BEC Atlas API. This model is also used by the SciHub service to
    validate updates to the device configuration.
    """

    name: str


_ModelDumpKeys = list[str]
_ModelDumpDict = dict[str, Any]
_InclusionT = HashInclusion | DictHashInclusion
_HashModelShallowItems = list[tuple[str, _InclusionT]]
_RawDataCache = tuple[_ModelDumpKeys, _ModelDumpDict, _HashModelShallowItems]


class HashableDevice(Device, validate_assignment=True):

    hash_model: DeviceHashModel = DeviceHashModel()

    names: set[str] = Field(default_factory=set, exclude=True)
    variants: set[Device] = Field(default_factory=set, exclude=True)
    _source_files: set[str] = PrivateAttr(default_factory=set)

    @model_validator(mode="after")
    def add_name(self) -> HashableDevice:
        self.names.add(self.name)
        return self

    #############################################
    ############### Hashing Logic ###############
    #############################################

    # We can't use lru_cache, cached_property or similar, because they will try to cache `self`
    # and recurse infinitely. Fortunately it is easy enough to manage. Maybe this can be removed
    # if we don't allow assignment and store calculated hashes on first init.

    @model_validator(mode="after")
    def _clear_caches(self):
        """On assignment to any field, this validator will be called and reset caches."""
        self._raw_data_cache = None
        self._hash_input_cache = None
        self._hash_cache = None
        return self

    _raw_data_cache: None | _RawDataCache = PrivateAttr(default=None)

    def _hashing_data(self) -> _RawDataCache:
        """Store and return the raw data used for calculating the device hash."""
        if self._raw_data_cache is not None:
            return self._raw_data_cache
        model_data = self.model_dump(exclude_defaults=True)
        self._raw_data_cache = (
            list(model_data.keys()),
            model_data,
            list(self.hash_model.shallow_dump().items()),
        )
        return self._raw_data_cache

    _hash_input_cache: None | bytes = PrivateAttr(default=None)

    @staticmethod
    def _mutate_data_dict(
        data_keys: _ModelDumpKeys, data_dict: _ModelDumpDict, hash_keys: _HashModelShallowItems
    ):
        """Delete anything which shouldn't be included in the model dump, based on the hash model"""
        for field_name, hash_inclusion in hash_keys:
            if field_name not in data_keys or hash_inclusion == HashInclusion.INCLUDE:
                continue
            if hash_inclusion in (HashInclusion.EXCLUDE, HashInclusion.VARIANT):
                del data_dict[field_name]
                continue
            if hash_inclusion.field_inclusion in (HashInclusion.EXCLUDE, HashInclusion.VARIANT):
                del data_dict[field_name]
            elif hash_inclusion.inclusion_keys is not None:
                data_dict[field_name] = {
                    k: v
                    for k, v in data_dict[field_name].items()
                    if k in hash_inclusion.inclusion_keys
                }

    def _hash_input(self, raw_data: _RawDataCache) -> bytes:
        """Make bytes object to hash by dumping the remaining data to json, deterministically ordered"""
        if self._hash_input_cache is not None:
            return self._hash_input_cache
        data_keys, data_dict, hash_keys = raw_data
        self._mutate_data_dict(data_keys, data_dict, hash_keys)
        self._hash_input_cache = json.dumps(data_dict, sort_keys=True, cls=ExtendedEncoder).encode()
        return self._hash_input_cache

    _hash_cache: None | int = PrivateAttr(default=None)

    def __hash__(self) -> int:
        if self._hash_cache is not None:
            return self._hash_cache
        self._hash_cache = int(hashlib.md5(self._hash_input(self._hashing_data())).hexdigest(), 16)
        return self._hash_cache

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, self.__class__):
            return False
        if hash(self) == hash(value):
            return True
        return False

    #############################################
    ############### Variant Logic ###############
    #############################################

    def _variant_info(self) -> dict:
        """Returns the content of this model instance relevant for device variants"""
        data = self.model_dump(exclude=["hash_model"])
        for field_name, hash_inclusion in self.hash_model.shallow_dump().items():
            # Keep everything with HashInclusion.VARIANT but don't delete DictHashInclusion
            if hash_inclusion == HashInclusion.VARIANT:
                continue
            if hash_inclusion in (HashInclusion.INCLUDE, HashInclusion.EXCLUDE):
                del data[field_name]
                continue
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

    #############################################
    ################## Utility ##################
    #############################################

    def as_normal_device(self):
        return Device.model_validate(self)

    def add_sources(self, other: HashableDevice):
        """Update the set of source files from another device"""
        self._source_files.update(other._source_files)

    def add_tags(self, other: HashableDevice):
        """Update the set of tags from another device"""
        self.deviceTags.update(other.deviceTags)

    def add_names(self, other: HashableDevice):
        """Update the set of names from another device"""
        self.names.update(other.names)

    def add_variant(self, other: HashableDevice):
        """Add another device as a variant of this one."""
        self.variants.add(other.as_normal_device())


class HashableDeviceSet(set):
    def __or__(self, value: AbstractSet[HashableDevice]) -> HashableDeviceSet:
        for item in self:
            if item not in value:
                continue
            for other_item in value:
                if other_item == item:
                    item.add_sources(other_item)
                    item.add_tags(other_item)
                    item.add_names(other_item)
                    if other_item.is_variant(item):
                        item.add_variant(other_item)
        for other_item in value:
            if other_item not in self:
                self.add(other_item)
        return self


DevicePartial = make_all_fields_optional(Device, "DevicePartial")
