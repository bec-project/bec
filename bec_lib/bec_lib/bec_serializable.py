# pylint: disable=too-many-lines
from __future__ import annotations

import base64
from io import BytesIO
from types import UnionType
from typing import Annotated, Any, Callable, ClassVar, Union, get_args, get_origin

import numpy as np
from pydantic import (
    BaseModel,
    ConfigDict,
    PlainSerializer,
    WithJsonSchema,
    computed_field,
    model_validator,
)

_NDARRAY_TAG = b"__NP_NDARRAY__"
_NDARRAY_TAG_STR = _NDARRAY_TAG.decode()
_NDARRAY_TAG_OFFSET = len(_NDARRAY_TAG)


def ndarray_to_bytes(arr: np.ndarray) -> bytes:
    if not isinstance(arr, np.ndarray):
        return arr
    out_buf = BytesIO()
    np.save(out_buf, arr)
    return _NDARRAY_TAG + base64.urlsafe_b64encode(out_buf.getvalue())


def numpy_decode(input: str | bytes):
    is_str = isinstance(input, str)
    is_bytes = isinstance(input, bytes)
    # let pydantic handle any other validation or coercion
    if not (is_str or is_bytes):
        return input
    if is_str and not input.startswith(_NDARRAY_TAG_STR):
        return input
    if is_bytes and not input.startswith(_NDARRAY_TAG):
        return input
    # strip the tag, decode, and load
    io = BytesIO(base64.urlsafe_b64decode(input[_NDARRAY_TAG_OFFSET:]))
    return np.load(io)


NumpyField = Annotated[
    np.ndarray,
    PlainSerializer(ndarray_to_bytes),
    WithJsonSchema({"type": "string", "contentEncoding": "base64"}),
]


def serialize_type(cls: type):
    return cls.__name__


class BecCodecInfo(BaseModel):
    type_name: str


class BECSerializable(BaseModel):

    _deserialization_registry: ClassVar[
        list[tuple[tuple[type | Annotated, ...], Callable[[Any], Any]]]
    ] = [((NumpyField, np.ndarray), numpy_decode)]

    model_config = ConfigDict(
        json_schema_serialization_defaults_required=True,
        json_encoders={np.ndarray: ndarray_to_bytes},
        arbitrary_types_allowed=True,
    )

    @computed_field()
    @property
    def bec_codec(self) -> BecCodecInfo:
        return BecCodecInfo(type_name=self.__class__.__name__)

    @classmethod
    def _try_apply_registry(cls, anno: type, data: dict, field: str):
        for entry, deserializer in cls._deserialization_registry:
            if anno in entry:
                data[field] = deserializer(data[field])

    @model_validator(mode="before")
    @classmethod
    def deser_custom(cls, data: dict[str, Any]):
        for field in data:
            if (field_info := cls.model_fields.get(field)) is not None:
                if field_info.annotation is None:
                    continue  # No need to do anything for NoneType
                if get_origin(field_info.annotation) in [UnionType, Union]:
                    for arg in get_args(field_info.annotation):
                        cls._try_apply_registry(arg, data, field)
                else:
                    cls._try_apply_registry(field_info.annotation, data, field)
        return data


class BecWrappedValue(BECSerializable):
    data: np.ndarray  # can be extended, must be in registry

    def __getattr__(self, name: str) -> Any:
        if hasattr(self.data, name):
            return getattr(self.data, name)
        else:
            raise AttributeError(
                f"{self.__class__.__name__} wrapping data type {type(self.data)} has no attribute {name} on either itself or its data."
            )

    def __getitem__(self, item):
        if hasattr(self.data, "__getitem__"):
            return self.data.__getitem__(item)
        else:
            raise AttributeError(f"Wrapped data type {type(self.data)} has no __getitem__.")
