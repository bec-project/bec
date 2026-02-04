# pylint: disable=too-many-lines
from __future__ import annotations

import base64
from io import BytesIO
from typing import Annotated, Any, Callable, ClassVar

import numpy as np
from pydantic import (
    BaseModel,
    ConfigDict,
    PlainSerializer,
    WithJsonSchema,
    computed_field,
    model_validator,
)


def ndarray_to_bytes(arr: np.ndarray):
    # b64 encoding of resulting bytes is handled for us by pydantic
    out_buf = BytesIO()
    np.save(out_buf, arr)
    return out_buf.getvalue()


def numpy_decode(input: str | bytes | np.ndarray):
    if isinstance(input, np.ndarray):
        return input
    if isinstance(input, str):
        input = base64.urlsafe_b64decode(input)
    io = BytesIO(input)
    return np.load(io)


NumpyField = Annotated[
    np.ndarray,
    PlainSerializer(ndarray_to_bytes),
    WithJsonSchema({"type": "string", "contentEncoding": "base64"}),
]


class BecCodecInfo(BaseModel):
    type_name: str


class BECSerializable(BaseModel):
    _deserialization_registry: ClassVar[dict[type, Callable[[Any], Any]]] = {
        np.ndarray: numpy_decode
    }

    model_config = ConfigDict(json_schema_serialization_defaults_required=True)

    @computed_field()
    @property
    def bec_codec(self) -> BecCodecInfo:
        return BecCodecInfo(type_name=self.__class__.__name__)

    @model_validator(mode="before")
    @classmethod
    def deser_custom(cls, data: dict[str, Any]):
        for field in data:
            if (field_info := cls.model_fields.get(field)) is not None:
                if field_info.annotation is None:
                    continue
                if deserializer := cls._deserialization_registry.get(field_info.annotation):
                    data[field] = deserializer(data[field])
        return data
