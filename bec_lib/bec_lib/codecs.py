from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from typing import Any, Type

import numpy as np
from pydantic import BaseModel

from bec_lib import messages as messages_module
from bec_lib import numpy_encoder
from bec_lib.device import DeviceBase
from bec_lib.endpoints import EndpointInfo
from bec_lib.messages import BECMessage, BECStatus


class BECCodec(ABC):
    """Abstract base class for custom encoders"""

    obj_type: Type | list[Type]

    @staticmethod
    @abstractmethod
    def encode(obj: Any) -> Any:
        """Encode an object into a serializable format."""

    @staticmethod
    @abstractmethod
    def decode(type_name: str, data: dict):
        """Decode data into an object."""


class NumpyEncoder(BECCodec):
    obj_type: list[Type] = [np.ndarray, np.bool_, np.number, complex]

    @staticmethod
    def encode(obj: np.ndarray) -> dict:
        return numpy_encoder.numpy_encode(obj)

    @staticmethod
    def decode(type_name: str, data: dict) -> np.ndarray:
        return numpy_encoder.numpy_decode(data)


class NumpyEncoderList(BECCodec):
    obj_type: list[Type] = [np.ndarray, np.bool_, np.number, complex]

    @staticmethod
    def encode(obj: np.ndarray) -> dict:
        return numpy_encoder.numpy_encode_list(obj)

    @staticmethod
    def decode(type_name: str, data: dict) -> np.ndarray:
        return numpy_encoder.numpy_decode_list(data)


class BECMessageEncoder(BECCodec):
    obj_type: Type = BECMessage

    @staticmethod
    def encode(obj: BECMessage) -> dict:
        return obj.__dict__

    @staticmethod
    def decode(type_name: str, data: dict) -> BECMessage:
        return getattr(messages_module, type_name)(**data)


class EnumEncoder(BECCodec):
    obj_type: Type = enum.Enum

    @staticmethod
    def encode(obj: enum.Enum) -> Any:
        return obj.value

    @staticmethod
    def decode(type_name: str, data: Any) -> Any:
        if type_name == "BECStatus":
            return BECStatus(data)
        return data


class BECDeviceEncoder(BECCodec):
    obj_type: Type = DeviceBase

    @staticmethod
    def encode(obj: DeviceBase) -> str:
        if hasattr(obj, "_compile_function_path"):
            # pylint: disable=protected-access
            return obj._compile_function_path()
        return obj.name

    @staticmethod
    def decode(type_name: str, data: str) -> str:
        """
        DeviceBase objects are encoded as strings. No decoding is necessary.
        """
        return data


class PydanticEncoder(BECCodec):
    obj_type: Type = BaseModel

    @staticmethod
    def encode(obj: BaseModel) -> dict:
        return obj.model_dump()

    @staticmethod
    def decode(type_name: str, data: dict) -> dict:
        return data


class EndpointInfoEncoder(BECCodec):
    obj_type: Type = EndpointInfo

    @staticmethod
    def encode(obj: EndpointInfo) -> dict:
        return {
            "endpoint": obj.endpoint,
            "message_type": obj.message_type.__name__,
            "message_op": obj.message_op,
        }

    @staticmethod
    def decode(type_name: str, data: dict) -> EndpointInfo:
        return EndpointInfo(
            endpoint=data["endpoint"],
            message_type=getattr(messages_module, data["message_type"]),
            message_op=data["message_op"],
        )


class SetEncoder(BECCodec):
    obj_type: Type = set

    @staticmethod
    def encode(obj: set) -> list:
        return list(obj)

    @staticmethod
    def decode(type_name: str, data: list) -> set:
        return set(data)


class BECTypeEncoder(BECCodec):
    obj_type: Type = type

    @staticmethod
    def encode(obj: type) -> dict:
        return {"type_name": obj.__name__, "module": obj.__module__}

    @staticmethod
    def decode(type_name: str, data: dict) -> type:
        if data["module"] == "builtins":
            return __builtins__.get(data["type_name"])
        if data["module"] == "bec_lib.messages":
            return getattr(messages_module, data["type_name"])
        if data["module"] == "numpy":
            return getattr(np, data["type_name"])
        raise ValueError(f"Unknown type {data['type_name']} in module {data['module']}")
