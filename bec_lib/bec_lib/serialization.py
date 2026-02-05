"""
Serialization module for BEC messages
"""

from __future__ import annotations

import contextlib
import gc
import json
import types
from functools import lru_cache
from typing import Any

import msgpack
from pydantic import BaseModel

from bec_lib import endpoints, messages
from bec_lib.bec_serializable import BECSerializable
from bec_lib.messages import BECMessage, BundleMessage, RawMessage


@lru_cache(maxsize=2048)
def _get_type(type_name: str) -> type[BECSerializable] | None:
    for mod in BecSerializableCodec.registry.values():
        if (T := mod.__dict__.get(type_name)) is not None:
            if not issubclass(T, BECSerializable):
                raise RuntimeError(
                    f"BecSerializableCodec found type {T} in module {mod} from bec_codec type info '{type_name}'. Please ensure another type isn't shadowing the correct one."
                )
            return T


class BecSerializableCodec:
    # dicts are ordered by insertion, so later additions will not override these.
    registry: dict[str, types.ModuleType] = {"messages": messages, "endpoints": endpoints}

    @classmethod
    def register_module(cls, mod: types.ModuleType) -> None:
        if mod.__name__ in cls.registry:
            raise ValueError(f"A module named {mod.__name__} is already registered!")
        cls.registry[mod.__name__] = mod
        _get_type.cache_clear()

    @classmethod
    def encode(cls, obj: BaseModel) -> dict:
        return obj.model_dump(mode="json")

    @classmethod
    def decode(cls, type_name: str, data: dict) -> BECSerializable | dict:
        if (BecType := _get_type(type_name)) is not None:
            return BecType.model_validate(data)
        return data


@contextlib.contextmanager
def pause_gc():
    """Pause the garbage collector while doing a lot of allocations, to prevent
    intempestive collect in case of big messages or if a lot of strings allocated;
    this follows the advice here: https://github.com/msgpack/msgpack-python?tab=readme-ov-file#performance-tips

    Maybe should be limited to big messages? Didn't evaluated the cost of pausing/re-enabling the GC
    """
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


def _msg_object_hook(msg: dict):
    bec_type_name: str | None = msg.get("bec_codec", {}).get("type_name")
    if bec_type_name is None:
        return msg
    return BecSerializableCodec.decode(bec_type_name, msg)


class MsgpackSerialization:
    """Message serialization using msgpack encoding"""

    @staticmethod
    def loads(msg: bytes) -> BECMessage | list[BECMessage] | Any:
        with pause_gc():
            try:
                msg_ = msgpack.loads(msg, object_hook=_msg_object_hook)
            except Exception as e:
                try:
                    return RawMessage(data=json.loads(msg, object_hook=_msg_object_hook))
                except Exception:
                    raise RuntimeError(f"Failed to decode BECMessage: {msg}") from e
            else:
                if isinstance(msg_, BundleMessage):
                    return msg_.messages
                return msg_

    @staticmethod
    def dumps(msg: BECMessage | Any) -> str:
        if not isinstance(msg, BECSerializable):
            return msgpack.dumps(msg)  # type: ignore
        return msgpack.dumps(msg.model_dump(mode="json"))  # type: ignore


class json_ext:
    """Message serialization using json encoding"""

    @staticmethod
    def loads(msg) -> BECMessage | list[BECMessage] | Any:
        with pause_gc():
            return json.loads(msg, object_hook=_msg_object_hook)

    @staticmethod
    def dumps(msg: BECMessage | Any, indent: int = 0) -> str:
        if not isinstance(msg, BECSerializable):
            return json.dumps(msg, indent=indent)  # type: ignore
        return msg.model_dump_json(indent=indent)
