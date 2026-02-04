"""
Serialization module for BEC messages
"""

from __future__ import annotations

import contextlib
import gc
import json
from abc import abstractmethod
from typing import Any

import msgpack

from bec_lib import messages as messages_module
from bec_lib.logger import bec_logger
from bec_lib.messages import BECMessage, BundleMessage

logger = bec_logger.logger


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


def _validate_dict(input: dict[str, Any] | Any) -> BECMessage | Any:
    if not isinstance(input, dict):
        return input
    if (msg_cls_name := input.get("__bec_codec__")) is None:
        return input
        # raise ValueError(f"Deserialization type ('__bec_codec__') not in data: {input}")
    if (msg_cls := messages_module.__dict__.get(msg_cls_name)) is None:
        raise TypeError(f"Deserialization type '{msg_cls_name}' not found.")
    if not issubclass(msg_cls, BECMessage):
        raise TypeError(f"'{msg_cls_name}' is not a deserializable type.")
    return msg_cls.model_validate(input)


class SerializationInterface:
    """Base class for message serialization"""

    @staticmethod
    @abstractmethod
    def loads(msg: bytes | str) -> BECMessage | list[BECMessage]:
        """load and de-serialize a message"""

    @staticmethod
    @abstractmethod
    def dumps(msg: BECMessage) -> bytes | str:
        """serialize a message"""


class MsgpackSerialization(SerializationInterface):
    """Message serialization using msgpack encoding"""

    @staticmethod
    def loads(msg: bytes | str):
        with pause_gc():
            try:
                _msg: BECMessage = msgpack.unpackb(msg, object_hook=_validate_dict)
            except Exception as exception:
                try:
                    _msg: BECMessage = json.loads(msg, object_hook=_validate_dict)
                except Exception:
                    pass
                raise RuntimeError("Failed to decode BECMessage") from exception
            else:
                if isinstance(_msg, BundleMessage):
                    return _msg.messages
                return _msg

    @staticmethod
    def dumps(msg: BECMessage) -> bytes:
        return msgpack.packb(msg.model_dump(mode="json"))  # type: ignore # yes it does


class JsonSerialization(SerializationInterface):
    @staticmethod
    def loads(msg: bytes | str) -> BECMessage:
        return json.loads(msg, object_hook=_validate_dict)

    @staticmethod
    def dumps(msg: BECMessage) -> str:
        """serialize a message"""
        return msg.model_dump_json()
