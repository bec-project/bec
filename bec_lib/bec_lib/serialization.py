"""
Serialization module for BEC messages
"""

from __future__ import annotations

import contextlib
import gc
import json
from abc import abstractmethod

import msgpack as msgpack_module

from bec_lib import messages as messages_module
from bec_lib.logger import bec_logger
from bec_lib.messages import BECMessage
from bec_lib.serialization_registry import SerializationRegistry
from bec_lib.serialization_registry_v1 import msgpack as deprecated_msgpack

logger = bec_logger.logger


class SerializationInterface:
    """Base class for message serialization"""

    @abstractmethod
    def loads(self, msg, **kwargs) -> dict:
        """load and de-serialize a message"""

    @abstractmethod
    def dumps(self, msg, **kwargs) -> str:
        """serialize a message"""


class BECMessagePack(SerializationRegistry):
    """Encapsulates msgpack dumps/loads with extensions"""

    def dumps(self, obj):
        """Pack object `obj` and return packed bytes."""
        return msgpack_module.packb(obj, default=self.encode)

    def loads(self, raw_bytes):
        """Unpack bytes and return the decoded object."""
        out = msgpack_module.unpackb(
            raw_bytes, raw=False, strict_map_key=True, object_hook=self.decode
        )
        if isinstance(out, msgpack_module.ExtType):
            # unpacking did not work, try using the legacy msgpack decoder
            return deprecated_msgpack.loads(raw_bytes)
        return out


class BECJson(SerializationRegistry):
    """Encapsulates JSON dumps/loads with extensions"""

    use_json = True

    def dumps(self, obj):
        """Pack object `obj` and return packed bytes."""
        return json.dumps(obj, default=self.encode)

    def loads(self, raw_bytes):
        """Unpack bytes and return the decoded object."""
        return json.loads(raw_bytes, object_hook=self.decode)


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


class MsgpackSerialization(SerializationInterface):
    """Message serialization using msgpack encoding"""

    ext_type_offset_to_data = {199: 3, 200: 4, 201: 6}

    @staticmethod
    def loads(msg) -> BECMessage | list[BECMessage]:
        with pause_gc():
            try:
                msg = msgpack.loads(msg)
            except Exception as exception:
                try:
                    data = json.loads(msg)
                    return messages_module.RawMessage(data=data)
                except Exception:
                    pass
                raise RuntimeError("Failed to decode BECMessage") from exception
            else:
                if isinstance(msg, BECMessage):
                    if msg.msg_type == "bundle_message":
                        return msg.messages
                return msg

    @staticmethod
    def dumps(msg, version=None) -> str:
        if version is None or version == 1.2:
            return msgpack.dumps(msg)
        raise RuntimeError(f"Unsupported BECMessage version {version}.")


msgpack = BECMessagePack()
json_ext = BECJson()
