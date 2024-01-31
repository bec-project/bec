from __future__ import annotations

import collections
import contextlib
import dataclasses
import enum
import gc
import inspect
import json
import sys
import time
from abc import abstractmethod
from copy import deepcopy

import msgpack as msgpack_module
import numpy as np

from bec_lib.logger import bec_logger
from bec_lib import numpy_encoder
from bec_lib import messages as messages_module
from bec_lib.messages import BECStatus, BECMessage

logger = bec_logger.logger


def encode_bec_message_v12(msg):
    if not isinstance(msg, BECMessage):
        return msg

    msg_version = 1.2
    msg_body = msgpack.dumps(msg.__dict__)
    msg_header = json.dumps(
        {
            "msg_type": msg.msg_type,
        }
    ).encode()
    header = f"BECMSG_{msg_version}_{len(msg_header)}_{len(msg_body)}_EOH_".encode()
    return header + msg_header + msg_body


def decode_bec_message_v12(raw_bytes):
    declaration, msg_header_body = raw_bytes.split(b"_EOH_", maxsplit=1)
    _, version, header_length, _ = declaration.split(b"_")
    header = msg_header_body[: int(header_length)]
    body = msg_header_body[int(header_length) :]
    header = json.loads(header.decode())
    msg_body = msgpack.loads(body)
    msg_class = get_message_class(header.pop("msg_type"))
    msg = msg_class(**header, **msg_body)
    # shouldn't this be checked when the msg is used? or when the message is created?
    if msg._is_valid():
        return msg


def encode_bec_status(status):
    if not isinstance(status, BECStatus):
        return status
    return status.value.to_bytes(1, "big")  # int.to_bytes


def decode_bec_status(value):
    return BECStatus(int.from_bytes(value, "big"))


class MsgpackExt:
    """Encapsulates msgpack dumps/loads with extensions"""

    def __init__(self):
        self._encoder = []
        self._ext_decoder = {}
        self._object_hook_decoder = []

    def register_ext_type(self, encoder, decoder):
        """Register an encoder and a decoder

        The order registrations are made counts, the encoding process is done
        in the same order until a compatible encoder is found.

        Args:
            encoder: Function encoding a data into a serializable data.
            decoder: Function decoding a serialized data into a usable data.
        """
        exttype = len(self._ext_decoder)
        if exttype in self._ext_decoder:
            ValueError("ExtType %d already used" % exttype)
        self._encoder.append((encoder, exttype))
        self._ext_decoder[exttype] = decoder

    def register_object_hook(self, encoder, decoder):
        """Register an encoder and a decoder that can convert a python object
        into data which can be serialized by msgpack.

        Args:
            encoder: Function encoding a data into a data serializable by msgpack
            decoder: Function decoding a python structure provided by msgpack
            into an usable data.
        """
        self._encoder.append((encoder, None))
        self._object_hook_decoder.append(decoder)

    def register_numpy(self):
        """
        Register BEC custom numpy encoder as a codec.
        """
        self.register_object_hook(numpy_encoder.numpy_encode, numpy_encoder.numpy_decode)

    def register_bec_message(self):
        """
        Register codec for BECMessage
        """
        # order matters
        self.register_ext_type(encode_bec_status, decode_bec_status)
        self.register_ext_type(encode_bec_message_v12, decode_bec_message_v12)

    def _default(self, obj):
        for encoder, exttype in self._encoder:
            result = encoder(obj)
            if result is obj:
                # Nothing was done, assume this encoder do not support this
                # object kind
                continue
            if exttype is not None:
                return msgpack_module.ExtType(exttype, result)
            else:
                return result
        raise TypeError("Unknown type: %r" % (obj,))

    def _ext_hooks(self, code, data):
        decoder = self._ext_decoder.get(code, None)
        if decoder is not None:
            obj = decoder(data)
            return obj
        return msgpack_module.ExtType(code, data)

    def _object_hook(self, data):
        for decoder in self._object_hook_decoder:
            try:
                result = decoder(data)
            except TypeError:
                continue
            if data is not result:
                # In case the input is not the same as the output,
                # consider it found the good decoder and it worked
                break
        else:
            return data

        return result

    def dumps(self, obj):
        """Pack object `o` and return packed bytes."""
        return msgpack_module.packb(obj, default=self._default)

    def loads(self, raw_bytes, raw=False, strict_map_key=True):
        return msgpack_module.unpackb(
            raw_bytes,
            object_hook=self._object_hook,
            ext_hook=self._ext_hooks,
            raw=raw,
            strict_map_key=strict_map_key,
        )

    # def Unpacker(self, raw=True, max_buffer_size=0) -> msgpack.Unpacker:
    #    """Streaming unpacker."""
    #    return msgpack.Unpacker(
    #        raw=raw,
    #        max_buffer_size=max_buffer_size,
    #        ext_hook=self._ext_hooks,
    #        object_hook=self._object_hook,
    #        strict_map_key=False,
    #    )


msgpack = MsgpackExt()
msgpack.register_numpy()
msgpack.register_bec_message()


class SerializationInterface:
    """Base class for message serialization"""

    @abstractmethod
    def loads(self, msg, **kwargs) -> dict:
        """load and de-serialize a message"""

    @abstractmethod
    def dumps(self, msg, **kwargs) -> str:
        """serialize a message"""


def get_message_class(msg_type: str):
    """Given a message type, tries to find the corresponding message class in the module"""
    module = messages_module
    # convert snake_style to CamelCase
    class_name = "".join(part.title() for part in msg_type.split("_"))
    try:
        # maybe as easy as that...
        klass = getattr(module, class_name)
        # belts and braces
        if getattr(klass, "msg_type") == msg_type:
            return klass
    except AttributeError:
        # try better
        module_classes = inspect.getmembers(module, inspect.isclass)
        for class_name, klass in module_classes:
            try:
                klass_msg_type = getattr(klass, "msg_type")
            except AttributeError:
                continue
            else:
                if msg_type == klass_msg_type:
                    return klass


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
    def loads(msg, version=None) -> dict:
        with pause_gc():
            if version is None:
                try:
                    if isinstance(msg, bytes):
                        if len(msg) < 10:
                            raise RuntimeError("Invalid BEC message")
                        offset = MsgpackSerialization.ext_type_offset_to_data[msg[0]]
                        if msg[offset:].startswith(b"BECMSG"):
                            version = float(msg[offset + 7 : offset + 7 + 3])
                    else:
                        msg = json.loads(msg)
                        version = msg["version"]
                except Exception:
                    version = 1.0
            if version == 1.2:
                msg = msgpack.loads(msg)
                if msg is not None:
                    if msg.msg_type == "bundle_message":
                        return msg.messages
                return msg
            raise RuntimeError(f"Unsupported BECMessage version {version}.")

    @staticmethod
    def dumps(msg, version=None) -> str:
        if version is None or version == 1.2:
            return msgpack.dumps(msg)
        else:
            raise RuntimeError(f"Unsupported BECMessage version {version}.")