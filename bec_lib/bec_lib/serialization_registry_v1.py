"""
Deprecated serialization module for BEC messages. Can be removed in future versions, cf issue #516.
"""

from __future__ import annotations

import inspect
import json

import msgpack as msgpack_module
from pydantic import BaseModel

from bec_lib import messages as messages_module
from bec_lib import numpy_encoder
from bec_lib.device import DeviceBase
from bec_lib.endpoints import EndpointInfo
from bec_lib.logger import bec_logger
from bec_lib.messages import BECMessage, BECStatus

logger = bec_logger.logger


def encode_bec_message_v2(msg):
    if not isinstance(msg, BECMessage):
        return msg
    return msg.__dict__


def decode_bec_message_v2(type_name: str, data: dict):
    if not isinstance(data, dict):
        return data
    return getattr(messages_module, type_name)(**data)


def encode_bec_message_v12(msg):
    if not isinstance(msg, BECMessage):
        return msg

    msg_version = 1.2
    msg_body = msgpack.dumps(msg.__dict__)
    msg_header = json.dumps({"msg_type": msg.msg_type}).encode()
    header = f"BECMSG_{msg_version}_{len(msg_header)}_{len(msg_body)}_EOH_".encode()
    return header + msg_header + msg_body


def decode_bec_message_v12(raw_bytes):
    try:
        # kept for the record:
        # offset = MsgpackSerialization.ext_type_offset_to_data[raw_bytes[0]]
        # (was not so easy to find from msgpack doc)
        if raw_bytes.startswith(b"BECMSG"):
            version = float(raw_bytes[7:10])
            if version < 1.2:
                raise RuntimeError(f"Unsupported BECMessage version {version}")
    except Exception as exception:
        raise RuntimeError("Failed to decode BECMessage") from exception

    try:
        declaration, msg_header_body = raw_bytes.split(b"_EOH_", maxsplit=1)
        _, version, header_length, _ = declaration.split(b"_")
        header = msg_header_body[: int(header_length)]
        body = msg_header_body[int(header_length) :]
        header = json.loads(header.decode())
        msg_body = msgpack.loads(body)
        msg_class = get_message_class(header.pop("msg_type"))
        msg = msg_class(**header, **msg_body)
    except Exception as exception:
        raise RuntimeError("Failed to decode BECMessage") from exception

    # shouldn't this be checked when the msg is used? or when the message is created?
    return msg


def encode_bec_message_json(msg):
    if not isinstance(msg, BECMessage):
        return msg

    msg_version = 1.2
    out = {"msg_type": msg.msg_type, "msg_version": msg_version, "msg_body": msg.__dict__}
    return out


def decode_bec_message_json(data):
    if not isinstance(data, dict):
        return data

    if set(["msg_type", "msg_version", "msg_body"]) != set(data.keys()):
        return data

    try:
        msg_class = get_message_class(data.pop("msg_type"))
        msg = msg_class(**data["msg_body"])
    except Exception as exception:
        raise RuntimeError("Failed to decode BECMessage") from exception

    return msg


def encode_bec_status(status):
    if not isinstance(status, BECStatus):
        return status
    return status.value.to_bytes(1, "big")  # int.to_bytes


def decode_bec_status(value):
    return BECStatus(int.from_bytes(value, "big"))


def encode_bec_status_json(status):
    if not isinstance(status, BECStatus):
        return status
    return {"__becstatus__": status.value}  # int.to_bytes


def decode_bec_status_json(value):
    if "__becstatus__" not in value:
        return value
    return BECStatus(value["__becstatus__"])


def encode_set(obj):
    if isinstance(obj, set):
        return {"__msgpack__": {"type": "set", "data": list(obj)}}
    return obj


def decode_set(obj):
    if isinstance(obj, dict) and "__msgpack__" in obj and obj["__msgpack__"]["type"] == "set":
        return set(obj["__msgpack__"]["data"])
    return obj


def encode_endpointInfo(obj):
    if isinstance(obj, EndpointInfo):
        return {
            "__msgpack__": {
                "type": "message_endpointinfo",
                "data": {
                    "endpoint": obj.endpoint,
                    "message_type": obj.message_type.__name__,
                    "message_op": obj.message_op,
                },
            }
        }
    return obj


def decode_endpointinfo(obj):
    if (
        isinstance(obj, dict)
        and "__msgpack__" in obj
        and obj["__msgpack__"]["type"] == "message_endpointinfo"
    ):
        return EndpointInfo(
            obj["__msgpack__"]["data"]["endpoint"],
            getattr(messages_module, obj["__msgpack__"]["data"]["message_type"]),
            obj["__msgpack__"]["data"]["message_op"],
        )
    return obj


def encode_bec_type(msg):
    if isinstance(msg, type):
        return {"__msgpack__": {"type": "bec_type", "data": msg.__name__, "module": msg.__module__}}
    return msg


def decode_bec_type(obj):
    if isinstance(obj, dict) and "__msgpack__" in obj and obj["__msgpack__"]["type"] == "bec_type":
        if obj["__msgpack__"]["module"] == "bec_lib.messages":
            return getattr(messages_module, obj["__msgpack__"]["data"])
        if (
            obj["__msgpack__"]["module"] == "builtins"
            and obj["__msgpack__"]["data"] in __builtins__
        ):
            return __builtins__.get(obj["__msgpack__"]["data"])
        raise ValueError("Unknown module")
    return obj


def encode_pydantic(obj):
    if isinstance(obj, BECMessage):
        # BECMessage is handled by the BECMessage codec
        return obj
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    return obj


def decode_pydantic(obj):
    return obj


def encode_bec_device(obj):
    """
    Encode a DeviceBase object into
    a string representation of the device name.
    """
    if isinstance(obj, DeviceBase):
        if hasattr(obj, "_compile_function_path"):
            # pylint: disable=protected-access
            return obj._compile_function_path()
        return obj.name
    return obj


def decode_bec_device(obj):
    """
    DeviceBase objects are encoded as strings. No decoding is necessary.
    """
    return obj


class SerializationRegistry:
    """Registry for serialization codecs"""

    use_json = False

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
            raise ValueError("ExtType %d already used" % exttype)
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

    def register_numpy(self, use_list=False):
        """
        Register BEC custom numpy encoder as a codec.
        """
        if use_list:
            self.register_object_hook(
                numpy_encoder.numpy_encode_list, numpy_encoder.numpy_decode_list
            )
        else:
            self.register_object_hook(numpy_encoder.numpy_encode, numpy_encoder.numpy_decode)

    def register_bec_message(self):
        """
        Register codec for BECMessage
        """
        if not self.use_json:
            # order matters
            self.register_ext_type(encode_bec_status, decode_bec_status)
            self.register_ext_type(encode_bec_message_v12, decode_bec_message_v12)
        else:
            self.register_object_hook(encode_bec_message_json, decode_bec_message_json)

    def register_bec_status(self):
        """
        Register codec for BECStatus
        """
        self.register_object_hook(encode_bec_status_json, decode_bec_status_json)

    def register_set_encoder(self):
        """
        Register codec for set
        """
        self.register_object_hook(encode_set, decode_set)

    def register_message_endpoint(self):
        """
        Register codec for MessageEndpoints
        """
        self.register_object_hook(encode_endpointInfo, decode_endpointinfo)

    def register_bec_message_type(self):
        """
        Register codec for BECMessage type
        """
        self.register_object_hook(encode_bec_type, decode_bec_type)

    def register_pydantic(self):
        """
        Register codec for pydantic models
        """
        self.register_object_hook(encode_pydantic, decode_pydantic)

    def register_bec_device(self):
        """
        Register codec for DeviceBase objects
        """
        self.register_object_hook(encode_bec_device, decode_bec_device)


class MsgpackExt(SerializationRegistry):
    """Encapsulates msgpack dumps/loads with extensions"""

    def _default(self, obj):
        for encoder, exttype in self._encoder:
            result = encoder(obj)
            if result is obj:
                # Nothing was done, assume this encoder do not support this
                # object kind
                continue
            if exttype is not None:
                return msgpack_module.ExtType(exttype, result)
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


class JsonExt(SerializationRegistry):
    """Encapsulates JSON dumps/loads with extensions"""

    use_json = True

    def _default(self, obj):
        for encoder, _ in self._encoder:
            result = encoder(obj)
            if result is obj:
                # Nothing was done, assume this encoder does not support this
                # object kind
                continue
            return result

    def _ext_hooks(self, data):
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
        """Serialize object `obj` and return serialized JSON string."""
        return json.dumps(obj, default=self._default)

    def loads(self, json_str):
        """Deserialize JSON string `json_str` and return the deserialized object."""
        return json.loads(json_str, object_hook=self._ext_hooks)


json_ext = JsonExt()
json_ext.register_numpy(use_list=True)
json_ext.register_bec_message()
json_ext.register_bec_status()
json_ext.register_set_encoder()
json_ext.register_message_endpoint()
json_ext.register_bec_message_type()
json_ext.register_pydantic()
json_ext.register_bec_device()

msgpack = MsgpackExt()
msgpack.register_numpy()
msgpack.register_bec_message()
msgpack.register_set_encoder()
msgpack.register_message_endpoint()
msgpack.register_bec_message_type()
msgpack.register_pydantic()
msgpack.register_bec_device()


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
