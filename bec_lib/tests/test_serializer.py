import enum
from unittest import mock

import numpy as np
import pytest
from pydantic import BaseModel

from bec_lib import messages
from bec_lib.codecs import BECCodec
from bec_lib.device import DeviceBase
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.serialization import MsgpackSerialization, json_ext, msgpack
from bec_lib.serialization_registry_v1 import msgpack as deprecated_msgpack


@pytest.fixture(params=[json_ext, msgpack, MsgpackSerialization])
def serializer(request):
    yield request.param


class CustomEnum(enum.Enum):
    VALUE1 = "value1"
    VALUE2 = "value2"


@pytest.mark.parametrize(
    "data",
    [
        {"a": 1, "b": 2},
        "hello",
        1,
        1.0,
        [1, 2, 3],
        np.array([1, 2, 3]),
        {1, 2, 3},
        {
            "hroz": {
                "hroz": {"value": 0, "timestamp": 1708336264.5731058},
                "hroz_setpoint": {"value": 0, "timestamp": 1708336264.573121},
            }
        },
        MessageEndpoints.progress("test"),
        messages.DeviceMessage,
        float,
        messages.RawMessage(data={"a": 1, "b": 2}),
        messages.BECStatus.RUNNING,
        np.uint32,
        messages.DeviceMessage(
            signals={
                "hroz": {
                    "value": np.random.rand(10).astype(np.uint32),
                    "timestamp": 1708336264.5731058,
                }
            },
            metadata={},
        ),
        messages.DeviceMessage(
            metadata={
                "readout_priority": "baseline",
                "file_suffix": None,
                "file_directory": None,
                "user_metadata": {},
            },
            signals={"pseudo_signal1": {"value": np.uint32(80), "timestamp": 1749392743.0512588}},
        ),
    ],
)
def test_serialize(serializer, data):
    res = serializer.loads(serializer.dumps(data)) == data
    assert all(res) if isinstance(data, np.ndarray) else res


def test_serialize_model(serializer):

    class DummyModel(BaseModel):
        a: int
        b: int

    data = DummyModel(a=1, b=2)
    converted_data = serializer.loads(serializer.dumps(data))
    assert data.model_dump() == converted_data


def test_device_serializer(serializer):
    device_manager = mock.MagicMock(spec=DeviceManagerBase)
    dummy = DeviceBase(name="dummy", parent=device_manager)
    assert serializer.loads(serializer.dumps(dummy)) == "dummy"


def test_enum_serializer(serializer):
    assert serializer.loads(serializer.dumps(CustomEnum.VALUE1)) == "value1"


def test_serializer_encoding_on_failure():
    """
    Test that an exception raised during serialization is caught and the original object is returned.
    """

    class DummyModel:
        def __init__(self, a, b):
            self.a = a
            self.b = b

        def __eq__(self, other):
            return isinstance(other, DummyModel) and self.a == other.a and self.b == other.b

    class RaiseEncoder(BECCodec):
        obj_type = DummyModel

        @staticmethod
        def encode(obj):
            raise ValueError("Serialization failed")

        @staticmethod
        def decode(type_name: str, data: dict):
            raise ValueError("Deserialization failed")

    try:
        msgpack.register_codec(RaiseEncoder)
        data = DummyModel(a=1, b=2)
        with pytest.raises(ValueError, match="Serialization failed"):
            serialized_data = msgpack.dumps(data)

        serialized_data = msgpack.dumps(
            {"__bec_codec__": {"encoder_name": "DummyModel", "type_name": "DummyModel", "data": {}}}
        )
        with pytest.raises(ValueError, match="Deserialization failed"):
            msgpack.loads(serialized_data)
    finally:
        # Unregister the codec to avoid side effects on other tests
        msgpack._registry.pop("DummyModel")


def test_legacy_serializer():
    # Can be removed in the future, cf issue #516
    msg = messages.DeviceMessage(
        signals={"hroz": {"value": 0, "timestamp": 1708336264.5731058}}, metadata={}
    )
    raw = deprecated_msgpack.dumps(msg)
    assert msgpack.loads(raw) == msg


@pytest.mark.parametrize(
    "data",
    [
        {"a": 1, "b": 2},
        "hello",
        1,
        1.0,
        [1, 2, 3],
        np.array([1, 2, 3]),
        {1, 2, 3},
        {
            "hroz": {
                "hroz": {"value": 0, "timestamp": 1708336264.5731058},
                "hroz_setpoint": {"value": 0, "timestamp": 1708336264.573121},
            }
        },
        MessageEndpoints.progress("test"),
        messages.DeviceMessage,
        float,
        messages.RawMessage(data={"a": 1, "b": 2}),
        messages.BECStatus.RUNNING,
        messages.DeviceMessage(
            signals={
                "hroz": {
                    "value": np.random.rand(10).astype(np.uint32),
                    "timestamp": 1708336264.5731058,
                }
            },
            metadata={},
        ),
    ],
)
def test_legacy_serialize(data):
    # Can be removed in the future, cf issue #516
    res = deprecated_msgpack.loads(deprecated_msgpack.dumps(data)) == data
    assert all(res) if isinstance(data, np.ndarray) else res


def test_serializer_registry_cache_resets():
    """
    Test that adding a new codec resets the cache.
    """

    class DummyType:
        pass

    class DummyCodec(BECCodec):
        obj_type = DummyType

        @staticmethod
        def encode(obj):
            return {"dummy": "data"}

        @staticmethod
        def decode(type_name: str, data: dict):
            return DummyType()

    assert not msgpack.is_registered(DummyType)
    msgpack.register_codec(DummyCodec)
    assert msgpack.is_registered(DummyType)
