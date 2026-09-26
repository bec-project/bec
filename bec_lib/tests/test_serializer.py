import enum
import json
import pickle
from unittest import mock

import msgpack as msgpack_module
import numpy as np
import pytest
from pydantic import BaseModel

from bec_lib import messages
from bec_lib.codecs import BECCodec
from bec_lib.device import DeviceBase
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.serialization import MsgpackSerialization, json_ext, msgpack


class _PickleExecutionMarker:
    def __reduce__(self):
        return print, ("object-array pickle executed",)


def _pack_numpy_payload(serializer, data):
    if serializer is not json_ext:
        data = {key.encode(): value for key, value in data.items()}
        if b"kind" in data:
            data[b"kind"] = data[b"kind"].encode()
    envelope = {"__bec_codec__": {"encoder_name": "ndarray", "type_name": "ndarray", "data": data}}
    return json.dumps(envelope) if serializer is json_ext else msgpack_module.packb(envelope)


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


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(np.array([1, "text"], dtype=object), id="object"),
        pytest.param(np.array([], dtype=object), id="empty"),
        pytest.param(np.array(None, dtype=object), id="zero-dimensional"),
        pytest.param(np.zeros(1, dtype=[("value", object)]), id="structured"),
        pytest.param(np.zeros(1, dtype=[("value", [("nested", object)])]), id="nested"),
        pytest.param(np.zeros(1, dtype=[("value", object, (2,))]), id="subarray"),
    ],
)
def test_serialize_rejects_object_arrays(serializer, data):
    message = messages.DeviceMessage(signals={"signal": {"value": data, "timestamp": 0}})
    with pytest.raises(ValueError, match="NumPy object arrays are not supported"):
        serializer.dumps(message)


@pytest.mark.parametrize(
    "metadata",
    [
        pytest.param({"type": "|O"}, id="missing-kind"),
        pytest.param({"type": "object", "kind": ""}, id="object-alias"),
        pytest.param({"type": "O8", "kind": ""}, id="sized-object-alias"),
        pytest.param({"type": "<i8", "kind": "O"}, id="legacy-pickle-marker"),
        pytest.param({"type": [("value", "|O")], "kind": "V"}, id="structured"),
        pytest.param({"type": [("value", [("nested", "|O")])], "kind": "V"}, id="nested"),
        pytest.param({"type": [("value", "|O", [2])], "kind": "V"}, id="subarray"),
        pytest.param({"type": {"names": ["value"], "formats": ["O"]}}, id="dtype-dictionary"),
        pytest.param({"nd": False, "type": "object_"}, id="scalar"),
    ],
)
def test_deserialize_rejects_object_dtypes(serializer, metadata):
    data = {"nd": True, "shape": [1], "data": [None] if serializer is json_ext else b"\0" * 16}
    data.update(metadata)
    payload = _pack_numpy_payload(serializer, data)
    error = RuntimeError if serializer is MsgpackSerialization else ValueError
    with pytest.raises(error, match="Failed to decode BECMessage|NumPy object arrays"):
        serializer.loads(payload)


@pytest.mark.parametrize("metadata", [{"kind": "O"}, {"type": "|O"}])
def test_deserialize_rejects_incomplete_object_payload(serializer, metadata):
    payload = _pack_numpy_payload(serializer, {"nd": True, **metadata})
    error = RuntimeError if serializer is MsgpackSerialization else ValueError
    with pytest.raises(error, match="Failed to decode BECMessage|NumPy object arrays"):
        serializer.loads(payload)


@pytest.mark.parametrize("decoder", [msgpack, MsgpackSerialization])
def test_deserialize_never_executes_object_array_pickle(decoder, capsys):
    payload = _pack_numpy_payload(
        decoder,
        {
            "nd": True,
            "kind": "O",
            "type": [("", "|O")],
            "shape": [1],
            "data": pickle.dumps(_PickleExecutionMarker()),
        },
    )
    error = RuntimeError if decoder is MsgpackSerialization else ValueError
    try:
        with pytest.raises(error, match="Failed to decode BECMessage|NumPy object arrays"):
            decoder.loads(payload)
    finally:
        assert capsys.readouterr().out == "", "The decoder executed the pickle payload"


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(np.arange(6, dtype=np.float32).reshape(2, 3), id="numeric"),
        pytest.param(np.arange(6)[::2], id="non-contiguous"),
        pytest.param(np.array([True, False]), id="boolean"),
        pytest.param(np.array([1 + 2j, 3 - 4j]), id="complex"),
        pytest.param(np.array(["one", "two"]), id="unicode"),
        pytest.param(np.array([], dtype=np.float64).reshape(0, 2), id="empty"),
        pytest.param(np.array(1, dtype=np.int16), id="zero-dimensional"),
    ],
)
def test_serialize_supported_numpy_arrays(serializer, data):
    message = messages.DeviceMessage(signals={"signal": {"value": data, "timestamp": 0}})
    decoded = serializer.loads(serializer.dumps(message))
    actual = decoded.signals["signal"]["value"]
    assert actual.dtype == data.dtype
    assert actual.shape == data.shape
    np.testing.assert_array_equal(actual, data)


@pytest.mark.parametrize("serializer", [msgpack, MsgpackSerialization])
@pytest.mark.parametrize(
    "data",
    [
        np.array([b"one", b"two"]),
        np.array([(1, 2.0)], dtype=[("count", "i4"), ("value", "f8")]),
        np.zeros(2, dtype=[("value", [("nested", "f8")])]),
        np.zeros(2, dtype=[("value", "f8", (2,))]),
    ],
)
def test_msgpack_supported_numpy_dtypes(serializer, data):
    actual = serializer.loads(serializer.dumps(data))
    assert actual.dtype == data.dtype
    assert actual.shape == data.shape
    np.testing.assert_array_equal(actual, data)


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
