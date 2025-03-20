from unittest import mock

import numpy as np
import pytest
from pydantic import BaseModel

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.serialization import json_ext, msgpack


@pytest.fixture(params=[json_ext, msgpack])
def serializer(request):
    yield request.param


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
        messages.StatusMessage(name="test", status=messages.BECStatus.IDLE, info={"x": "y"}),
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
