import enum
from unittest import mock

import numpy as np
import pytest
from pydantic import BaseModel

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.serialization import MsgpackSerialization


@pytest.fixture(params=[MsgpackSerialization])
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
        {
            "hroz": {
                "hroz": {"value": 0, "timestamp": 1708336264.5731058},
                "hroz_setpoint": {"value": 0, "timestamp": 1708336264.573121},
            }
        },
        MessageEndpoints.progress("test"),
        messages.RawMessage(data={"a": 1, "b": 2}),
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


def test_device_serializer(serializer):
    device_manager = mock.MagicMock(spec=DeviceManagerBase)
    dummy = DeviceBase(name="dummy", parent=device_manager)
    assert serializer.loads(serializer.dumps(dummy)) == "dummy"
