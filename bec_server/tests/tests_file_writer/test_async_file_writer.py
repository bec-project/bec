import h5py
import numpy as np
import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.file_writer.async_writer import AsyncWriter


@pytest.fixture
def async_writer(tmp_path, connected_connector):
    file_path = tmp_path / "test.nxs"
    writer = AsyncWriter(file_path, "scan_id", 1234, connected_connector, ["monitor_async"])
    writer.initialize_stream_keys()
    yield writer


@pytest.mark.parametrize(
    "data, shape",
    [
        (
            [
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": [1, 2, 3], "timestamp": 1}},
                    metadata={"async_update": {"type": "add", "max_shape": [None]}},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": [1, 2, 3, 4, 5], "timestamp": 2}},
                    metadata={"async_update": {"type": "add", "max_shape": [None]}},
                ),
            ],
            (8,),
        ),
        (
            [
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": [1, 2, 3], "timestamp": 1}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, 3]}},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": [1, 2, 3], "timestamp": 2}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, 3]}},
                ),
            ],
            (2, 3),
        ),
        (
            [
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(5, 5), "timestamp": 1}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, 5, 5]}},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(5, 5), "timestamp": 2}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, 5, 5]}},
                ),
            ],
            (2, 5, 5),
        ),
        (
            [
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(5), "timestamp": 1}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, None]}},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(6), "timestamp": 2}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, None]}},
                ),
            ],
            (2,),
        ),
        (
            [
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(2, 10), "timestamp": 1}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, 10]}},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(1, 10), "timestamp": 2}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, 10]}},
                ),
            ],
            (3, 10),
        ),
        (
            [
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(1, 8), "timestamp": 1}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, 10]}},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(1, 9), "timestamp": 2}},
                    metadata={"async_update": {"type": "add", "max_shape": [None, 10]}},
                ),
            ],
            (2, 10),
        ),
    ],
)
def test_async_writer_add(async_writer, data, shape):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert np.asarray(out).shape == shape


@pytest.mark.parametrize(
    "data",
    [
        [
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 1}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 0, "max_shape": [None, None]}
                },
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 0, "max_shape": [None, None]}
                },
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 1, "max_shape": [None, None]}
                },
            ),
        ]
    ],
)
def test_async_writer_add_slice_var_size(async_writer, data):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (2,)
    assert out[0].shape == (20,)
    assert out[1].shape == (10,)


@pytest.mark.parametrize(
    "data",
    [
        [
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 1}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 0, "max_shape": [None, 20]}
                },
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 0, "max_shape": [None, 20]}
                },
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 1, "max_shape": [None, 20]}
                },
            ),
        ]
    ],
)
def test_async_writer_add_slice_fixed_size(async_writer, data):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (2, 20)


def test_async_writer_add_slice_fixed_size_data_consistency(async_writer):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    data = [
        messages.DeviceMessage(
            signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 1}},
            metadata={"async_update": {"type": "add_slice", "index": 0, "max_shape": [None, 20]}},
        ),
        messages.DeviceMessage(
            signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
            metadata={"async_update": {"type": "add_slice", "index": 0, "max_shape": [None, 20]}},
        ),
        messages.DeviceMessage(
            signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
            metadata={"async_update": {"type": "add_slice", "index": 1, "max_shape": [None, 20]}},
        ),
    ]
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (2, 20)
    assert np.allclose(
        out[0, :],
        np.hstack(
            (data[0].signals["monitor_async"]["value"], data[1].signals["monitor_async"]["value"])
        ),
    )
    assert np.allclose(out[1, :10], data[2].signals["monitor_async"]["value"])
    assert np.allclose(out[1, 10:], np.zeros(10))


@pytest.mark.parametrize(
    "data",
    [
        [
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(5), "timestamp": 1}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 0, "max_shape": [None, 10]}
                },
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 0, "max_shape": [None, 10]}
                },
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 1, "max_shape": [None, 10]}
                },
            ),
        ]
    ],
)
def test_async_writer_add_slice_fixed_size_exceeded_raises_warning(async_writer, data):
    """
    Test that adding a slice that exceeds the max_shape raises a warning but writes the
    truncated data.
    """
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (2, 10)


@pytest.mark.parametrize(
    "data",
    [
        [
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(12), "timestamp": 1}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 0, "max_shape": [None, 10]}
                },
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
                metadata={
                    "async_update": {"type": "add_slice", "index": 1, "max_shape": [None, 10]}
                },
            ),
        ]
    ],
)
def test_async_writer_add_single_slice_fixed_size_exceeded_raises_warning(async_writer, data):
    """
    Test that adding a slice that exceeds the max_shape raises a warning but writes the
    truncated data.
    """
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (2, 10)


@pytest.mark.parametrize(
    "data",
    [
        [
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(5), "timestamp": 1}},
                metadata={"async_update": {"type": "replace"}},
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
                metadata={"async_update": {"type": "replace"}},
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10), "timestamp": 2}},
                metadata={"async_update": {"type": "replace"}},
            ),
        ]
    ],
)
def test_async_writer_replace(async_writer, data):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()
    async_writer.poll_and_write_data(final=True)

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (10,)
    assert np.allclose(out, data[-1].signals["monitor_async"]["value"])


def test_async_write_raises_warning_for_unknown_type(async_writer):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    entry = messages.DeviceMessage(
        signals={"monitor_async": {"value": np.random.rand(5), "timestamp": 1}},
        metadata={"async_update": {"type": "unknown"}},
    )
    async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
    async_writer.poll_and_write_data()
    msg = async_writer.connector.get(MessageEndpoints.alarm())
    assert msg is not None
    assert msg.source["device"] == "/entry/collection/devices/monitor_async/monitor_async"
    assert msg.msg == "Unknown async update type: unknown. Data will not be written."


def test_async_writer_raises_warning_for_3d_shape(async_writer):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    entry = messages.DeviceMessage(
        signals={"monitor_async": {"value": np.random.rand(5, 5, 5), "timestamp": 1}},
        metadata={"async_update": {"type": "add_slice", "max_shape": [None, 5, 5]}},
    )
    async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
    async_writer.poll_and_write_data()
    msg = async_writer.connector.get(MessageEndpoints.alarm())
    assert msg is not None
    assert msg.source["device"] == "/entry/collection/devices/monitor_async/monitor_async"
    assert (
        msg.msg
        == "Invalid max_shape for async update type 'add_slice': [None, 5, 5]. max_shape cannot exceed two dimensions. Data will not be written."
    )
