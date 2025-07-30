import h5py
import numpy as np
import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.file_writer.async_writer import AsyncWriter


@pytest.fixture(
    params=[
        [],
        [
            (
                "waveform",
                "data",
                {
                    "component_name": "data",
                    "signal_class": "AsyncSignal",
                    "obj_name": "waveform_data",
                    "kind_int": 5,
                    "kind_str": "hinted",
                    "doc": "",
                    "describe": {
                        "source": "BECMessageSignal:waveform_data",
                        "dtype": "DeviceMessage",
                        "shape": [],
                        "signal_info": {
                            "data_type": "raw",
                            "saved": True,
                            "ndim": 1,
                            "scope": "scan",
                            "role": "main",
                            "enabled": True,
                            "rpc_access": False,
                            "signals": [["data", 5]],
                            "signal_metadata": {"max_size": 1000},
                        },
                    },
                    "metadata": {
                        "connected": True,
                        "read_access": True,
                        "write_access": True,
                        "timestamp": 1753813467.96813,
                        "status": None,
                        "severity": None,
                        "precision": None,
                    },
                },
            )
        ],
    ]
)
def async_signals(request):
    return request.param


@pytest.fixture
def async_writer(tmp_path, connected_connector, async_signals):
    file_path = tmp_path / "test.nxs"
    writer = AsyncWriter(
        file_path,
        "scan_id",
        1234,
        connected_connector,
        ["monitor_async", "waveform"],
        async_signals,
    )
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


def test_async_writer_async_signal(async_writer):
    """Test that async signals are written correctly using the device_async_signal endpoint."""
    # Only test when async_signals is not empty (when parameterized fixture provides the signal)
    if not async_writer.async_signals:
        return

    # Use the device_async_signal endpoint instead of device_async_readback
    endpoint = MessageEndpoints.device_async_signal(
        scan_id="scan_id", device="waveform", signal="waveform_data"
    )

    data = [
        messages.DeviceMessage(
            signals={"waveform_data": {"value": [1, 2, 3, 4, 5], "timestamp": 1}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
        messages.DeviceMessage(
            signals={"waveform_data": {"value": [6, 7, 8], "timestamp": 2}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
    ]

    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # Read the data back from the async signal device path
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["waveform"]["waveform_data"]["value"][:]

    # Check that the data was appended correctly
    expected_data = np.array([1, 2, 3, 4, 5, 6, 7, 8])
    assert np.array_equal(out, expected_data)


def test_async_writer_mixed_readback_and_signal(async_writer):
    """Test that a device can have both normal async readback data and async signal data."""
    # Only test when async_signals is not empty (when parameterized fixture provides the signal)
    if not async_writer.async_signals:
        return

    # Send data to both the normal async readback endpoint and the async signal endpoint
    readback_endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    signal_endpoint = MessageEndpoints.device_async_signal(
        scan_id="scan_id", device="waveform", signal="waveform_data"
    )

    # Normal async readback data
    readback_data = [
        messages.DeviceMessage(
            signals={"monitor_async": {"value": [10, 20, 30], "timestamp": 1}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
        messages.DeviceMessage(
            signals={"monitor_async": {"value": [40, 50], "timestamp": 2}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
    ]

    # Async signal data
    signal_data = [
        messages.DeviceMessage(
            signals={"waveform_data": {"value": [100, 200, 300], "timestamp": 1}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
        messages.DeviceMessage(
            signals={"waveform_data": {"value": [400, 500], "timestamp": 2}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
    ]

    # Send all data
    for entry in readback_data:
        async_writer.connector.xadd(readback_endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    for entry in signal_data:
        async_writer.connector.xadd(signal_endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # Read both datasets back and verify they were written correctly
    with h5py.File(async_writer.file_path, "r") as f:
        # Check readback data
        readback_out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]
        expected_readback = np.array([10, 20, 30, 40, 50])
        assert np.array_equal(readback_out, expected_readback)

        # Check signal data
        signal_out = f[async_writer.BASE_PATH]["waveform"]["waveform_data"]["value"][:]
        expected_signal = np.array([100, 200, 300, 400, 500])
        assert np.array_equal(signal_out, expected_signal)


def test_async_writer_same_device_readback_and_signal(async_writer):
    """Test that the same device can have both normal async readback data and async signal data."""
    # Only test when async_signals is not empty (when parameterized fixture provides the signal)
    if not async_writer.async_signals:
        return

    # Send data to both endpoints for the same device "waveform"
    readback_endpoint = MessageEndpoints.device_async_readback("scan_id", "waveform")
    signal_endpoint = MessageEndpoints.device_async_signal(
        scan_id="scan_id", device="waveform", signal="waveform_data"
    )

    # Normal async readback data for waveform device
    readback_data = [
        messages.DeviceMessage(
            signals={"waveform": {"value": [1, 2, 3], "timestamp": 1}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
        messages.DeviceMessage(
            signals={"waveform": {"value": [4, 5], "timestamp": 2}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
    ]

    # Async signal data for waveform device
    signal_data = [
        messages.DeviceMessage(
            signals={"waveform_data": {"value": [10, 20, 30], "timestamp": 1}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
        messages.DeviceMessage(
            signals={"waveform_data": {"value": [40, 50], "timestamp": 2}},
            metadata={"async_update": {"type": "add", "max_shape": [None]}},
        ),
    ]

    # Send all data
    for entry in readback_data:
        async_writer.connector.xadd(readback_endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    for entry in signal_data:
        async_writer.connector.xadd(signal_endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # Read both datasets back and verify they were written correctly
    with h5py.File(async_writer.file_path, "r") as f:
        # Check readback data for waveform device
        readback_out = f[async_writer.BASE_PATH]["waveform"]["waveform"]["value"][:]
        expected_readback = np.array([1, 2, 3, 4, 5])
        assert np.array_equal(readback_out, expected_readback)

        # Check signal data for waveform device
        signal_out = f[async_writer.BASE_PATH]["waveform"]["waveform_data"]["value"][:]
        expected_signal = np.array([10, 20, 30, 40, 50])
        assert np.array_equal(signal_out, expected_signal)
