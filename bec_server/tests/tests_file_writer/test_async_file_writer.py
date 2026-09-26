from unittest import mock

import h5py
import numpy as np
import pytest

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
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
                    "storage_name": "waveform_data",
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


def test_async_writer_add_slice_var_size_2D_data_warns(async_writer):
    """
    Test that adding a slice with 2D data when max_shape is [None, None] raises a warning and skips writing the data.
    """
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    data = [
        messages.DeviceMessage(
            signals={"monitor_async": {"value": np.random.rand(10, 10), "timestamp": 1}},
            metadata={"async_update": {"type": "add_slice", "index": 0, "max_shape": [None, None]}},
        )
    ]
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        with mock.patch.object(async_writer.connector, "raise_alarm") as mock_raise_alarm:
            async_writer.poll_and_write_data()
            mock_raise_alarm.assert_called_once()
            args, kwargs = mock_raise_alarm.call_args
            assert kwargs["severity"] == Alarms.WARNING
            assert any(
                signal_name in kwargs["info"].error_message for signal_name in entry.signals.keys()
            )


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


@pytest.mark.parametrize("max_rows", [None, 4])
@pytest.mark.parametrize("later_row", [1, 3])
@pytest.mark.parametrize("earlier_slice", [[3], [3, 4, 5]])
def test_async_writer_add_slice_fixed_size_preserves_later_rows(
    async_writer, tmp_path, max_rows, later_row, earlier_slice
):
    """Interleaved slices preserve rows and append positions, including after truncation."""
    with h5py.File(tmp_path / "interleaved.h5", "w") as file:
        signal_group = file.create_group("signal")
        with mock.patch.object(async_writer.connector, "raise_alarm") as raise_alarm:
            for row_index, values in [(0, [1, 2]), (later_row, [10, 11]), (0, earlier_slice)]:
                async_writer.write_value_data(
                    signal_group,
                    values,
                    {"type": "add_slice", "index": row_index, "max_shape": [max_rows, 4]},
                )

            expected = np.zeros((later_row + 1, 4), dtype=int)
            row_values = [1, 2] + earlier_slice[:2]
            expected[0, : len(row_values)] = row_values
            expected[later_row, :2] = [10, 11]
            np.testing.assert_array_equal(signal_group["value"][:], expected)
            assert raise_alarm.call_count == (len(earlier_slice) > 2)

            async_writer.write_value_data(
                signal_group,
                [12, 13],
                {"type": "add_slice", "index": later_row, "max_shape": [max_rows, 4]},
            )
            expected[later_row] = [10, 11, 12, 13]
            np.testing.assert_array_equal(signal_group["value"][:], expected)


@pytest.mark.parametrize("max_rows", [None, 4])
@pytest.mark.parametrize("initial_values", [[7, 8], [7, 8, 9, 10, 11], []])
def test_async_writer_add_slice_fixed_size_starts_at_requested_row(
    async_writer, tmp_path, max_rows, initial_values
):
    """A nonzero first row keeps its data and cursor when earlier rows arrive later."""
    with h5py.File(tmp_path / "initial_row.h5", "w") as file:
        signal_group = file.create_group("signal")
        async_writer.write_value_data(
            signal_group,
            np.asarray(initial_values, dtype=np.int16),
            {"type": "add_slice", "index": 3, "max_shape": [max_rows, 4]},
        )
        expected_initial = np.zeros((4, min(len(initial_values), 4)), dtype=np.int16)
        expected_initial[3] = initial_values[:4]
        np.testing.assert_array_equal(signal_group["value"][:], expected_initial)
        assert signal_group["value"].dtype == np.dtype(np.int16)

        for row_index, values in [(0, [1, 2]), (3, [12, 13])]:
            async_writer.write_value_data(
                signal_group,
                values,
                {"type": "add_slice", "index": row_index, "max_shape": [max_rows, 4]},
            )
        expected = np.zeros((4, 4), dtype=np.int16)
        expected[0, :2] = [1, 2]
        row_values = (initial_values + [12, 13])[:4]
        expected[3, : len(row_values)] = row_values
        np.testing.assert_array_equal(signal_group["value"][:], expected)


def test_async_writer_add_slice_fixed_size_nonzero_first_row_preserves_string_dtype(
    async_writer, tmp_path
):
    """Moving the initial row preserves HDF5's inference of variable-length strings."""
    with h5py.File(tmp_path / "initial_string_row.h5", "w") as file:
        signal_group = file.create_group("signal")
        async_writer.write_value_data(
            signal_group,
            np.array(["a", "bb"], dtype=object),
            {"type": "add_slice", "index": 2, "max_shape": [None, 4]},
        )
        dataset = signal_group["value"]
        np.testing.assert_array_equal(dataset.asstr()[:], [["", ""], ["", ""], ["a", "bb"]])
        assert h5py.check_string_dtype(dataset.dtype).length is None


def test_async_writer_add_slice_fixed_size_nonzero_first_row_preserves_vlen_dtype(
    async_writer, tmp_path
):
    """HDF5 initializes skipped rows correctly for variable-length numeric values."""
    values = np.empty(2, dtype=h5py.vlen_dtype(np.dtype(np.int32)))
    values[0] = np.array([1, 2], dtype=np.int32)
    values[1] = np.array([3], dtype=np.int32)
    with h5py.File(tmp_path / "initial_vlen_row.h5", "w") as file:
        signal_group = file.create_group("signal")
        async_writer.write_value_data(
            signal_group, values, {"type": "add_slice", "index": 2, "max_shape": [None, 4]}
        )
        dataset = signal_group["value"]
        assert dataset.shape == (3, 2)
        assert h5py.check_vlen_dtype(dataset.dtype) == np.dtype(np.int32)
        assert all(cell.size == 0 for row in dataset[:2] for cell in row)
        np.testing.assert_array_equal(dataset[2, 0], values[0])
        np.testing.assert_array_equal(dataset[2, 1], values[1])


@pytest.mark.parametrize("populated", [False, True])
@pytest.mark.parametrize("index, max_rows", [(-1, None), (4, 4)])
def test_async_writer_add_slice_fixed_size_rejects_invalid_index(
    async_writer, tmp_path, populated, index, max_rows
):
    """An invalid row index cannot overwrite existing rows or initialize an invalid cursor."""
    with h5py.File(tmp_path / "invalid_index.h5", "w") as file:
        signal_group = file.create_group("signal")
        if populated:
            for row_index, values in [(0, [1, 2]), (1, [10, 11])]:
                async_writer.write_value_data(
                    signal_group,
                    values,
                    {"type": "add_slice", "index": row_index, "max_shape": [max_rows, 4]},
                )
            expected = signal_group["value"][:]
        cursor_before = dict(async_writer.cursor.get(signal_group.name, {}))

        with mock.patch.object(async_writer.connector, "raise_alarm") as raise_alarm:
            async_writer.write_value_data(
                signal_group,
                [99],
                {"type": "add_slice", "index": index, "max_shape": [max_rows, 4]},
            )
        raise_alarm.assert_called_once()
        assert raise_alarm.call_args.kwargs["severity"] == Alarms.WARNING
        assert "nonnegative row index" in raise_alarm.call_args.kwargs["info"].error_message
        assert async_writer.cursor.get(signal_group.name, {}) == cursor_before
        if populated:
            np.testing.assert_array_equal(signal_group["value"][:], expected)
        else:
            assert "value" not in signal_group

        async_writer.write_value_data(
            signal_group, [3], {"type": "add_slice", "index": 0, "max_shape": [max_rows, 4]}
        )
        expected_row = [1, 2, 3, 0] if populated else [3]
        np.testing.assert_array_equal(signal_group["value"][0], expected_row)


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
    assert async_writer.written_signals == {"waveform": ["waveform_data"]}


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
    assert async_writer.written_signals == {
        "monitor_async": ["monitor_async"],
        "waveform": ["waveform_data"],
    }


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


def test_async_writer_raises_on_wrong_data_type(async_writer):
    """Test that the async writer raises a TypeError when non-DeviceMessage data is sent."""
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")

    # Send invalid data (not a DeviceMessage)
    invalid_data = messages.DeviceMessage(
        signals={"monitor_async": {"value": {"data": None}, "timestamp": 1}},
        metadata={"async_update": {"type": "add", "max_shape": [None]}},
    )

    async_writer.connector.xadd(endpoint, msg_dict={"data": invalid_data})

    with pytest.raises(
        TypeError,
        match="Failed to create dataset value in group /entry/collection/devices/monitor_async/monitor_async.",
    ):
        async_writer.poll_and_write_data()
