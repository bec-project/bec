import os

import h5py
import numpy as np
import pytest

from bec_lib import messages
from bec_lib.device import Device
from bec_lib.endpoints import MessageEndpoints
from bec_server.file_writer.async_writer import AsyncWriter


@pytest.fixture
def async_writer(tmp_path, connected_connector):
    file_path = tmp_path / "test.nxs"
    writer = AsyncWriter(file_path, "scan_id", connected_connector, [Device(name="monitor_async")])
    writer.initialize_stream_keys()
    yield writer


@pytest.mark.parametrize(
    "data",
    [
        [
            {
                "monitor_async": {
                    "async_update": "extend",
                    "data": {"monitor_async": {"value": np.random.rand(2, 10), "timestamp": [1]}},
                }
            },
            {
                "monitor_async": {
                    "async_update": "extend",
                    "data": {"monitor_async": {"value": np.random.rand(4, 10), "timestamp": [2]}},
                }
            },
        ]
    ],
)
def test_async_writer_extend_array(async_writer, data):
    signal_data = []
    for entry in data:
        async_writer.write_data(entry)
        signal_data.append(entry["monitor_async"]["data"]["monitor_async"]["value"])

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (6, 10)
    assert np.allclose(out, np.concatenate(signal_data))


@pytest.mark.parametrize(
    "data",
    [
        [
            messages.DeviceMessage(
                signals={"monitor_async": {"value": [1, 2, 3], "timestamp": 1}},
                metadata={"async_update": "extend"},
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": [1, 2, 3, 4, 5], "timestamp": 2}},
                metadata={"async_update": "extend"},
            ),
        ]
    ],
)
def test_async_writer_extend_list(async_writer, data):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert len(out) == 8
    assert all(out == [1, 2, 3, 1, 2, 3, 4, 5])


@pytest.mark.parametrize(
    "data",
    [
        [
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10, 10), "timestamp": 1}},
                metadata={"async_update": "append"},
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": np.random.rand(10, 10), "timestamp": 2}},
                metadata={"async_update": "append"},
            ),
        ]
    ],
)
def test_async_writer_append_array(async_writer, data):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (2, 10, 10)


@pytest.mark.parametrize(
    "data",
    [
        [
            messages.DeviceMessage(
                signals={"monitor_async": {"value": [1.2, 2, 3], "timestamp": 1}},
                metadata={"async_update": "append"},
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": [4, 5, 6], "timestamp": 2}},
                metadata={"async_update": "append"},
            ),
        ]
    ],
)
def test_async_writer_append_list_equal_shapes(async_writer, data):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (2, 3)


@pytest.mark.parametrize(
    "data",
    [
        [
            messages.DeviceMessage(
                signals={"monitor_async": {"value": [1, 2, 3, 4], "timestamp": 1}},
                metadata={"async_update": "append"},
            ),
            messages.DeviceMessage(
                signals={"monitor_async": {"value": [4, 5, 6], "timestamp": 2}},
                metadata={"async_update": "append"},
            ),
        ]
    ],
)
def test_async_writer_append_list_unequal_shapes(async_writer, data):
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out[0].shape == (4,)
    assert out[1].shape == (3,)


@pytest.mark.parametrize(
    "data, expected_shape",
    [
        (
            [
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(112), "timestamp": 1}},
                    metadata={"async_update": "append"},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(100), "timestamp": 2}},
                    metadata={"async_update": "append"},
                ),
            ],
            (112, 100),
        ),
        (
            [
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(112), "timestamp": 1}},
                    metadata={"async_update": "append"},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(100), "timestamp": 2}},
                    metadata={"async_update": "append"},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(101), "timestamp": 3}},
                    metadata={"async_update": "append"},
                ),
            ],
            (112, 100, 101),
        ),
        (
            [
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(100), "timestamp": 1}},
                    metadata={"async_update": "append"},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(100), "timestamp": 2}},
                    metadata={"async_update": "append"},
                ),
                messages.DeviceMessage(
                    signals={"monitor_async": {"value": np.random.rand(101), "timestamp": 3}},
                    metadata={"async_update": "append"},
                ),
            ],
            (100, 100, 101),
        ),
    ],
)
def test_async_writer_append_array_variable_length_single_entry(async_writer, data, expected_shape):
    """
    Test that async data streams with append update type are written correctly.
    This test simulates that each entry is read back separately.
    """
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    for entry in data:
        async_writer.connector.xadd(endpoint, msg_dict={"data": entry})
        async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]
    for ii, shape in enumerate(expected_shape):
        assert out[ii].shape == (shape,)


def test_async_writer_append_list_xread_multiple_entries(async_writer):
    """
    Test that async data streams with append update type are written correctly.
    This test simulates that multiple entries are read back at once.
    """
    data = messages.DeviceMessage(
        signals={"monitor_async": {"value": np.random.rand(100)}},
        metadata={"async_update": "append"},
    )
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")
    async_writer.connector.xadd(endpoint, msg_dict={"data": data})
    async_writer.connector.xadd(endpoint, msg_dict={"data": data})

    # pull the data from the stream. Here we are simulating that multiple entries have been added to the stream before
    # the data is read
    async_writer.poll_and_write_data()

    # let's assume the data shape changes
    data_2 = messages.DeviceMessage(
        signals={"monitor_async": {"value": np.random.rand(120)}},
        metadata={"async_update": "append"},
    )
    async_writer.connector.xadd(endpoint, msg_dict={"data": data})
    async_writer.connector.xadd(endpoint, msg_dict={"data": data_2})
    async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out[0].shape == (100,)
    assert out[1].shape == (100,)
    assert out[2].shape == (100,)
    assert out[3].shape == (120,)


def test_async_writer_append_list_xread_multiple_entries_equal_shape(async_writer):
    """
    Test that async data streams with append update type are written correctly.
    This test simulates that multiple entries are read back at once.
    """
    data = messages.DeviceMessage(
        signals={"monitor_async": {"value": np.random.rand(100)}},
        metadata={"async_update": "append"},
    )
    endpoint = MessageEndpoints.device_async_readback("scan_id", "monitor_async")

    async_writer.connector.xadd(endpoint, msg_dict={"data": data})
    async_writer.connector.xadd(endpoint, msg_dict={"data": data})

    # pull the data from the stream. Here we are simulating that multiple entries have been added to the stream before
    # the data is read
    async_writer.poll_and_write_data()

    async_writer.connector.xadd(endpoint, msg_dict={"data": data})
    async_writer.connector.xadd(endpoint, msg_dict={"data": data})
    async_writer.poll_and_write_data()

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert out.shape == (4, 100)


def test_async_writer_replace_list(async_writer):
    """
    Test that async data streams with replace update type are written correctly.
    Only the last data stream should be written to the file.
    """
    data = messages.DeviceMessage(
        signals={"monitor_async": {"value": [1, 2, 3]}}, metadata={"async_update": "replace"}
    )
    async_writer.connector.xadd(
        MessageEndpoints.device_async_readback("scan_id", "monitor_async"), msg_dict={"data": data}
    )

    data = messages.DeviceMessage(
        signals={"monitor_async": {"value": [4, 5, 6]}}, metadata={"async_update": "replace"}
    )
    async_writer.connector.xadd(
        MessageEndpoints.device_async_readback("scan_id", "monitor_async"), msg_dict={"data": data}
    )

    # normal poll and write should not write the data for replace
    async_writer.poll_and_write_data()

    assert not os.path.exists(async_writer.file_path)

    # write the final data
    async_writer.poll_and_write_data(final=True)

    # read the data back
    with h5py.File(async_writer.file_path, "r") as f:
        out = f[async_writer.BASE_PATH]["monitor_async"]["monitor_async"]["value"][:]

    assert len(out) == 3
    assert all(out == [4, 5, 6])
