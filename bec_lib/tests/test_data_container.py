from unittest import mock

import numpy as np
import pytest

from bec_lib.scan_data_container import FileReference, ScanDataContainer, _file_cache

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring


@pytest.fixture
def file_cache():
    _file_cache.clear_cache()
    yield _file_cache
    _file_cache.clear_cache()


def test_file_cache(mock_file, file_cache):
    """
    Test that the file access is cached and repeated access does not trigger a reload.
    """
    reference = FileReference(file_path=mock_file)
    entry_path = "entry/collection/devices/samx/samx/value"
    reference.read(entry_path=entry_path)
    assert file_cache._cache[0][0] == f"{reference.file_path}::{entry_path}"

    with mock.patch.object(file_cache, "add_item") as add_item:
        reference.read(entry_path=entry_path)
        add_item.assert_not_called()


def test_file_read_groups(mock_file):
    """
    Test that the file reference can read groups.
    """
    reference = FileReference(file_path=mock_file)
    groups = reference.get_hdf5_structure()
    assert groups["entry"]["collection"]["devices"]["samx"]["samx"]["value"] == {
        "type": "dataset",
        "shape": (3,),
        "dtype": int,
        "mem_size": 24,
    }


def test_data_container(mock_file):

    container = ScanDataContainer(file_path=mock_file)
    assert "samx" in container.devices.keys()
    assert "samx" in dir(container.devices)

    assert all(container.devices.samx.read()["samx"]["value"] == np.array([1, 2, 3]))
    assert all(container.devices.samx["samx"].read()["timestamp"] == np.array([1, 2, 3]))


def test_data_container_raises_without_file():
    with pytest.raises(RuntimeError):
        container = ScanDataContainer(file_path="does_not_exist.h5")
        container._load_devices(timeout_time=0.3)


def test_data_container_readout_group_access(mock_file):
    container = ScanDataContainer(file_path=mock_file)

    assert all(
        container.readout_groups.baseline_devices.samz.read()["samz"]["value"]
        == np.array([1, 2, 3])
    )
    assert all(
        container.readout_groups.baseline_devices.samz["samz"].read()["timestamp"]
        == np.array([1, 2, 3])
    )
    assert "samz" in container.readout_groups.baseline_devices.read()
    assert "samx" not in container.readout_groups.baseline_devices.read()
    assert "samx" in container.readout_groups.monitored_devices.read()


def test_data_container_read_metadata(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert container.metadata.sample_name == "test_sample"
    assert container.metadata.bec["scan_id"] == "scan_id_1"


def test_data_container_repr_without_msg(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert repr(container) == f"ScanDataContainer: {mock_file}"


def test_data_container_repr_with_msg(mock_file, file_history_messages):
    container = ScanDataContainer(file_path=mock_file, msg=file_history_messages[0])
    out = repr(container)
    assert "ScanDataContainer" in out
    assert "Scan number: 1" in out
    assert "Start time" in out
    assert "End time" in out
    assert "Scan ID: scan_id_1" in out


def test_data_container_devices_repr(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert "samx" in repr(container.devices)
    assert "samz" in repr(container.devices)


def test_data_container_readout_groups_repr(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert "samz" in repr(container.readout_groups.baseline_devices)
    assert "samx" in repr(container.readout_groups.monitored_devices)
    assert "waveform" in repr(container.readout_groups.async_devices)
    assert "samx" not in repr(container.readout_groups.baseline_devices)


def test_data_container_single_device_repr(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert "samx" in repr(container.devices.samx)
    assert "samz" not in repr(container.devices.samx)
    assert "(3,)" in repr(container.devices.samx)
    assert "0.00 MB" in repr(container.devices.samx)
    assert "int64" in repr(container.devices.samx)


def test_data_container_raises_if_no_file():
    with pytest.raises(ValueError):
        container = ScanDataContainer()
        container.devices.samx.read()


def test_data_container_to_pandas(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    df = container.readout_groups.monitored_devices.to_pandas()
    assert df["samx"]["samx"]["value"].tolist() == [1, 2, 3]
    assert df["samx"]["samx"]["timestamp"].tolist() == [1, 2, 3]
