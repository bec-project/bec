import subprocess
import sys
from collections.abc import Callable
from typing import get_type_hints
from unittest import mock

import h5py
import hdf5plugin
import numpy as np
import pytest

from bec_lib.scan_data_container import (
    FileReference,
    LazyAttributeDict,
    ScanDataContainer,
    _file_cache,
)


@pytest.fixture
def file_cache():
    _file_cache.clear_cache()
    yield _file_cache
    _file_cache.clear_cache()


def test_compressed_file_read_registers_filters_in_fresh_process(tmp_path, monkeypatch):
    file_path = tmp_path / "compressed.h5"
    values = np.arange(64)
    with h5py.File(file_path, "w") as file:
        file.create_dataset("data", data=values, **hdf5plugin.Blosc(cname="zstd"))

    # Filter registration must come from the data reader, not another test or a system plugin path.
    monkeypatch.delenv("HDF5_PLUGIN_PATH", raising=False)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; from bec_lib.scan_data_container import FileReference; "
                "print(FileReference(sys.argv[1]).read('data', cached=False).tolist())"
            ),
            str(file_path),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == str(values.tolist())


def test_lazy_attribute_dict_annotations_can_be_resolved():
    assert get_type_hints(LazyAttributeDict.__init__)["load_function"] == Callable | None


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
    # Attribute discovery loads the lazy device mapping before membership is checked.
    assert "samx" in dir(container.devices)
    assert "samx" in container.devices

    assert all(container.devices.samx.read()["samx"]["value"] == np.array([1, 2, 3]))
    assert all(container.devices.samx["samx"].read()["timestamp"] == np.array([1, 2, 3]))


def test_data_container_raises_without_file():
    with pytest.raises(RuntimeError):
        container = ScanDataContainer(file_path="does_not_exist.h5")
        container._load_devices(timeout_time=0.3)


def test_data_container_readout_group_access(mock_file):
    container = ScanDataContainer(file_path=mock_file)

    assert container.readout_groups.baseline_devices.samz.read()["samz"]["value"] == 1
    assert container.readout_groups.baseline_devices.samz["samz"].read()["timestamp"] == 1

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


def test_data_container_get_device(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert container.devices.get("samx") == container.devices.samx
    assert container.devices.get("samz") == container.devices.samz
    assert container.devices.get("doesn't exist") is None


def test_data_container_get_signal(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert container.devices.samx.get("samx") == container.devices.samx["samx"]
    assert container.devices.samx.get("doesn't exist") is None


def test_data_container_get_device_from_readout_group(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert (
        container.readout_groups.monitored_devices.get("samx")
        == container.readout_groups.monitored_devices.samx
    )
    assert (
        container.readout_groups.monitored_devices.get("samz")
        == container.readout_groups.monitored_devices.samz
    )
    assert container.readout_groups.monitored_devices.get("doesn't exist") is None


def test_data_container_get_on_signal_directly(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert all(container.devices.samx.samx.get() == container.devices.samx.samx.read()["value"])
    assert all(
        container.readout_groups.monitored_devices.samx.samx.get()
        == container.devices.samx.samx.read()["value"]
    )


def test_data_container_get_on_device_directly(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    out = container.devices.samx.get()
    assert all(out.samx == container.devices.samx.samx.get())
    assert all(out.samx_setpoint == container.devices.samx.samx_setpoint.get())

    out = container.readout_groups.monitored_devices.samx.get()
    assert all(out.samx == container.devices.samx.samx.get())


def test_data_container_read_device_from_data(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    assert all(container.data.samx.read()["value"] == container.devices.samx.samx.read()["value"])
    assert all(
        container.data.samx_setpoint.read()["value"]
        == container.devices.samx.samx_setpoint.read()["value"]
    )


def test_data_container_read_monitored_pandas(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    df = container.readout_groups.monitored_devices.to_pandas()
    assert df["samx"]["samx"]["value"].tolist() == [1, 2, 3]
    assert df["samx"]["samx"]["timestamp"].tolist() == [1, 2, 3]
    assert df["samx"]["samx_setpoint"]["value"].tolist() == [1, 2, 3]
    assert df["samx"]["samx_setpoint"]["timestamp"].tolist() == [1, 2, 3]


def test_data_container_read_baseline_pandas(mock_file):
    container = ScanDataContainer(file_path=mock_file)
    df = container.readout_groups.baseline_devices.to_pandas()
    assert df["samz"]["samz"]["value"].tolist() == [1]
    assert df["samz"]["samz"]["timestamp"].tolist() == [1]
