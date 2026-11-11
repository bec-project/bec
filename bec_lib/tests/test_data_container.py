from unittest import mock

import h5py
import numpy as np
import pytest

from bec_lib.scan_data_container import (
    FileReference,
    ScanDataContainer,
    SignalDataReference,
    SignalSlab,
    _file_cache,
)

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


def test_signal_read_slabs_of_the_default_size(tmp_path):
    """A dataset larger than one slab comes in several slabs that add up to the plain read."""
    file_path = tmp_path / "scan.h5"
    values = np.arange(20_000_000, dtype=np.uint16)  # > _SLAB_ELEMENTS
    timestamps = np.arange(2_000, dtype=float)
    with h5py.File(file_path, "w") as f:
        group = f.create_group("entry/collection/devices/waveform/waveform_waveform")
        group.create_dataset("value", data=values)
        group.create_dataset("timestamp", data=timestamps)
    reference = SignalDataReference(
        str(file_path), "entry/collection/devices/waveform/waveform_waveform"
    )

    slabs = list(reference.read_slabs())

    assert [(slab.start, slab.stop) for slab in slabs] == [
        (0, 8_000_000),
        (8_000_000, 16_000_000),
        (16_000_000, 20_000_000),
    ]
    assert all(slab.total == 20_000_000 for slab in slabs)
    np.testing.assert_array_equal(np.concatenate([slab.data["value"] for slab in slabs]), values)
    np.testing.assert_array_equal(slabs[0].other["timestamp"], timestamps)
    np.testing.assert_array_equal(reference.read()["value"], values)


def test_signal_read_slabs_cover_the_signal_in_order(mock_file, monkeypatch):
    reference = SignalDataReference(str(mock_file), "entry/collection/devices/samx/samx")
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 1)

    slabs = list(reference.read_slabs())

    assert all(isinstance(slab, SignalSlab) for slab in slabs)
    assert [(slab.start, slab.stop, slab.total) for slab in slabs] == [
        (0, 1, 3),
        (1, 2, 3),
        (2, 3, 3),
    ]
    plain = reference.read()
    for key in ("value", "timestamp"):
        joined = np.concatenate([slab.data[key] for slab in slabs])
        np.testing.assert_array_equal(joined, plain[key])


def test_signal_read_slabs_are_whole_hdf5_chunks(tmp_path, monkeypatch):
    """Slabs of a chunked dataset end on chunk borders; other lengths come with the first slab."""
    file_path = tmp_path / "scan.h5"
    values = np.arange(1000 * 4, dtype=np.int32).reshape(1000, 4)
    with h5py.File(file_path, "w") as f:
        group = f.create_group("entry/collection/devices/cam/cam_frames")
        group.create_dataset("value", data=values, chunks=(64, 4), maxshape=(None, 4))
        group.create_dataset("timestamp", data=np.arange(7, dtype=float))
    reference = SignalDataReference(str(file_path), "entry/collection/devices/cam/cam_frames")
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 4 * 100)  # 100 rows

    slabs = list(reference.read_slabs())

    assert [slab.stop - slab.start for slab in slabs[:-1]] == [64] * (len(slabs) - 1)
    assert slabs[-1].stop == 1000
    np.testing.assert_array_equal(np.concatenate([slab.data["value"] for slab in slabs]), values)
    # the timestamps are not one per row here: they come whole, once, apart from the rows
    assert all(set(slab.data) == {"value"} for slab in slabs)
    assert [set(slab.other) for slab in slabs] == [{"timestamp"}] + [set()] * (len(slabs) - 1)
    np.testing.assert_array_equal(slabs[0].other["timestamp"], np.arange(7, dtype=float))


def test_signal_read_slabs_bound_the_elements_of_ragged_rows(tmp_path, monkeypatch):
    """
    The async writer stores a signal whose rows differ in length as a variable-length
    dataset of shape (rows,). Its slabs are sized by the elements in the rows, not by the
    number of rows.
    """
    file_path = tmp_path / "scan.h5"
    rows = [np.full(1000, float(index)) for index in range(10)]
    with h5py.File(file_path, "w") as f:
        group = f.create_group("entry/collection/devices/det/det_wave")
        value = group.create_dataset(
            "value", shape=(10,), maxshape=(None,), dtype=h5py.vlen_dtype(np.dtype("float64"))
        )
        for index, row in enumerate(rows):
            value[index] = row
        group.create_dataset("timestamp", data=np.arange(10, dtype=float), maxshape=(None,))
    reference = SignalDataReference(str(file_path), "entry/collection/devices/det/det_wave")
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 2000)  # two rows

    slabs = list(reference.read_slabs())

    # The first slab is one row: nothing is known about the rows before one is read.
    assert [(slab.start, slab.stop) for slab in slabs] == [
        (0, 1),
        (1, 3),
        (3, 5),
        (5, 7),
        (7, 9),
        (9, 10),
    ]
    assert all(sum(len(row) for row in slab.data["value"]) <= 2000 for slab in slabs)
    for slab in slabs:
        for index, row in zip(range(slab.start, slab.stop), slab.data["value"]):
            np.testing.assert_array_equal(row, rows[index])
        np.testing.assert_array_equal(
            slab.data["timestamp"], np.arange(slab.start, slab.stop, dtype=float)
        )


def test_signal_read_slabs_follow_the_length_of_ragged_rows(tmp_path, monkeypatch):
    """Short rows are read many at a time; when the rows get longer the slabs get shorter."""
    file_path = tmp_path / "scan.h5"
    lengths = [10] * 6 + [100] * 6
    with h5py.File(file_path, "w") as f:
        group = f.create_group("entry/collection/devices/det/det_wave")
        value = group.create_dataset(
            "value", shape=(len(lengths),), dtype=h5py.vlen_dtype(np.dtype("int32"))
        )
        for index, length in enumerate(lengths):
            value[index] = np.arange(length, dtype=np.int32)
    reference = SignalDataReference(str(file_path), "entry/collection/devices/det/det_wave")
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 50)

    slabs = list(reference.read_slabs())

    # one row, two rows, four rows; the slab of four meets the first long row, after
    # which every row is a slab of its own
    assert [(slab.start, slab.stop) for slab in slabs] == [
        (0, 1),
        (1, 3),
        (3, 7),
        (7, 8),
        (8, 9),
        (9, 10),
        (10, 11),
        (11, 12),
    ]
    assert [len(row) for slab in slabs for row in slab.data["value"]] == lengths


@pytest.mark.parametrize("first_row", [0, 3])
def test_signal_read_slabs_grow_slowly_after_a_short_ragged_row(tmp_path, monkeypatch, first_row):
    """
    A short or empty first row says little about the rows after it. The slabs double in
    rows from there, so the long rows that follow are not read in one piece.
    """
    file_path = tmp_path / "scan.h5"
    lengths = [first_row] + [1000] * 9
    with h5py.File(file_path, "w") as f:
        group = f.create_group("entry/collection/devices/det/det_wave")
        value = group.create_dataset(
            "value", shape=(len(lengths),), dtype=h5py.vlen_dtype(np.dtype("float64"))
        )
        for index, length in enumerate(lengths):
            value[index] = np.full(length, float(index))
    reference = SignalDataReference(str(file_path), "entry/collection/devices/det/det_wave")
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 2000)  # two long rows

    slabs = list(reference.read_slabs())

    assert [(slab.start, slab.stop) for slab in slabs] == [
        (0, 1),
        (1, 3),
        (3, 5),
        (5, 7),
        (7, 9),
        (9, 10),
    ]
    assert all(sum(len(row) for row in slab.data["value"]) <= 2000 for slab in slabs)
    assert [len(row) for slab in slabs for row in slab.data["value"]] == lengths


def test_signal_read_slabs_count_a_string_as_one_element(tmp_path, monkeypatch):
    file_path = tmp_path / "scan.h5"
    with h5py.File(file_path, "w") as f:
        group = f.create_group("entry/collection/devices/det/det_label")
        group.create_dataset("value", data=[f"label {index}" for index in range(10)])
    reference = SignalDataReference(str(file_path), "entry/collection/devices/det/det_label")
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 4)

    slabs = list(reference.read_slabs())

    assert [(slab.start, slab.stop) for slab in slabs] == [(0, 4), (4, 8), (8, 10)]


def test_signal_read_slabs_of_an_empty_signal(tmp_path):
    file_path = tmp_path / "scan.h5"
    with h5py.File(file_path, "w") as f:
        group = f.create_group("entry/collection/devices/cam/cam_frames")
        group.create_dataset("value", shape=(0, 4), maxshape=(None, 4), dtype=np.int32)
    reference = SignalDataReference(str(file_path), "entry/collection/devices/cam/cam_frames")

    slabs = list(reference.read_slabs())

    assert [(slab.start, slab.stop, slab.total) for slab in slabs] == [(0, 0, 0)]
    assert slabs[0].data["value"].shape == (0, 4)


def test_signal_read_slabs_close_the_file_at_the_end_of_a_with_block(mock_file, monkeypatch):
    """A loop that ends early inside ``with`` leaves no open file behind."""
    reference = SignalDataReference(str(mock_file), "entry/collection/devices/samx/samx")
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 1)

    with reference.read_slabs() as slabs:
        for slab in slabs:
            assert slab.start == 0
            break
        # the reader still holds the file: it is suspended between two slabs
        with pytest.raises(OSError):
            h5py.File(mock_file, "r+")

    with h5py.File(mock_file, "r+"):
        pass
    assert list(slabs) == []


def test_signal_read_slabs_close_the_file_when_the_with_block_raises(mock_file, monkeypatch):
    reference = SignalDataReference(str(mock_file), "entry/collection/devices/samx/samx")
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 1)

    with pytest.raises(RuntimeError, match="consumer failed"):
        with reference.read_slabs() as slabs:
            next(slabs)
            raise RuntimeError("consumer failed")

    with h5py.File(mock_file, "r+"):
        pass


def test_signal_read_slabs_close_the_file_when_abandoned(mock_file, monkeypatch):
    reference = SignalDataReference(str(mock_file), "entry/collection/devices/samx/samx")
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 1)

    slabs = reference.read_slabs()
    next(slabs)
    slabs.close()

    # the file can be opened for writing again, so the reader let go of it
    with h5py.File(mock_file, "r+"):
        pass
