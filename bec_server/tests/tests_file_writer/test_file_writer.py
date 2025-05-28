import datetime
import os
from unittest import mock

import h5py
import numpy as np
import pytest
from test_file_writer_manager import file_writer_manager_mock

from bec_lib import messages
from bec_server import file_writer
from bec_server.file_writer import HDF5FileWriter
from bec_server.file_writer.file_writer import HDF5Storage
from bec_server.file_writer.file_writer_manager import ScanStorage
from bec_server.file_writer_plugins.cSAXS import cSAXSFormat

dir_path = os.path.dirname(file_writer.__file__)


@pytest.fixture
def file_writer_manager_mock_with_dm(file_writer_manager_mock, dm_with_devices):
    file_writer = file_writer_manager_mock
    file_writer.device_manager = dm_with_devices
    yield file_writer


@pytest.fixture
def hdf5_file_writer(file_writer_manager_mock_with_dm):
    file_manager = file_writer_manager_mock_with_dm
    file_writer = HDF5FileWriter(file_manager)
    yield file_writer


@pytest.fixture
def scan_storage_mock():
    storage = ScanStorage("2", "scan_id-string")
    storage.metadata = {
        "readout_priority": {
            "baseline": ["eyefoc", "field"],
            "monitored": ["samx", "samy"],
            "async": ["mokev"],
        }
    }
    storage.file_references = {
        "master": messages.FileMessage(
            file_path="master.h5", is_master_file=True, done=False, successful=False
        ),
        "eiger": messages.FileMessage(
            file_path="eiger.h5",
            is_master_file=False,
            done=True,
            successful=True,
            hinted_h5_entries={"entry": "/entry"},
        ),
    }
    yield storage


def test_csaxs_nexus_format(file_writer_manager_mock_with_dm):
    file_manager = file_writer_manager_mock_with_dm
    writer_storage = cSAXSFormat(
        storage=HDF5Storage(),
        data={"samx": {"samx": {"value": [0, 1, 2]}}, "mokev": {"mokev": {"value": 12.456}}},
        file_references={},
        info_storage={
            "bec": {"readout_priority": {"baseline": ["mokev"], "monitored": ["samx", "samy"]}}
        },
        configuration={},
        device_manager=file_manager.device_manager,
    ).get_storage_format()
    assert writer_storage["entry"].attrs["definition"] == "NXsas"
    assert writer_storage["entry"]._storage["sample"]._storage["x_translation"]._data == [0, 1, 2]


def test_nexus_file_writer(hdf5_file_writer, scan_storage_mock):
    file_writer = hdf5_file_writer
    with mock.patch.object(
        file_writer,
        "_create_device_data_storage",
        return_value={
            "samx": [
                {"samx": {"value": 0}},
                {"samx": {"value": 1}},
                {"samx": {"value": 2}},
                {"samx": {"value": 3}},
                {"samx": {"value": 4}},
            ]
        },
    ):
        file_writer.write("./test.h5", scan_storage_mock, configuration_data={})
    with h5py.File("./test.h5", "r") as test_file:
        assert list(test_file) == ["entry"]
        assert list(test_file["entry"]) == ["collection", "control", "instrument", "sample"]
        assert np.allclose(
            test_file["entry/collection/devices/samx/samx/value"][...], [0, 1, 2, 3, 4]
        )
        assert test_file["entry/collection/file_references/eiger"] is not None
        # assert list(test_file["entry"]["sample"]) == ["x_translation"]
        # assert test_file["entry"]["sample"].attrs["NX_class"] == "NXsample"
        # assert test_file["entry"]["sample"]["x_translation"].attrs["units"] == "mm"
        # assert all(np.asarray(test_file["entry"]["sample"]["x_translation"]) == [0, 1, 2])


def test_create_device_data_storage(hdf5_file_writer, scan_storage_mock):
    file_writer = hdf5_file_writer
    storage = scan_storage_mock
    storage.num_points = 2
    storage.scan_segments = {
        0: {"samx": {"samx": {"value": 0.1}}, "samy": {"samy": {"value": 1.1}}},
        1: {"samx": {"samx": {"value": 0.2}}, "samy": {"samy": {"value": 1.2}}},
    }
    storage.baseline = {}
    device_storage = file_writer._create_device_data_storage(storage)
    assert len(device_storage.keys()) == 2
    assert len(device_storage["samx"]) == 2
    assert device_storage["samx"][0]["samx"]["value"] == 0.1
    assert device_storage["samx"][1]["samx"]["value"] == 0.2


@pytest.mark.parametrize(
    "segments,baseline,metadata",
    [
        (
            {
                0: {
                    "samx": {"samx": {"value": 0.11}, "samx_setpoint": {"value": 0.1}},
                    "samy": {"samy": {"value": 1.1}},
                },
                1: {
                    "samx": {"samx": {"value": 0.21}, "samx_setpoint": {"value": 0.2}},
                    "samy": {"samy": {"value": 1.2}},
                },
            },
            {
                "eyefoc": {
                    "eyefoc": {"value": 0, "timestamp": 1679226971.564248},
                    "eyefoc_setpoint": {"value": 0, "timestamp": 1679226971.564235},
                    "eyefoc_motor_is_moving": {"value": 0, "timestamp": 1679226971.564249},
                },
                "field": {
                    "field_x": {"value": 0, "timestamp": 1679226971.579148},
                    "field_x_setpoint": {"value": 0, "timestamp": 1679226971.579145},
                    "field_x_motor_is_moving": {"value": 0, "timestamp": 1679226971.579148},
                    "field_y": {"value": 0, "timestamp": 1679226971.5799649},
                    "field_y_setpoint": {"value": 0, "timestamp": 1679226971.579962},
                    "field_y_motor_is_moving": {"value": 0, "timestamp": 1679226971.579966},
                    "field_z_zsub": {"value": 0, "timestamp": 1679226971.58087},
                    "field_z_zsub_setpoint": {"value": 0, "timestamp": 1679226971.580867},
                    "field_z_zsub_motor_is_moving": {"value": 0, "timestamp": 1679226971.58087},
                },
            },
            {
                "RID": "5ee455b8-d0ef-452d-b54a-e7cea5cea19e",
                "scan_id": "a9fb36e4-3f38-486c-8434-c8eca19472ba",
                "queue_id": "14463a5b-1c65-4888-8f87-4808c90a241f",
                "primary": ["samx"],
                "num_points": 2,
                "positions": [[-100], [100]],
                "scan_name": "monitor_scan",
                "scan_type": "fly",
                "scan_number": 88,
                "dataset_number": 88,
                "exp_time": 0.1,
                "scan_report_devices": ["samx"],
                "scan_msgs": [
                    "ScanQueueMessage(({'scan_type': 'monitor_scan', 'parameter': {'args': {'samx':"
                    " [-100, 100]}, 'kwargs': {'relative': False}}, 'queue': 'primary'}, {'RID':"
                    " '5ee455b8-d0ef-452d-b54a-e7cea5cea19e'})))"
                ],
                "readout_priority": {
                    "baseline": ["eyefoc", "field"],
                    "monitored": ["samx", "samy"],
                    "async": ["mokev"],
                },
            },
        )
    ],
)
def test_write_data_storage(segments, baseline, metadata, hdf5_file_writer):
    file_writer = hdf5_file_writer
    storage = ScanStorage("2", "scan_id-string")
    storage.num_points = 2
    storage.scan_segments = segments
    storage.baseline = baseline
    storage.metadata = metadata
    storage.start_time = 1679226971.564235
    storage.end_time = 1679226971.580867

    file_writer.write("./test.h5", storage, configuration_data={})

    # open file and check that time stamps are correct
    with h5py.File("./test.h5", "r") as test_file:
        assert (
            test_file["entry"].attrs["start_time"]
            == datetime.datetime.fromtimestamp(1679226971.564235).isoformat()
        )

        assert (
            test_file["entry"].attrs["end_time"]
            == datetime.datetime.fromtimestamp(1679226971.580867).isoformat()
        )


def test_load_format_from_plugin(tmp_path, hdf5_file_writer):
    file_writer = hdf5_file_writer
    file_writer.file_writer_manager.file_writer_config["plugin"] = "cSAXS"

    with mock.patch(
        "bec_lib.plugin_helper.get_file_writer_plugins"
    ) as mock_get_file_writer_plugins:
        mock_get_file_writer_plugins.return_value = {"cSAXS": cSAXSFormat}
        data = ScanStorage(2, "scan_id-string")
        data.metadata = {
            "readout_priority": {
                "baseline": ["eyefoc", "field"],
                "monitored": ["samx", "samy"],
                "async": ["mokev"],
            }
        }
        file_writer.write(f"{tmp_path}/test.h5", data, configuration_data={})
    with h5py.File(f"{tmp_path}/test.h5", "r") as test_file:
        assert test_file["entry"].attrs["definition"] == "NXsas"


def test_load_format_from_plugin_uses_default(tmp_path, hdf5_file_writer, scan_storage_mock):
    """
    Test that the default plugin is used if multiple plugins are available but the specified plugin
    is not found.
    """
    file_writer = hdf5_file_writer
    file_writer.file_writer_manager.file_writer_config["plugin"] = "wrong_plugin"

    with mock.patch(
        "bec_lib.plugin_helper.get_file_writer_plugins"
    ) as mock_get_file_writer_plugins:
        mock_get_file_writer_plugins.return_value = {
            "cSAXS": cSAXSFormat,
            "anotherPlugin": cSAXSFormat,
        }
        file_writer.write(f"{tmp_path}/test.h5", scan_storage_mock, configuration_data={})
    with h5py.File(f"{tmp_path}/test.h5", "r") as test_file:
        assert "definition" not in test_file["entry"].attrs


def test_load_format_from_plugin_uses_plugin(tmp_path, hdf5_file_writer, scan_storage_mock):
    """
    Test that the plugin is used if only one plugin is available, ignoring the config file.
    """
    file_writer = hdf5_file_writer

    with mock.patch(
        "bec_lib.plugin_helper.get_file_writer_plugins"
    ) as mock_get_file_writer_plugins:
        mock_get_file_writer_plugins.return_value = {"cSAXS": cSAXSFormat}
        file_writer.write(f"{tmp_path}/test.h5", scan_storage_mock, configuration_data={})
    with h5py.File(f"{tmp_path}/test.h5", "r") as test_file:
        assert test_file["entry"].attrs["definition"] == "NXsas"
