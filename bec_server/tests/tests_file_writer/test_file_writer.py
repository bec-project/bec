import datetime
from unittest import mock

import h5py
import numpy as np
import pytest
from test_file_writer_manager import file_writer_manager_mock

from bec_lib import messages, plugin_helper
from bec_lib.endpoints import MessageEndpoints
from bec_server.file_writer import HDF5FileWriter
from bec_server.file_writer.default_writer import DefaultFormat
from bec_server.file_writer.file_writer import AdditionalScanMetadata, HDF5Storage
from bec_server.file_writer.file_writer_manager import ScanStorage


class PluginFormatForTest(DefaultFormat):
    def format(self) -> None:
        entry = self.storage.create_group("entry")
        entry.attrs["definition"] = "NXtest"


class CapturingFormatForTest(DefaultFormat):
    last_written_async_signals = None

    def __init__(self, *args, written_async_signals=None, **kwargs):
        self.__class__.last_written_async_signals = written_async_signals
        super().__init__(*args, written_async_signals=written_async_signals, **kwargs)

    def format(self) -> None:
        entry = self.storage.create_group("entry")
        entry.attrs["definition"] = "NXcapture"


def _make_additional_scan_metadata(
    *,
    start_time: str = "2026-08-17T10:00:00+02:00",
    end_time: str = "2026-08-17T10:05:00+02:00",
    entry_identifier_uuid: str = "a9fb36e4-3f38-486c-8434-c8eca19472ba",
    deployment_info: messages.DeploymentInfoMessage | None = None,
    versions: messages.ServiceVersions | None = None,
) -> AdditionalScanMetadata:
    return AdditionalScanMetadata(
        start_time=start_time,
        end_time=end_time,
        entry_identifier_uuid=entry_identifier_uuid,
        deployment_info=deployment_info,
        versions=versions or messages.ServiceVersions._get_version_numbers(),
    )


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


@pytest.fixture(autouse=True)
def no_file_writer_plugins(monkeypatch):
    monkeypatch.setattr(plugin_helper, "get_file_writer_plugins", lambda: {})


@pytest.fixture
def scan_storage_mock(tmp_path):
    storage = ScanStorage("2", "scan_id-string")
    storage.metadata = {
        "readout_priority": {
            "baseline": ["eyefoc", "field"],
            "monitored": ["samx", "samy"],
            "async": ["mokev"],
        }
    }
    eiger_h5_path = f"{tmp_path}/eiger.h5"
    with h5py.File(eiger_h5_path, "w") as f:
        entry = f.create_group("entry")
        data = entry.create_group("data")
        data.create_dataset("detector_data", data=np.random.rand(10, 10))
    storage.file_references = {
        "master": messages.FileMessage(
            file_path="master.h5", is_master_file=True, done=False, successful=False
        ),
        "eiger": messages.FileMessage(
            file_path=eiger_h5_path,
            is_master_file=False,
            done=True,
            successful=True,
            hinted_h5_entries={"entry": "/entry"},
        ),
    }

    yield storage


@pytest.fixture
def default_format(file_writer_manager_mock_with_dm):
    yield DefaultFormat(
        storage=HDF5Storage(),
        data={},
        file_references={},
        info_storage={"bec": {"readout_priority": {}, "scan_report_devices": []}},
        configuration={},
        device_manager=file_writer_manager_mock_with_dm.device_manager,
        beamline_states={},
        additional_scan_metadata=_make_additional_scan_metadata(),
        written_async_signals=None,
    )


def test_get_entry_returns_values_for_scalar_and_list_data(default_format):
    default_format.data = {
        "samx": [{"samx": {"value": 1}}, {"samx": {"value": 2}}],
        "temperature": {"readback": {"value": 273.15}},
    }

    assert default_format.get_entry("samx") == [1, 2]
    assert default_format.get_entry("temperature", signal="readback") == 273.15


def test_get_entry_returns_default_for_missing_signal(default_format):
    default_format.data = {"samx": {"samx": {"value": 1}}}

    assert default_format.get_entry("samx", signal="missing", default="fallback") == "fallback"
    assert default_format.get_entry("missing", default="fallback") == "fallback"


def test_has_async_signal_returns_false_without_written_async_signals(default_format):
    with mock.patch.object(
        default_format.device_manager, "get_bec_signals"
    ) as mock_get_bec_signals:
        assert default_format.has_async_signal("samx", "samx") is False
        assert default_format.has_async_signal("waveform", "waveform") is False
        assert default_format.has_async_signal("samx", "other") is False

    mock_get_bec_signals.assert_not_called()


def test_has_async_signal_uses_written_async_signals_when_provided(default_format):
    default_format.written_async_signals = {"waveform": ["waveform_data"]}

    with mock.patch.object(
        default_format.device_manager, "get_bec_signals"
    ) as mock_get_bec_signals:
        assert default_format.has_async_signal("waveform", "waveform_data") is True
        assert default_format.has_async_signal("waveform", "other") is False
        assert default_format.has_async_signal("samx", "samx") is False

    mock_get_bec_signals.assert_not_called()


def test_safe_dataset_skips_missing_device(default_format):
    group = default_format.storage.create_group("group")

    default_format.safe_dataset(group, name="samx", device="samx")

    assert "samx" not in group._storage


def test_safe_dataset_writes_dataset_attrs_and_softlink(default_format):
    default_format.data = {"samx": {"samx": {"value": [0, 1, 2]}}}
    group = default_format.storage.create_group("group")

    default_format.safe_dataset(
        group,
        name="x_translation",
        device="samx",
        units="mm",
        description="sample x position",
        attributes={"long_name": "sample_x"},
        softlink=False,
    )
    default_format.safe_dataset(group, name="samx_link", device="samx", softlink=True)

    dataset = group._storage["x_translation"]
    assert dataset._data == [0, 1, 2]
    assert dataset.attrs["units"] == "mm"
    assert dataset.attrs["description"] == "sample x position"
    assert dataset.attrs["long_name"] == "sample_x"
    assert group._storage["samx_link"]._storage_type == "softlink"
    assert group._storage["samx_link"]._data == "/entry/collection/devices/samx/samx/value"


def test_safe_dataset_forces_softlink_for_async_signal(default_format):
    default_format.data = {"samx": {"samx": {"value": [0, 1, 2]}}}
    group = default_format.storage.create_group("group")

    with mock.patch.object(default_format, "has_async_signal", return_value=True):
        default_format.safe_dataset(group, name="samx", device="samx", softlink=False)

    assert group._storage["samx"]._storage_type == "softlink"
    assert group._storage["samx"]._data == "/entry/collection/devices/samx/samx/value"


def test_device_shape_matches_returns_false_for_missing_or_unshapeable_values(default_format):
    default_format.data = {
        "samx": {"samx": {"value": [0, 1, 2]}},
        "broken": {"broken": {"value": mock.Mock(side_effect=TypeError("boom"))}},
    }

    assert default_format._device_shape_matches("samx", "missing") is False

    with mock.patch(
        "bec_server.file_writer.default_writer.np.asarray", side_effect=TypeError("boom")
    ):
        assert default_format._device_shape_matches("samx", "broken") is False


def test_scan_report_data_only_includes_shape_compatible_auxiliary_signals(default_format):
    default_format.data = {
        "samx": {"samx": {"value": [0, 1, 2]}},
        "samy": {"samy": {"value": [3, 4, 5]}},
        "mokev": {"mokev": {"value": 12.456}},
    }
    default_format.info_storage["bec"]["scan_report_devices"] = ["samx", "samy", "mokev"]

    writer_storage = default_format.get_storage_format()
    data_group = writer_storage["entry"]._storage["data"]

    assert data_group.attrs["signal"] == "samx"
    assert data_group.attrs["auxiliary_signals"] == ["samy"]
    assert set(data_group._storage) == {"samx", "samy"}


def test_write_scan_report_data_skips_attrs_when_no_devices(default_format):
    entry = default_format.storage.create_group("entry")

    default_format._write_scan_report_data(entry)

    data_group = entry._storage["data"]
    assert data_group.attrs["NX_class"] == "NXdata"
    assert "signal" not in data_group.attrs
    assert "auxiliary_signals" not in data_group.attrs
    assert data_group._storage == {}


def test_default_format_writes_entry_datasets_from_info_storage(
    default_format, deployment_info_factory
):
    versions = messages.ServiceVersions._get_version_numbers()
    default_format.info_storage.update({"title": "Explicit Scan Title"})
    default_format.additional_scan_metadata = _make_additional_scan_metadata(
        deployment_info=deployment_info_factory(), versions=versions
    )

    writer_storage = default_format.get_storage_format()
    entry = writer_storage["entry"]

    assert entry._storage["start_time"]._data == "2026-08-17T10:00:00+02:00"
    assert entry._storage["end_time"]._data == "2026-08-17T10:05:00+02:00"
    assert entry._storage["title"]._data == "Explicit Scan Title"
    assert entry._storage["entry_identifier_uuid"]._data == "a9fb36e4-3f38-486c-8434-c8eca19472ba"
    assert (
        entry._storage["entry_identifier_uuid"].attrs["description"]
        == "Scan identifier (scan_id) used by BEC"
    )
    assert entry._storage["experiment_identifier"]._data == "p12345"
    assert (
        entry._storage["experiment_identifier"].attrs["description"]
        == "Proposal group (pgroup) of the experiment"
    )
    assert entry._storage["experiment_description"]._data == "Experiment Title"
    assert (
        entry._storage["experiment_description"].attrs["description"]
        == "Title of the experiment according to the proposal"
    )
    assert entry._storage["program_name"]._data == "BEC"
    assert entry._storage["program_name"].attrs["version"] == versions.bec_server
    assert entry._storage["user"].attrs["NX_class"] == "NXuser"
    assert entry._storage["user"]._storage["name"]._data == "John Doe"
    assert entry._storage["user"]._storage["role"]._data == "proposer"


def test_default_format_falls_back_to_experiment_title(default_format, deployment_info_factory):
    default_format.additional_scan_metadata = _make_additional_scan_metadata(
        deployment_info=deployment_info_factory()
    )

    writer_storage = default_format.get_storage_format()
    entry = writer_storage["entry"]

    assert "title" not in entry._storage
    assert entry._storage["experiment_identifier"]._data == "p12345"
    assert (
        entry._storage["experiment_identifier"].attrs["description"]
        == "Proposal group (pgroup) of the experiment"
    )
    assert entry._storage["experiment_description"]._data == "Experiment Title"
    assert (
        entry._storage["experiment_description"].attrs["description"]
        == "Title of the experiment according to the proposal"
    )
    assert entry._storage["user"]._storage["name"]._data == "John Doe"
    assert entry._storage["user"]._storage["role"]._data == "proposer"


def test_nexus_file_writer(hdf5_file_writer, scan_storage_mock, tmp_path):
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
        file_writer.write(f"{tmp_path}/test.h5", scan_storage_mock, configuration_data={})
        with h5py.File(f"{tmp_path}/test.tmp", "r") as test_file:
            assert list(test_file) == ["entry"]
            assert list(test_file["entry"]) == [
                "collection",
                "control",
                "data",
                "entry_identifier_uuid",
                "instrument",
                "program_name",
                "sample",
            ]
            assert np.allclose(
                test_file["entry/collection/devices/samx/samx/value"][...], [0, 1, 2, 3, 4]
            )
            assert test_file["entry/collection/file_references/eiger"] is not None
            assert test_file["entry/data"].attrs["NX_class"] == "NXdata"
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
                "scan_report_devices": ["samx", "samy"],
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
def test_write_data_storage(segments, baseline, metadata, hdf5_file_writer, tmp_path):
    file_writer = hdf5_file_writer
    storage = ScanStorage("2", "scan_id-string")
    storage.num_points = 2
    storage.scan_segments = segments
    storage.baseline = baseline
    storage.metadata = metadata
    storage.start_time = 1679226971.564235
    storage.end_time = 1679226971.580867
    storage.file_references = {
        "non_existing_file": messages.FileMessage(
            file_path="", done=True, successful=True, is_master_file=False, file_type="h5"
        )
    }

    file_writer.write(f"{tmp_path}/test.h5", storage, configuration_data={})

    data_info = file_writer.stored_data_info.get("samx")
    assert data_info.get("samx").get("shape") == (2,)
    assert data_info.get("samx_setpoint").get("shape") == (2,)
    assert data_info.get("samx").get("dtype") == "float64"
    # open file and check that time stamps are correct
    with h5py.File(f"{tmp_path}/test.tmp", "r") as test_file:
        assert (
            test_file["entry"].attrs["start_time"]
            == datetime.datetime.fromtimestamp(1679226971.564235).isoformat()
        )

        assert (
            test_file["entry"].attrs["end_time"]
            == datetime.datetime.fromtimestamp(1679226971.580867).isoformat()
        )
        assert (
            test_file["entry/start_time"].asstr()[()]
            == datetime.datetime.fromtimestamp(1679226971.564235).isoformat()
        )
        assert (
            test_file["entry/end_time"].asstr()[()]
            == datetime.datetime.fromtimestamp(1679226971.580867).isoformat()
        )
        assert (
            test_file["entry/collection/metadata/start_time"].asstr()[()]
            == datetime.datetime.fromtimestamp(1679226971.564235).isoformat()
        )
        assert (
            test_file["entry/collection/metadata/end_time"].asstr()[()]
            == datetime.datetime.fromtimestamp(1679226971.580867).isoformat()
        )
        assert (
            test_file["entry/entry_identifier_uuid"].asstr()[()]
            == "a9fb36e4-3f38-486c-8434-c8eca19472ba"
        )
        assert (
            test_file["entry/entry_identifier_uuid"].attrs["description"]
            == "Scan identifier (scan_id) used by BEC"
        )
        assert "non_existing_file" not in test_file["entry/collection/file_references"].keys()
        assert test_file["entry/data"].attrs["NX_class"] == "NXdata"
        assert test_file["entry/data"].attrs["signal"] == "samx"
        assert list(test_file["entry/data"].attrs["auxiliary_signals"]) == ["samy"]
        assert np.allclose(test_file["entry/data/samx"][...], [0.11, 0.21])
        assert np.allclose(test_file["entry/data/samy"][...], [1.1, 1.2])


def test_write_data_storage_injects_deployment_experiment_info(
    hdf5_file_writer, scan_storage_mock, tmp_path, deployment_info_factory
):
    deployment_info = deployment_info_factory(title="Test Experiment Title")

    with mock.patch.object(
        hdf5_file_writer.file_writer_manager.connector, "get_last", return_value=deployment_info
    ) as mock_get_last:
        hdf5_file_writer.write(f"{tmp_path}/test.h5", scan_storage_mock, configuration_data={})

    mock_get_last.assert_called_once_with(MessageEndpoints.deployment_info(), "data")

    with h5py.File(f"{tmp_path}/test.tmp", "r") as test_file:
        assert test_file["entry/entry_identifier_uuid"].asstr()[()] == "scan_id-string"
        assert (
            test_file["entry/entry_identifier_uuid"].attrs["description"]
            == "Scan identifier (scan_id) used by BEC"
        )
        assert test_file["entry/experiment_identifier"].asstr()[()] == "p12345"
        assert (
            test_file["entry/experiment_identifier"].attrs["description"]
            == "Proposal group (pgroup) of the experiment"
        )
        assert test_file["entry/experiment_description"].asstr()[()] == "Test Experiment Title"
        assert (
            test_file["entry/experiment_description"].attrs["description"]
            == "Title of the experiment according to the proposal"
        )
        assert test_file["entry/program_name"].asstr()[()] == "BEC"
        assert test_file["entry/user"].attrs["NX_class"] == "NXuser"
        assert test_file["entry/user/name"].asstr()[()] == "John Doe"
        assert test_file["entry/user/role"].asstr()[()] == "proposer"
        assert (
            test_file["entry/program_name"].attrs["version"]
            == messages.ServiceVersions._get_version_numbers().bec_server
        )
        assert "deployment" not in test_file["entry/collection/metadata"]
        assert "session" not in test_file["entry/collection/metadata"]
        assert "experiment" not in test_file["entry/collection/metadata"]


def test_load_format_from_plugin(tmp_path, hdf5_file_writer):
    file_writer = hdf5_file_writer
    file_writer.file_writer_manager.file_writer_config["plugin"] = "test_plugin"

    with mock.patch(
        "bec_lib.plugin_helper.get_file_writer_plugins"
    ) as mock_get_file_writer_plugins:
        mock_get_file_writer_plugins.return_value = {"test_plugin": PluginFormatForTest}
        data = ScanStorage(2, "scan_id-string")
        data.metadata = {
            "readout_priority": {
                "baseline": ["eyefoc", "field"],
                "monitored": ["samx", "samy"],
                "async": ["mokev"],
            }
        }
        file_writer.write(f"{tmp_path}/test.h5", data, configuration_data={})
    with h5py.File(f"{tmp_path}/test.tmp", "r") as test_file:
        assert test_file["entry"].attrs["definition"] == "NXtest"


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
            "test_plugin": PluginFormatForTest,
            "anotherPlugin": PluginFormatForTest,
        }
        file_writer.write(f"{tmp_path}/test.h5", scan_storage_mock, configuration_data={})
    with h5py.File(f"{tmp_path}/test.tmp", "r") as test_file:
        assert "definition" not in test_file["entry"].attrs


def test_load_format_from_plugin_uses_plugin(tmp_path, hdf5_file_writer, scan_storage_mock):
    """
    Test that the plugin is used if only one plugin is available, ignoring the config file.
    """
    file_writer = hdf5_file_writer

    with mock.patch(
        "bec_lib.plugin_helper.get_file_writer_plugins"
    ) as mock_get_file_writer_plugins:
        mock_get_file_writer_plugins.return_value = {"test_plugin": PluginFormatForTest}
        file_writer.write(f"{tmp_path}/test.h5", scan_storage_mock, configuration_data={})
    with h5py.File(f"{tmp_path}/test.tmp", "r") as test_file:
        assert test_file["entry"].attrs["definition"] == "NXtest"


def test_states_are_converted_to_compound_types(tmp_path, hdf5_file_writer, scan_storage_mock):
    """
    Test that the beamline states are correctly converted to numpy compound types and stored in the file.
    """
    file_writer = hdf5_file_writer
    storage = scan_storage_mock
    storage.beamline_states = {
        "State1": [
            messages.BeamlineStateMessage(
                name="State1", status="valid", label="Shutter open", timestamp=1.23
            ),
            messages.BeamlineStateMessage(
                name="State1", status="warning", label="Shutter moving", timestamp=2.34
            ),
        ]
    }

    file_writer.write(f"{tmp_path}/test.h5", storage, configuration_data={})

    with h5py.File(f"{tmp_path}/test.tmp", "r") as test_file:
        states_group = test_file["entry/collection/states"]
        assert "State1" in states_group

        state_dataset = states_group["State1"]
        assert state_dataset.dtype.names == ("label", "status", "timestamp")

        values = state_dataset[...]
        assert len(values) == 2

        label_0 = values[0]["label"]
        status_0 = values[0]["status"]
        label_1 = values[1]["label"]
        status_1 = values[1]["status"]
        if isinstance(label_0, bytes):
            label_0 = label_0.decode()
        if isinstance(status_0, bytes):
            status_0 = status_0.decode()
        if isinstance(label_1, bytes):
            label_1 = label_1.decode()
        if isinstance(status_1, bytes):
            status_1 = status_1.decode()

        assert label_0 == "Shutter open"
        assert status_0 == "valid"
        assert values[0]["timestamp"] == 1.23

        assert label_1 == "Shutter moving"
        assert status_1 == "warning"
        assert values[1]["timestamp"] == 2.34


def test_hdf5_writer_forwards_written_async_signals(tmp_path, hdf5_file_writer, scan_storage_mock):
    with mock.patch(
        "bec_lib.plugin_helper.get_file_writer_plugins"
    ) as mock_get_file_writer_plugins:
        mock_get_file_writer_plugins.return_value = {"capture": CapturingFormatForTest}
        hdf5_file_writer.write(
            f"{tmp_path}/test.h5",
            scan_storage_mock,
            configuration_data={},
            written_async_signals={"waveform": ["waveform_data"]},
        )

    assert CapturingFormatForTest.last_written_async_signals == {"waveform": ["waveform_data"]}
