import builtins
from unittest import mock

import pytest

from bec_lib.scans import DatasetIdOnHold, FileWriter, HideReport, Metadata, ScanExport

# pylint: disable=no-member
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access


def test_filewriter_cm(bec_client_mock):
    client = bec_client_mock
    with mock.patch.dict(builtins.__dict__, {"bec": client}):
        client.scans._file_writer = None
        client.system_config.file_directory = None
        client.system_config.file_suffix = None
        with FileWriter(file_suffix="testsuffix", file_directory="testdirectory"):
            assert client.system_config.file_directory == "testdirectory"
            assert client.system_config.file_suffix == "testsuffix"
        assert client.system_config.file_directory is None
        assert client.system_config.file_suffix is None


def test_metadata_handler(bec_client_mock):
    client = bec_client_mock

    with mock.patch.dict(builtins.__dict__, {"bec": client}):
        client.metadata = {"descr": "test", "uid": "12345"}
        with Metadata({"descr": "alignment", "pol": 1}):
            assert client.metadata == {"descr": "alignment", "uid": "12345", "pol": 1}

        assert client.metadata == {"descr": "test", "uid": "12345"}


def test_hide_report_cm(bec_client_mock):
    client = bec_client_mock
    client.scans._hide_report = None
    hrep = HideReport(client.scans)
    with hrep:
        assert client.scans._hide_report is True

    assert client.scans._hide_report is None


def test_dataset_id_on_hold_cm(bec_client_mock):
    client = bec_client_mock
    client.scans._dataset_id_on_hold = None
    dataset_id_on_hold = DatasetIdOnHold(client.scans)
    with mock.patch.object(client, "queue"):
        with dataset_id_on_hold:
            assert client.scans._dataset_id_on_hold is True

    assert client.scans._dataset_id_on_hold is None


def test_dataset_id_on_hold_cm_nested(bec_client_mock):
    client = bec_client_mock
    client.scans._dataset_id_on_hold = None
    dataset_id_on_hold = DatasetIdOnHold(client.scans)
    with mock.patch.object(client, "queue"):
        with dataset_id_on_hold:
            assert client.scans._dataset_id_on_hold is True
            with dataset_id_on_hold:
                assert client.scans._dataset_id_on_hold is True
            assert client.scans._dataset_id_on_hold is True
    assert client.scans._dataset_id_on_hold is None


def test_dataset_id_on_hold_cleanup_on_error(bec_client_mock):
    client = bec_client_mock
    client.scans._dataset_id_on_hold = None
    dataset_id_on_hold = DatasetIdOnHold(client.scans)
    with pytest.raises(AttributeError):
        with mock.patch.object(client, "queue"):
            with dataset_id_on_hold:
                assert client.scans._dataset_id_on_hold is True
                with dataset_id_on_hold:
                    assert client.scans._dataset_id_on_hold is True
                    raise AttributeError()
    assert client.scans._dataset_id_on_hold is None


@pytest.mark.parametrize("abort_on_ctrl_c", [True, False])
def test_scan_export_cm(abort_on_ctrl_c):
    scan_export = ScanExport("temp")
    with mock.patch("bec_lib.scans._get_client") as mock_get_client:
        mock_get_client.return_value = mock_client = mock.MagicMock()
        mock_client._service_config = mock_abort = mock.PropertyMock()
        mock_abort.abort_on_ctrl_c = abort_on_ctrl_c
        scan_export._export_to_csv = mock_to_csv = mock.MagicMock()
        if not abort_on_ctrl_c:
            with pytest.raises(RuntimeError):
                with scan_export:
                    ...  # Do nothing
        else:
            with scan_export:
                ...  # Do nothgin
            assert mock_to_csv.call_count == 1


def test_parameter_bundler(bec_client_mock):
    client = bec_client_mock
    dev = client.device_manager.devices
    res = client.scans._parameter_bundler((dev.samx, -5, 5, dev.samy, -5, 5), 3)
    assert res == {dev.samx: [-5, 5], dev.samy: [-5, 5]}

    res = client.scans._parameter_bundler((dev.samx, -5, 5, 5), 4)
    assert res == {dev.samx: [-5, 5, 5]}

    res = client.scans._parameter_bundler((-5, 5, 5), 0)
    assert res == (-5, 5, 5)


@pytest.mark.parametrize(
    ("arg", "dtype", "matches"),
    [
        ([1, 2.5], list[float], True),
        ([1, "nope"], list[float], False),
        ({1: 2.0}, dict[int, float], True),
        ({1: "nope"}, dict[int, float], False),
    ],
)
def test_arg_matches_type_for_generic_containers(bec_client_mock, arg, dtype, matches):
    client = bec_client_mock

    assert client.scans._input_validator._arg_matches_type(arg, dtype) is matches
