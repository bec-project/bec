from unittest import mock

import pytest

from bec_lib.device import DeviceBase
from bec_lib.scans import DatasetIdOnHold, HideReport, Metadata, ScanDef, ScanExport, ScanGroup

# pylint: disable=no-member
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access


def test_metadata_handler(bec_client_mock):
    client = bec_client_mock
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


def test_scan_def_cm(bec_client_mock):
    client = bec_client_mock
    client.scans._scan_def_id = None
    scan_def_id_cm = ScanDef(client.scans)
    with scan_def_id_cm:
        assert isinstance(client.scans._scan_def_id, str)

    assert client.scans._scan_def_id is None


def test_scan_group_cm(bec_client_mock):
    client = bec_client_mock
    client.scans._scan_group = None
    scan_group_cm = ScanGroup(client.scans)
    with scan_group_cm:
        assert isinstance(client.scans._scan_group, str)

    assert client.scans._scan_group is None


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
    assert res == {"samx": [-5, 5], "samy": [-5, 5]}

    res = client.scans._parameter_bundler((dev.samx, -5, 5, 5), 4)
    assert res == {"samx": [-5, 5, 5]}

    res = client.scans._parameter_bundler((-5, 5, 5), 0)
    assert res == (-5, 5, 5)


@pytest.mark.parametrize(
    "in_type,out",
    [
        ("float", (float, int)),
        ("int", int),
        ("list", list),
        ("boolean", bool),
        ("str", str),
        ("dict", dict),
        ("device", DeviceBase),
    ],
)
def test_get_arg_type(bec_client_mock, in_type, out):
    client = bec_client_mock
    res = client.scans.get_arg_type(in_type)
    assert res == out


def test_get_arg_type_raises(bec_client_mock):
    client = bec_client_mock
    with pytest.raises(TypeError):
        client.scans.get_arg_type("not_existing")


def test_interactive_scan_cm(bec_client_mock):
    client = bec_client_mock
    client.scans._open_interactive_scan = mock.MagicMock()
    client.scans._close_interactive_scan = mock.MagicMock()
    client.scans._interactive_trigger = mock.MagicMock()
    client.scans._interactive_read_monitored = mock.MagicMock()

    with client.scans.interactive_scan("samx") as scan:
        scan.trigger()
        scan.read_monitored_devices()

    client.scans._open_interactive_scan.assert_called_once()
    client.scans._close_interactive_scan.assert_called_once()
    client.scans._interactive_trigger.assert_called_once()
    client.scans._interactive_read_monitored.assert_called_once()


def test_interactive_scan_cm_raise_calls_close(bec_client_mock):
    client = bec_client_mock
    client.scans._open_interactive_scan = mock.MagicMock()
    client.scans._close_interactive_scan = mock.MagicMock()
    client.scans._interactive_trigger = mock.MagicMock()
    client.scans._interactive_read_monitored = mock.MagicMock()

    with pytest.raises(AttributeError):
        with client.scans.interactive_scan("samx") as scan:
            scan.trigger()
            scan.read_monitored_devices()
            raise AttributeError()

    client.scans._open_interactive_scan.assert_called_once()
    client.scans._close_interactive_scan.assert_called_once()
    client.scans._interactive_trigger.assert_called_once()
    client.scans._interactive_read_monitored.assert_called_once()


def test_interactive_scan_cm_raises_scan_motors(bec_client_mock):
    client = bec_client_mock
    client.scans._open_interactive_scan = mock.MagicMock()
    client.scans._close_interactive_scan = mock.MagicMock()
    client.scans._interactive_trigger = mock.MagicMock()
    client.scans._interactive_read_monitored = mock.MagicMock()

    with pytest.raises(TypeError):
        with client.scans.interactive_scan() as scan:
            scan.trigger()
            scan.read_monitored_devices()

    client.scans._open_interactive_scan.assert_not_called()
    client.scans._close_interactive_scan.assert_not_called()
    client.scans._interactive_trigger.assert_not_called()
    client.scans._interactive_read_monitored.assert_not_called()
