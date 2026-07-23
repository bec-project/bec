"""This module tests the bec_lib.client module."""

from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.client import SystemConfig
from bec_lib.endpoints import MessageEndpoints
from bec_lib.file_utils import sanitize_relative_subdir
from bec_lib.tests.fixtures import bec_client_mock


def test_system_config():
    """Test the SystemConfig class."""
    config = SystemConfig(file_suffix="suff", file_directory="dir")
    assert config.file_suffix == "suff"
    assert config.file_directory == "dir"
    config = SystemConfig()
    assert config.file_suffix is None
    assert config.file_directory is None
    config.file_suffix = "suff_-"
    config.file_directory = "/dir_-/blabla"
    assert config.file_suffix == "suff_-"
    assert config.file_directory == "dir_-/blabla"
    with pytest.raises(ValueError):
        config = SystemConfig(file_suffix="@")
        config = SystemConfig(file_directory="ä")


def test_show_all_commands(bec_client_mock, capsys):
    """Test the show_all_commands method."""
    client = bec_client_mock
    client.show_all_commands()
    captured = capsys.readouterr()
    assert "User macros" in captured.out
    assert "Scans" in captured.out


def test_request_scan_reload(bec_client_mock):
    client = bec_client_mock

    client._request_scan_reload()

    client.connector.send.assert_called_once_with(
        MessageEndpoints.service_request(), messages.ServiceRequestMessage(action="reload_scans")
    )


def test_request_scan_reload_requires_initialized_client(bec_client_mock):
    client = bec_client_mock
    client.connector = None

    with pytest.raises(RuntimeError, match="Client not initialized. Cannot reload scans."):
        client._request_scan_reload()


def test_beamline_storage_copy(bec_client_mock):
    client = bec_client_mock

    with mock.patch("bec_lib.client.get_file_writer_storage_copy_plugin", return_value=mock.Mock()):
        client.beamline_storage_copy("/tmp/test.h5", "flomni_alignment")

    client.connector.send.assert_called_once_with(
        MessageEndpoints.storage_copy_request(),
        messages.StorageCopyRequestMessage(
            source_file="/tmp/test.h5", scope="flomni_alignment", subdir=None
        ),
    )


def test_beamline_storage_copy_sanitizes_subdir(bec_client_mock):
    client = bec_client_mock

    with mock.patch("bec_lib.client.get_file_writer_storage_copy_plugin", return_value=mock.Mock()):
        client.beamline_storage_copy("/tmp/test.h5", "flomni_alignment", "../results/./nested")

    client.connector.send.assert_called_once_with(
        MessageEndpoints.storage_copy_request(),
        messages.StorageCopyRequestMessage(
            source_file="/tmp/test.h5", scope="flomni_alignment", subdir="results/nested"
        ),
    )


def test_beamline_storage_copy_requires_plugin(bec_client_mock):
    client = bec_client_mock

    with mock.patch("bec_lib.client.get_file_writer_storage_copy_plugin", return_value=None):
        with pytest.raises(RuntimeError, match="No file-writer storage copy plugin is installed."):
            client.beamline_storage_copy("/tmp/test.h5", "flomni_alignment")

    client.connector.send.assert_not_called()


@pytest.mark.parametrize(
    "subdir, expected",
    [
        (None, None),
        ("", None),
        ("../foo", "foo"),
        ("/absolute/path", "absolute/path"),
        ("..\\windows\\path", "windows/path"),
        ("safe/dir", "safe/dir"),
    ],
)
def test_sanitize_relative_subdir(subdir, expected):
    assert sanitize_relative_subdir(subdir) == expected
