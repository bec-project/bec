"""This module tests the bec_lib.client module."""

import threading
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.client import SystemConfig
from bec_lib.endpoints import MessageEndpoints, MessageOp
from bec_lib.file_utils import sanitize_relative_subdir
from bec_lib.redis_connector import MessageObject
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


def test_request_stop_all_devices(bec_client_mock):
    client = bec_client_mock

    client._request_stop_all_devices()

    client.connector.send.assert_called_once_with(
        MessageEndpoints.stop_devices(), messages.VariableMessage(value=None)
    )


def test_request_stop_all_devices_requires_initialized_client(bec_client_mock):
    client = bec_client_mock
    client.connector = None

    with pytest.raises(RuntimeError, match="Client not initialized. Cannot stop devices."):
        client._request_stop_all_devices()


def test_request_server_restart_refreshes_devices_without_notification(bec_client_mock):
    client = bec_client_mock
    manager = client.device_manager
    updated_config = {**manager.devices.samx._config, "enabled": False}
    manager._allow_override = False

    with (
        mock.patch.object(client, "_update_existing_services"),
        mock.patch.object(client, "_services_info", {"SciHub": mock.Mock()}, create=True),
        mock.patch.object(client, "wait_for_service"),
        mock.patch.object(client, "_load_scans"),
        mock.patch("bec_lib.client.time.sleep"),
        mock.patch.object(manager, "_get_redis_device_config", return_value=[updated_config]),
    ):
        client._request_server_restart()

    assert set(manager.devices) == {"samx"}
    assert not manager.devices.samx.enabled
    assert manager._allow_override is False


@pytest.mark.parametrize("first_operation", ["restart", "notification"])
def test_request_server_restart_serializes_device_reload(bec_client_mock, first_operation):
    client = bec_client_mock
    manager = client.device_manager
    updated_config = {**manager.devices.samx._config, "enabled": False}
    manager._allow_override = False
    first_load_started = threading.Event()
    finish_first_load = threading.Event()
    second_operation_started = threading.Event()
    overlapping_load = threading.Event()
    get_device_info = manager._get_device_info

    def load_device_info(name):
        if not first_load_started.is_set():
            first_load_started.set()
            assert finish_first_load.wait(timeout=5)
        elif not finish_first_load.is_set():
            overlapping_load.set()
        return get_device_info(name)

    def run_operation(operation):
        if operation != first_operation:
            second_operation_started.set()
        if operation == "restart":
            client._request_server_restart()
        else:
            manager._device_config_update_callback(
                MessageObject(
                    value=messages.DeviceConfigMessage(action="reload", config={}), topic=""
                )
            )

    second_operation = "notification" if first_operation == "restart" else "restart"
    with (
        mock.patch.object(client, "_update_existing_services"),
        mock.patch.object(client, "_services_info", {"SciHub": mock.Mock()}, create=True),
        mock.patch.object(client, "wait_for_service"),
        mock.patch.object(client, "_load_scans"),
        mock.patch("bec_lib.client.time.sleep"),
        mock.patch.object(manager, "_get_redis_device_config", return_value=[updated_config]),
        mock.patch.object(manager, "_get_device_info", side_effect=load_device_info),
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        first = executor.submit(run_operation, first_operation)
        try:
            assert first_load_started.wait(timeout=5)
            second = executor.submit(run_operation, second_operation)
            assert second_operation_started.wait(timeout=5)
            assert not overlapping_load.wait(timeout=0.1), "Device reloads overlapped"
        finally:
            finish_first_load.set()
        first.result(timeout=5)
        second.result(timeout=5)

    assert set(manager.devices) == {"samx"}
    assert not manager.devices.samx.enabled
    assert manager._allow_override is False
    with pytest.raises(AttributeError, match="Cannot overwrite 'read'"):
        manager.devices.samx.read = "overwritten"


def test_client_restart_endpoint():
    endpoint = MessageEndpoints.client_restart()

    assert endpoint.endpoint == "info/client_restart"
    assert endpoint.message_type is messages.ClientRestartMessage
    assert endpoint.message_op == MessageOp.SEND


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
