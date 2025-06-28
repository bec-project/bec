"""Module to test file_utils.py"""

# pylint: skip-file
import os
from unittest import mock

import pytest

from bec_lib.bec_errors import ServiceConfigError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.file_utils import (
    DeviceConfigWriter,
    FileWriter,
    FileWriterError,
    LogWriter,
    compile_file_components,
    get_full_path,
)
from bec_lib.messages import ScanStatusMessage
from bec_lib.tests.utils import ConnectorMock


@pytest.fixture(scope="function")
def mock_connector():
    """Mock connector fixture"""
    connector = ConnectorMock("")
    yield connector


@pytest.fixture(scope="function")
def file_writer(mock_connector):
    """File writer fixture"""
    with mock.patch("os.makedirs") as mock_make_dirs:
        yield FileWriter(service_config={"base_path": "/tmp"}, connector=mock_connector)


@pytest.fixture(scope="function")
def scan_msg():
    """Scan message fixture"""
    yield ScanStatusMessage(
        scan_id="1111",
        scan_parameters={"system_config": {"file_directory": None, "file_suffix": None}},
        info={"scan_number": 5, "file_components": ("S00000-00999/S00005/S00005", "h5")},
        status="closed",
    )


def test_device_config_writer():
    """Device config writer fixture and ServiceConfigParser class"""
    with mock.patch("os.makedirs") as mock_make_dirs:
        with mock.patch("os.chmod") as mock_chmod:
            dcw = DeviceConfigWriter(service_config={"base_path": "/tmp"})
            assert mock_make_dirs.call_count == 1
            assert dcw.directory == "/tmp/device_configs"
            assert dcw.get_recovery_directory() == "/tmp/device_configs/recovery_configs"
            mock_chmod.assert_called_once_with("/tmp/device_configs", int("0o771", 8))


def test_log_writer():
    """Device config writer fixture and ServiceConfigParser class"""
    with mock.patch("os.makedirs") as mock_make_dirs:
        with mock.patch("os.chmod") as mock_chmod:
            lw = LogWriter(service_config={"base_path": "/tmp/logs"})
            assert mock_make_dirs.call_count == 1
            assert lw.directory == "/tmp/logs"
            mock_chmod.assert_called_once_with("/tmp/logs", int("0o771", 8))


@pytest.mark.parametrize(
    "service_config, connector, raises",
    [
        ({"base_path": "/tmp"}, ConnectorMock(""), False),
        ({"base_path": "/tmp"}, None, False),
        ({"base_path": None}, ConnectorMock(""), True),
    ],
)
def test_file_writer_init(service_config, connector, raises):
    """Test file writer init"""
    if raises:
        with pytest.raises(ServiceConfigError):
            with mock.patch("os.makedirs") as mock_make_dirs:
                fw = FileWriter(service_config=service_config, connector=connector)
    else:
        with mock.patch("os.makedirs") as mock_make_dirs:
            fw = FileWriter(service_config=service_config, connector=ConnectorMock(""))
            if service_config.get("base_path"):
                assert fw._base_path == service_config.get("base_path")
                assert fw._configured is True
            else:
                assert fw._base_path == "."
                assert fw._configured is False


def test_get_scan_msg(file_writer, scan_msg):
    """Test get_scan_msg method"""
    file_writer.connector._get_buffer = {MessageEndpoints.scan_status().endpoint: scan_msg}
    rtr = file_writer.get_scan_msg()
    assert rtr == scan_msg


@pytest.mark.parametrize(
    "scan_number,bundle,lead,user_suffix,ref_path",
    [
        (10, 1000, None, None, "S0000-0999/S0010"),
        (2001, 1000, None, "", "S2000-2999/S2001"),
        (20, 50, 4, "sampleA", "S0000-0049/S0020_sampleA"),
        (20, 50, 5, "sampleB", "S00000-00049/S00020_sampleB"),
        (1200, 1000, 5, "", "S01000-01999/S01200"),
    ],
)
def test_file_writer_get_scan_dir(scan_number, bundle, lead, user_suffix, ref_path, file_writer):

    dir_path = file_writer.get_scan_directory(
        scan_bundle=bundle, scan_number=scan_number, leading_zeros=lead, user_suffix=user_suffix
    )
    assert dir_path == ref_path


def test_compile_full_filename_not_configured(file_writer, scan_msg):
    file_writer.get_scan_msg = mock.MagicMock(return_value=scan_msg)
    file_writer._configured = False
    suffix = "test"
    path = os.path.join(file_writer._base_path, "data", f"S00000_default_{suffix}.h5")
    fp = file_writer.compile_full_filename(suffix=suffix)
    assert fp == path


def test_compile_full_filename(file_writer, scan_msg):
    suffix = "test"
    file_type = ".csv"
    # case 1
    with mock.patch.object(file_writer, "get_scan_msg", return_value=None):
        return_value = file_writer.compile_full_filename(suffix=suffix)
        assert return_value == ""
    # case 2
    with mock.patch.object(file_writer, "get_scan_msg", return_value=scan_msg):
        return_value = file_writer.compile_full_filename(suffix=suffix)
        scannr = scan_msg.info.get("scan_number")
        expected = os.path.join(
            file_writer._base_path,
            "data",
            f"{scan_msg.info['file_components'][0]}_{suffix}.{scan_msg.info['file_components'][1]}",
        )
        assert return_value == expected

    # case 3
    scan_msg.scan_parameters.pop("system_config")
    with mock.patch.object(file_writer, "get_scan_msg", return_value=scan_msg):
        scannr = scan_msg.info.get("scan_number")
        scan_dir = f"S00000-00999/S{scannr:0{file_writer._leading_zeros}d}"
        expected = os.path.join(
            file_writer._base_path,
            "data",
            scan_dir,
            f"S{scannr:0{file_writer._leading_zeros}d}_{suffix}.h5",
        )
        with mock.patch.object(file_writer, "get_scan_directory", return_value=scan_dir):
            return_value = file_writer.compile_full_filename(suffix=suffix)
            assert return_value == expected


def test_compile_file_components():
    """Test compile_file_components method.
    Case 1: test default values
    Case 2: test with file_directory and file_suffix (file suffix will not play a role)
    Case 3: test with file suffix, no file directory
    """
    basepath = "/tmp"
    scan_nr = 10
    file_path, extension = compile_file_components(basepath, scan_nr)
    assert extension == "h5"
    assert file_path == "/tmp/S00000-00999/S00010/S00010"
    scan_bundle = 1000
    leading_zeros = 5
    file_directory = "test_dir"
    user_suffix = "SampleA"
    file_path, extension = compile_file_components(
        base_path=basepath,
        scan_nr=scan_nr,
        scan_bundle=scan_bundle,
        leading_zeros=leading_zeros,
        file_directory=file_directory,
        user_suffix=user_suffix,
    )
    assert extension == "h5"
    assert file_path == "/tmp/test_dir/S00010"
    file_path, extension = compile_file_components(
        base_path=basepath,
        scan_nr=scan_nr,
        scan_bundle=scan_bundle,
        leading_zeros=leading_zeros,
        user_suffix=user_suffix,
    )
    assert extension == "h5"
    assert file_path == "/tmp/S00000-00999/S00010_SampleA/S00010"


@pytest.mark.parametrize(
    "scan_info",
    [
        (
            ScanStatusMessage(
                scan_id="1111",
                scan_parameters={"system_config": {"file_directory": None, "file_suffix": None}},
                info={"scan_number": 5, "file_components": ("S00000-00999/S00005/S00005", "h5")},
                status="closed",
            )
        ),
        (
            ScanStatusMessage(
                scan_id="1111",
                scan_parameters={
                    "system_config": {"file_directory": "/my_dir", "file_suffix": None}
                },
                info={"scan_number": 5, "file_components": ("/my_dir/S00005", "h5")},
                status="closed",
            )
        ),
        (
            ScanStatusMessage(
                scan_id="1111",
                scan_parameters={
                    "system_config": {"file_directory": None, "file_suffix": "sampleA"}
                },
                info={
                    "scan_number": 5,
                    "file_components": ("S00000-00999/S00005_sampleA/S00005", "h5"),
                },
                status="closed",
            )
        ),
    ],
)
@pytest.mark.parametrize(
    "name, expect_error",
    [
        ("valid_name", False),  # Valid case
        ("valid-name", False),  # Valid case with '-'
        ("valid_name123", False),  # Valid case with '_'
        ("", True),  # Empty string should raise an error
        ("invalid name", True),  # Contains space
        ("name$", True),  # Contains non-alphanumeric character
        ("名前", True),  # Non-ASCII characters
        ("1234", False),  # Numeric values are allowed
        ("_hidden", False),  # Leading underscore is allowed
        ("-dashprefix", False),  # Leading dash is allowed
    ],
)
def test_get_full_path(scan_info, name, expect_error):
    file_components = scan_info.info.get("file_components")
    suffix = scan_info.scan_parameters.get("system_config").get("file_suffix")

    if expect_error:
        with pytest.raises(FileWriterError):
            get_full_path(scan_info, name, create_dir=False)
    else:
        if suffix is None:
            full_path = f"{file_components[0]}_{name}.{file_components[1]}"
        else:
            full_path = f"{file_components[0]}_{name}_{suffix}.{file_components[1]}"
        ret = get_full_path(scan_info, name, create_dir=False)
        assert ret == full_path
