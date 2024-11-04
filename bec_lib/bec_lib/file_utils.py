""" Module for file utilities."""

from __future__ import annotations

import os
import warnings
from typing import TYPE_CHECKING

from bec_lib.bec_errors import ServiceConfigError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.utils.import_utils import lazy_import_from

# TODO: put back normal import when Pydantic gets faster
ScanStatusMessage = lazy_import_from("bec_lib.messages", ("ScanStatusMessage",))

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector


class ServiceConfigParser:
    """Service Config Parser"""

    def __init__(self, service_config: dict = None) -> None:
        """Initialize the service config parser.

        Args:
            service_config (dict): Service config from BEC startup.
        """
        self._service_config = service_config

    @property
    def service_config(self) -> dict:
        """Get the service config."""
        return self._service_config

    def get_base_path(self) -> str:
        """Get the base path from the config."""
        if not self.service_config:
            raise ServiceConfigError("Service config must contain a file writer definition.")
        if not self.service_config.get("base_path"):
            raise ServiceConfigError("File writer config must define a base path.")
        return os.path.abspath(os.path.expanduser(self.service_config.get("base_path")))

    def create_directory(self, directory: str, mode=0o771) -> None:
        """Create a directory if it does not exist."""
        directory_existed = os.path.exists(directory)
        os.makedirs(directory, mode=mode, exist_ok=True)
        if directory_existed is False:
            os.chmod(directory, mode)


class LogWriter:
    """Log writer class"""

    def __init__(self, service_config: dict) -> None:
        """
        Initialize the log writer class.

        Args:
            service_config (dict): Log writer service config. Must at least contain a base_path.
        """
        self.service_config_parser = ServiceConfigParser(service_config)
        self._base_path = self.service_config_parser.get_base_path()
        self._directory = os.path.join(self._base_path, "logs")
        self.create_directory(self.directory)

    def create_directory(self, fname: str = None) -> None:
        """Create the log directory."""
        self.service_config_parser.create_directory(fname)

    @property
    def directory(self) -> str:
        """Get the log directory.

        Returns:
            str: String representation of log directory
        """
        return self._directory


class DeviceConfigWriter:
    """Device config writer class"""

    def __init__(self, service_config: dict) -> None:
        """
        Initialize the device config writer class.
        At the moment, this uses the same base_path as the log writer

        Args:
            service_config (dict): Device config writer service config. Must at least contain a base_path.
        """
        self.service_config_parser = ServiceConfigParser(service_config)
        self._base_path = self.service_config_parser.get_base_path()
        self._directory = os.path.join(self._base_path, "device_configs")
        self.create_directory(fname=self.directory)

    def create_directory(self, fname: str = None) -> None:
        """Create the device config directory."""
        self.service_config_parser.create_directory(fname)

    @property
    def directory(self) -> str:
        """Get the device config directory.

        Returns:
            str: String representation of device config directory
        """
        return self._directory

    def get_recovery_directory(self) -> str:
        """
        Compile the recovery config directory.
        """
        return os.path.join(self.directory, "recovery_configs")


class FileWriterError(Exception):
    """Exception for errors in the file writer"""


class FileWriter:
    """FileWriter for creating file paths and directories for services and devices."""

    def __init__(
        self,
        *args,
        service_config: dict = None,
        connector: RedisConnector = None,
        scan_bundle: int = 1000,
        leading_zeros: int = 5,
        **kwargs,
    ) -> None:
        """
        Initialize the file writer mixin class.
        If no RedisConnector is provided, the class will not be able to communicate with the REDIS server.

        In this case, it will fall back to a default base path in the current directory.

        Args:
            service_config (dict): File writer service config. Must at least contain base_path.
            connector (Redisconnector, optional): Connector class to use. Defaults to RedisConnector.
            scan_bundle (int, optional): Scan bundle size. Defaults to 1000.
            leading_zeros (int, optional): Number of leading zeros. Defaults to 5.

        """
        self.service_config = service_config
        self._scan_bundle = scan_bundle
        self._leading_zeros = leading_zeros
        self._kwargs = kwargs
        self._base_path = "."  # default to current directory
        self.connector = connector
        self.service_config_parser = None
        self._configured = False
        self._initialized = True
        if self.connector:
            self.configure_file_writer()
            self._configured = True

    def configure_file_writer(self):
        """Configure the file writer mixin class in case service_config is provided"""
        self.service_config_parser = ServiceConfigParser(self.service_config)
        self._base_path = self.service_config_parser.get_base_path()

    @property
    def scan_bundle(self) -> int:
        """Get the scan bundle size."""
        return self._scan_bundle

    @property
    def leading_zeros(self) -> int:
        """Get the number of leading zeros."""
        return self._leading_zeros

    def get_scan_directory(
        self, scan_number: int, scan_bundle: int, leading_zeros: int = None, user_suffix: str = None
    ) -> str:
        """
        Get the scan directory for a given scan number and scan bundle.

        Args:
            scan_bundle (int): Scan bundle size
            scan_number (int): Scan number
            leading_zeros (int, optional): Number of leading zeros. Defaults to None.
            user_suffix (str, optional): User defined suffix. Defaults to None.

        Returns:
            str: Scan directory

        Examples:
            >>> get_scan_directory(1234, 1000, 5)
            'S01000-01999/S01234'
            >>> get_scan_directory(1234, 1000, 5, 'sampleA')
            'S01000-01999/S01234_sampleA'
        """
        if leading_zeros is None:
            leading_zeros = len(str(scan_bundle))
        floor_dir = scan_number // scan_bundle * scan_bundle
        rtr = f"S{floor_dir:0{leading_zeros}d}-{floor_dir+scan_bundle-1:0{leading_zeros}d}/S{scan_number:0{leading_zeros}d}"
        if user_suffix:
            rtr += f"_{user_suffix}"
        return rtr

    def get_scan_msg(self):
        """Get the scan message for the next scan"""
        msg = self.connector.get(MessageEndpoints.scan_status())
        if not isinstance(msg, ScanStatusMessage):
            return None
        return msg

    def compile_full_filename(self, suffix: str, create_dir: bool = True) -> str:
        """
        Compile a full filename for a given scan number and suffix.

        This method should only be called after a scan has been opened,
        i.e. preferable in stage and not during __init__.

        The method will use the last scan message received in REDIS,
        and will return an empty string if no scan message is available.

        Args:
            suffix (str): Filename suffix including extension. We allow alphanumeric, ascii characters - and _.
            file_type (str) : Optional, will default to h5.
            create_dir (bool, optional): Create the directory if it does not exist. Defaults to True.

        Returns:
            str: Full filename
        """
        # to check if suffix is alphanumeric and ascii, however we allow in addition - and _
        check_suffix = suffix.replace("_", "").replace("-", "")
        if not check_suffix.isalnum() or not check_suffix.isascii():
            raise FileWriterError(
                f"Can't use suffix {suffix}; formatting is alphanumeric:{suffix.isalnum()} and ascii {suffix.isascii()}"
            )
        if self._configured:
            scan_msg = self.get_scan_msg()
            if not scan_msg:
                warnings.warn("No scan message available.")
                return ""
            scannr = scan_msg.content["info"]["scan_number"]
            user_suffix = scan_msg.info.get("file_suffix")
            scan_dir = scan_msg.info.get("file_directory")
            if not scan_dir:
                scan_dir = self.get_scan_directory(
                    scannr, self.scan_bundle, self.leading_zeros, user_suffix=user_suffix
                )
            if user_suffix:
                suffix += f"_{user_suffix}"
            full_file = os.path.join(
                self._base_path, "data", scan_dir, f"S{scannr:0{self._leading_zeros}d}_{suffix}.h5"
            )
        else:
            full_file = os.path.join(self._base_path, "data", f"S00000_default_{suffix}.h5")
            warnings.warn(f"No service config provided, using default base path {full_file}.")
        if create_dir:
            os.makedirs(os.path.dirname(full_file), exist_ok=True)
        return full_file
