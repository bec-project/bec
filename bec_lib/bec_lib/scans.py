"""
This module contains the Scans class and related classes for defining and running scans in BEC
from the client side.
"""

from __future__ import annotations

import builtins
import time
import uuid
from collections.abc import Callable
from contextlib import ContextDecorator
from copy import deepcopy
from typing import TYPE_CHECKING

from toolz import partition
from typeguard import typechecked

from bec_lib.bec_errors import ScanAbortion
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.scan_report import ScanReport
from bec_lib.signature_serializer import dict_to_signature
from bec_lib.utils import scan_to_csv
from bec_lib.utils.import_utils import lazy_import

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages
    from bec_lib.client import BECClient
else:
    # TODO: put back normal import when Pydantic gets faster
    messages = lazy_import("bec_lib.messages")

logger = bec_logger.logger


class ScanObject:
    """ScanObject is a class for scans"""

    def __init__(self, scan_name: str, scan_info: dict, client: BECClient = None) -> None:
        self.scan_name = scan_name
        self.scan_info = scan_info
        self.client = client

        # run must be an anonymous function to allow for multiple doc strings
        # pylint: disable=unnecessary-lambda
        self.run = lambda *args, **kwargs: self._run(*args, **kwargs)

    def _run(
        self,
        *args,
        callback: Callable | None = None,
        async_callback: Callable | None = None,
        hide_report: bool = False,
        metadata: dict | None = None,
        monitored: list[str | DeviceBase] | None = None,
        file_suffix: str | None = None,
        file_directory: str | None = None,
        scan_queue: str | None = None,
        **kwargs,
    ) -> ScanReport:
        """
        Run the request with the given arguments.

        Args:
            *args: Arguments for the scan
            callback: Callback function
            async_callback: Asynchronous callback function
            hide_report: Hide the report
            metadata: Metadata dictionary
            monitored: List of monitored devices
            file_suffix: File suffix for the scan data
            file_directory: File directory for the scan data
            scan_queue: Scan queue name. If None, the default queue will be used.
            **kwargs: Keyword arguments

        Returns:
            ScanReport
        """
        if self.client.alarm_handler.alarms_stack:
            logger.info("The alarm stack is not empty but will be cleared now.")
            self.client.clear_all_alarms()
        scans = self.client.scans

        # pylint: disable=protected-access
        hide_report = hide_report or scans._hide_report

        user_metadata = deepcopy(self.client.metadata)

        sys_config = self.client.system_config.model_copy(deep=True)
        if file_suffix:
            sys_config.file_suffix = file_suffix
        if file_directory:
            sys_config.file_directory = file_directory

        if "sample_name" not in user_metadata:
            var = self.client.get_global_var("sample_name")
            if var is not None:
                user_metadata["sample_name"] = var

        if metadata is not None:
            user_metadata.update(metadata)

        if monitored is not None:
            if not isinstance(monitored, list):
                monitored = [monitored]
            for mon_device in monitored:
                if isinstance(mon_device, str):
                    mon_device = self.client.device_manager.devices.get(mon_device)
                    if not mon_device:
                        raise RuntimeError(
                            f"Specified monitored device {mon_device} does not exist in the current device configuration."
                        )
            kwargs["monitored"] = monitored

        sys_config = sys_config.model_dump()
        # pylint: disable=protected-access
        if scans._scan_group:
            sys_config["queue_group"] = scans._scan_group
        if scans._scan_def_id:
            sys_config["scan_def_id"] = scans._scan_def_id
        if scans._dataset_id_on_hold:
            sys_config["dataset_id_on_hold"] = scans._dataset_id_on_hold

        kwargs["user_metadata"] = user_metadata
        kwargs["system_config"] = sys_config

        scan_queue = scan_queue or self.client.queue.get_default_scan_queue()
        kwargs["scan_queue"] = scan_queue

        request = Scans.prepare_scan_request(self.scan_name, self.scan_info, *args, **kwargs)
        request_id = str(uuid.uuid4())

        # pylint: disable=unsupported-assignment-operation
        request.metadata["RID"] = request_id

        self._send_scan_request(request)

        report = ScanReport.from_request(request, client=self.client)
        report.request.callbacks.register_many("scan_segment", callback, sync=True)
        report.request.callbacks.register_many("scan_segment", async_callback, sync=False)

        if scans._scan_export and scans._scan_export.scans is not None:
            scans._scan_export.scans.append(report)

        if not hide_report and self.client._live_updates:
            self.client._live_updates.process_request(request, callback)

        self.client.callbacks.poll()

        return report

    def _start_register(self, request: messages.ScanQueueMessage) -> None:
        """Start a register for the given request"""
        self.client.device_manager.connector.register(
            [
                MessageEndpoints.device_readback(dev)
                for dev in request.content["parameter"]["args"].keys()
            ],
            threaded=False,
            cb=(lambda msg: msg),
        )

    def _send_scan_request(self, request: messages.ScanQueueMessage) -> None:
        """Send a scan request to the scan server"""
        self.client.device_manager.connector.send(MessageEndpoints.scan_queue_request(), request)


class Scans:
    """Scans is a class for available scans in BEC"""

    def __init__(self, parent):
        self.parent = parent
        self._available_scans = {}
        self._import_scans()
        self._scan_group = None
        self._scan_def_id = None
        self._interactive_scan = False
        self._scan_group_ctx = ScanGroup(parent=self)
        self._scan_def_ctx = ScanDef(parent=self)
        self._hide_report = None
        self._hide_report_ctx = HideReport(parent=self)
        self._dataset_id_on_hold = None
        self._dataset_id_on_hold_ctx = DatasetIdOnHold(parent=self)
        self._scan_export = None
        setattr(self.interactive_scan, "__doc__", InteractiveScan.__doc__)

    def _import_scans(self):
        """Import scans from the scan server"""
        available_scans = self.parent.connector.get(MessageEndpoints.available_scans())
        if available_scans is None:
            logger.warning("No scans available. Are redis and the BEC server running?")
            return
        for scan_name, scan_info in available_scans.resource.items():
            self._available_scans[scan_name] = ScanObject(scan_name, scan_info, client=self.parent)
            setattr(self, scan_name, self._available_scans[scan_name].run)
            setattr(getattr(self, scan_name), "__doc__", scan_info.get("doc"))
            setattr(
                getattr(self, scan_name),
                "__signature__",
                dict_to_signature(scan_info.get("signature")),
            )

    @staticmethod
    def get_arg_type(in_type: str):
        """translate type string into python type"""
        # pylint: disable=too-many-return-statements
        if in_type == "float":
            return (float, int)
        if in_type == "int":
            return int
        if in_type == "list":
            return list
        if in_type == "boolean":
            return bool
        if in_type == "str":
            return str
        if in_type == "dict":
            return dict
        if in_type == "device":
            return DeviceBase
        raise TypeError(f"Unknown type {in_type}")

    @staticmethod
    def prepare_scan_request(
        scan_name: str, scan_info: dict, *args, **kwargs
    ) -> messages.ScanQueueMessage:
        """Prepare scan request message with given scan arguments

        Args:
            scan_name (str): scan name (matching a scan name on the scan server)
            scan_info (dict): dictionary describing the scan (e.g. doc string, required kwargs etc.)

        Raises:
            TypeError: Raised if not all required keyword arguments have been specified.
            TypeError: Raised if the number of args do fit into the required bundling pattern.
            TypeError: Raised if an argument is not of the required type as specified in scan_info.

        Returns:
            messages.ScanQueueMessage: scan request message
        """
        scan_queue = kwargs.pop("scan_queue", "primary")
        # check that all required keyword arguments have been specified
        if not all(req_kwarg in kwargs for req_kwarg in scan_info.get("required_kwargs")):
            raise TypeError(
                f"{scan_info.get('doc')}\n Not all required keyword arguments have been"
                f" specified. The required arguments are: {scan_info.get('required_kwargs')}"
            )

        # check that all required arguments have been specified
        arg_input = list(scan_info.get("arg_input", {}).values())
        arg_bundle_size = scan_info.get("arg_bundle_size", {})
        bundle_size = arg_bundle_size.get("bundle")
        if len(arg_input) > 0:
            if len(args) % len(arg_input) != 0:
                raise TypeError(
                    f"{scan_info.get('doc')}\n {scan_name} takes multiples of"
                    f" {len(arg_input)} arguments ({len(args)} given)."
                )
            # check that all specified devices in args are different objects
            for arg in args:
                if not isinstance(arg, DeviceBase):
                    continue
                if args.count(arg) > 1:
                    raise TypeError(
                        f"{scan_info.get('doc')}\n All specified devices must be different"
                        f" objects."
                    )

            # check that all arguments are of the correct type
            for ii, arg in enumerate(args):
                if not isinstance(arg, Scans.get_arg_type(arg_input[ii % len(arg_input)])):
                    raise TypeError(
                        f"{scan_info.get('doc')}\n Argument {ii} must be of type"
                        f" {arg_input[ii%len(arg_input)]}, not {type(arg).__name__}."
                    )

        metadata = {}
        metadata.update(kwargs["system_config"])
        metadata["user_metadata"] = kwargs.pop("user_metadata", {})

        params = {"args": Scans._parameter_bundler(args, bundle_size), "kwargs": kwargs}
        # check the number of arg bundles against the number of required bundles
        if bundle_size:
            num_bundles = len(params["args"])
            min_bundles = arg_bundle_size.get("min")
            max_bundles = arg_bundle_size.get("max")
            if min_bundles and num_bundles < min_bundles:
                raise TypeError(
                    f"{scan_info.get('doc')}\n {scan_name} requires at least {min_bundles} bundles"
                    f" of arguments ({num_bundles} given)."
                )
            if max_bundles and num_bundles > max_bundles:
                raise TypeError(
                    f"{scan_info.get('doc')}\n {scan_name} requires at most {max_bundles} bundles"
                    f" of arguments ({num_bundles} given)."
                )
        return messages.ScanQueueMessage(
            scan_type=scan_name, parameter=params, queue=scan_queue, metadata=metadata
        )

    @staticmethod
    def _parameter_bundler(args: tuple, bundle_size: int) -> tuple | dict:
        """
        Bundle the arguments into the correct format for the scan server.
        If the bundle size is 0, return the arguments as is.
        If the bundle size is not 0, return a dictionary with the first argument as the key and the rest as the value.

        Args:
            args: arguments for the scan
            bundle_size: number of parameters per bundle

        Returns:
            tuple | dict: bundled arguments

        """
        if not bundle_size:
            return args
        params = {}
        for cmds in partition(bundle_size, args):
            params[cmds[0]] = list(cmds[1:])
        return params

    @property
    def scan_group(self):
        """Context manager / decorator for defining scan groups"""
        return self._scan_group_ctx

    @property
    def scan_def(self):
        """Context manager / decorator for defining new scans"""
        return self._scan_def_ctx

    @property
    def hide_report(self):
        """Context manager / decorator for hiding the report"""
        return self._hide_report_ctx

    @property
    def dataset_id_on_hold(self):
        """Context manager / decorator for setting the dataset id on hold"""
        return self._dataset_id_on_hold_ctx

    def scan_export(self, output_file: str):
        """Context manager / decorator for exporting scans"""
        return ScanExport(output_file)

    @property
    def interactive_scan(self):
        return InteractiveScan


class ScanGroup(ContextDecorator):
    """ScanGroup is a ContextDecorator for defining a scan group"""

    def __init__(self, parent: Scans = None) -> None:
        super().__init__()
        self.parent = parent

    def __enter__(self):
        group_id = str(uuid.uuid4())
        self.parent._scan_group = group_id
        return self

    def __exit__(self, *exc):
        self.parent.close_scan_group()
        self.parent._scan_group = None


class ScanDef(ContextDecorator):
    """ScanDef is a ContextDecorator for defining a new scan"""

    def __init__(self, parent: Scans = None) -> None:
        super().__init__()
        self.parent = parent

    def __enter__(self):
        if self.parent._scan_def_id is not None:
            raise ScanAbortion("Nested scan definitions currently not supported.")
        scan_def_id = str(uuid.uuid4())
        self.parent._scan_def_id = scan_def_id
        self.parent.open_scan_def()
        return self

    def __exit__(self, *exc):
        if exc[0] is None:
            self.parent.close_scan_def()
        self.parent._scan_def_id = None


class HideReport(ContextDecorator):
    """HideReport is a ContextDecorator for hiding the report"""

    def __init__(self, parent: Scans = None) -> None:
        super().__init__()
        self.parent = parent

    def __enter__(self):
        if self.parent._hide_report is None:
            self.parent._hide_report = True
        return self

    def __exit__(self, *exc):
        self.parent._hide_report = None


class DatasetIdOnHold(ContextDecorator):
    """DatasetIdOnHold is a ContextDecorator for setting the dataset id on hold"""

    def __init__(self, parent: Scans = None) -> None:
        super().__init__()
        self.parent = parent
        self._call_count = 0

    def __enter__(self):
        self._call_count += 1
        if self.parent._dataset_id_on_hold is None:
            self.parent._dataset_id_on_hold = True
        return self

    def __exit__(self, *exc):
        self._call_count -= 1
        if self._call_count:
            return
        self.parent._dataset_id_on_hold = None
        queue = self.parent.parent.queue
        queue.next_dataset_number += 1


class FileWriter:
    @typechecked
    def __init__(self, file_suffix: str = None, file_directory: str = None) -> None:
        """Context manager for updating metadata

        Args:
            fw_config (dict): Dictionary with metadata for the filewriter, can only have keys "file_suffix" and "file_directory"
        """
        self.client = _get_client()
        self.system_config = self.client.system_config
        self._orig_system_config = None
        self._orig_metadata = None
        self.file_suffix = file_suffix
        self.file_directory = file_directory

    def __enter__(self):
        """Enter the context manager"""
        self._orig_metadata = deepcopy(self.client.metadata)
        self._orig_system_config = self.system_config.model_copy(deep=True)
        self.system_config.file_suffix = self.file_suffix
        self.system_config.file_directory = self.file_directory
        return self

    def __exit__(self, *exc):
        """Exit the context manager"""
        self.client.metadata = self._orig_metadata
        self.system_config.file_suffix = self._orig_system_config.file_suffix
        self.system_config.file_directory = self._orig_system_config.file_directory


class Metadata:
    @typechecked
    def __init__(self, metadata: dict) -> None:
        """Context manager for updating metadata

        Args:
            metadata (dict): Metadata dictionary
        """
        self.client = _get_client()
        self._metadata = metadata
        self._orig_metadata = None

    def __enter__(self):
        """Enter the context manager"""
        self._orig_metadata = deepcopy(self.client.metadata)
        self.client.metadata.update(self._metadata)
        return self

    def __exit__(self, *exc):
        """Exit the context manager"""
        self.client.metadata = self._orig_metadata


class ScanExport:
    def __init__(self, output_file: str) -> None:
        """Context manager for exporting scans

        Args:
            output_file (str): Output file name
        """
        self.output_file = output_file
        self.client = None
        self.scans = None

    def _check_abort_on_ctrl_c(self):
        """Check if scan should be aborted on Ctrl-C"""
        # pylint: disable=protected-access
        if not self.client._service_config.abort_on_ctrl_c:
            raise RuntimeError(
                "ScanExport context manager can only be used if abort_on_ctrl_c is set to True"
            )

    def __enter__(self):
        self.scans = []
        self.client = _get_client()
        self.client.scans._scan_export = self
        self._check_abort_on_ctrl_c()
        return self

    def _export_to_csv(self):
        scan_to_csv(self.scans, self.output_file)

    def __exit__(self, *exc):
        try:
            for scan in self.scans:
                scan.wait()
        finally:
            try:
                self._export_to_csv()
                self.scans = None
            except Exception as exc:
                logger.warning(f"Could not export scans to csv file, due to exception {exc}")


class InteractiveScan(ContextDecorator):
    """
    InteractiveScan is a context manager for running interactive scans.
    Opening the interactive scan will stage all devices and perform the baseline reading.
    Exiting the context manager will unstage all devices.
    """

    def __init__(
        self, monitored: list[str] | list[DeviceBase], exp_time: float = 0, metadata=None, **kwargs
    ) -> None:
        """
        InteractiveScan is a context manager for running interactive scans.
        Opening the interactive scan will stage all devices and perform the baseline reading.
        Exiting the context manager will unstage all devices.

        Use "monitored" to specify the devices that should be monitored during the scan in addition
        to the default monitored devices.

        Args:
            scan_motors (list[str] | list[DeviceBase]): List of scan motors that should be monitored.
            exp_time (float): Exposure time for the scan. Default is 0.
            metadata (dict): Metadata dictionary. Default is None.
            kwargs: Keyword arguments that should be passed to the scan.

        Example:
            >>> with scans.interactive_scan(monitored=["samx"], exp_time=0.1, metadata={"sample": "A"}) as scan:
            >>>     for i in range(10):
            >>>         samx_status = dev.samx.set(i)
            >>>         samx_status.wait()
            >>>         scan.trigger()
            >>>         scan.read_all_monitored_devices()
        """

        self._client = None
        self._scans = None
        self._point_id = 0
        self._scan_kwargs = kwargs
        self._scan_kwargs["exp_time"] = exp_time
        self._scan_kwargs["metadata"] = metadata

        self._input_monitored_devices = monitored

    def _update_monitored_devices(self):
        """
        Update the monitored devices based on the scan_motors or monitored_devices arguments.
        """
        monitored_devices = self._input_monitored_devices
        if monitored_devices is not None:
            if not isinstance(monitored_devices, list):
                monitored_devices = [monitored_devices]
            self._scan_kwargs["monitored"] = monitored_devices

        self._scan_kwargs["monitored"] = [
            device.name if isinstance(device, DeviceBase) else device
            for device in self._scan_kwargs["monitored"]
        ]

    def __enter__(self):
        self._client = _get_client()
        self._scans = self._client.scans
        if self._scans._scan_def_id is not None:
            raise ScanAbortion("Cannot run interactive scans within a scan definition.")
        if self._scans._interactive_scan:
            raise ScanAbortion("Cannot run interactive scans within another interactive scan.")
        self._update_monitored_devices()
        self._scans._interactive_scan = True
        self._scans._scan_def_id = str(uuid.uuid4())
        status = self._scans._open_interactive_scan(hide_report=True, **self._scan_kwargs)
        self.status = status
        return self

    def __exit__(self, *exc):
        self._scans._close_interactive_scan(hide_report=True, **self._scan_kwargs)
        self._scans._scan_def_id = None
        self._scans._interactive_scan = False
        if not exc[0]:
            self.status.wait()

    def trigger(self):
        """
        Trigger all enabled devices that have softwareTrigger set to True
        """
        # self._client.alarm_handler.check_for_alarms()
        # pylint: disable=protected-access
        self._scans._interactive_trigger(hide_report=True, **self._scan_kwargs)

    def read_monitored_devices(self, devices: list[str | DeviceBase] = None, wait: bool = False):
        """
        Read all monitored devices. If devices is specified, only read the specified devices.
        Please note that in an interactive scan, scan motors are not automatically added to the monitored devices,
        so they need to be specified explicitly.

        Args:
            devices(list[str | DeviceBase]): List of devices that should be added to the monitored devices

        """
        if devices is None:
            devices = []
        if not isinstance(devices, list):
            devices = [devices]
        devices = [device.name if isinstance(device, DeviceBase) else device for device in devices]
        # pylint: disable=protected-access
        self._scans._interactive_read_monitored(
            devices=devices, point_id=self._point_id, hide_report=True, **self._scan_kwargs
        )
        self._point_id += 1
        if wait:
            self.wait()

    def wait(self):
        """
        Wait for the all readings to arrive at the client
        """
        while len(self.status.scan.data) != self._point_id:
            self._client.alarm_handler.raise_alarms()
            time.sleep(0.1)


# this is a workaround to make the InteractiveScan doc string available to the interactive_scan property
Scans.interactive_scan.__doc__ = InteractiveScan.__init__.__doc__


def _get_client():
    """
    Get the BEC client
    """
    return builtins.__dict__["bec"]
