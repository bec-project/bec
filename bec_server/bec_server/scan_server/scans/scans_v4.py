"""
Module for handling scans for v4 of BEC. In contrast to previous implementations, the scan logic does not rely on generators
executed on the worker but instead uses the RedisConnector to send commands directly to the devices.
"""

from __future__ import annotations

import enum
import threading
from typing import Annotated

import numpy as np
import pint
from pydantic import BaseModel, ConfigDict, Field
from toolz import partition

from bec_lib.devicemanager import DeviceManagerBase as DeviceManager
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.scans.scan_actions import ScanActions
from bec_server.scan_server.scans.scan_components import ScanComponents

Units = pint.UnitRegistry()


class ScanType(str, enum.Enum):
    HARDWARE_TRIGGERED = "hardware_triggered"
    SOFTWARE_TRIGGERED = "software_triggered"


def bundle_args(args: tuple, bundle_size: int) -> dict:
    """
    Bundle the given arguments into bundles of the given size.

    Args:
        args (tuple): arguments to bundle
        bundle_size (int): size of the bundles

    Returns:
        dict: bundled arguments

    """
    params = {}
    for cmds in partition(bundle_size, args):
        params[cmds[0]] = list(cmds[1:])
    return params


class ScanInfo(BaseModel):

    # General scan information
    scan_name: Annotated[str, Field(description="Name of the scan type, e.g. 'grid_scan'")]
    scan_id: Annotated[str, Field(description="Unique identifier for the scan")]
    scan_type: Annotated[
        ScanType | None,
        Field(
            None, description="Type of the scan, e.g. 'software_triggered' or 'hardware_triggered'"
        ),
    ]
    scan_number: Annotated[int | None, Field(description="Scan number, if applicable")] = None
    dataset_number: Annotated[int | None, Field(description="Dataset number, if applicable")] = None

    # Scan parameters
    num_points: Annotated[int, Field(description="Number of points in the scan.")] = 0
    positions: Annotated[
        np.ndarray | None,
        Field(description="Positions for the scan, shape (num_points, num_motors)"),
    ] = None
    exp_time: Annotated[float, Field(description="Exposure time for the scan", ge=0.0)] = 0.0
    frames_per_trigger: Annotated[int, Field(description="Number of frames per trigger", ge=1)] = 1
    settling_time: Annotated[
        float, Field(description="Settling time before the software trigger", ge=0.0)
    ] = 0.0
    settling_time_after_trigger: Annotated[
        float, Field(description="Settling time after the software trigger", ge=0.0)
    ] = 0.0
    readout_time: Annotated[float, Field(description="Readout time after the trigger", ge=0.0)] = (
        0.0
    )
    burst_at_each_point: Annotated[
        int, Field(description="Number of bursts at each point", ge=1)
    ] = 1
    relative: Annotated[
        bool, Field(description="Whether the positions are relative or absolute")
    ] = False
    run_on_exception_hook: Annotated[
        bool, Field(description="Whether to run the on_exception hook if the scan is interrupted")
    ] = True

    request_inputs: Annotated[dict, Field(description="Request inputs")] = {}
    readout_priority_modification: Annotated[
        dict, Field(description="Readout priority modification")
    ] = {"baseline": [], "monitored": [], "on_request": [], "async": []}
    scan_report_instructions: Annotated[
        list[dict], Field(description="List of scan report instructions")
    ] = []
    scan_report_devices: Annotated[
        list[str], Field(description="List of devices to report during the scan")
    ] = []
    monitor_sync: Annotated[
        str | None,
        Field(description="Monitor synchronization mode for fly scans"),  # Will be removed!
    ] = None
    additional_scan_parameters: Annotated[dict, Field(description="Additional scan parameters")] = (
        {}
    )
    user_metadata: Annotated[dict, Field(description="User-provided metadata for the scan")] = {}
    system_config: Annotated[dict, Field(description="System configuration for the scan")] = {}
    scan_queue: Annotated[str, Field(description="Name of the queue the scan belongs to")] = (
        "primary"
    )
    metadata: Annotated[dict, Field(description="Additional metadata for the scan")] = {}

    # progress tracking
    num_monitored_readouts: Annotated[
        int,
        Field(
            description="Number of performed readouts of monitored devices. For a step scan, this is equal to num_points * burst_at_each_point."
        ),
    ] = 0

    def __str__(self) -> str:
        data = self.model_dump(mode="python")
        positions = self.positions
        if isinstance(positions, np.ndarray):
            data["positions"] = np.array2string(positions, threshold=8, edgeitems=2, precision=4)
        return f"{self.__class__.__name__}({data})"

    __repr__ = __str__

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)


class ScanBase:
    scan_type = ScanType.SOFTWARE_TRIGGERED
    scan_name = "_v4_base_scan"
    required_kwargs = []
    arg_input = {}
    arg_bundle_size = {"bundle": len(arg_input), "min": None, "max": None}
    is_scan = True

    def __init__(
        self,
        scan_id: str,
        redis_connector: RedisConnector,
        device_manager: DeviceManager,
        instruction_handler: InstructionHandler,
        request_inputs: dict,
        system_config: dict,
        user_metadata: dict | None = None,
        metadata: dict | None = None,
        scan_queue: str | None = None,
        run_on_exception_hook: bool | None = None,
        additional_scan_parameters: dict | None = None,
    ):
        """Base class for all scans."""
        self.redis_connector = redis_connector
        self.device_manager = device_manager
        self._instruction_handler = instruction_handler
        self.dev = self.device_manager.devices
        self._shutdown_event = threading.Event()
        self.actions = ScanActions(scan=self)
        self.components = ScanComponents(scan=self)

        optional_kwargs = {}
        for kwarg in [
            "metadata",
            "user_metadata",
            "scan_queue",
            "run_on_exception_hook",
            "additional_scan_parameters",
        ]:
            data = locals()[kwarg]
            if data is not None:
                optional_kwargs[kwarg] = data
        self.scan_info = ScanInfo(
            scan_name=self.scan_name, scan_id=scan_id, scan_type=self.scan_type, **optional_kwargs
        )
        self.scan_info.request_inputs = request_inputs
        self.scan_info.system_config = system_config
        self._baseline_readout_status = None
        self._premove_motor_status = None
        self.positions = np.array([])
        self.start_positions = []

    def update_scan_info(
        self,
        num_points: int | None = None,
        num_monitored_readouts: int | None = None,
        positions: np.ndarray | None = None,
        exp_time: float | None = None,
        frames_per_trigger: int | None = None,
        settling_time: float | None = None,
        settling_time_after_trigger: float | None = None,
        burst_at_each_point: int | None = None,
        relative: bool | None = None,
        run_on_exception_hook: bool | None = None,
        **kwargs,
    ):
        """
        Update the scan info with the given keyword arguments.
        If the scan info model has an attribute with the same name as the keyword argument,
        it will be updated. Otherwise, the keyword argument will be added to the additional_scan_parameters dictionary.
        This allows for flexible scan info management, where standard parameters can be defined as attributes of the
        ScanInfo model, and any additional parameters can be stored in the additional_scan_parameters dictionary.

        Args:
            num_points (int, optional): Number of points in the scan. Defaults to None.
            num_monitored_readouts (int, optional): Number of monitored readouts that will be collected during the scan. Defaults to None.
            positions (np.ndarray, optional): Positions for the scan, shape (num_points, num_motors). Defaults to None.
            exp_time (float, optional): Exposure time for the scan. Defaults to None.
            frames_per_trigger (int, optional): Number of frames per trigger. Defaults to None.
            settling_time (float, optional): Settling time before the software trigger. Defaults to None.
            settling_time_after_trigger (float, optional): Settling time after the software trigger. Defaults to None.
            burst_at_each_point (int, optional): Number of bursts at each point. Defaults to None.
            relative (bool, optional): Whether the positions are relative or absolute. Defaults to None.
            run_on_exception_hook (bool, optional): Whether to run the on_exception hook if the scan is interrupted. Defaults to None.
            **kwargs: Keyword arguments to update the scan info with.
        """
        for attr_name, value in [
            ("num_points", num_points),
            ("num_monitored_readouts", num_monitored_readouts),
            ("positions", positions),
            ("exp_time", exp_time),
            ("frames_per_trigger", frames_per_trigger),
            ("settling_time", settling_time),
            ("settling_time_after_trigger", settling_time_after_trigger),
            ("burst_at_each_point", burst_at_each_point),
            ("relative", relative),
            ("run_on_exception_hook", run_on_exception_hook),
        ]:
            if value is not None:
                setattr(self.scan_info, attr_name, value)
        for key, value in kwargs.items():
            if hasattr(self.scan_info, key):
                setattr(self.scan_info, key, value)
            else:
                self.scan_info.additional_scan_parameters[key] = value
