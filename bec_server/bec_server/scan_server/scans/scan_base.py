"""
Module for handling scans for v4 of BEC. In contrast to previous implementations, the scan logic does not rely on generators
executed on the worker but instead uses the RedisConnector to send commands directly to the devices.
"""

from __future__ import annotations

import enum
import threading
from abc import ABC, abstractmethod
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Annotated, Callable, Literal, Type

import numpy as np
import pint
from pydantic import BaseModel, ConfigDict, Field, field_validator
from toolz import partition

from bec_lib.device import DeviceBase
from bec_lib.messages import sequence_id_for_acquisition
from bec_lib.devicemanager import DeviceManagerBase as DeviceManager
from bec_lib.redis_connector import RedisConnector
from bec_lib.serialization import msgpack
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.scans.scan_actions import ScanActions
from bec_server.scan_server.scans.scan_components import ScanComponents
from bec_server.scan_server.scans.scan_modifier import ScanModifier, get_scan_hooks_impl, scan_hook

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


SubscanPhase = Literal["before", "after"]


@dataclass(frozen=True)
class SubscanSpec:
    """A scan class and its user inputs, registered for later execution."""

    scan_class: type[ScanBase]
    args: tuple
    kwargs: dict
    when: SubscanPhase


class ScanEntryPlan(BaseModel):
    """The announced identity and inputs of one acquisition in a parent scan."""

    model_config = ConfigDict(frozen=True)

    entry_index: int
    scan_id: str
    role: Literal["before", "main", "after"]
    scan_name: str
    request_inputs: dict


class ScanInfo(BaseModel):

    # General scan information
    scan_name: Annotated[str, Field(description="Name of the scan type, e.g. 'grid_scan'")]
    scan_id: Annotated[str | None, Field(description="Unique identifier for the scan")] = None
    scan_type: Annotated[
        ScanType | None,
        Field(
            None, description="Type of the scan, e.g. 'software_triggered' or 'hardware_triggered'"
        ),
    ]
    scan_number: Annotated[int | None, Field(description="Scan number, if applicable")] = None
    dataset_number: Annotated[int | None, Field(description="Dataset number, if applicable")] = None

    sequence_index: int = 0
    total_sequences: int = 1
    entry_sequence: tuple[ScanEntryPlan, ...] | None = None

    @property
    def sequence_id(self) -> str | None:
        """Return the shared key used for queue and file bookkeeping."""
        if self.total_sequences > 1:
            return sequence_id_for_acquisition(self.scan_id, self.sequence_index) or self.scan_id
        return self.scan_id

    # Preview metadata uses the same mapping as the advertised scan definition.
    position_preview: str | None = None
    position_preview_parameter_mapping: dict = Field(default_factory=dict)

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
    baseline_readout_requested: Annotated[
        bool, Field(description="Whether a read of nonempty baseline devices was requested.")
    ] = False
    num_monitored_readouts: Annotated[
        int,
        Field(
            description="Number of performed readouts of monitored devices. For a step scan, this is equal to num_points * burst_at_each_point."
        ),
    ] = 0

    file_components: Annotated[
        tuple[str, str],
        Field(
            description="File components for the scan containing the full path and the file suffix."
        ),
    ] = ("", "")

    def __str__(self) -> str:
        data = self.model_dump(mode="python")
        positions = self.positions
        if isinstance(positions, np.ndarray):
            data["positions"] = np.array2string(positions, threshold=8, edgeitems=2, precision=4)
        return f"{self.__class__.__name__}({data})"

    __repr__ = __str__

    @field_validator("scan_report_devices", mode="before")
    @classmethod
    def _serialize_scan_report_devices(cls, value: object) -> object:
        """
        Convert scan report devices to device names.

        Args:
            value (object): List of device names or ``DeviceBase`` instances.

        Returns:
            object: List with ``DeviceBase`` instances replaced by their names.
        """
        if value is None:
            return value
        return [device.name if isinstance(device, DeviceBase) else device for device in value]

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)


class ScanBase(ABC):
    scan_type = ScanType.SOFTWARE_TRIGGERED
    scan_name = "base_scan"
    arg_input = {}
    arg_bundle_size = {"bundle": len(arg_input), "min": None, "max": None}
    is_scan = True
    is_internal = False
    position_preview: str | None = None
    position_preview_parameter_mapping: dict = {}

    def __init__(
        self,
        scan_id: str | None,
        redis_connector: RedisConnector,
        device_manager: DeviceManager,
        instruction_handler: InstructionHandler,
        request_inputs: dict,
        system_config: dict,
        monitored: list[str] | None = None,
        on_request: list[str] | None = None,
        scan_modifier: Type[ScanModifier] | None = None,
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
        self._entry_scans: tuple[ScanBase, ...] = ()
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
            scan_name=self.scan_name,
            scan_id=scan_id,
            scan_type=self.scan_type,
            position_preview=self.position_preview,
            position_preview_parameter_mapping=self.position_preview_parameter_mapping,
            **optional_kwargs,
        )
        self.update_scan_info(monitored=monitored, on_request=on_request)

        self.scan_info.request_inputs = request_inputs
        self.scan_info.system_config = system_config
        self._baseline_readout_status = None
        self._premove_motor_status = None
        self.positions = np.array([])
        self.start_positions = []
        self._subscan_registration: list[SubscanSpec] | None = None
        self._subscans_configured = False
        self._scan_original_hooks = self._collect_original_scan_hooks()
        self._scan_modifier_hooks = (
            get_scan_hooks_impl(scan_modifier) if scan_modifier is not None else {}
        )
        self._scan_modifier = scan_modifier(scan=self) if scan_modifier is not None else None

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
        scan_report_devices: Sequence[str | DeviceBase] | None = None,
        monitored: Sequence[str | DeviceBase] | None = None,
        on_request: Sequence[str | DeviceBase] | None = None,
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
            scan_report_devices (Sequence[str | DeviceBase], optional): Devices to report
                during the scan. Device objects are stored by name. Defaults to None.
            monitored (Sequence[str | DeviceBase], optional): Devices to be put on the readout priority `monitored`. Defaults to None.
            on_request (Sequence[str | DeviceBase], optional): Devices to be put on the readout priority `on_request`. Defaults to None.
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
            ("scan_report_devices", scan_report_devices),
        ]:
            if value is not None:
                setattr(self.scan_info, attr_name, value)
        for key, value in kwargs.items():
            if hasattr(self.scan_info, key):
                setattr(self.scan_info, key, value)
            else:
                self.scan_info.additional_scan_parameters[key] = value
        if monitored is not None:
            self.scan_info.readout_priority_modification["monitored"] = [
                device.name if isinstance(device, DeviceBase) else device for device in monitored
            ]
        if on_request is not None:
            self.scan_info.readout_priority_modification["on_request"] = [
                device.name if isinstance(device, DeviceBase) else device for device in on_request
            ]

    @scan_hook
    def configure_subscans(self) -> None:
        """Declare acquisitions before preparation or device staging begins.

        Decorate overrides with `@scan_hook` to allow beamline scan modifiers to
        extend the declaration, as for other scan hooks.
        """

    def add_subscan(
        self, scan_class: type[ScanBase], *args, when: SubscanPhase = "before", **kwargs
    ) -> None:
        """Register a scan class with its ordinary scan arguments.

        Call this from `configure_subscans`, for example
        `self.add_subscan(Acquire, exp_time=0.1, when="after")`. The worker
        announces the sequence before opening the first acquisition. Each entry
        has its own staging lifecycle and acquisition ID in the parent's file.
        """
        if self._subscan_registration is None:
            raise RuntimeError("Subscans can only be registered during configure_subscans.")
        if not isinstance(scan_class, type) or not issubclass(scan_class, ScanBase):
            raise TypeError("A subscan must be a ScanBase class.")
        if not scan_class.is_scan or scan_class.is_internal:
            raise ValueError("A subscan must be a public acquisition scan.")
        if when not in ("before", "after"):
            raise ValueError("Subscan phase must be 'before' or 'after'.")
        # Match ordinary scan requests and snapshot mutable inputs at registration.
        args, kwargs = msgpack.loads(msgpack.dumps((args, kwargs)))
        self._subscan_registration.append(SubscanSpec(scan_class, tuple(args), kwargs, when))

    def _configure_entry_sequence(
        self, describe: Callable[[SubscanSpec], dict]
    ) -> tuple[tuple[SubscanSpec, ...], tuple[SubscanSpec, ...]]:
        """Collect and validate the flat plan once, before the worker prepares the scan."""
        if self._subscans_configured or self._subscan_registration is not None:
            raise RuntimeError("Subscans have already been configured.")
        self._subscan_registration = []
        try:
            self.configure_subscans()
            before = tuple(spec for spec in self._subscan_registration if spec.when == "before")
            after = tuple(spec for spec in self._subscan_registration if spec.when == "after")
        finally:
            self._subscan_registration = None
        if before or after:
            entries = []
            for index, spec in enumerate((*before, None, *after)):
                entries.append(
                    ScanEntryPlan(
                        entry_index=index,
                        scan_id=f"{self.scan_info.scan_id}-{index}",
                        role="main" if spec is None else spec.when,
                        scan_name=self.scan_name if spec is None else spec.scan_class.scan_name,
                        request_inputs=deepcopy(
                            self.scan_info.request_inputs if spec is None else describe(spec)
                        ),
                    )
                )
            self.scan_info.entry_sequence = tuple(entries)
        self._subscans_configured = True
        return before, after

    @scan_hook
    @abstractmethod
    def prepare_scan(self):
        """Prepare scan metadata and state before opening the scan."""

    @scan_hook
    @abstractmethod
    def open_scan(self):
        """Open the scan and publish the initial scan state."""

    @scan_hook
    @abstractmethod
    def stage(self):
        """Stage devices before the scan starts."""

    @scan_hook
    @abstractmethod
    def pre_scan(self):
        """Run pre-scan device preparation immediately before scan execution."""

    @scan_hook
    @abstractmethod
    def scan_core(self):
        """Execute the main scan procedure."""

    @scan_hook
    @abstractmethod
    def at_each_point(self, *args, **kwargs):
        """Run per-point scan logic when the scan iterates over points."""

    @scan_hook
    @abstractmethod
    def post_scan(self):
        """Run completion logic after the scan core finishes."""

    @scan_hook
    @abstractmethod
    def unstage(self):
        """Unstage devices after scan completion."""

    @scan_hook
    @abstractmethod
    def close_scan(self):
        """Close the scan and finalize scan bookkeeping."""

    @scan_hook
    @abstractmethod
    def on_exception(self, exception: Exception):
        """Handle scan exceptions and perform emergency cleanup."""

    def _collect_original_scan_hooks(self) -> dict[str, Callable]:
        """
        Bind the undecorated scan hook implementations to this scan instance.

        Returns:
            dict[str, Callable]: Mapping from hook name to the original bound method.
        """
        original_hooks = {}
        for attr_name in dir(type(self)):
            attr = getattr(type(self), attr_name)
            hook_info = getattr(attr, "_scan_hook_info", None)
            original_func = getattr(attr, "_scan_hook_original", None)
            if hook_info is None or original_func is None:
                continue
            original_hooks[hook_info["method_name"]] = original_func.__get__(self, type(self))
        return original_hooks
