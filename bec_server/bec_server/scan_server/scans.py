from __future__ import annotations

import ast
import enum
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal

import numpy as np

from bec_lib.alarm_handler import Alarms
from bec_lib.device import DeviceBase
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .errors import LimitError, ScanAbortion
from .path_optimization import PathOptimizerMixin
from .scan_stubs import ScanStubs

if TYPE_CHECKING:
    from bec_server.scan_server.instruction_handler import InstructionHandler

logger = bec_logger.logger


class ScanArgType(str, enum.Enum):
    DEVICE = "device"
    FLOAT = "float"
    INT = "int"
    BOOL = "boolean"
    STR = "str"
    LIST = "list"
    DICT = "dict"


def unpack_scan_args(scan_args: dict[str, Any]) -> list:
    """unpack_scan_args unpacks the scan arguments and returns them as a tuple.

    Args:
        scan_args (dict[str, Any]): scan arguments

    Returns:
        list: list of arguments
    """
    args = []
    if not scan_args:
        return args
    if not isinstance(scan_args, dict):
        return scan_args
    for cmd_name, cmd_args in scan_args.items():
        args.append(cmd_name)
        args.extend(cmd_args)
    return args


def get_2D_raster_pos(axis, snaked=True):
    """get_2D_raster_post calculates and returns the positions for a 2D

    snaked==True:
        ->->->->-
        -<-<-<-<-
        ->->->->-
    snaked==False:
        ->->->->-
        ->->->->-
        ->->->->-

    Args:
        axis (list): list of positions for each axis
        snaked (bool, optional): If true, the positions will be calculcated for a snake scan. Defaults to True.

    Returns:
        array: calculated positions
    """

    x_grid, y_grid = np.meshgrid(axis[0], axis[1])
    if snaked:
        y_grid.T[::2] = np.fliplr(y_grid.T[::2])
    x_flat = x_grid.T.ravel()
    y_flat = y_grid.T.ravel()
    positions = np.vstack((x_flat, y_flat)).T
    return positions


# pylint: disable=too-many-arguments
def get_fermat_spiral_pos(
    m1_start, m1_stop, m2_start, m2_stop, step=1, spiral_type=0, center=False
):
    """get_fermat_spiral_pos calculates and returns the positions for a Fermat spiral scan.

    Args:
        m1_start (float): start position motor 1
        m1_stop (float): end position motor 1
        m2_start (float): start position motor 2
        m2_stop (float): end position motor 2
        step (float, optional): Step size. Defaults to 1.
        spiral_type (float, optional): Angular offset in radians that determines the shape of the spiral.
        A spiral with spiral_type=2 is the same as spiral_type=0. Defaults to 0.
        center (bool, optional): Add a center point. Defaults to False.

    Returns:
        array: calculated positions in the form [[m1, m2], ...]
    """
    positions = []
    phi = 2 * np.pi * ((1 + np.sqrt(5)) / 2.0) + spiral_type * np.pi

    start = int(not center)

    length_axis1 = abs(m1_stop - m1_start)
    length_axis2 = abs(m2_stop - m2_start)
    n_max = int(length_axis1 * length_axis2 * 3.2 / step / step)

    for ii in range(start, n_max):
        radius = step * 0.57 * np.sqrt(ii)
        if abs(radius * np.sin(ii * phi)) > length_axis1 / 2:
            continue
        if abs(radius * np.cos(ii * phi)) > length_axis2 / 2:
            continue
        positions.extend([(radius * np.sin(ii * phi), radius * np.cos(ii * phi))])
    return np.array(positions)


def get_round_roi_scan_positions(lx: float, ly: float, dr: float, nth: int, cenx=0, ceny=0):
    """
    get_round_roi_scan_positions calculates and returns the positions for a round scan in a rectangular region of interest.

    Args:
        lx (float): length in x
        ly (float): length in y
        dr (float): step size
        nth (int): number of angles in the inner ring
        cenx (int, optional): center in x. Defaults to 0.
        ceny (int, optional): center in y. Defaults to 0.

    Returns:
        array: calculated positions in the form [[x, y], ...]
    """
    positions = []
    nr = 1 + int(np.floor(max([lx, ly]) / dr))
    for ir in range(1, nr + 2):
        rr = ir * dr
        dth = 2 * np.pi / (nth * ir)
        pos = [
            (rr * np.cos(ith * dth) + cenx, rr * np.sin(ith * dth) + ceny)
            for ith in range(nth * ir)
            if np.abs(rr * np.cos(ith * dth)) < lx / 2 and np.abs(rr * np.sin(ith * dth)) < ly / 2
        ]
        positions.extend(pos)
    return np.array(positions)


def get_round_scan_positions(r_in: float, r_out: float, nr: int, nth: int, cenx=0, ceny=0):
    """
    get_round_scan_positions calculates and returns the positions for a round scan.

    Args:
        r_in (float): inner radius
        r_out (float): outer radius
        nr (int): number of radii
        nth (int): number of angles in the inner ring
        cenx (int, optional): center in x. Defaults to 0.
        ceny (int, optional): center in y. Defaults to 0.

    Returns:
        array: calculated positions in the form [[x, y], ...]

    """
    positions = []
    dr = (r_in - r_out) / nr
    for ir in range(1, nr + 2):
        rr = r_in + ir * dr
        dth = 2 * np.pi / (nth * ir)
        positions.extend(
            [
                (rr * np.sin(ith * dth) + cenx, rr * np.cos(ith * dth) + ceny)
                for ith in range(nth * ir)
            ]
        )
    return np.array(positions, dtype=float)


class RequestBase(ABC):
    """
    Base class for all scan requests.
    """

    scan_name = ""
    arg_input = {}
    arg_bundle_size = {"bundle": len(arg_input), "min": None, "max": None}
    gui_args = {}
    required_kwargs = []
    return_to_start_after_abort = False
    use_scan_progress_report = False

    def __init__(
        self,
        *args,
        device_manager: DeviceManagerBase = None,
        monitored: list = None,
        parameter: dict = None,
        metadata: dict = None,
        instruction_handler: InstructionHandler = None,
        scan_id: str = None,
        return_to_start: bool = False,
        request_inputs: dict = None,
        **kwargs,
    ) -> None:
        super().__init__()
        self.scan_id = scan_id
        self._shutdown_event = threading.Event()
        self.parameter = parameter if parameter is not None else {}
        self.caller_args = self.parameter.get("args", {})
        self.caller_kwargs = self.parameter.get("kwargs", {})
        self.metadata = metadata
        self.device_manager = device_manager
        self.connector = device_manager.connector
        self.DIID = 0
        self.scan_motors = []
        self.positions = []
        self.return_to_start = return_to_start
        self._pre_scan_macros = []
        self._scan_report_devices = None
        self.update_scan_motors()
        self.readout_priority = {
            "monitored": monitored if monitored is not None else [],
            "baseline": [],
            "on_request": [],
            "async": [],
        }
        self.update_readout_priority()
        if metadata is None:
            self.metadata = {}
        self.stubs = ScanStubs(
            device_manager=self.device_manager,
            instruction_handler=instruction_handler,
            connector=self.device_manager.connector,
            device_msg_callback=self.device_msg_metadata,
            shutdown_event=self._shutdown_event,
        )
        self.request_inputs = request_inputs

    @property
    def scan_report_devices(self):
        """devices to be included in the scan report"""
        if self._scan_report_devices is None:
            return self.readout_priority["monitored"]
        return self._scan_report_devices

    @scan_report_devices.setter
    def scan_report_devices(self, devices: list):
        self._scan_report_devices = devices

    def device_msg_metadata(self):
        default_metadata = {"readout_priority": "monitored"}
        metadata = {**default_metadata, **self.metadata}
        self.DIID += 1
        return metadata

    @staticmethod
    def _get_func_name_from_macro(macro: str):
        return ast.parse(macro).body[0].name

    def run_pre_scan_macros(self):
        """run pre scan macros if any"""
        macros = self.device_manager.connector.lrange(MessageEndpoints.pre_scan_macros(), 0, -1)
        for macro in macros:
            macro = macro.value.strip()
            func_name = self._get_func_name_from_macro(macro)
            exec(macro)
            eval(func_name)(self.device_manager.devices, self)

    def initialize(self):
        self.run_pre_scan_macros()

    def _check_limits(self):
        logger.debug("check limits")
        for ii, dev in enumerate(self.scan_motors):
            low_limit, high_limit = self.device_manager.devices[dev].limits
            if low_limit >= high_limit:
                # if both limits are equal or low > high, no restrictions ought to be applied
                return
            for pos in self.positions:
                pos_axis = pos[ii]
                if not low_limit <= pos_axis <= high_limit:
                    raise LimitError(
                        f"Target position {pos} for motor {dev} is outside of range: [{low_limit},"
                        f" {high_limit}]"
                    )

    def update_scan_motors(self):
        """
        Scan motors are automatically elevated to readout priority monitored and read out in the beginning of the scan.
        """
        if len(self.caller_args) == 0:
            return
        if self.arg_bundle_size.get("bundle"):
            self.scan_motors = list(self.caller_args.keys())
            return
        for motor in self.caller_args:
            if motor not in self.device_manager.devices:
                continue
            self.scan_motors.append(motor)

    def update_readout_priority(self):
        """update the readout priority for this request. Typically the monitored devices should also include the scan motors."""
        self.readout_priority["monitored"].extend(self.scan_motors)
        self.readout_priority["monitored"] = list(
            sorted(
                set(self.readout_priority["monitored"]),
                key=self.readout_priority["monitored"].index,
            )
        )

    @abstractmethod
    def run(self):
        pass


class ScanBase(RequestBase, PathOptimizerMixin):
    """
    Base class for all scans. The following methods are called in the following order during the scan
    1. initialize
        - run_pre_scan_macros
    2. read_scan_motors
    3. prepare_positions
        - _calculate_positions
        - _optimize_trajectory
        - _set_position_offset
        - _check_limits
    4. open_scan
    5. stage
    6. run_baseline_reading
    7. pre_scan
    8. scan_core
    9. finalize
    10. unstage
    11. cleanup

    A subclass of ScanBase must implement the following methods:
    - _calculate_positions

    Attributes:
        scan_name (str): name of the scan
        scan_type (str): scan type. Can be "step" or "fly"
        arg_input (list): list of scan argument types
        arg_bundle_size (dict):
            - bundle: number of arguments that are bundled together
            - min: minimum number of bundles
            - max: maximum number of bundles
        required_kwargs (list): list of required kwargs
        return_to_start_after_abort (bool): if True, the scan will return to the start position after an abort if
            return_to_start is set to True in the kwargs
    """

    scan_name = ""
    scan_type = "step"
    required_kwargs = []
    return_to_start_after_abort = True
    use_scan_progress_report = True

    # perform pre-move action before the pre_scan trigger is sent
    pre_move = True

    def __init__(
        self,
        *args,
        device_manager: DeviceManagerBase = None,
        parameter: dict = None,
        exp_time: float = 0,
        readout_time: float = 0,
        settling_time: float = 0,
        relative: bool = False,
        burst_at_each_point: int = 1,
        frames_per_trigger: int = 1,
        optim_trajectory: Literal["corridor", None] = None,
        monitored: list = None,
        return_to_start: bool = False,
        metadata: dict = None,
        **kwargs,
    ):
        super().__init__(
            *args,
            device_manager=device_manager,
            monitored=monitored,
            parameter=parameter,
            metadata=metadata,
            return_to_start=return_to_start,
            **kwargs,
        )
        self.DIID = 0
        self.point_id = 0
        self.exp_time = exp_time
        self.readout_time = readout_time
        self.settling_time = settling_time
        self.relative = relative
        self.burst_at_each_point = burst_at_each_point
        self.frames_per_trigger = frames_per_trigger
        self.optim_trajectory = optim_trajectory
        self.burst_index = 0
        self._baseline_status = None

        # flag to indicate if the scan has been closed; this is only needed as long
        # as the close_scan method is not used everywhere. Once all scans use close_scan,
        # this can be removed but for now, it provides a backward-compatible way to close the scan.
        self._scan_closed = False

        self.start_pos = []
        self.positions = []
        self.num_pos = 0

        if "return_to_start" not in self.caller_kwargs:
            # if return_to_start is not set in the kwargs, return to start only if
            # it is a relative scan
            self.return_to_start = self.relative

        if self.scan_name == "":
            raise ValueError("scan_name cannot be empty")

        self.scan_parameters = {
            "exp_time": self.exp_time,
            "frames_per_trigger": self.frames_per_trigger,
            "settling_time": self.settling_time,
            "readout_time": self.readout_time,
            "optim_trajectory": self.optim_trajectory,
            "return_to_start": self.return_to_start,
            "relative": self.relative,
        }

        self.scan_parameters.update(**kwargs)
        self.scan_parameters.pop("device_manager", None)
        self.scan_parameters.pop("instruction_handler", None)
        self.scan_parameters.pop("scan_id", None)
        self.scan_parameters.pop("request_inputs", None)

    @property
    def monitor_sync(self):
        """
        monitor_sync is a property that defines how monitored devices are synchronized.
        It can be either bec or the name of the device. If set to bec, the scan bundler
        will synchronize scan segments based on the bec triggered readouts. If set to a device name,
        the scan bundler will synchronize based on the readouts of the device, i.e. upon
        receiving a new readout of the device, cached monitored readings will be added
        to the scan segment.
        """
        return "bec"

    def read_scan_motors(self):
        """read the scan motors"""
        yield from self.stubs.read(device=self.scan_motors)

    def _calculate_positions(self) -> None:
        """Calculate the positions"""

    def _optimize_trajectory(self):
        if not self.optim_trajectory:
            return
        if self.optim_trajectory == "corridor":
            self.positions = self.optimize_corridor(self.positions)
            return
        return

    def prepare_positions(self):
        """prepare the positions for the scan"""
        self._calculate_positions()
        self._optimize_trajectory()
        self.num_pos = len(self.positions) * self.burst_at_each_point
        yield from self._set_position_offset()
        self._check_limits()

    def open_scan(self):
        """open the scan"""
        positions = self.positions if isinstance(self.positions, list) else self.positions.tolist()
        yield from self.stubs.open_scan(
            scan_motors=self.scan_motors,
            readout_priority=self.readout_priority,
            num_pos=self.num_pos,
            positions=positions,
            scan_name=self.scan_name,
            scan_type=self.scan_type,
        )

    def stage(self):
        """call the stage procedure"""
        yield from self.stubs.stage()

    def run_baseline_reading(self):
        """perform a reading of all baseline devices"""
        self._baseline_status = yield from self.stubs.baseline_reading()

    def _set_position_offset(self):
        for dev in self.scan_motors:
            val = yield from self.stubs.send_rpc_and_wait(dev, "read")
            obj = self.device_manager.devices[dev]
            self.start_pos.append(val[obj.full_name].get("value"))
        if self.relative and len(self.start_pos) > 0:
            self.positions += self.start_pos

    def close_scan(self):
        """close the scan"""
        self._scan_closed = True
        yield from self.stubs.close_scan()

    def scan_core(self):
        """perform the scan core procedure"""
        for ind, pos in self._get_position():
            for self.burst_index in range(self.burst_at_each_point):
                yield from self._at_each_point(ind, pos)
            self.burst_index = 0

    def move_to_start(self):
        """return to the start position"""
        if not self.return_to_start:
            return
        yield from self._move_scan_motors_and_wait(self.start_pos)

    def finalize(self):
        """finalize the scan"""
        yield from self.move_to_start()
        yield from self.stubs.complete(device=None)

        if self._baseline_status:
            self._baseline_status.wait()

    def unstage(self):
        """call the unstage procedure"""
        yield from self.stubs.unstage()

    def cleanup(self):
        """call the cleanup procedure"""
        if not self._scan_closed:
            logger.warning(
                "Closing the scan during cleanup is deprecated. Please call .close_scan() explicitly in your scan code."
            )
            yield from self.close_scan()

        # Check if there are any unchecked status objects left.
        # Their done status was not checked nor were they waited for
        # While this is not an error, it is a warning that the scan
        # might not have completed as expected.
        unchecked_status_objects = self.stubs.get_remaining_status_objects(
            exclude_done=False, exclude_checked=True
        )
        if unchecked_status_objects:
            self.connector.raise_alarm(
                severity=Alarms.WARNING,
                source={"Scan": self.scan_name},
                msg=f"Scan completed with unchecked status objects: {unchecked_status_objects}. Use .wait() or .done within the scan to check their status.",
                alarm_type="ScanCleanupWarning",
                metadata={},
            )

        # Check if there are any remaining status objects that are not done.
        # This is not an error but we send a warning and wait for them to complete.
        remaining_status_objects = self.stubs.get_remaining_status_objects(
            exclude_done=True, exclude_checked=False
        )
        if remaining_status_objects:
            self.connector.raise_alarm(
                severity=Alarms.WARNING,
                source={"Scan": self.scan_name},
                msg=f"Scan completed with remaining status objects: {remaining_status_objects}",
                alarm_type="ScanCleanupWarning",
                metadata={},
            )
            for obj in remaining_status_objects:
                obj.wait()

    def _at_each_point(self, ind=None, pos=None):
        yield from self._move_scan_motors_and_wait(pos)

        time.sleep(self.settling_time)

        trigger_time = self.exp_time * self.frames_per_trigger
        yield from self.stubs.trigger(min_wait=trigger_time)

        yield from self.stubs.read(group="monitored", point_id=self.point_id)

        self.point_id += 1

    def _move_scan_motors_and_wait(self, pos):
        if pos is None:
            return
        if not isinstance(pos, list) and not isinstance(pos, np.ndarray):
            pos = [pos]
        if len(pos) == 0:
            return
        yield from self.stubs.set(device=self.scan_motors, value=pos)

    def _get_position(self):
        for ind, pos in enumerate(self.positions):
            yield (ind, pos)

    def scan_report_instructions(self):
        yield None

    def pre_scan(self):
        """
        pre scan procedure. This method is called before the scan_core method and can be used to
        perform additional tasks before the scan is started. This
        """
        if self.pre_move and len(self.positions) > 0:
            yield from self._move_scan_motors_and_wait(self.positions[0])
        yield from self.stubs.pre_scan()

    def run(self):
        """run the scan. This method is called by the scan server and is the main entry point for the scan."""
        self.initialize()
        yield from self.read_scan_motors()
        yield from self.prepare_positions()
        yield from self.scan_report_instructions()
        yield from self.open_scan()
        yield from self.stage()
        yield from self.run_baseline_reading()
        yield from self.pre_scan()
        yield from self.scan_core()
        yield from self.finalize()
        yield from self.unstage()
        yield from self.close_scan()
        self.cleanup()

    @classmethod
    def scan(cls, *args, **kwargs):
        scan = cls(args, **kwargs)
        yield from scan.run()


class SyncFlyScanBase(ScanBase, ABC):
    """
    Fly scan base class for all synchronous fly scans. A synchronous fly scan is a scan where the flyer is
    synced with the monitored devices.
    Classes inheriting from SyncFlyScanBase must at least implement the scan_core method and the monitor_sync property.
    """

    scan_type = "fly"
    pre_move = False

    def update_scan_motors(self) -> None:
        # fly scans normally do not have stepper scan motors so
        # the default way of retrieving scan motors is not applicable
        return None

    @property
    @abstractmethod
    def monitor_sync(self) -> str:
        """
        monitor_sync is the flyer that will be used to synchronize the monitor readings in the scan bundler.
        The return value should be the name of the flyer device.
        """

    def _calculate_positions(self) -> None:
        pass

    def read_scan_motors(self):
        yield None

    def prepare_positions(self):
        yield None

    @abstractmethod
    def scan_core(self):
        """perform the scan core procedure"""
        ############################################
        # Example of how to kickoff and wait for a flyer:
        ############################################

        # yield from self.stubs.kickoff(device=self.flyer, parameter=self.caller_kwargs)
        # yield from self.stubs.complete(device=self.flyer)
        # target_diid = self.DIID - 1

        # while True:
        #     status = self.stubs.get_req_status(
        #         device=self.flyer, RID=self.metadata["RID"], DIID=target_diid
        #     )
        #     progress = self.stubs.get_device_progress(
        #         device=self.flyer, RID=self.metadata["RID"]
        #     )
        #     if progress:
        #         self.num_pos = progress
        #     if status:
        #         break
        #     time.sleep(1)

    # def _get_flyer_status(self) -> list:
    #     connector = self.device_manager.connector

    #     pipe = connector.pipeline()
    #     connector.lrange(
    #         MessageEndpoints.device_req_status_container(self.metadata["RID"]), 0, -1, pipe
    #     )
    #     connector.get(MessageEndpoints.device_readback(self.flyer), pipe)
    #     return connector.execute_pipeline(pipe)


class AsyncFlyScanBase(SyncFlyScanBase):
    """
    Fly scan base class for all asynchronous fly scans. An asynchronous fly scan is a scan where the flyer is
    not synced with the monitored devices.
    Classes inheriting from AsyncFlyScanBase must at least implement the scan_core method.
    """

    @property
    def monitor_sync(self):
        return "bec"


class ScanStub(RequestBase):
    pass


class OpenScanDef(ScanStub):
    scan_name = "open_scan_def"

    def run(self):
        yield from self.stubs.open_scan_def()


class CloseScanDef(ScanStub):
    scan_name = "close_scan_def"

    def run(self):
        yield from self.stubs.close_scan_def()


class CloseScanGroup(ScanStub):
    scan_name = "close_scan_group"

    def run(self):
        yield from self.stubs.close_scan_group()


class DeviceRPC(ScanStub):
    scan_name = "device_rpc"
    arg_input = {
        "device": ScanArgType.DEVICE,
        "func": ScanArgType.STR,
        "args": ScanArgType.LIST,
        "kwargs": ScanArgType.DICT,
    }
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": 1}

    def update_scan_motors(self):
        pass

    def run(self):
        # different to calling self.device_rpc, this procedure will not wait for a reply and therefore not check any errors.
        status = yield from self.stubs.send_rpc(
            self.parameter.get("device"),
            self.parameter.get("func"),
            *self.parameter.get("args"),
            rpc_id=self.parameter.get("rpc_id"),
            metadata=self.metadata,
            **self.parameter.get("kwargs"),
        )
        self.stubs._status_registry.pop(status._device_instr_id)


class Move(RequestBase):
    scan_name = "mv"
    arg_input = {"device": ScanArgType.DEVICE, "target": ScanArgType.FLOAT}
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": None}
    required_kwargs = ["relative"]

    def __init__(self, *args, relative=False, **kwargs):
        """
        Move device(s) to an absolute position
        Args:
            *args (Device, float): pairs of device / position arguments
            relative (bool): if True, move relative to current position

        Returns:
            ScanReport

        Examples:
            >>> scans.mv(dev.samx, 1, dev.samy,2, relative=False)
        """
        super().__init__(**kwargs)
        self.relative = relative
        self.start_pos = []

    def _calculate_positions(self):
        self.positions = np.asarray([[val[0] for val in self.caller_args.values()]], dtype=float)

    def _at_each_point(self, pos=None):
        for ii, motor in enumerate(self.scan_motors):
            status = yield from self.stubs.set(
                device=motor, value=self.positions[0][ii], metadata={"response": True}, wait=False
            )
            # we won't wait for the status object to complete, hence we remove it from the status registry
            # to avoid warnings about incomplete status objects
            # pylint: disable=protected-access
            self.stubs._status_registry.pop(status._device_instr_id)

    def cleanup(self):
        pass

    def _set_position_offset(self):
        self.start_pos = []
        for dev in self.scan_motors:
            val = yield from self.stubs.send_rpc_and_wait(dev, "read")
            obj = self.device_manager.devices[dev]
            self.start_pos.append(val[obj.full_name].get("value"))
        if not self.relative:
            return
        self.positions += self.start_pos

    def prepare_positions(self):
        self._calculate_positions()
        yield from self._set_position_offset()
        self._check_limits()

    def scan_report_instructions(self):
        yield None

    def run(self):
        self.initialize()
        yield from self.prepare_positions()
        yield from self.scan_report_instructions()
        yield from self._at_each_point()


class UpdatedMove(Move):
    """
    Move device(s) to an absolute position and show live updates. This is a blocking call. For non-blocking use Move.
    Args:
        *args (Device, float): pairs of device / position arguments
        relative (bool): if True, move relative to current position

    Returns:
        ScanReport

    Examples:
        >>> scans.umv(dev.samx, 1, dev.samy,2, relative=False)
    """

    scan_name = "umv"

    def _at_each_point(self, pos=None):
        yield from self.stubs.set(device=self.scan_motors, value=self.positions[0])

    def scan_report_instructions(self):
        yield from self.stubs.scan_report_instruction(
            {
                "readback": {
                    "RID": self.metadata["RID"],
                    "devices": self.scan_motors,
                    "start": self.start_pos,
                    "end": self.positions[0],
                }
            }
        )


class Scan(ScanBase):
    scan_name = "grid_scan"
    arg_input = {
        "device": ScanArgType.DEVICE,
        "start": ScanArgType.FLOAT,
        "stop": ScanArgType.FLOAT,
        "steps": ScanArgType.INT,
    }
    arg_bundle_size = {"bundle": len(arg_input), "min": 2, "max": None}
    required_kwargs = ["relative"]
    gui_config = {
        "Scan Parameters": [
            "exp_time",
            "settling_time",
            "burst_at_each_point",
            "relative",
            "snaked",
        ]
    }

    def __init__(
        self,
        *args,
        exp_time: float = 0,
        settling_time: float = 0,
        relative: bool = False,
        burst_at_each_point: int = 1,
        snaked: bool = True,
        **kwargs,
    ):
        """
        Scan two or more motors in a grid.

        Args:
            *args (Device, float, float, int): pairs of device / start / stop / steps arguments
            exp_time (float): exposure time in seconds. Default is 0.
            settling_time (float): settling time in seconds. Default is 0.
            relative (bool): if True, the motors will be moved relative to their current position. Default is False.
            burst_at_each_point (int): number of exposures at each point. Default is 1.
            snaked (bool): if True, the scan will be snaked. Default is True.

        Returns:
            ScanReport

        Examples:
            >>> scans.grid_scan(dev.motor1, -5, 5, 10, dev.motor2, -5, 5, 10, exp_time=0.1, relative=True)

        """
        self.snaked = snaked
        super().__init__(
            exp_time=exp_time,
            settling_time=settling_time,
            relative=relative,
            burst_at_each_point=burst_at_each_point,
            **kwargs,
        )

    def _calculate_positions(self):
        axis = []
        for _, val in self.caller_args.items():
            axis.append(np.linspace(val[0], val[1], val[2], dtype=float))
        if len(axis) > 1:
            self.positions = get_2D_raster_pos(axis, snaked=self.snaked)
        else:
            self.positions = np.vstack(tuple(axis)).T


class FermatSpiralScan(ScanBase):
    scan_name = "fermat_scan"
    required_kwargs = ["step", "relative"]
    gui_config = {
        "Device 1": ["motor1", "start_motor1", "stop_motor1"],
        "Device 2": ["motor2", "start_motor2", "stop_motor2"],
        "Movement Parameters": ["step", "spiral_type", "relative"],
        "Acquisition Parameters": ["exp_time", "settling_time", "burst_at_each_point"],
    }

    def __init__(
        self,
        motor1: DeviceBase,
        start_motor1: float,
        stop_motor1: float,
        motor2: DeviceBase,
        start_motor2: float,
        stop_motor2: float,
        step: float = 0.1,
        exp_time: float = 0,
        settling_time: float = 0,
        relative: bool = False,
        burst_at_each_point: int = 1,
        spiral_type: float = 0,
        optim_trajectory: Literal["corridor", None] = None,
        **kwargs,
    ):
        """
        A scan following Fermat's spiral.

        Args:
            motor1 (DeviceBase): first motor
            start_motor1 (float): start position motor 1
            stop_motor1 (float): end position motor 1
            motor2 (DeviceBase): second motor
            start_motor2 (float): start position motor 2
            stop_motor2 (float): end position motor 2
            step (float): step size in motor units. Default is 0.1.
            exp_time (float): exposure time in seconds. Default is 0.
            settling_time (float): settling time in seconds. Default is 0.
            relative (bool): if True, the motors will be moved relative to their current position. Default is False.
            burst_at_each_point (int): number of exposures at each point. Default is 1.
            spiral_type (float): Angular offset (e.g. 0, 0.25,... ) in radians that determines the shape of the spiral. Default is 0.
            optim_trajectory (str): trajectory optimization method. Default is None. Options are "corridor" and "none".

        Returns:
            ScanReport

        Examples:
            >>> scans.fermat_scan(dev.motor1, -5, 5, dev.motor2, -5, 5, step=0.5, exp_time=0.1, relative=True, optim_trajectory="corridor")

        """
        self.motor1 = motor1
        self.motor2 = motor2
        super().__init__(
            exp_time=exp_time,
            settling_time=settling_time,
            relative=relative,
            burst_at_each_point=burst_at_each_point,
            optim_trajectory=optim_trajectory,
            **kwargs,
        )

        self.start_motor1 = start_motor1
        self.stop_motor1 = stop_motor1
        self.start_motor2 = start_motor2
        self.stop_motor2 = stop_motor2
        self.step = step
        self.spiral_type = spiral_type

    def update_scan_motors(self):
        self.scan_motors = [self.motor1, self.motor2]

    def _calculate_positions(self):
        self.positions = get_fermat_spiral_pos(
            self.start_motor1,
            self.stop_motor1,
            self.start_motor2,
            self.stop_motor2,
            step=self.step,
            spiral_type=self.spiral_type,
            center=False,
        )


class RoundScan(ScanBase):
    scan_name = "round_scan"
    required_kwargs = ["relative"]
    gui_config = {
        "Motors": ["motor_1", "motor_2"],
        "Ring Parameters": ["inner_ring", "outer_ring", "number_of_rings", "pos_in_first_ring"],
        "Scan Parameters": ["relative", "burst_at_each_point"],
    }

    def __init__(
        self,
        motor_1: DeviceBase,
        motor_2: DeviceBase,
        inner_ring: float,
        outer_ring: float,
        number_of_rings: int,
        pos_in_first_ring: int,
        relative: bool = False,
        burst_at_each_point: int = 1,
        **kwargs,
    ):
        """
        A scan following a round shell-like pattern with increasing number of points in each ring. The scan starts at the inner ring and moves outwards.
        The user defines the inner and outer radius, the number of rings and the number of positions in the first ring.

        Args:
            motor_1 (DeviceBase): first motor
            motor_2 (DeviceBase): second motor
            inner_ring (float): inner radius
            outer_ring (float): outer radius
            number_of_rings (int): number of rings
            pos_in_first_ring (int): number of positions in the first ring
            relative (bool): if True, the motors will be moved relative to their current position. Default is False.
            burst_at_each_point (int): number of exposures at each point. Default is 1.

        Returns:
            ScanReport

        Examples:
            >>> scans.round_scan(dev.motor1, dev.motor2, 0, 25, 5, 3, exp_time=0.1, relative=True)

        """
        self.motor_1 = motor_1
        self.motor_2 = motor_2
        super().__init__(relative=relative, burst_at_each_point=burst_at_each_point, **kwargs)
        self.axis = []
        self.inner_ring = inner_ring
        self.outer_ring = outer_ring
        self.number_of_rings = number_of_rings
        self.pos_in_first_ring = pos_in_first_ring

    def update_scan_motors(self):
        self.scan_motors = [self.motor_1, self.motor_2]

    def _calculate_positions(self):
        self.positions = get_round_scan_positions(
            r_in=self.inner_ring,
            r_out=self.outer_ring,
            nr=self.number_of_rings,
            nth=self.pos_in_first_ring,
        )


class ContLineScan(ScanBase):
    scan_name = "cont_line_scan"
    required_kwargs = ["steps", "relative"]
    scan_type = "step"
    gui_config = {
        "Device": ["device", "start", "stop"],
        "Movement Parameters": ["steps", "relative", "offset", "atol"],
        "Acquisition Parameters": ["exp_time", "burst_at_each_point"],
    }

    def __init__(
        self,
        device: DeviceBase,
        start: float,
        stop: float,
        offset: float = None,
        atol: float = None,
        exp_time: float = 0,
        steps: int = 10,
        relative: bool = False,
        burst_at_each_point: int = 1,
        **kwargs,
    ):
        """
        A continuous line scan. Use this scan if you want to move a motor continuously from start to stop position whilst
        acquiring data at predefined positions. The scan will abort if the motor moves too fast to acquire data within the
        given absolute tolerance.
        If no offset is provided, the offset is calculated as 0.5*acc_time*target_velocity.
        If no atol is provided, the atol is calculated at 10% of the step size, however, we take into consideration
        that EPICs typically only updates the position with 100ms intervals, which defines a lower limit for the atol.

        Please note that the motor limits have to be set correctly for allowing the motor to reach the start position including the offset.

        Args:
            device (DeviceBase): motor to move continuously from start to stop position
            start (float): start position
            stop (float): stop position
            exp_time (float): exposure time in seconds. Default is 0.
            steps (int): number of steps. Default is 10.
            relative (bool): if True, the motors will be moved relative to their current position. Default is False.
            burst_at_each_point (int): number of exposures at each point. Default is 1.
            offset (float): offset in motor units. Default is 1.
            atol (float): absolute tolerance for position check. Default is 0.5.

        Returns:
            ScanReport

        Examples:
            >>> scans.cont_line_scan(dev.motor1, -5, 5, steps=10, exp_time=0.1, relative=True)

        """
        self.device = device
        super().__init__(
            exp_time=exp_time, relative=relative, burst_at_each_point=burst_at_each_point, **kwargs
        )
        self.steps = steps
        self.offset = offset
        self.start = start
        self.stop = stop
        self.atol = atol
        self.motor_acceleration = None
        self.motor_velocity = None
        self.dist_step = None
        self.time_per_step = None
        self.hinted_signal = None

    def update_scan_motors(self):
        self.scan_motors = [self.device]

    def _get_motor_attributes(self):
        """Get the motor attributes"""
        if hasattr(self.device_manager.devices[self.device], "velocity"):
            self.motor_velocity = yield from self.stubs.send_rpc_and_wait(
                self.device, "velocity.get"
            )
        else:
            raise ScanAbortion(f"Motor {self.device} does not have a velocity attribute.")
        if hasattr(self.device_manager.devices[self.device], "acceleration"):
            self.motor_acceleration = yield from self.stubs.send_rpc_and_wait(
                self.device, "acceleration.get"
            )
        else:
            raise ScanAbortion(f"Motor {self.device} does not have an acceleration attribute.")
        # pylint: disable=protected-access
        hinted_signal = self.device_manager.devices[self.device]._info["hints"]["fields"]
        if len(hinted_signal) > 1:
            raise ScanAbortion(
                f"Device {self.device} has more than one signal {hinted_signal}. Only one signal is allowed."
            )
        self.hinted_signal = hinted_signal[0]

    def _calculate_atol(self):
        """Utility function to calculate the tolerance for the scan if not provided.
        The tolerance is calculated at 10% of the step size, however, we take into consideration
        that EPICs typically only updates the position with 100ms intervals, which defines a lower limit for the atol.
        """
        update_freq = 10  # Hz for Epics
        tolerance = 0.1
        precision = 10 ** (-self.device_manager.devices[self.device].precision)
        if self.atol is not None:
            return
        # Use 10% of the step size as atol
        self.atol = tolerance * self.motor_velocity * self.exp_time
        self.atol = max(self.atol, 2 * precision)
        if self.atol / update_freq > self.motor_velocity:
            raise ScanAbortion(
                f"Motor {self.device} is moving too fast with the calculated tolerance. Consider reducing speed {self.motor_velocity} or increasing the atol {self.atol}"
            )
        # the lower udate limit is 100ms, so we set the atol to 0.2s/v if the atol is smaller
        self.atol = max(self.atol, 2 * 1 / update_freq * self.motor_velocity)

    def _calculate_offset(self):
        """Utility function to calculate the offset for the acceleration if not provided.
        The offset is calculated as 0.5*acc_time*target_velocity"""
        if self.offset is not None:
            return
        self.offset = 0.5 * self.motor_acceleration * self.motor_velocity

    def prepare_positions(self):
        """prepare the positions for the scan"""
        yield from self._calculate_positions()
        self._optimize_trajectory()
        self.num_pos = len(self.positions) * self.burst_at_each_point
        yield from self._set_position_offset()
        self._check_limits()

    def _calculate_positions(self):
        yield from self._get_motor_attributes()
        self.positions = np.linspace(self.start, self.stop, self.steps, dtype=float)[
            np.newaxis, :
        ].T
        # Check if the motor is moving faster than the exp_time
        self.dist_step = self.positions[1][0] - self.positions[0][0]
        self._calculate_offset()
        self._calculate_atol()
        self.time_per_step = self.dist_step / self.motor_velocity
        if self.time_per_step < self.exp_time:
            raise ScanAbortion(
                f"Motor {self.device} is moving too fast. Time per step: {self.time_per_step:.03f} < Exp_time: {self.exp_time:.03f}."
                + f" Consider reducing speed {self.motor_velocity} or reducing exp_time {self.exp_time}"
            )

    def _check_limits(self):
        logger.debug("check limits")
        low_limit, high_limit = self.device_manager.devices[self.device].limits
        if low_limit >= high_limit:
            # if both limits are equal or low > high, no restrictions ought to be applied
            return
        for ii, pos in enumerate(self.positions):
            if ii == 0:
                pos_axis = pos - self.offset
            else:
                pos_axis = pos
            if not low_limit <= pos_axis <= high_limit:
                raise LimitError(
                    f"Target position including offset {pos_axis} (offset: {self.offset}) for motor {self.device} is outside of range: [{low_limit},"
                    f" {high_limit}]"
                )

    def _at_each_point(self, _ind=None, _pos=None):
        yield from self.stubs.trigger(min_wait=self.exp_time)
        yield from self.stubs.read(group="monitored", point_id=self.point_id)
        self.point_id += 1

    def scan_core(self):
        yield from self._move_scan_motors_and_wait(self.positions[0] - self.offset)
        # send the slow motor on its way
        status = yield from self.stubs.set(
            device=self.scan_motors[0], value=self.positions[-1][0], wait=False
        )

        while self.point_id < len(self.positions[:]):
            cont_motor_positions = self.device_manager.devices[self.scan_motors[0]].read()

            if not cont_motor_positions:
                continue

            cont_motor_positions = cont_motor_positions[self.scan_motors[0]].get("value")
            logger.debug(f"Current position of {self.scan_motors[0]}: {cont_motor_positions}")
            # TODO: consider the alternative, which triggers a readout for each point right after the motor passed it
            # if cont_motor_positions > self.positions[self.point_id][0]:
            if np.isclose(cont_motor_positions, self.positions[self.point_id][0], atol=self.atol):
                logger.debug(f"reading point {self.point_id}")
                yield from self._at_each_point()
                continue
            if cont_motor_positions > self.positions[self.point_id][0]:
                raise ScanAbortion(
                    f"Skipped point {self.point_id + 1}:"
                    f"Consider reducing speed {self.device_manager.devices[self.scan_motors[0]].velocity.get()}, "
                    f"increasing the atol {self.atol}, or increasing the offset {self.offset}"
                )
        # not needed, but for clarity
        status.wait()


class ContLineFlyScan(AsyncFlyScanBase):
    scan_name = "cont_line_fly_scan"
    required_kwargs = []
    use_scan_progress_report = False
    gui_config = {"Device": ["motor", "start", "stop"], "Scan Parameters": ["exp_time", "relative"]}

    def __init__(
        self,
        motor: DeviceBase,
        start: float,
        stop: float,
        exp_time: float = 0,
        relative: bool = False,
        **kwargs,
    ):
        """
        A continuous line fly scan. Use this scan if you want to move a motor continuously from start to stop position whilst
        acquiring data as fast as possible (respecting the exposure time). The scan will stop automatically when the motor
        reaches the end position.

        Args:
            motor (DeviceBase): motor to move continuously from start to stop position
            start (float): start position
            stop (float): stop position
            exp_time (float): exposure time in seconds. Default is 0.
            relative (bool): if True, the motor will be moved relative to its current position. Default is False.

        Returns:
            ScanReport

        Examples:
            >>> scans.cont_line_fly_scan(dev.sam_rot, 0, 180, exp_time=0.1)

        """
        self.motor = motor
        self.start = start
        self.stop = stop
        self.device_move_request_id = str(uuid.uuid4())
        super().__init__(relative=relative, exp_time=exp_time, **kwargs)

    def update_scan_motors(self):
        # fly scans normally do not have stepper scan motors so
        # the default way of retrieving scan motors is not applicable
        self.scan_motors = [self.motor]

    def prepare_positions(self):
        self.positions = np.array([[self.start], [self.stop]], dtype=float)
        self.num_pos = None
        yield from self._set_position_offset()

    def scan_report_instructions(self):
        yield from self.stubs.scan_report_instruction(
            {
                "readback": {
                    "RID": self.device_move_request_id,
                    "devices": [self.motor],
                    "start": [self.start],
                    "end": [self.stop],
                }
            }
        )

    def scan_core(self):
        # move the motor to the start position
        yield from self.stubs.set(device=self.motor, value=self.positions[0][0])

        # start the flyer
        status_flyer = yield from self.stubs.set(
            device=self.motor,
            value=self.positions[1][0],
            metadata={"response": True, "RID": self.device_move_request_id},
            wait=False,
        )

        while not status_flyer.done:
            yield from self.stubs.trigger(min_wait=self.exp_time)
            yield from self.stubs.read(group="monitored", point_id=self.point_id)
            self.point_id += 1

        self.num_pos = self.point_id


class RoundScanFlySim(SyncFlyScanBase):
    scan_name = "round_scan_fly"
    scan_type = "fly"
    pre_move = False
    required_kwargs = ["relative"]
    gui_config = {
        "Fly Parameters": ["flyer", "relative"],
        "Ring Parameters": ["inner_ring", "outer_ring", "number_of_rings", "number_pos"],
    }

    def __init__(
        self,
        flyer: DeviceBase,
        inner_ring: float,
        outer_ring: float,
        number_of_rings: int,
        number_pos: int,
        relative: bool = False,
        **kwargs,
    ):
        """
        A fly scan following a round shell-like pattern.

        Args:
            flyer (DeviceBase): flyer device
            inner_ring (float): inner radius
            outer_ring (float): outer radius
            number_of_rings (int): number of rings
            number_pos (int): number of positions in the first ring
            relative (bool): if True, the motors will be moved relative to their current position. Default is False.
            burst_at_each_point (int): number of exposures at each point. Default is 1.

        Returns:
            ScanReport

        Examples:
            >>> scans.round_scan_fly(dev.flyer_sim, 0, 50, 5, 3, exp_time=0.1, relative=True)

        """
        super().__init__(**kwargs)
        self.flyer = flyer
        self.inner_ring = inner_ring
        self.outer_ring = outer_ring
        self.number_of_rings = number_of_rings
        self.number_pos = number_pos

    def update_scan_motors(self):
        self.scan_motors = []

    @property
    def monitor_sync(self):
        return self.flyer

    def prepare_positions(self):
        self._calculate_positions()
        self.num_pos = len(self.positions) * self.burst_at_each_point
        self._check_limits()
        yield None

    def finalize(self):
        yield

    def _calculate_positions(self):
        self.positions = get_round_scan_positions(
            r_in=self.inner_ring,
            r_out=self.outer_ring,
            nr=self.number_of_rings,
            nth=self.number_pos,
        )

    def scan_core(self):
        status = yield from self.stubs.kickoff(
            device=self.flyer,
            parameter={
                "num_pos": self.num_pos,
                "positions": self.positions.tolist(),
                "exp_time": self.exp_time,
            },
            wait=False,
        )

        while not status.done:
            yield from self.stubs.read(group="monitored")

            time.sleep(1)
            logger.debug("reading monitors")


class RoundROIScan(ScanBase):
    scan_name = "round_roi_scan"
    required_kwargs = ["dr", "nth", "relative"]
    gui_config = {
        "Motor 1": ["motor_1", "width_1"],
        "Motor 2": ["motor_2", "width_2"],
        "Shell Parametes": ["dr", "nth"],
        "Acquisition Parameters": ["exp_time", "relative", "burst_at_each_point"],
    }

    def __init__(
        self,
        motor_1: DeviceBase,
        width_1: float,
        motor_2: DeviceBase,
        width_2: float,
        dr: float = 1,
        nth: int = 5,
        exp_time: float = 0,
        relative: bool = False,
        burst_at_each_point: int = 1,
        **kwargs,
    ):
        """
        A scan following a round-roi-like pattern.

        Args:
            motor_1 (DeviceBase): first motor
            width_1 (float): width of region of interest for motor_1
            motor_2 (DeviceBase): second motor
            width_2 (float): width of region of interest for motor_2
            dr (float): shell width. Default is 1.
            nth (int): number of points in the first shell. Default is 5.
            exp_time (float): exposure time in seconds. Default is 0.
            relative (bool): Start from an absolute or relative position. Default is False.
            burst_at_each_point (int): number of acquisition per point. Default is 1.

        Returns:
            ScanReport

        Examples:
            >>> scans.round_roi_scan(dev.motor1, 20, dev.motor2, 20, dr=2, nth=3, exp_time=0.1, relative=True)

        """
        self.motor_1 = motor_1
        self.motor_2 = motor_2
        super().__init__(
            exp_time=exp_time, relative=relative, burst_at_each_point=burst_at_each_point, **kwargs
        )
        self.width_1 = width_1
        self.width_2 = width_2
        self.dr = dr
        self.nth = nth

    def update_scan_motors(self):
        self.scan_motors = [self.motor_1, self.motor_2]

    def _calculate_positions(self) -> None:
        self.positions = get_round_roi_scan_positions(
            lx=self.width_1, ly=self.width_2, dr=self.dr, nth=self.nth
        )


class ListScan(ScanBase):
    scan_name = "list_scan"
    required_kwargs = ["relative"]
    arg_input = {"device": ScanArgType.DEVICE, "positions": ScanArgType.LIST}
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": None}

    def __init__(self, *args, parameter: dict = None, **kwargs):
        """
        A scan following the positions specified in a list.
        Please note that all lists must be of equal length.

        Args:
            *args: pairs of motors and position lists
            relative: Start from an absolute or relative position
            burst_at_each_point: number of acquisition per point

        Returns:
            ScanReport

        Examples:
            >>> scans.list_scan(dev.motor1, [0,1,2,3,4], dev.motor2, [4,3,2,1,0], exp_time=0.1, relative=True)

        """
        super().__init__(parameter=parameter, **kwargs)
        if len(set(len(entry[0]) for entry in self.caller_args.values())) != 1:
            raise ValueError("All position lists must be of equal length.")

    def _calculate_positions(self):
        self.positions = np.vstack(tuple(self.caller_args.values()), dtype=float).T


class TimeScan(ScanBase):
    scan_name = "time_scan"
    required_kwargs = []
    gui_config = {"Scan Parameters": ["points", "interval", "exp_time", "burst_at_each_point"]}

    def __init__(
        self,
        points: int,
        interval: float,
        exp_time: float = 0,
        burst_at_each_point: int = 1,
        **kwargs,
    ):
        """
        Trigger and readout devices at a fixed interval.
        Note that the interval time cannot be less than the exposure time.
        The effective "sleep" time between points is
            sleep_time = interval - exp_time

        Args:
            points: number of points
            interval: time interval between points
            exp_time: exposure time in s
            burst_at_each_point: number of acquisition per point

        Returns:
            ScanReport

        Examples:
            >>> scans.time_scan(10, 1.5, exp_time=0.1, relative=True)

        """
        super().__init__(exp_time=exp_time, burst_at_each_point=burst_at_each_point, **kwargs)
        self.points = points
        self.interval = interval
        self.interval -= self.exp_time

    def _calculate_positions(self) -> None:
        pass

    def prepare_positions(self):
        self.num_pos = self.points
        yield None

    def scan_core(self):
        for ind in range(self.num_pos):
            yield from self._at_each_point(ind)


class MonitorScan(ScanBase):
    scan_name = "monitor_scan"
    required_kwargs = ["relative"]
    scan_type = "fly"
    gui_config = {"Device": ["device", "start", "stop"], "Scan Parameters": ["relative"]}

    def __init__(
        self,
        device: DeviceBase,
        start: float,
        stop: float,
        min_update: float = 0,
        relative: bool = False,
        **kwargs,
    ):
        """
        Readout all primary devices at each update of the monitored device.

        Args:
            device (Device): monitored device
            start (float): start position of the monitored device
            stop (float): stop position of the monitored device
            min_update (float): minimum update interval in seconds
            relative (bool): if True, the motor will be moved relative to its current position. Default is False.

        Returns:
            ScanReport

        Examples:
            >>> scans.monitor_scan(dev.motor1, -5, 5, exp_time=0.1, relative=True)

        """
        self.device = device
        super().__init__(relative=relative, **kwargs)
        self.start = start
        self.stop = stop
        self.min_update = min_update

    def update_scan_motors(self):
        self.scan_motors = [self.device]
        self.flyer = self.device

    @property
    def monitor_sync(self):
        return self.flyer

    def _calculate_positions(self) -> None:
        self.positions = np.array([[self.start], [self.stop]], dtype=float)

    def prepare_positions(self):
        self._calculate_positions()
        self.num_pos = 0
        yield from self._set_position_offset()
        self._check_limits()

    def _get_flyer_status(self) -> list:
        connector = self.device_manager.connector
        connector.get(MessageEndpoints.device_readback(self.flyer))
        return connector.get(MessageEndpoints.device_readback(self.flyer))

    def scan_core(self):
        yield from self.stubs.set(device=self.flyer, value=self.positions[0][0])

        # send the slow motor on its way
        status = yield from self.stubs.set(
            device=self.flyer, value=self.positions[1][0], metadata={"response": True}, wait=False
        )

        while not status.done:
            readback = self._get_flyer_status()

            if not readback:
                continue
            readback = readback.content["signals"]
            yield from self.stubs.publish_data_as_read(
                device=self.flyer, data=readback, point_id=self.point_id
            )
            self.point_id += 1
            self.num_pos += 1

            if self.min_update:
                time.sleep(self.min_update)


class Acquire(ScanBase):
    scan_name = "acquire"
    required_kwargs = []
    gui_config = {"Scan Parameters": ["exp_time", "burst_at_each_point"]}

    def __init__(self, exp_time: float = 0, burst_at_each_point: int = 1, **kwargs):
        """
        A simple acquisition at the current position.

        Args:
            exp_time (float): exposure time in s
            burst_at_each_point: number of acquisition per point

        Returns:
            ScanReport

        Examples:
            >>> scans.acquire(exp_time=0.1)

        """
        super().__init__(exp_time=exp_time, burst_at_each_point=burst_at_each_point, **kwargs)

    def _calculate_positions(self) -> None:
        self.num_pos = self.burst_at_each_point

    def prepare_positions(self):
        self._calculate_positions()

    def _at_each_point(self, ind=None, pos=None):
        yield from self.stubs.trigger(min_wait=self.exp_time)
        yield from self.stubs.read(group="monitored", point_id=self.point_id)
        self.point_id += 1

    def scan_core(self):
        for self.burst_index in range(self.burst_at_each_point):
            yield from self._at_each_point(self.burst_index)
        self.burst_index = 0

    def run(self):
        self.initialize()
        self.prepare_positions()
        yield from self.open_scan()
        yield from self.stage()
        yield from self.run_baseline_reading()
        yield from self.pre_scan()
        yield from self.scan_core()
        yield from self.finalize()
        yield from self.unstage()
        yield from self.close_scan()
        self.cleanup()


class LineScan(ScanBase):
    scan_name = "line_scan"
    required_kwargs = ["steps", "relative"]
    arg_input = {
        "device": ScanArgType.DEVICE,
        "start": ScanArgType.FLOAT,
        "stop": ScanArgType.FLOAT,
    }
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": None}
    gui_config = {
        "Movement Parameters": ["steps", "relative"],
        "Acquisition Parameters": ["exp_time", "burst_at_each_point"],
    }

    def __init__(
        self,
        *args,
        exp_time: float = 0,
        steps: int = None,
        relative: bool = False,
        burst_at_each_point: int = 1,
        **kwargs,
    ):
        """
        A line scan for one or more motors.

        Args:
            *args (Device, float, float): pairs of device / start position / end position
            exp_time (float): exposure time in s. Default: 0
            steps (int): number of steps. Default: 10
            relative (bool): if True, the start and end positions are relative to the current position. Default: False
            burst_at_each_point (int): number of acquisition per point. Default: 1

        Returns:
            ScanReport

        Examples:
            >>> scans.line_scan(dev.motor1, -5, 5, dev.motor2, -5, 5, steps=10, exp_time=0.1, relative=True)

        """
        super().__init__(
            exp_time=exp_time, relative=relative, burst_at_each_point=burst_at_each_point, **kwargs
        )
        self.steps = steps

    def _calculate_positions(self) -> None:
        axis = []
        for _, val in self.caller_args.items():
            ax_pos = np.linspace(val[0], val[1], self.steps, dtype=float)
            axis.append(ax_pos)
        self.positions = np.array(list(zip(*axis)), dtype=float)


class ScanComponent(ScanBase):
    pass


class OpenInteractiveScan(ScanComponent):
    scan_name = "_open_interactive_scan"
    required_kwargs = []
    # arg_input = {}
    # arg_bundle_size = {"bundle": len(arg_input), "min": None, "max": None}

    def __init__(self, *args, **kwargs):
        """
        An interactive scan for one or more motors.

        Args:
            *args: devices
            exp_time: exposure time in s
            steps: number of steps (please note: 5 steps == 6 positions)
            relative: Start from an absolute or relative position
            burst_at_each_point: number of acquisition per point

        Returns:
            ScanReport

        Examples:
            >>> scans.open_interactive_scan(dev.motor1, dev.motor2, exp_time=0.1)

        """
        super().__init__(**kwargs)

    def _calculate_positions(self):
        pass

    def run(self):
        yield from self.stubs.open_scan_def()
        self.initialize()
        yield from self.open_scan()
        yield from self.stage()
        yield from self.run_baseline_reading()


class InteractiveTrigger(ScanComponent):
    scan_name = "_interactive_trigger"
    required_kwargs = []

    def __init__(self, *args, **kwargs):
        """
        Send a trigger to all enabled devices that are on softwareTrigger mode.
        """
        super().__init__(**kwargs)

    def run(self):
        yield from self.stubs.trigger(min_wait=self.exp_time)


class InteractiveReadMontiored(ScanComponent):
    scan_name = "_interactive_read_monitored"
    required_kwargs = {"point_id": ScanArgType.INT}

    def __init__(self, *args, monitored: list = None, point_id: int = 0, **kwargs):
        """
        Read the devices that are on readoutPriority "monitored".
        """

        self.monitored = monitored
        super().__init__(**kwargs)
        self.point_id = point_id

    def update_scan_motors(self):
        self.scan_motors = self.monitored if self.monitored else []

    def run(self):
        self.update_readout_priority()
        self.stubs._readout_priority = self.readout_priority
        yield from self.stubs.read(group="monitored", point_id=self.point_id)


class CloseInteractiveScan(ScanComponent):
    scan_name = "_close_interactive_scan"

    def __init__(self, *args, **kwargs):
        """
        An interactive scan for one or more motors.

        Args:
            *args: devices
            exp_time: exposure time in s
            steps: number of steps (please note: 5 steps == 6 positions)
            relative: Start from an absolute or relative position
            burst_at_each_point: number of acquisition per point

        Returns:
            ScanReport

        Examples:
            >>> scans.close_interactive_scan(dev.motor1, dev.motor2, exp_time=0.1)

        """
        super().__init__(**kwargs)

    def run(self):
        yield from self.finalize()
        yield from self.unstage()
        yield from self.close_scan()
        self.cleanup()
        yield from self.stubs.close_scan_def()
