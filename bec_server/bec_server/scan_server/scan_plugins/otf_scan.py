import time

from bec_lib.logger import bec_logger
from bec_server.scan_server.scans import ScanArgType, ScanBase, SyncFlyScanBase

logger = bec_logger.logger


class OTFScan(SyncFlyScanBase):
    scan_name = "otf_scan"
    required_kwargs = ["e1", "e2", "time"]
    arg_input = {}
    arg_bundle_size = {"bundle": len(arg_input), "min": None, "max": None}

    def __init__(self, *args, parameter: dict = None, **kwargs):
        """Scans the energy from e1 to e2 in <time> minutes.

        Examples:
            >>> scans.otf_scan(e1=700, e2=740, time=4)

        """
        super().__init__(parameter=parameter, **kwargs)
        self.axis = []
        self.scan_motors = []
        self.num_pos = 0
        self.mono = self.caller_kwargs.get("mono", "mono")
        self.otf_device = self.caller_kwargs.get("otf", "otf")

    def pre_scan(self):
        yield None

    @property
    def monitor_sync(self):
        return self.otf_device

    def scan_core(self):
        yield from self.stubs.set(device=self.mono, value=self.caller_kwargs["e1"], wait=True)
        yield from self.stubs.kickoff(
            device=self.otf_device,
            parameter={
                key: val for key, val in self.caller_kwargs.items() if key in ["e1", "e2", "time"]
            },
            wait=True,
        )
        status = yield from self.stubs.complete(device=self.otf_device)

        while not status.done:
            yield from self.stubs.read(group="primary", wait=True)
            progress = self.stubs.get_device_progress(
                device=self.otf_device, RID=self.metadata["RID"]
            )
            if progress:
                self.num_pos = progress
            time.sleep(1)


class HystScan(ScanBase):
    scan_name = "hyst_scan"
    required_kwargs = []
    arg_input = {
        "field_motor": ScanArgType.DEVICE,
        "start_field": ScanArgType.FLOAT,
        "end_field": ScanArgType.FLOAT,
        "mono": ScanArgType.DEVICE,
        "energy1": ScanArgType.FLOAT,
        "energy2": ScanArgType.FLOAT,
    }
    arg_bundle_size = {"bundle": 3, "min": 1, "max": 1}
    scan_type = "step"
    default_ramp_rate = 2

    def __init__(self, *args, parameter: dict = None, **kwargs):
        """
        A hysteresis scan.

        scans.hyst_scan(field_motor, start_field, end_field, mono, energy1, energy2)

        Examples:
            >>> scans.hyst_scan(dev.field_x, 0, 0.5, dev.mono, 600, 640, ramp_rate=2)

        """
        super().__init__(parameter=parameter, **kwargs)
        self.axis = []
        self.flyer = list(self.caller_args.keys())[0]
        self.energy_motor = list(self.caller_args.keys())[1]
        self.scan_motors = [self.energy_motor, self.flyer]
        self.flyer_positions = self.caller_args[self.flyer]
        self._current_scan_motor_index = 0
        self._scan_motor_direction = 1
        self.ramp_rate = self.caller_kwargs.get("ramp_rate", self.default_ramp_rate)

    def _calculate_positions(self) -> None:
        self.positions = [[pos] for pos in self.caller_args[self.energy_motor]]

    def prepare_positions(self):
        self._calculate_positions()
        self.num_pos = 0
        yield None
        self._check_limits()

    def _at_each_point(self):
        yield from self.stubs.read(group="primary", point_id=self.point_id)
        self.point_id += 1

    def _get_next_scan_motor_position(self):
        while True:
            yield self.positions[self._current_scan_motor_index][0]

            if len(self.positions) - 1 == self._current_scan_motor_index or (
                self._current_scan_motor_index == 0 and self._scan_motor_direction < 0
            ):
                self._scan_motor_direction *= -1
            self._current_scan_motor_index += self._scan_motor_direction

    def scan_core(self):
        # yield from self._move_scan_motors_and_wait(self.positions[0])
        status = yield from self.stubs.send_rpc_and_wait(
            "field_x", "ramprate.set", self.default_ramp_rate
        )
        status.wait()
        yield from self.stubs.set(device=self.flyer, value=self.flyer_positions[0], wait=True)

        status = yield from self.stubs.send_rpc_and_wait("field_x", "ramprate.set", self.ramp_rate)
        status.wait()
        # send the slow motor on its way
        status = yield from self.stubs.set(device=self.flyer, value=self.flyer_positions[1])

        pos_generator = self._get_next_scan_motor_position()

        dev = self.device_manager.devices
        while not status.done:
            val = next(pos_generator)
            logger.info(f"Moving mono to {val}.")
            yield from self.stubs.set(device=self.energy_motor, value=val, wait=True)

            monitored_devices = [dev.name for dev in dev.monitored_devices([])]
            yield from self.stubs.read(
                device=[self.flyer, self.scan_motors[0], *monitored_devices],
                point_id=self.point_id,
                wait=True,
            )
            # time.sleep(1)
            self.point_id += 1
            self.num_pos += 1

        status = yield from self.stubs.send_rpc_and_wait(
            "field_x", "ramprate.set", self.default_ramp_rate
        )
        status.wait()

    def return_to_start(self):
        yield None
