# """
# Flomni Fermat scan.

# Scan procedure:
#     - prepare_scan
#     - open_scan
#     - stage
#     - pre_scan
#     - scan_core
#         - at_each_point (optionally called by scan_core)
#     - post_scan
#     - unstage
#     - close_scan
#     - on_exception (called if any exception is raised during the scan)
# """

# from __future__ import annotations

# import time
# from typing import Annotated

# import numpy as np
# from csaxs_bec.devices.epics.delay_generator_csaxs.delay_generator_csaxs import TRIGGERSOURCE

# from bec_lib import messages
# from bec_lib.logger import MessageEndpoints, bec_logger
# from bec_server.scan_server.errors import ScanAbortion
# from bec_server.scan_server.scans import ScanBase, ScanType, Units, scan_hook

# logger = bec_logger.logger


# class FlomniFermatScan(ScanBase):
#     # Scan Type: Hardware triggered or software triggered?
#     # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
#     # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
#     # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
#     # for every point.
#     scan_type = ScanType.HARDWARE_TRIGGERED

#     # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
#     # Choose a descriptive name that does not conflict with existing scan names.
#     scan_name = "flomni_fermat_scan"

#     def __init__(
#         self,
#         fovx: Annotated[float, Units.um],
#         fovy: Annotated[float, Units.um],
#         cenx: Annotated[float, Units.um],
#         ceny: Annotated[float, Units.um],
#         exp_time: Annotated[float, Units.s],
#         step: Annotated[float, Units.um],
#         zshift: Annotated[float, Units.um],
#         angle: Annotated[float, Units.deg],
#         corridor_size: Annotated[float, Units.um] = 3,
#         frames_per_trigger: int = 1,
#         **kwargs,
#     ):
#         """
#         A flomni scan following Fermat's spiral.

#         Args:
#             fovx(float) [um]: Fov in the piezo plane (i.e. piezo range). Max 200 um
#             fovy(float) [um]: Fov in the piezo plane (i.e. piezo range). Max 100 um
#             cenx(float) [um]: center position in x.
#             ceny(float) [um]: center position in y.
#             exp_time(float) [s]: exposure time per burst frame
#             frames_per_trigger(int) : Number of burst frames per point
#             step(float) [um]: stepsize
#             zshift(float) [um]: shift in z
#             angle(float) [deg]: rotation angle (will rotate first)
#             corridor_size(float) [um]: corridor size for the corridor optimization. Default 3 um

#         Returns:

#         Examples:
#             >>> scans.flomni_fermat_scan(fovx=20, fovy=25, cenx=0.02, ceny=0, zshift=0, angle=0, step=0.5, exp_time=0.01, frames_per_trigger=1)
#         """
#         super().__init__(**kwargs)
#         self.axis = []
#         self.fovx = fovx
#         self.fovy = fovy
#         self.cenx = cenx
#         self.ceny = ceny
#         self.step = step
#         self.zshift = zshift
#         self.angle = angle
#         self.optim_trajectory = "corridor"
#         self.optim_trajectory_corridor = corridor_size
#         if self.fovy > 100:
#             raise ScanAbortion("The FOV in y must be smaller than 100 um.")
#         if self.fovx > 200:
#             raise ScanAbortion("The FOV in x must be smaller than 200 um.")
#         if self.zshift > 100:
#             logger.warning("The zshift is larger than 100 um. It will be limited to 100 um.")
#             self.zshift = 100
#         if self.zshift < -100:
#             logger.warning("The zshift is smaller than -100 um. It will be limited to -100 um.")
#             self.zshift = -100
#         self.flomni_rotation_status = None

#         # We update the scan info with the parameters of the scan as provided by the user.
#         self.update_scan_info(exp_time=exp_time, frames_per_trigger=frames_per_trigger)

#     @scan_hook
#     def prepare_scan(self):
#         """
#         Prepare the scan. This can include any steps that need to be executed
#         before the scan is opened, such as preparing the positions (if not done already)
#         or setting up the devices.
#         """
#         positions = self.get_flomni_fermat_spiral_pos(
#             -np.abs(self.fovx / 2),
#             np.abs(self.fovx / 2),
#             -np.abs(self.fovy / 2),
#             np.abs(self.fovy / 2),
#             step=self.step,
#             spiral_type=0,
#             center=False,
#         )

#         if len(positions) < 20:
#             raise ScanAbortion(
#                 f"The number of positions must exceed 20. Currently: {len(positions)}"
#             )

#         positions = self.components.optimize_trajectory(
#             optimization_type=self.optim_trajectory,
#             positions=positions,
#             corridor_size=self.optim_trajectory_corridor,
#             num_iterations=0,
#         )

#         if self.reverse_trajectory():
#             positions = np.flipud(positions)

#         self.update_scan_info(positions=positions, num_points=len(positions))
#         self.positions = positions

#         self.prepare_setup()

#         self._baseline_readout_status = self.actions.read_baseline_devices(wait=False)

#     @scan_hook
#     def open_scan(self):
#         """
#         Open the scan.
#         This step must call self.actions.open_scan() to ensure that a new scan is
#         opened. Make sure to prepare the scan metadata before, either in
#         prepare_scan() or in open_scan() itself.
#         """
#         self.actions.open_scan()

#     @scan_hook
#     def stage(self):
#         """
#         Stage the devices for the upcoming scan. The stage logic is typically
#         implemented on the device itself (i.e. by the device's stage method).
#         However, if there are any additional steps that need to be executed before
#         staging the devices, they can be implemented here.
#         """
#         self.actions.stage_all_devices()

#     @scan_hook
#     def pre_scan(self):
#         """
#         Pre-scan steps to be executed before the main scan logic.
#         This is typically the last chance to prepare the devices before the core scan
#         logic is executed. For example, this is a good place to initialize time-critical
#         devices, e.g. devices that have a short timeout.
#         The pre-scan logic is typically implemented on the device itself.
#         """

#         self.prepare_setup_part_2()
#         self.actions.pre_scan()

#     @scan_hook
#     def scan_core(self):
#         """
#         Core scan logic to be executed during the scan.
#         This is where the main scan logic should be implemented.
#         """
#         self.dev.rt_flyer.kickoff().wait()
#         complete_status = self.dev.rt_flyer.complete()
#         while not complete_status.done:
#             self.at_each_point()
#             time.sleep(1)

#     @scan_hook
#     def at_each_point(self):
#         """
#         Logic to be executed at each point during the scan. This is called by the step_scan method at each point.
#         """
#         self.actions.read_monitored_devices()

#     @scan_hook
#     def post_scan(self):
#         """
#         Post-scan steps to be executed after the main scan logic.
#         """
#         status = self.actions.complete_all_devices(wait=False)

#         # in flomni, we need to move to the start position of the next scan, which is the end position of the current scan
#         if isinstance(self.positions, np.ndarray) and len(self.positions[-1]) == 3:
#             # in x we move to cenx, then we avoid jumps in centering routine
#             value = self.positions[-1]
#             value[0] = self.cenx
#             self.components.move_and_wait(motors=["rtx", "rty", "rtz"], positions=value)

#         # Reset ddg1 to single-shot mode
#         self.dev.ddg1.set_trigger(TRIGGERSOURCE.SINGLE_SHOT.value)

#         status.wait()

#     @scan_hook
#     def unstage(self):
#         """Unstage all devices."""
#         self.actions.unstage_all_devices()

#     @scan_hook
#     def close_scan(self):
#         """Close the scan."""
#         if self._baseline_readout_status is not None:
#             self._baseline_readout_status.wait()
#         self.actions.close_scan()
#         self.actions.check_for_unchecked_statuses()

#     @scan_hook
#     def on_exception(self, exception: Exception):
#         """
#         Handle exceptions that occur during the scan.
#         This is a good place to implement any cleanup logic that needs to be executed in case of an exception,
#         such as returning the devices to a safe state or moving the motors back to their starting position.
#         """

#     #################################################################
#     ############## Helper methods ###################################
#     #################################################################

#     def reverse_trajectory(self):
#         """
#         Reverse the trajectory. Every other scan should be reversed to
#         shorten the movement time. In order to keep the last state, even if the
#         server is restarted, the state is stored in a global variable in redis.
#         """
#         msg = self.redis_connector.get(MessageEndpoints.global_vars("reverse_flomni_trajectory"))
#         if msg:
#             val = msg.content.get("value", False)
#         else:
#             val = False
#         self.redis_connector.set(
#             MessageEndpoints.global_vars("reverse_flomni_trajectory"),
#             messages.VariableMessage(value=(not val)),
#         )
#         return val

#     def prepare_setup(self):
#         """
#         Prepare the first part of the setup:
#         - Clear the trajectory of the rt controller
#         - Rotate the flomni to the requested angle
#         - Move rty to the start position
#         """
#         self.dev.rtx.controller.clear_trajectory_generator()
#         self.flomni_rotation(self.angle)
#         self.dev.rty.set(self.positions[0][1])

#     def prepare_setup_part_2(self):
#         """
#         Prepare the second part of the setup:
#         - Set the delay generator ddg1 to external rising edge
#         - Wait for flomni rotation to complete
#         - Move rtx and rtz to the start position
#         - Turn on the laser tracker
#         - Check the signal strength of the laser tracker and raise an alarm if it is low
#         - Move samx to the scan region
#         """
#         dev = self.dev

#         # Prepare DDG1
#         dev.ddg1.set_trigger(TRIGGERSOURCE.EXT_RISING_EDGE.value)

#         if self.flomni_rotation_status:
#             self.flomni_rotation_status.wait()

#         rtx_status = dev.rtx.set(self.cenx, wait=False)
#         rtz_status = dev.rtz.set(self.positions[0][2], wait=False)

#         dev.rtx.controller.laser_tracker_on()

#         rtx_status.wait()
#         rtz_status.wait()

#         dev.rtx.controller.add_pos_to_scan(self.positions.tolist())

#         tracker_signal_status = dev.rtx.controller.laser_tracker_check_signalstrength()
#         dev.rtx.controller.move_samx_to_scan_region(self.cenx)

#         if tracker_signal_status == "low":
#             error_info = messages.ErrorInfo(
#                 error_message="Signal strength of the laser tracker is low, but sufficient to continue. Realignment recommended!",
#                 compact_error_message="Low signal strength of the laser tracker. Realignment recommended!",
#                 exception_type="LaserTrackerSignalStrengthLow",
#                 device="rtx",
#             )
#             self.device_manager.connector.raise_alarm(severity=Alarms.WARNING, info=error_info)
#         elif tracker_signal_status == "toolow":
#             raise ScanAbortion(
#                 "Signal strength of the laser tracker is too low for scanning. Realignment required!"
#             )

#     def flomni_rotation(self, angle):
#         """
#         Rotate the flomni to the requested angle.
#         We also emit a scan report instruction to keep users informed about the progress of the
#         rotation as it may take a few seconds.

#         Note that we do not wait for the rotation to complete here, but
#         instead wait in prepare_setup_part_2.

#         Args:
#             angle (float): The target angle for the flomni rotation.
#         """
#         fsamroy_current_setpoint = self.dev.fsamroy.user_setpoint.get()
#         if angle == fsamroy_current_setpoint:
#             logger.info("No rotation required")
#             return

#         logger.info("Rotating to requested angle")
#         self.actions.add_scan_report_instruction_readback(
#             devices=["fsamroy"],
#             start=[fsamroy_current_setpoint],
#             stop=[angle],
#             request_id=self.scan_info.metadata["RID"],
#         )
#         self.flomni_rotation_status = self.dev.fsamroy.set(angle, wait=False)

#     def get_flomni_fermat_spiral_pos(
#         self, m1_start, m1_stop, m2_start, m2_stop, step=1, spiral_type=0, center=False
#     ):
#         """
#         Calculate positions for a Fermat spiral scan.

#         Args:
#             m1_start(float): start position in m1
#             m1_stop(float): stop position in m1
#             m2_start(float): start position in m2
#             m2_stop(float): stop position in m2
#             step(float): stepsize
#             spiral_type(int): 0 for traditional Fermat spiral
#             center(bool): whether to include the center position

#         Returns:
#             positions(array): positions
#         """
#         positions = []
#         phi = 2 * np.pi * ((1 + np.sqrt(5)) / 2.0) + spiral_type * np.pi

#         start = int(not center)

#         length_axis1 = np.abs(m1_stop - m1_start)
#         length_axis2 = np.abs(m2_stop - m2_start)
#         n_max = int(length_axis1 * length_axis2 * 3.2 / step / step)

#         z_pos = self.zshift

#         for ii in range(start, n_max):
#             radius = step * 0.57 * np.sqrt(ii)
#             # FOV is restructed below at check pos in range
#             if abs(radius * np.sin(ii * phi)) > length_axis1 / 2:
#                 continue
#             if abs(radius * np.cos(ii * phi)) > length_axis2 / 2:
#                 continue
#             x = radius * np.sin(ii * phi)
#             y = radius * np.cos(ii * phi)
#             positions.append([x + self.cenx, y + self.ceny, z_pos])
#         left_lower_corner = [
#             min(m1_start, m1_stop) + self.cenx,
#             min(m2_start, m2_stop) + self.ceny,
#             z_pos,
#         ]
#         right_upper_corner = [
#             max(m1_start, m1_stop) + self.cenx,
#             max(m2_start, m2_stop) + self.ceny,
#             z_pos,
#         ]
#         positions.append(left_lower_corner)
#         positions.append(right_upper_corner)
#         return np.array(positions)
