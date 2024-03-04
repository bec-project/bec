import time

from rich import box
from rich.console import Console
from rich.table import Table

from bec_client.plugins.cSAXS import epics_put, fshclose


class FlomniOpticsMixin:
    @staticmethod
    def _get_user_param_safe(device, var):
        param = dev[device].user_parameter
        if not param or param.get(var) is None:
            raise ValueError(f"Device {device} has no user parameter definition for {var}.")
        return param.get(var)

    def feye_out(self):
        fshclose()
        self.foptics_in()
        feyex_out = self._get_user_param_safe("feyex", "out")
        umv(dev.feyex, feyex_out)

        epics_put("XOMNYI-XEYE-ACQ:0", 2)
        # move rotation stage to zero to avoid problems with wires
        umv(dev.fsamroy, 0)
        # umv(dev.fttrx1, 9.2)

    def feye_in(self):
        bec.queue.next_dataset_number += 1
        # umv(dev.fttrx1, -17)

        feyex_in = self._get_user_param_safe("feyex", "in")
        feyey_in = self._get_user_param_safe("feyey", "in")
        umv(dev.feyex, feyex_in, dev.feyey, feyey_in)
        self._align.update_frame()

    def _ffzp_in(self):
        foptx_in = self._get_user_param_safe("foptx", "in")
        fopty_in = self._get_user_param_safe("fopty", "in")
        umv(dev.foptx, foptx_in)
        umv(
            dev.fopty, fopty_in
        )  # for 7.2567 keV and 150 mu, 60 nm fzp, loptz 83.6000 for propagation 1.4 mm

    def ffzp_in(self):
        """
        move in the flomni zone plate.
        This will disable rt feedback, move the FZP and re-enabled the feedback.
        """
        if "rtx" in dev and dev.rtx.enabled:
            dev.rtx.controller.feedback_disable()

        self._ffzp_in()

        if "rtx" in dev and dev.rtx.enabled:
            dev.rtx.controller.feedback_enable_with_reset()

    def foptics_in(self):
        """
        Move in the flomni optics, including the FZP and the OSA.
        """
        self.ffzp_in()
        self.fosa_in()

    def foptics_out(self):
        """Move out the flomni optics"""
        if "rtx" in dev and dev.rtx.enabled:
            dev.rtx.controller.feedback_disable()

        self.fosa_out()
        fopty_out = self._get_user_param_safe("fopty", "out")
        umv(dev.fopty, fopty_out)

        if "rtx" in dev and dev.rtx.enabled:
            time.sleep(1)
            dev.rtx.controller.feedback_enable_with_reset()

    def fosa_in(self):
        # 6.2 keV, 170 um FZP
        # umv(dev.losax, -1.4450000, dev.losay, -0.1800)
        # umv(dev.losaz, -1)
        # 6.7, 170
        # umv(dev.losax, -1.4850, dev.losay, -0.1930)
        # umv(dev.losaz, 1.0000)
        # 7.2, 150
        fosax_in = self._get_user_param_safe("fosax", "in")
        fosay_in = self._get_user_param_safe("fosay", "in")
        fosaz_in = self._get_user_param_safe("fosaz", "in")
        dev.fosax.limits = [fosax_in - 0.1, fosax_in + 0.1]
        dev.fosay.limits = [fosay_in - 0.1, fosay_in + 0.1]
        dev.fosaz.limits = [fosaz_in - 0.1, fosaz_in + 0.1]
        umv(dev.fosax, fosax_in, dev.losay, fosay_in)
        umv(dev.fosaz, fosaz_in)

        # 11 kev
        # umv(dev.losax, -1.161000, dev.losay, -0.196)
        # umv(dev.losaz, 1.0000)

    def fosa_out(self):
        self.ensure_fheater_up()
        curtain_is_triggered = dev.foptz.controller.fosaz_light_curtain_is_triggered()
        if not curtain_is_triggered:
            fosaz_out = self._get_user_param_safe("fosaz", "out")
            dev.fosaz.limits = [fosaz_out - 0.1, fosaz_out + 0.1]
            umv(dev.fosaz, fosaz_out)
        fosax_out = self._get_user_param_safe("fosax", "out")
        dev.fosax.limits = [fosax_out - 0.1, fosax_out + 0.1]
        umv(dev.fosax, fosax_out)

    def ffzp_info(self):
        foptz_val = dev.foptz.readback.get()
        distance = -foptz_val + 43.15 + 36.7
        print(f"The sample is in a distance of {distance:.1f} mm from the FZP.")

        diameters = [80e-6, 100e-6, 120e-6, 150e-6, 170e-6, 200e-6, 220e-6, 250e-6]

        mokev_val = dev.mokev.readback.get()
        console = Console()
        table = Table(
            title=f"At the current energy of {mokev_val:.4f} keV we have following options:",
            box=box.SQUARE,
        )
        table.add_column("Diameter", justify="center")
        table.add_column("Focal distance", justify="center")
        table.add_column("Current beam size", justify="center")

        wavelength = 1.2398e-9 / mokev_val

        for diameter in diameters:
            outermost_zonewidth = 60e-9
            focal_distance = diameter * outermost_zonewidth / wavelength
            beam_size = (
                -diameter / (focal_distance * 1000) * (focal_distance * 1000 - distance) * 1e6
            )
            table.add_row(
                f"{diameter*1e6:.2f} microns",
                f"{focal_distance:.2f} mm",
                f"{beam_size:.2f} microns",
            )

        console.print(table)

        print("OSA Information:")
        print(
            "The numbers presented here are for a sample in the plane of the flomni sample"
            " holder.\n"
        )