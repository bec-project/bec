# ruff: noqa: N999 -- Preserve the established beamline plugin filename.
import builtins
import time

bec = builtins.__dict__.get("bec")
dev = builtins.__dict__.get("dev")
umv = builtins.__dict__.get("umv")


class XTreme:
    def set_au_mesh(self, val: float):
        """Set the position of gold mesh, used for IO

        Args:
            val (float): target position for gold mesh

        Example:
            >>> xtreme.set_au_mesh(163.0)
        """
        umv(dev.gold_mesh1, val)

    def set_slit(self, val: float):
        """Set the exit slit

        Args:
            val (float): width of the exit slit

        Example:
            >>> xtreme.set_slit(30)
        """
        umv(dev.exit_slit, val)

    def set_har(self, val: float):
        """Set the ID harmonics

        Args:
            val (float): Target value

        Example:
            >>> xtreme.set_har(1.0)
        """
        raise NotImplementedError()

    def set_energy(self, val: float):
        """Set the x-ray energy. Internally, it will adjust the id gap and the mono accordingly.

        Args:
            val (float): Target energy in keV

        Example:
            >>> xtreme.set_energy(700.0)
        """
        raise NotImplementedError()

    def open_valve(self, delay=0.85):
        """Open the valve"""
        dev.valve.set(1).wait()
        time.sleep(delay)
        while True:
            valve_val = dev.valve.read()["valve"]["value"]
            if valve_val == 5:
                break
            print(f"Valve did not open. Current status: {valve_val}. Trying again...")
            time.sleep(1)
        print("Valve opened")

    def close_valve(self, delay=0.85):
        """Close the valve"""
        dev.valve.set(0).wait()
        time.sleep(delay)
        while True:
            valve_val = dev.valve.read()["valve"]["value"]
            if valve_val == 2:
                break
            print(f"Valve did not close. Current status: {valve_val}. Trying again...")
            time.sleep(1)
        print("Valve closed")

    def set_hor(self, val: float):
        """Set the horizontal position the endstation.

        Args:
            val (float): Target value

        Example:
            >>> xtreme.set_hor(104.5)
        """
        umv(dev.sample_hor, val)

    def set_vert(self, val: float):
        """Set the vertical position the sample stick.

        Args:
            val (float): Target value

        Example:
            >>> xtreme.set_vert(7.5)
        """
        umv(dev.sample_vert, val)

    def set_hx(self, val: float):
        """Set the magnetic field (hx)

        Args:
            val (float): Target value.

        Example:
            >>> xtreme.set_hx(104.5)
        """
        umv(dev.field_x, val)

    def set_temp(self, val: float):
        """Set the sample temperature.

        Args:
            val (float): Target value.

        Example:
            >>> xtreme.set_temp(300.0)
        """
        umv(dev.temperature, val)

    def set_fe(self, opening: float):
        """Set the FE aperture. Must be either 0, 0.1, 0.25, 0.5, 1, 1.25 or 2.

        Args:
            opening (float): Opening value in mm.

        Example:
            >>> xtreme.set_fe(1)
        """
        # aperture returns an int, not a string?

        # opening *= 1000
        # opening = int(opening)

        # if opening == 0:
        #     dev.

    def wait_temp(self):
        """Check if the 1K pot is refilling and wait if needed."""

        # Not entirely sure what the script is supposed to do...
        raise NotImplementedError()

    def set_range(self):
        # Could not find the keithley pvs...
        raise NotImplementedError()
