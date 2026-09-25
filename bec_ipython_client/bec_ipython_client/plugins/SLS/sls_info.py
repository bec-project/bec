import builtins

from rich import box
from rich.table import Table

from bec_ipython_client.beamline_mixin import BeamlineShowInfo


class SLSInfo(BeamlineShowInfo):
    def _get_sls_info(self) -> dict:
        dev = builtins.__dict__.get("dev")
        info = dev.sls_info.read(cached=True)
        return info

    def show(self):
        """Display information about the current SLS status"""

        console = self._get_console()

        table = Table(title="SLS Info", box=box.SQUARE)
        table.add_column("Key", justify="left")
        table.add_column("Value", justify="left")

        info = self._get_sls_info()

        self._add_machine_status(table, info)
        self._add_injection_mode(table, info)
        self._add_ring_current(table, info)
        self._add_current_threshold(table, info)
        self._add_current_deadband(table, info)
        self._add_filling_pattern(table, info)
        self._add_filling_lifetime(table, info)
        self._add_ofb_mode(table, info)
        self._add_fofb(table, info)
        self._add_crane_usage(table, info)
        console.print(table)

    def _add_machine_status(self, table, info):
        val = self._get_info_val(info, "sls_info_machine_status")
        if val not in ["Light Available", "Light-Available"]:
            return table.add_row("Machine status", val, style=self.ALARM_STYLE)
        return table.add_row("Machine status", val, style=self.DEFAULT_STYLE)

    def _add_injection_mode(self, table, info):
        val = self._get_info_val(info, "sls_info_injection_mode")
        if val not in ["TOP-UP", "FREQ-REFILL"]:
            return table.add_row("Injection mode", val, style=self.ALARM_STYLE)
        return table.add_row("Injection mode", val, style=self.DEFAULT_STYLE)

    def _add_current_threshold(self, table, info):
        val = info["sls_info_current_threshold"]["value"]
        if val < 350:
            return table.add_row("Current threshold", str(val), style=self.ALARM_STYLE)
        return table.add_row("Current threshold", str(val), style=self.DEFAULT_STYLE)

    def _add_current_deadband(self, table, info):
        val = info["sls_info_current_deadband"]["value"]
        if val > 2:
            return table.add_row("Current deadband", str(val), style=self.ALARM_STYLE)
        return table.add_row("Current deadband", str(val), style=self.DEFAULT_STYLE)

    def _add_filling_pattern(self, table, info):
        val = self._get_info_val(info, "sls_info_filling_pattern")
        return table.add_row("Filling pattern", val, style=self.DEFAULT_STYLE)

    def _add_filling_lifetime(self, table, info):
        val = info["sls_info_filling_life_time"]["value"]
        return table.add_row("SLS filling lifetime", f"{val:.2f} h", style=self.DEFAULT_STYLE)

    def _add_ofb_mode(self, table, info):
        val = self._get_info_val(info, "sls_info_orbit_feedback_mode")
        if val not in ["fast"]:
            return table.add_row("Orbit feedback mode", val, style=self.ALARM_STYLE)
        return table.add_row("Orbit feedback mode", val, style=self.DEFAULT_STYLE)

    def _add_fofb(self, table, info):
        val = self._get_info_val(info, "sls_info_fast_orbit_feedback")
        if val not in ["running"]:
            return table.add_row("Fast orbit feedback", val, style=self.ALARM_STYLE)
        return table.add_row("Fast orbit feedback", val, style=self.DEFAULT_STYLE)

    def _add_ring_current(self, table, info):
        val = info["sls_info_ring_current"]["value"]
        if val < 300:
            return table.add_row("Ring current", f"{val:.3f} mA", style=self.ALARM_STYLE)
        return table.add_row("Ring current", f"{val:.3f} mA", style=self.DEFAULT_STYLE)

    def _add_crane_usage(self, table, info):
        val = self._get_info_val(info, "sls_info_crane_usage")
        return table.add_row("SLS crane usage", val, style=self.DEFAULT_STYLE)


class OperatorInfo(BeamlineShowInfo):
    def _get_operator_messages(self) -> dict:
        dev = builtins.__dict__.get("dev")
        info = dev.sls_operator.read(cached=True)
        return info

    def show(self):
        """Display information about the current SLS status"""

        console = self._get_console()
        table = Table(title="SLS Operator messages", box=box.SQUARE)
        table.add_column("Message", justify="left")
        table.add_column("Time", justify="left")

        info = self._get_operator_messages()

        for i in range(1, 6):
            msg = info[f"sls_operator_messages_message{i}"]["value"]
            date = info[f"sls_operator_date_message{i}"]["value"]
            if msg:
                table.add_row(msg, date)
        if table.row_count == 0:
            table.add_row("No messages available", "")
        console.print(table)
