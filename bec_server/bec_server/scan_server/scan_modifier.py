from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal

import yaml
from epics import caput

if TYPE_CHECKING:
    from bec_server.scan_server.scans import ScanBase


class ScanModifier:
    def __init__(self, scan: ScanBase):
        self.connector = scan.connector
        self.scan = scan
        self.modifiers = {}
        self.update_modifiers()

    def update_modifiers(self):
        """
        Update the modifiers from the redis database
        """
        with open("/Users/wakonig_k/software/work/bec/modifier.yml", "r") as f:
            modifiers = yaml.safe_load(f)
        self.modifiers = self.parse_modifiers(modifiers)

    def has_modifier(self, modifier: str) -> bool:
        """
        Check if a modifier exists for a hook
        """
        return self.modifiers.get(modifier) is not None

    def parse_modifiers(self, modifiers: dict) -> dict:
        """
        Parse the modifiers from the redis database
        """
        for modifier, hooks in modifiers.items():
            if not isinstance(hooks, dict):
                raise ValueError(f"Invalid modifier {modifier}")
            for hook, args in hooks.items():
                if not isinstance(args, list):
                    raise ValueError(f"Invalid modifier {modifier}")
                mods = []
                for arg in args:
                    mod_type = arg.pop("type")
                    if mod_type == "DeviceModifier":
                        mods.append(DeviceModifier(self, **arg))
                    elif mod_type == "EpicsModifier":
                        mods.append(EpicsModifier(self, **arg))
                    else:
                        raise ValueError(f"Invalid modifier {modifier}")
                hooks[hook] = mods
        return modifiers


class DeviceModifier:
    def __init__(self, modifier: ScanModifier, device: str, operation: str, *args, **kwargs):
        self.modifier = modifier
        self.device = device
        self.operation = operation
        self.args = args
        self.kwargs = kwargs

    def run(self):
        yield from self.modifier.scan.stubs.send_rpc_and_wait(
            self.device, self.operation, **self.kwargs
        )


class EpicsModifier:
    def __init__(self, modifier: ScanModifier, pv_name: str, value: Any, delay: float = 0):
        self.modifier = modifier
        self.pv_name = pv_name
        self.value = value
        self.delay = delay

    def run(self):
        yield caput(self.pv_name, self.value)
        time.sleep(self.delay)
