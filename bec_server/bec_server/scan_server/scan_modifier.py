from __future__ import annotations

import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import yaml
from epics import caput

from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.messages import ScanModifierMessage
    from bec_server.scan_server.scans import ScanBase


class ScanModifier:
    """
    A class that is responsible for modifying a scan with various modifiers.
    """

    def __init__(self, scan: ScanBase):
        self.connector = scan.connector
        self.scan = scan
        self.modifiers = defaultdict(list)
        self.update_modifiers()

    def update_modifiers(self):
        """
        Update the modifiers from the redis database
        """
        modifiers = self.connector.xread(MessageEndpoints.scan_modifier(), from_start=True)
        if not modifiers:
            return
        modifier: ScanModifierMessage = modifiers[0].get("data")
        if modifier is None:
            return
        self.modifiers.clear()
        self.modifiers.update(modifier.modifier)
        mods = self.modifiers.get(self.scan.scan_name) or self.modifiers.get("default", {})
        self.parse_modifiers(mods)

    def has_modifier(self, modifier: str) -> bool:
        """
        Check if a modifier exists for a hook
        """
        return self.modifiers.get(modifier) is not None

    def parse_modifiers(self, modifiers: dict) -> None:
        """
        Parse the modifiers from the redis database
        """
        for modifier, hooks in modifiers.items():
            if not isinstance(hooks, dict):
                raise ValueError(f"Invalid modifier {modifier}")
            for hook, args in hooks.items():
                if not isinstance(args, list):
                    raise ValueError(f"Invalid modifier {modifier}")
                for arg in args:
                    mod_type = arg.pop("type")
                    if mod_type is None:
                        raise ValueError(f"Invalid modifier {modifier}")

                    match mod_type:
                        case "DeviceModifier":
                            self.modifiers[hook].append(DeviceModifier(self, **arg))
                        case "EpicsModifier":
                            self.modifiers[hook].append(EpicsModifier(self, **arg))
                        case "DelayModifier":
                            self.modifiers[hook].append(DelayModifier(self, **arg))
                        case _:
                            raise ValueError(f"Invalid modifier {modifier}")


class DeviceModifier:
    """
    A device modifier that can execute any method on a device through RPC calls.
    """

    def __init__(self, modifier: ScanModifier, device: str, operation: str, *args, **kwargs):
        self.modifier = modifier
        self.device = device
        self.operation = operation
        self.args = args
        self.kwargs = kwargs

    def run(self):
        """
        Run the device modifier
        """
        yield from self.modifier.scan.stubs.send_rpc_and_wait(
            self.device, self.operation, **self.kwargs
        )


class DelayModifier:
    """
    A delay modifier that can be used to pause the scan for a specified amount of
    time.
    """

    def __init__(self, modifier: ScanModifier, delay: float):
        self.modifier = modifier
        self.delay = delay

    def run(self):
        """
        Run the delay modifier
        """
        time.sleep(self.delay)


class EpicsModifier:
    """
    An EPICS modifier that can be used to set a PV to a specific value.
    """

    def __init__(self, modifier: ScanModifier, pv_name: str, value: Any, delay: float = 0):
        self.modifier = modifier
        self.pv_name = pv_name
        self.value = value
        self.delay = delay

    def run(self):
        """
        Run the EPICS modifier
        """
        yield caput(self.pv_name, self.value)
        time.sleep(self.delay)


if __name__ == "__main__":  # pragma: no cover
    from bec_lib import messages
    from bec_lib.redis_connector import RedisConnector

    connector = RedisConnector("localhost:6379")
    with open("/Users/wakonig_k/software/work/bec/modifier.yml", "r", encoding="utf-8") as f:
        _modifiers = yaml.safe_load(f)

    msg = messages.ScanModifierMessage(modifier=_modifiers)
    connector.xadd(MessageEndpoints.scan_modifier(), {"data": msg})
