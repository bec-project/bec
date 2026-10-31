"""Compatibility module alias; implementation belongs to the device library."""

import sys
from importlib import import_module

sys.modules[__name__] = import_module("ophyd_devices.bec_adapters.diagnostics")
