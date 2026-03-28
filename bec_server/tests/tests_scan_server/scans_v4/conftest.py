import importlib
import inspect
import pkgutil
from types import SimpleNamespace
from typing import get_type_hints
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.tests.utils import ConnectorMock
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.scan_assembler import ScanAssembler
from bec_server.scan_server.scan_gui_models import GUIInput
from bec_server.scan_server.scans import ScanArgType
from bec_server.scan_server.scans.scans_v4 import ScanBase as ScanBaseV4


class _DoneAfterNthCheckStatusMock:
    def __init__(self, resolve_after: int = 1, result=None) -> None:
        self.resolve_after = max(resolve_after, 1)
        self.result = result
        self.wait_calls = 0
        self._done_checks = 0

    @property
    def done(self):
        self._done_checks += 1
        return self._done_checks >= self.resolve_after

    def wait(self, *args, **kwargs):
        self.wait_calls += 1
        return self


@pytest.fixture
def nth_done_status_mock():
    def _build(resolve_after: int = 1, result=None):
        return _DoneAfterNthCheckStatusMock(resolve_after=resolve_after, result=result)

    return _build


@pytest.fixture
def readout_priority():
    return SimpleNamespace(
        monitored=[], baseline=["samx", "samy", "samz"], on_request=[], async_=[]
    )


class _MockV4Device(DeviceBase):
    def __init__(self, name: str, limits=(-10.0, 10.0), value: float = 0.0):
        info = {
            "device_info": {
                "signals": {
                    name: {"obj_name": name, "kind_str": "hinted", "describe": {"precision": 3}}
                }
            }
        }
        super().__init__(name=name, info=info)
        self._limits = limits
        self._value = value
        self._enabled = True
        self._precision = 3

    def read(self, *args, **kwargs):
        return {self.full_name: {"value": self._value}}

    @property
    def root(self):
        return self

    @property
    def full_name(self):
        return self.name

    @property
    def limits(self):
        return self._limits

    @property
    def enabled(self):
        return self._enabled

    @property
    def precision(self):
        return self._precision


class _MockV4Devices(dict):
    def __init__(self, devices: dict[str, _MockV4Device], readout_priority: dict | None = None):
        super().__init__(devices)
        readout_priority = readout_priority or {}
        self._base_readout_priority = {
            "baseline": list(readout_priority.get("baseline", [])),
            "monitored": list(readout_priority.get("monitored", [])),
            "on_request": list(readout_priority.get("on_request", [])),
            "async": list(readout_priority.get("async", [])),
        }

    @property
    def enabled_devices(self):
        return list(self.values())

    def _applied_readout_priority(self, readout_priority=None) -> dict[str, list[str]]:
        groups = {
            group_name: [device_name for device_name in device_names if device_name in self]
            for group_name, device_names in self._base_readout_priority.items()
        }

        for group_name in ["baseline", "monitored", "on_request", "async"]:
            for device_name in (readout_priority or {}).get(group_name, []):
                if device_name not in self:
                    continue
                for existing_group in groups.values():
                    if device_name in existing_group:
                        existing_group.remove(device_name)
                groups[group_name].append(device_name)

        for group_name, device_names in groups.items():
            groups[group_name] = sorted(set(device_names))
        return groups

    def monitored_devices(self, readout_priority=None):
        monitored = self._applied_readout_priority(readout_priority)["monitored"]
        return [self[name] for name in monitored if name in self]

    def baseline_devices(self, readout_priority=None):
        baseline = self._applied_readout_priority(readout_priority)["baseline"]
        return [self[name] for name in baseline if name in self]

    def async_devices(self, readout_priority=None):
        async_devices = self._applied_readout_priority(readout_priority)["async"]
        return [self[name] for name in async_devices if name in self]

    def on_request_devices(self, readout_priority=None):
        on_request = self._applied_readout_priority(readout_priority)["on_request"]
        return [self[name] for name in on_request if name in self]

    def continuous_devices(self, readout_priority=None):
        return []

    def get_software_triggered_devices(self):
        return []


def _infer_v4_device_names(scan_cls, scan_args: tuple, scan_kwargs: dict) -> list[str]:
    arg_input = getattr(scan_cls, "arg_input", {}) or {}
    if not arg_input:
        type_hints = get_type_hints(scan_cls.__init__)
        signature = inspect.signature(scan_cls)
        arg_input = {
            name: type_hints.get(name, parameter.annotation)
            for name, parameter in signature.parameters.items()
            if name not in {"args", "kwargs"}
            and parameter.annotation is not inspect.Parameter.empty
        }
    if not arg_input:
        return []

    device_names = []
    bundle_size = scan_cls.arg_bundle_size["bundle"]

    def _is_device_arg(arg_type) -> bool:
        converted = GUIInput.convert_to_legacy_scan_arg_type(arg_type)
        if converted == ScanArgType.DEVICE:
            return True
        return inspect.isclass(converted) and issubclass(converted, DeviceBase)

    if bundle_size > 0:
        arg_names = list(arg_input.keys())
        for bundle_start in range(0, len(scan_args), bundle_size):
            for offset, arg_name in enumerate(arg_names):
                arg_index = bundle_start + offset
                if arg_index >= len(scan_args):
                    break
                if _is_device_arg(arg_input.get(arg_name)):
                    device_names.append(scan_args[arg_index])
    else:
        bound = inspect.signature(scan_cls).bind_partial(*scan_args, **scan_kwargs)
        for arg_name, value in bound.arguments.items():
            if arg_name == "args":
                continue
            if _is_device_arg(arg_input.get(arg_name)):
                device_names.append(value)

    for arg_name, arg_type in arg_input.items():
        if _is_device_arg(arg_type) and arg_name in scan_kwargs:
            device_names.append(scan_kwargs[arg_name])

    return [name for name in device_names if isinstance(name, str)]


def _base_readout_priority(readout_priority) -> dict[str, list[str]]:
    return {
        "monitored": list(readout_priority.monitored),
        "baseline": list(readout_priority.baseline),
        "on_request": list(readout_priority.on_request),
        "async": list(readout_priority.async_),
    }


def _get_v4_scan_classes() -> dict[str, type[ScanBaseV4]]:
    import bec_server.scan_server.scans as scans_v4_module

    scan_classes = {}
    for module_info in pkgutil.iter_modules(
        scans_v4_module.__path__, prefix=f"{scans_v4_module.__name__}."
    ):
        module = importlib.import_module(module_info.name)
        for _, scan_cls in inspect.getmembers(module, predicate=inspect.isclass):
            if scan_cls.__module__ != module.__name__:
                continue
            if not issubclass(scan_cls, ScanBaseV4):
                continue
            scan_name = getattr(scan_cls, "scan_name", None)
            if not scan_name or scan_name == "_v4_base_scan":
                continue
            scan_classes[scan_name] = scan_cls
            if scan_name.startswith("_v4_"):
                scan_classes[scan_name.removeprefix("_v4_")] = scan_cls
    return scan_classes


@pytest.fixture
def v4_scan_assembler(readout_priority):
    scan_classes = _get_v4_scan_classes()

    def _assemble_scan(scan_type, *scan_args, **scan_kwargs):
        scan_id = scan_kwargs.pop("scan_id", "scan-id-test")

        try:
            scan_cls = scan_classes[scan_type]
        except KeyError as exc:
            available = ", ".join(sorted(scan_classes))
            raise KeyError(f"Unknown v4 scan type '{scan_type}'. Available: {available}") from exc

        connector = ConnectorMock("")
        instruction_handler = InstructionHandler(connector)
        base_readout_priority = _base_readout_priority(readout_priority)
        device_names = sorted(
            set(_infer_v4_device_names(scan_cls, scan_args, scan_kwargs))
            | set(base_readout_priority["monitored"])
            | set(base_readout_priority["baseline"])
            | set(base_readout_priority["on_request"])
            | set(base_readout_priority["async"])
        )
        devices = _MockV4Devices(
            {name: _MockV4Device(name) for name in device_names},
            readout_priority=base_readout_priority,
        )
        device_manager = SimpleNamespace(devices=devices, connector=connector)
        resolved_scan_kwargs = {
            "system_config": {"file_directory": "/tmp/data/S00000"},
            **scan_kwargs,
        }

        parent = mock.MagicMock()
        parent.device_manager = device_manager
        parent.connector = connector
        parent.queue_manager.instruction_handler = instruction_handler
        parent.scan_manager = SimpleNamespace(scan_dict={scan_type: scan_cls})

        assembler = ScanAssembler(parent=parent)
        msg = messages.ScanQueueMessage(
            metadata={"RID": "rid-test"},
            scan_type=scan_type,
            parameter={"args": list(scan_args), "kwargs": resolved_scan_kwargs},
            queue="primary",
        )
        scan = assembler.assemble_direct_scan(msg, scan_id)
        scan._test = SimpleNamespace(
            connector=connector,
            instruction_handler=instruction_handler,
            device_manager=device_manager,
            assembler=assembler,
        )
        return scan

    return _assemble_scan
