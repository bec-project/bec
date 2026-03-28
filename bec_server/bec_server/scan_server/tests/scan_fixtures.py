import copy
import inspect
from collections.abc import Iterator
from types import SimpleNamespace, UnionType
from typing import Annotated, Any, get_args, get_origin, get_type_hints
from unittest import mock

import pytest
from pydantic import BaseModel, Field

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.tests.utils import ConnectorMock
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.scan_assembler import ScanAssembler
from bec_server.scan_server.scan_manager import ScanManager


class ReadoutPriorityContainer(BaseModel):
    monitored_devices: list[str] = Field(default_factory=list)
    baseline_devices: list[str] = Field(default_factory=list)
    on_request_devices: list[str] = Field(default_factory=list)
    continuous_devices: list[str] = Field(default_factory=list)
    async_devices: list[str] = Field(default_factory=list)

    def update_with_dict(self, update_dict: dict[str, list[str]]):
        for key in self.model_fields:
            # we allow for keys to be specified with or without the "_devices" suffix for convenience
            dict_key = key if key in update_dict else key.removesuffix("_devices")
            if dict_key in update_dict:
                setattr(self, key, update_dict[dict_key])


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
def readout_priority() -> ReadoutPriorityContainer:
    return ReadoutPriorityContainer(
        monitored_devices=[],
        baseline_devices=["samx", "samy", "samz"],
        on_request_devices=[],
        async_devices=[],
        continuous_devices=[],
    )


@pytest.fixture
def device_manager(device_manager_class, session_from_test_config):
    service_mock = mock.MagicMock()
    service_mock.connector = ConnectorMock("", store_data=False)
    dev_manager = device_manager_class(service_mock)
    dev_manager._allow_override = True
    dev_manager.config_update_handler = mock.MagicMock()
    dev_manager._session = copy.deepcopy(session_from_test_config)
    dev_manager._load_session()
    dev_manager._custom_devices = {}

    def _add_device(device: DeviceBase, replace=False):
        if not isinstance(device, DeviceBase):
            raise TypeError("device must be an instance of DeviceBase.")
        if device.name in dev_manager.devices and not replace:
            raise ValueError(
                f"Device {device.name!r} already exists. Use replace=True to overwrite it."
            )
        dev_manager.devices[device.name] = device
        dev_manager._custom_devices[device.name] = device

    dev_manager.add_device = _add_device
    yield dev_manager
    dev_manager.shutdown()


class _MockDevice(DeviceBase):
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


class MockCustomDevice(DeviceBase):
    def __init__(
        self,
        name: str,
        device_info: dict,
        signal_read_values: dict[str, float | Iterator] | None = None,
        limits=(-10.0, 10.0),
        precision: int = 3,
        enabled: bool = True,
    ):
        """
        A mock device that implements the DeviceBase interface and allows for custom signal definitions and read values.

        Args:
            name (str): The name of the device.
            device_info (dict): A dictionary containing the device information, including signal definitions. Typically taken from
                the "_info" field, e.g. dev.samx._info["signals"].
            signal_read_values (dict[str, float | Iterator], optional): A dictionary mapping signal names or their corresponding
                obj_names to their read values. If a value is an iterator, the next value will be returned on each read. If not provided,
                signals will return None. Note that the signal name should be the signal name, not the readout name (obj_name).
            limits (tuple, optional): The limits of the device. Defaults to (-10.0, 10.0).
            precision (int, optional): The precision of the device. Defaults to 3.
            enabled (bool, optional): Whether the device is enabled. Defaults to True.
        """
        info = {"device_info": device_info}
        super().__init__(name=name, info=info)
        self._limits = limits
        self._precision = precision
        self._enabled = enabled
        self._signal_read_values = signal_read_values or {}

        for signal_name, signal_info in device_info.get("signals", {}).items():
            signal = getattr(self, signal_name, None)
            if signal is None:
                continue
            obj_name = signal_info.get("obj_name", signal_name)
            signal.get = mock.MagicMock(
                side_effect=lambda signal_name=signal_name, obj_name=obj_name: (
                    self._read_signal_value(signal_name, obj_name)
                )
            )

    def _read_signal_value(self, signal_name: str, obj_name: str):
        value = self._signal_read_values.get(signal_name, self._signal_read_values.get(obj_name))
        if isinstance(value, Iterator):
            return next(value)
        return value

    def set_signal_value(self, signal_name: str, value: Any):
        """
        Set the simulated read value for a signal.

        Args:
            signal_name (str): The name of the signal to set the value for. This should be the signal name, not the obj_name.
            value (Any): The value to set for the signal. This can be a single value or an iterator for multiple reads.
        """
        if signal_name not in self._info.get("signals", {}):
            raise ValueError(f"Signal {signal_name!r} is not defined in the device info.")
        self._signal_read_values[signal_name] = value

    def read(self, *args, **kwargs):
        data = {}
        for signal_name, signal_info in self._info.get("signals", {}).items():
            kind = signal_info.get("kind_str", "").lower()
            if kind in {"config", "omitted"}:
                continue
            obj_name = signal_info.get("obj_name", signal_name)
            value = self._read_signal_value(signal_name, obj_name)
            data[obj_name] = {"value": value}
        return data

    def read_configuration(self, *args, **kwargs):
        data = {}
        for signal_name, signal_info in self._info.get("signals", {}).items():
            kind = signal_info.get("kind_str", "").lower()
            if kind != "config":
                continue
            obj_name = signal_info.get("obj_name", signal_name)
            value = self._read_signal_value(signal_name, obj_name)
            data[obj_name] = {"value": value}
        return data

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


class _MockDeviceContainer(dict):
    def __init__(self, devices: dict[str, DeviceBase], readout_priority: ReadoutPriorityContainer):
        super().__init__(devices)
        self._base_readout_priority = {
            "baseline": list(readout_priority.baseline_devices),
            "monitored": list(readout_priority.monitored_devices),
            "on_request": list(readout_priority.on_request_devices),
            "continuous": list(readout_priority.continuous_devices),
            "async": list(readout_priority.async_devices),
        }

    @property
    def enabled_devices(self):
        return list(self.values())

    def _applied_readout_priority(self, readout_priority=None) -> dict[str, list[str]]:
        groups = {
            group_name: [device_name for device_name in device_names if device_name in self]
            for group_name, device_names in self._base_readout_priority.items()
        }

        for group_name in ["baseline", "monitored", "on_request", "continuous", "async"]:
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

    def continuous_devices(self, readout_priority=None):
        continuous = self._applied_readout_priority(readout_priority)["continuous"]
        return [self[name] for name in continuous if name in self]

    def on_request_devices(self, readout_priority=None):
        on_request = self._applied_readout_priority(readout_priority)["on_request"]
        return [self[name] for name in on_request if name in self]

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
        if arg_type is None:
            return False
        origin = get_origin(arg_type)
        if origin is Annotated:
            args = get_args(arg_type)
            if not args:
                return False
            converted = args[0]
        elif origin is UnionType:
            args = get_args(arg_type)
            converted = next(
                (arg for arg in args if inspect.isclass(arg) and issubclass(arg, DeviceBase)), None
            )
            if converted is None:
                return False
        else:
            converted = arg_type
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


@pytest.fixture
def v4_scan_assembler(readout_priority: ReadoutPriorityContainer, device_manager, tmpdir):
    scan_classes = {cls.scan_name: cls for _, cls in ScanManager.get_available_scans()}

    def _assemble_scan(scan_type, *scan_args, **scan_kwargs):
        scan_id = scan_kwargs.pop("scan_id", "scan-id-test")

        try:
            scan_cls = scan_classes[scan_type]
        except KeyError as exc:
            available = ", ".join(sorted(scan_classes))
            raise KeyError(f"Unknown scan type '{scan_type}'. Available: {available}") from exc

        connector = ConnectorMock("")
        instruction_handler = InstructionHandler(connector)
        device_names = sorted(
            set(_infer_v4_device_names(scan_cls, scan_args, scan_kwargs))
            | set(readout_priority.monitored_devices)
            | set(readout_priority.baseline_devices)
            | set(readout_priority.continuous_devices)
            | set(readout_priority.on_request_devices)
            | set(readout_priority.async_devices)
        )
        custom_devices = getattr(device_manager, "_custom_devices", {})
        preloaded_devices = {
            name: custom_devices[name] for name in device_names if name in custom_devices
        }
        devices = _MockDeviceContainer(preloaded_devices, readout_priority=readout_priority)
        for name in device_names:
            if name in devices:
                continue
            devices[name] = _MockDevice(name)
        scan_device_manager = SimpleNamespace(devices=devices, connector=connector)
        resolved_scan_kwargs = {"system_config": {}, **scan_kwargs}

        parent = mock.MagicMock()
        parent.device_manager = scan_device_manager
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

        # we pretend that the queue is scheduling the scan immediately,
        # so we can assign the scan number and dataset number here for testing purposes
        scan.scan_info.scan_number = 20
        scan.scan_info.dataset_number = 15

        scan.actions._get_file_base_path = mock.MagicMock(
            return_value=str(tmpdir / "p12345" / "data")
        )

        scan._test = SimpleNamespace(
            connector=connector,
            instruction_handler=instruction_handler,
            device_manager=scan_device_manager,
            assembler=assembler,
        )
        return scan

    return _assemble_scan
