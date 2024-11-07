import enum
import inspect
import time
from typing import Generator, Literal, Protocol

from ophyd import DeviceStatus, Kind
from ophyd_devices.interfaces.protocols.bec_protocols import (
    BECDeviceProtocol,
    BECPositionerProtocol,
    BECSignalProtocol,
)

from bec_lib.devicemanager import DeviceContainer
from bec_lib.tests.utils import ConnectorMock

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


class DeviceMockType(enum.Enum):
    POSITIONER = "positioner"
    SIGNAL = "signal"


class DeviceObjectMock(BECDeviceProtocol):
    def __init__(self, name: str, kind=Kind.normal, dev_type=None, parent=None):
        self._name = name
        self._kind = kind if isinstance(kind, Kind) else getattr(Kind, kind)
        self._parent = parent
        self._dev_type = dev_type
        self._read_only = False
        self._enabled = True
        self._connected = True
        self._destroyed = False

    @property
    def destroyed(self):
        return self._destroyed

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, val):
        self._name = val

    @property
    def kind(self):
        return self._kind

    @kind.setter
    def kind(self, val):
        self._kind = val

    @property
    def parent(self):
        return self._parent

    @property
    def obj(self):
        return self

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, val: bool):
        self._enabled = val

    @property
    def read_only(self):
        return self._read_only

    @read_only.setter
    def read_only(self, val: bool):
        self._read_only = val

    @property
    def hints(self):
        if isinstance(self, PositionerMock):
            return {"fields": [self.name]}
        else:
            return {"fields": []}

    @property
    def root(self):
        return self if self.parent is None else self.parent

    @property
    def connected(self):
        return self._connected

    @connected.setter
    def connected(self, val):
        self._connected = val

    def destroy(self):
        self.connected = False
        self.enabled = False
        for _, obj in inspect.getmembers(self):
            if isinstance(obj, DeviceObjectMock) and obj.destroyed is False:
                obj.destroy()
        self._destroyed = True

    def trigger(self) -> None:
        pass


class MockSignal(DeviceObjectMock, BECSignalProtocol):

    def __init__(
        self,
        name: str,
        value: any = 0,
        kind: Kind = Kind.normal,
        parent=None,
        precision=None,
        readout_priority: Literal["monitored", "baseline", "async"] = "monitored",
        software_trigger: bool = False,
    ):
        super().__init__(name=name, parent=parent, kind=kind)
        self._value = value
        self._config = {
            "deviceConfig": {"limits": [0, 0]},
            "userParameter": None,
            "readoutPriority": readout_priority,
            "softwareTrigger": software_trigger,
        }
        self._metadata = dict(read_access=True, write_access=True, precision=precision)
        self._info = {
            "signals": {},
            "hints": self.hints,
            "write_access": self.write_access,
            "read_access": self.read_access,
        }

    def read(self):
        return {self.name: {"value": self._value, "timestamp": time.time()}}

    def read_configuration(self):
        return self.read()

    def get(self):
        return self._value

    def put(self, value: any, force: bool = False, timeout: float = None):
        self._value = value

    def set(self, value: any, timeout: float = None):
        self._value = value

    @property
    def metadata(self):
        return self._metadata.copy()

    @property
    def write_access(self):
        return self._metadata["write_access"]

    @property
    def read_access(self):
        return self._metadata["read_access"]

    @property
    def hints(self):
        if (~Kind.normal & Kind.hinted) & self.kind:
            return {"fields": [self.name]}
        else:
            return {"fields": []}

    @property
    def limits(self):
        return self._limits

    @limits.setter
    def limits(self, val):
        self._limits = val

    @property
    def low_limit(self):
        return self.limits[0]

    @property
    def high_limit(self):
        return self.limits[1]

    @property
    def precision(self):
        return self._metadata["precision"]

    def check_value(self, value):
        limits = self.limits
        if limits[0] == limits[1]:
            return
        if not limits[0] <= value <= limits[1]:
            raise ValueError(f"Value {value} is outside limits {limits}")


class PositionerMock(DeviceObjectMock, BECPositionerProtocol):

    def __init__(
        self,
        name: str,
        kind: Kind = Kind.normal,
        parent=None,
        readout_priority: Literal["monitored", "baseline", "async"] = "baseline",
        software_trigger: bool = False,
    ):
        super().__init__(name=name, parent=parent, kind=kind)
        # Name of readback signal is the same as the name of the positioner
        self.readback = MockSignal(name=self.name, kind=Kind.normal, value=0, precision=3)
        self.setpoint = MockSignal(name=f"{self.name}_setpoint", kind=Kind.normal, value=0)
        self.motor_is_moving = MockSignal(
            name=f"{self.name}_motor_is_moving", kind=Kind.normal, value=0
        )
        self.velocity = MockSignal(name=f"{self.name}_velocity", kind=Kind.config, value=0)
        self.acceleration = MockSignal(name=f"{self.name}_acceleration", kind=Kind.config, value=0)
        self._config = {
            "deviceConfig": {"limits": [-50, 50]},
            "userParameter": None,
            "readoutPriority": readout_priority,
            "softwareTrigger": software_trigger,
        }
        self._read_attrs = ["readback", "setpoint", "motor_is_moving"]
        self._read_config_attrs = ["velocity", "acceleration"]
        self._info = {
            "signals": {
                name: {
                    "read_access": getattr(self, name).read_access,
                    "write_access": getattr(self, name).write_access,
                }
                for name in self._walk_components()
            },
            "hints": self.hints,
        }

    def _walk_components(self) -> Generator[str, None, None]:
        for name, obj in inspect.getmembers(self):
            if isinstance(obj, MockSignal):
                yield name

    def read(self):
        ret = {}
        for name in self._read_attrs:
            ret.update(getattr(self, name).read())
        return ret

    def read_configuration(self):
        ret = {}
        for name in self._read_config_attrs:
            ret.update(getattr(self, name).read())
        return ret

    def describe_configuration(self) -> dict:
        ret = {}
        for name in self._read_config_attrs:
            ret.update(
                {
                    name: {
                        "source": getattr(self, name).__class__.__name__,
                        "dtype": type(getattr(self, name)._value),
                        "shape": [],
                    }
                }
            )
        return ret

    def describe(self) -> dict:
        ret = {}
        for name in self._read_attrs:
            ret.update(
                {
                    name: {
                        "source": getattr(self, name).__class__.__name__,
                        "dtype": type(getattr(self, name)._value),
                        "shape": [],
                    }
                }
            )
        return ret

    @property
    def full_name(self):
        return self.name

    @property
    def user_parameter(self):
        return self._config["userParameter"]

    @property
    def precision(self):
        return self.readback.precision

    @property
    def limits(self):
        return self._config["deviceConfig"]["limits"]

    @limits.setter
    def limits(self, val: tuple):
        self._config["deviceConfig"]["limits"] = val

    @property
    def low_limit(self):
        return self.limits[0]

    @property
    def high_limit(self):
        return self.limits[1]

    def check_value(self, value):
        limits = self.limits
        if limits[0] == limits[1]:
            return
        if not limits[0] <= value <= limits[1]:
            raise ValueError(f"Value {value} is outside limits {limits}")

    def move(self, position: float, wait=False, **kwargs) -> None:
        self.check_value(position)
        self.setpoint.put(position)
        self.readback.put(position)

    def set(self, position: float, **kwargs):
        self.move(position)


class DMMock:

    def __init__(self):
        self.devices = DeviceContainer()
        self.connector = ConnectorMock()

    def add_device(
        self, name, value=None, dev_type: DeviceMockType = DeviceMockType.POSITIONER, **kwargs
    ):
        if dev_type == DeviceMockType.POSITIONER:
            self.devices[name] = PositionerMock(name=name, **kwargs)
        elif dev_type == DeviceMockType.SIGNAL:
            self.devices[name] = MockSignal(name=name, **kwargs)
        else:
            raise ValueError(f"Unknown device type {dev_type}")
        if value is not None:
            self.devices[name].readback.put(value)
