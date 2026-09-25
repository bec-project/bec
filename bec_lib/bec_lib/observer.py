"""
This module provides the Observer class and the ObserverManager class. The Observer class is used
to define an observer object, which is used to monitor a device for a specific condition. The
ObserverManager class is used to manage the observer objects.
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any

from typeguard import typechecked

from bec_lib.device import Device
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.devicemanager import DeviceManagerBase


class ObserverAction(str, enum.Enum):
    PAUSE = "pause"
    RESUME = "resume"
    RESTART = "restart"


class Observer:
    def __init__(
        self,
        name: str,
        device: str,
        on_trigger: ObserverAction | None = None,
        on_resume: ObserverAction | None = None,
        limits: list | None = None,
        low_limit: float | None = None,
        high_limit: float | None = None,
        target_value=None,
    ):
        self.name = name
        self.device = device
        self.on_trigger = on_trigger
        self.on_resume = on_resume
        self._limits = [None, None]

        self._check_limits(limits, low_limit, high_limit)

        self.target_value = target_value
        self._enabled = False

        self._check_device()
        self._check_trigger()
        self._check_targets()

    @property
    def limits(self):
        return self._limits

    @limits.setter
    @typechecked
    def limits(self, val: list[Any]):
        self._limits = val

    @property
    def low_limit(self):
        return self.limits[0]

    @low_limit.setter
    @typechecked
    def low_limit(self, val: float):
        self.limits[0] = val

    @property
    def high_limit(self):
        return self.limits[1]

    @high_limit.setter
    @typechecked
    def high_limit(self, val: float):
        self.limits[1] = val

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, val: bool):
        self._enabled = val

    def _check_limits(self, limits, low_limit, high_limit):
        if limits is not None and (low_limit is not None or high_limit is not None):
            raise AttributeError(
                "Ambiguous condition: Limits are set multiple times. Use either limits or"
                " low_limit/high_limit."
            )
        if limits is not None:
            self.limits = limits
        if low_limit is not None:
            self.low_limit = low_limit
        if high_limit is not None:
            self.high_limit = high_limit

    def _check_device(self):
        if isinstance(self.device, Device):
            self.device = self.device.name

    def _check_trigger(self):
        if not isinstance(self.on_trigger, ObserverAction):
            self.on_trigger = ObserverAction(self.on_trigger)
        if not isinstance(self.on_resume, ObserverAction):
            self.on_resume = ObserverAction(self.on_resume)

    def _check_targets(self):
        if self.limits is not None and self.target_value is not None:
            raise AttributeError(
                "Ambiguous condition: Only one target (limits, target_value) can be set."
            )
        if all(val is None for val in self.limits) and self.target_value is None:
            raise AttributeError("No condition set.")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "device": self.device,
            "on_trigger": self.on_trigger,
            "on_resume": self.on_resume,
            "limits": self.limits,
            "target_value": self.target_value,
        }

    @classmethod
    def from_dict(cls, config: dict) -> Observer:
        return cls(**config)


class ObserverManager:
    def __init__(self, device_manager: DeviceManagerBase):
        self.device_manager = device_manager
        self._observer = self._get_installed_observer()

    @typechecked
    def add_observer(self, observer: Observer, ignore_existing: bool = False):
        if observer.device not in self.device_manager.devices:
            raise AttributeError(
                f"The specified observer uses device {observer.device} which is currently not"
                " configured."
            )
        if self._is_device_observed(observer.device) and not ignore_existing:
            raise AttributeError(
                f"Device {observer.device} is already being observed. If you want to add an"
                " additional observer for this device, use 'ignore_existing=True'."
            )
        self._observer.append(observer)
        self.update_observer()

    def _is_device_observed(self, device: str) -> bool:
        return any(obs.device == device for obs in self._observer)

    def _get_installed_observer(self):
        # get current observer list from Redis
        observer_msg = self.device_manager.connector.get(MessageEndpoints.observer())
        if observer_msg is None:
            return []
        return [Observer.from_dict(obs) for obs in observer_msg.content["observer"]]

    def update_observer(self):
        # send the current observer list to MongoDB and Redis
        pass

    def list_observer(self):
        pass
