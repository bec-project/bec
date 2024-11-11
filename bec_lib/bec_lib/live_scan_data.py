"""
This module contains the classes for storing scan data from scan_segments. 
"""

from __future__ import annotations

import collections
from typing import TYPE_CHECKING, Any

from _collections_abc import dict_items, dict_keys, dict_values

if TYPE_CHECKING:
    from bec_lib import messages


class SignalData:
    """
    SignalData is a container for storing signal data.
    """

    def __init__(self):
        self.metadata = {}
        self.scan_id = None
        self.num_points = None
        self.data = {}

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return self.data.get(key)
        if key in ["val", "value"]:
            return self.val
        if key == "timestamp":
            return self.timestamps
        return self.get(key)

    @property
    def val(self):
        """return a list of values of the signal data"""
        return [self.data.get(index, {}).get("value") for index in sorted(self.data.keys())]

    @property
    def timestamps(self):
        """return a list of timestamps of the signal data"""
        return [self.data.get(index, {}).get("timestamp") for index in sorted(self.data.keys())]

    def get(self, index: Any, default=None) -> dict:
        """
        Get the signal data at the given index.

        Args:
            index(int): the index of the scan point

        Returns:
            dict: the signal data at the given index
        """
        if isinstance(index, int):
            return self.data.get(index, default)
        if index in ["val", "value"]:
            return self.val
        if index == "timestamp":
            return self.timestamps
        return self.get(index, default)

    def set(self, index: Any, device_data: dict) -> None:
        """
        Set the signal data at the given index to the given device data.

        Args:
            index(int): the index of the scan point
            device_data(dict): the device data to store

        """

        self.data[index] = device_data

    def __eq__(self, __value: object) -> bool:
        return self.data == __value

    def items(self) -> dict_items:
        return self.data.items()

    def keys(self) -> dict_keys:
        return self.data.keys()

    def values(self) -> dict_values:
        return self.data.values()

    def val_to_dict(self) -> dict:
        return {k: v.get("value") for k, v in self.data.items()}

    def __str__(self) -> str:
        return f"{self.data}"

    def __len__(self) -> int:
        return len(self.data)


class DeviceData(dict):
    """
    DeviceData is a container for storing device data.
    """

    def __init__(self):
        self.__signals = collections.defaultdict(SignalData)
        super().__init__()

    def __getitem__(self, key: Any) -> Any:
        if key in self.__signals:
            return self.__signals[key]
        if isinstance(key, int):
            return {signal: val.get(key) for signal, val in self.__signals.items()}
        return self.get(key)

    def __setattr__(self, attr: Any, value: Any) -> None:
        self.__setitem__(attr, value)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        super().__setitem__(__key, __value)
        self.__dict__.update({__key: __value})

    def __delitem__(self, __key: Any) -> None:
        super().__delitem__(__key)
        del self.__dict__[__key]

    def get(self, index: Any, default=None) -> Any:
        if index in self.__signals:
            return self.__signals[index]
        if isinstance(index, int):
            return {signal: val.get(index) for signal, val in self.__signals.items()}
        return self.__signals.get(index, default)

    def set(self, index: Any, signals: dict) -> None:
        for signal, signal_data in signals.items():
            self.__signals[signal].set(index, signal_data)
            self.__setattr__(signal, self.__signals[signal])

    def __str__(self) -> str:
        return f"{dict(self.__signals)}"

    def __eq__(self, ref_data: object) -> bool:
        return {name: self.__signals[name].data for name in self.__signals} == ref_data

    def keys(self) -> dict_keys:
        return self.__signals.keys()

    def items(self) -> dict_items:
        return self.__signals.items()

    def values(self) -> dict_values:
        return self.__signals.values()


class LiveScanData(dict):
    """
    LiveScanData is a container for storing scan data.
    """

    def __init__(self, *args, **kwargs):
        self.devices = collections.defaultdict(DeviceData)
        self.messages = collections.defaultdict()
        super().__init__(*args, **kwargs)

    def get(self, index: Any, default=None) -> Any:
        if index in self.devices:
            return self.devices.get(index, default)
        if isinstance(index, int) and index in self.messages:
            return self.messages.get(index)
        return self.devices.get(index, default)

    def __getitem__(self, key: Any) -> Any:
        if key in self.devices:
            return self.devices[key]
        return self.get(key)

    def __contains__(self, key: Any) -> bool:
        if key in self.devices:
            return True
        return False

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value)

    def __setattr__(self, attr: Any, value: Any) -> None:
        self.__setitem__(attr, value)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        super().__setitem__(__key, __value)
        self.__dict__.update({__key: __value})

    def __delitem__(self, __key: Any) -> None:
        super().__delitem__(__key)
        del self.__dict__[__key]

    def set(self, index: Any, message: messages.BECMessage) -> None:
        """
        Set the scan data at the given index to the values in the given content.

        Args:
            index(int): the index of the scan point
            content(dict): the content to store

        """
        if not isinstance(index, int):
            raise TypeError("ScanData can only store data with integer indices.")

        self.messages[index] = message
        for dev, dev_data in message.content["data"].items():
            self.devices[dev].set(index, dev_data)
            self.__setattr__(dev, self.devices[dev])

    def keys(self) -> dict_keys:
        return self.devices.keys()

    def items(self) -> dict_items:
        return self.devices.items()

    def values(self) -> dict_values:
        return self.devices.values()

    def __str__(self) -> str:
        return f"{dict(self.devices)}"

    def __len__(self) -> int:
        return len(self.messages)
