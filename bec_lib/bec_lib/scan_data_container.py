"""
This module contains classes for accessing data in an HDF5 file.
"""

from __future__ import annotations

import copy
import datetime
import importlib
import time
from collections import deque
from collections.abc import Iterable
from functools import wraps
from typing import TYPE_CHECKING, Any, Dict, Literal, Tuple

import h5py
from _collections_abc import dict_items, dict_keys
from prettytable import PrettyTable

from bec_lib.logger import bec_logger

if TYPE_CHECKING:
    from bec_lib import messages

logger = bec_logger.logger


class DataCache:
    """
    Data cache for repeated file reads, implementing a least-recently-used cache.
    This class is a singleton and stores the estimated memory usage of the
    data cache by reading the HDF5 file and group sizes. The cache is cleared
    when the memory usage exceeds a certain threshold.
    """

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(DataCache, cls).__new__(cls)
        return cls._instance

    def __init__(self, max_memory: int = 1e9) -> None:
        self._cache = deque()
        self._memory_usage = 0
        self._max_memory = max_memory

    def add_item(self, key: str, value: Any, memory_usage: int) -> None:
        """
        Add an item to the cache.

        Args:
            key (str): The key to store the item under.
            value (Any): The value to store.
            memory_usage (int): The memory usage of the item.
        """
        self._cache.appendleft((key, value, memory_usage))
        self._memory_usage += memory_usage
        if self._memory_usage > self._max_memory:
            self.run_cleanup()

    def get_item(self, key: str) -> Any:
        """
        Get an item from the cache and move it to the front of the cache.

        Args:
            key (str): The key to get the item from.

        Returns:
            Any: The item.
        """
        for i, (item_key, item_value, _) in enumerate(self._cache):
            if item_key == key:
                self._cache.rotate(-i)
                return item_value
        return None

    def run_cleanup(self) -> None:
        """
        Run the cache cleanup and remove the least-recently-used items.
        """
        while self._memory_usage > self._max_memory:
            _, _, memory_usage = self._cache.pop()
            self._memory_usage -= memory_usage

    def clear_cache(self) -> None:
        """
        Clear the cache.
        """
        self._cache.clear()
        self._memory_usage = 0


_file_cache = DataCache()


def retry_file_access(func):
    """
    Retry accessing the file three times with a 0.1 second delay between retries.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        for _ in range(3):
            try:
                return func(*args, **kwargs)
            # pylint: disable=broad-except
            except Exception as e:
                logger.info(f"Error accessing file: {e}")
                time.sleep(0.1)
        raise RuntimeError("Error accessing file.")

    return wrapper


class FileReference:
    """
    This class is a wrapper around an HDF5 file reference, adding convenience methods for accessing the
    data in the file.
    """

    def __init__(self, file_path: str) -> None:
        """
        Initialize the FileReference object.

        Args:
            file_path (str): The path to the HDF5 file.
        """
        self.file_path = file_path

    @retry_file_access
    def read(self, entry_path: str, cached=True, entry_filter: list[str] | None = None) -> Any:
        """
        Recurisively read the data from the HDF5 file and return it as a dictionary.

        Args:
            entry_path (str): The path to the entry in the HDF5 file, e.g. "entry/collection/devices/samx".
            cached (bool): Whether to use the cache for reading the data.
            entry_filter (list[str], optional): The dict entry to further filter the data. Defaults to None.

        Returns:
            dict: The data from the HDF5 file.
        """
        if cached:
            out = _file_cache.get_item(f"{self.file_path}::{entry_path}")
            if out is not None:
                return self._filter_entry(out, entry_filter)
        out = {}
        with h5py.File(self.file_path, "r") as f:
            entry = f[entry_path]
            if isinstance(entry, h5py.Group):
                out, size = self._read_group(entry)
            elif isinstance(entry, h5py.Dataset):
                # TODO: Add here a safeguard for large datasets to avoid loading them into memory all at once
                out = self._read_value(entry)
                size = entry.size * entry.dtype.itemsize
            else:
                raise ValueError(f"Entry at {entry_path} is not a group or dataset.")

        _file_cache.add_item(f"{self.file_path}::{entry_path}", out, size)
        return self._filter_entry(out, entry_filter)

    def _filter_entry(self, entry: Any, entry_filter: list[str]) -> Any:
        """
        Filter the entry by the filter list.

        Args:
            entry (Any): The entry to filter.
            entry_filter (list[str]): The list of keys to filter the entry by.

        Returns:
            Any: The filtered entry.
        """
        if not entry_filter:
            return copy.deepcopy(entry)
        out = entry
        for key in entry_filter:
            out = out.get(key)
        return copy.deepcopy(out)

    def _read_group(self, group: h5py.Group) -> Tuple[Dict[str, Any], int]:
        """
        Recursively read the data from a group in the HDF5 file and return it as a dictionary.
        It also returns the memory usage of the group as specified by the HDF5 file.

        Args:
            group (h5py.Group): The group to read the data from.

        Returns:
            Tuple[Dict[str, Any], int]: The data from the group and the memory usage of the group.
        """
        out = {}
        size = 0
        for key, value in group.items():
            if value is None:
                continue
            if isinstance(value, h5py.Group):
                out[key], group_size = self._read_group(value)
                size += group_size
            else:
                out[key] = self._read_value(value)
                size += value.size * value.dtype.itemsize
        return out, size

    def _read_value(self, value: h5py.Dataset) -> Any:
        """
        Read the value from a dataset in the HDF5 file.

        Args:
            value (h5py.Dataset): The dataset to read the value from.

        Returns:
            Any: The value of the dataset.
        """
        out = value[()]
        if isinstance(out, bytes):
            try:
                out = out.decode("utf-8")
            except UnicodeDecodeError:
                pass
        return out

    @retry_file_access
    def get_hdf5_structure(self) -> dict:
        """
        Get the structure of the HDF5 file.

        Returns:
            dict: The structure of the HDF5 file.
        """
        with h5py.File(self.file_path, "r") as f:
            return self._get_hdf5_structure(f)

    def _get_hdf5_structure(self, group: h5py.Group) -> dict:
        """
        Recursively get the structure of the HDF5 file.

        Args:
            group (h5py.Group): The group to get the structure from.

        Returns:
            dict: The structure of the HDF5 file.
        """
        out = {}
        for key, value in group.items():
            if value is None:
                continue
            if isinstance(value, h5py.Group):
                out[key] = self._get_hdf5_structure(value)
            else:
                out[key] = {
                    "type": "dataset",
                    "shape": value.shape,
                    "dtype": value.dtype,
                    "mem_size": value.size * value.dtype.itemsize,
                }
        return out


class AttributeDict(dict):
    """
    This class is a Pydantic model for the DeviceContainer class.
    """

    def __dir__(self) -> Iterable[str]:
        return list(self.keys())

    def __getattr__(self, name: str) -> Any:
        if name in self:
            return self[name]
        raise AttributeError(f"Attribute '{name}' not found in data or instance attributes.")

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value

    def __delattr__(self, name: str) -> None:
        del self[name]


class SignalDataReference:
    """
    This class is a reference to a signal in an HDF5 file.
    """

    def __init__(
        self, file_path: str, entry_path: str, dict_entry: str | list[str] = None, info: dict = None
    ):
        self._file_reference = FileReference(file_path)
        self._entry_path = entry_path
        self._info = info
        if dict_entry is None:
            self._dict_entry = None
        else:
            self._dict_entry = dict_entry if isinstance(dict_entry, list) else [dict_entry]

    def read(self) -> dict[Literal["value", "timestamp"], Any]:
        """
        Read the data from the HDF5 file.

        Returns:
            dict: The data from the HDF5 file, including the value and timestamp (optional).
        """
        return self._get_entry()

    def _get_entry(self) -> dict:
        data = self._file_reference.read(self._entry_path, entry_filter=self._dict_entry)
        return data


class DeviceDataReference(AttributeDict, SignalDataReference):
    """
    This class is a reference to a device in an HDF5 file.
    """

    def __init__(
        self,
        content: dict,
        file_path: str,
        entry_path: str,
        dict_entry: str | list[str] = None,
        info: dict = None,
        device_group: str = None,
    ):
        super().__init__(content)
        SignalDataReference.__init__(self, file_path, entry_path, dict_entry, info)
        self._group = device_group

    def __repr__(self) -> str:
        table = PrettyTable(title=self._dict_entry[0] if self._dict_entry else None)
        table.field_names = ["Signal", "Shape", "Size", "DType"]
        for signal in self:
            if signal.startswith("_"):
                continue
            signal_info = self._info.get(signal, {}).get("value", {})
            table.add_row(
                [
                    signal,
                    signal_info.get("shape", "N/A"),
                    f"{signal_info.get('mem_size', 0)/1024/1024:.2f} MB",
                    signal_info.get("dtype", "N/A"),
                ]
            )
        return str(table)


class LazyAttributeDict(AttributeDict):
    """
    This class is a lazy attribute dictionary that loads the data using a load function when the data is accessed.
    """

    def __init__(self, load_function: callable = None):
        self._load_function = load_function
        self._loaded = False

    def _load(self) -> None:
        if not super().__getitem__("_loaded"):
            super().__getitem__("_load_function")()
            super().__setitem__("_loaded", True)

    def __dir__(self) -> Iterable[str]:
        object.__getattribute__(self, "_load")()
        return list(self.keys())

    def __getattr__(self, name: str) -> Any:
        object.__getattribute__(self, "_load")()
        return super().__getattr__(name)

    def __getitem__(self, key: Any) -> Any:
        object.__getattribute__(self, "_load")()
        return super().__getitem__(key)

    def keys(self) -> dict_keys:
        object.__getattribute__(self, "_load")()
        return super().keys()

    def items(self) -> dict_items:
        object.__getattribute__(self, "_load")()
        return super().items()

    def values(self) -> Iterable:
        object.__getattribute__(self, "_load")()
        return super().values()


class LazyDeviceAttributeDict(LazyAttributeDict):
    """
    This class is a lazy attribute dictionary for device data.
    """

    def __repr__(self) -> str:
        object.__getattribute__(self, "_load")()
        table = PrettyTable(title="Devices")
        table.field_names = ["Device", "Readout priority"]
        for device, device_container in self.items():
            if device.startswith("_"):
                continue
            table.add_row([device, device_container._group])
        return str(table)


class LazyMetadataAttributeDict(LazyAttributeDict):
    """
    This class is a lazy attribute dictionary for metadata.
    """

    def __repr__(self) -> str:
        object.__getattribute__(self, "_load")()
        table = PrettyTable(title="Metadata")
        table.field_names = ["Key", "Value"]
        for key, value in self.items():
            if key.startswith("_"):
                continue
            table.add_row([key, value])
        return str(table)


class LinkedAttributeDict(AttributeDict):
    """
    This class is a linked attribute dictionary that groups devices by readout priority.
    """

    def __init__(self, container: LazyAttributeDict, group: str):
        self._container = container
        self._group = group

    def __dir__(self) -> Iterable[str]:
        return [
            name
            for name, device in self._container.items()
            if not name.startswith("_") and device._group == self._group
        ]

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_") or name == "read":
            return super().__getattr__(name)
        return self._container[name]

    def __getitem__(self, key: Any) -> Any:
        if key.startswith("_") or key == "read":
            return super().__getitem__(key)
        return self._container[key]

    def __get_filtered_items(self) -> dict:
        return {
            name: device
            for name, device in self._container.items()
            # pylint: disable=protected-access
            if not name.startswith("_") and device._group == self._group
        }

    def items(self) -> dict_items:
        return self.__get_filtered_items().items()

    def keys(self) -> dict_keys:
        return self.__get_filtered_items().keys()

    def values(self) -> Iterable:
        return self.__get_filtered_items().values()

    def __repr__(self) -> str:
        table = PrettyTable(title=self._group)
        table.field_names = ["Device", "Readout priority"]
        for device, device_container in self._container.items():
            if device.startswith("_") or device_container._group != self._group:
                continue
            table.add_row([device, device_container._group])
        return str(table)


class ReadableLinkedAttributeDict(LinkedAttributeDict):
    """
    This class is a linked attribute dictionary that groups devices by readout priority
    and provides a read method for reading the data from the devices.
    """

    def read(self) -> dict:
        """
        Read the data from the devices.

        Returns:
            dict: The data from the devices.
        """
        return {name: device.read() for name, device in self.items()}

    def _get_pandas(self):
        try:
            return importlib.import_module("pandas")
        except ImportError as exc:
            raise ImportError("Install `pandas` to use to_pandas() method") from exc

    def to_pandas(self):
        """
        Convert the data to a pandas DataFrame.

        Returns:
            pandas.DataFrame: The data as a pandas DataFrame.
        """
        pd = self._get_pandas()
        data = self.read()
        frame = {}
        for device, device_data in data.items():
            for signal, signal_data in device_data.items():
                for key, value in signal_data.items():
                    frame[(device, signal, key)] = value
        return pd.DataFrame(frame)


class ReadoutGroup:
    """
    This class is a container for readout groups.
    """

    baseline_devices: LinkedAttributeDict
    monitored_devices: LinkedAttributeDict
    async_devices: LinkedAttributeDict

    def __init__(self, container: LazyDeviceAttributeDict):
        self.baseline_devices = ReadableLinkedAttributeDict(container, "baseline")
        self.monitored_devices = ReadableLinkedAttributeDict(container, "monitored")
        self.async_devices = LinkedAttributeDict(container, "async")


class ScanDataContainer:
    """
    This is a helper class for accessing data in an HDF5 file.
    """

    def __init__(self, file_path: str = None, msg: messages.ScanHistoryMessage = None):
        self._file_reference = None
        self._msg = msg
        self.devices = LazyDeviceAttributeDict(self._load_devices)
        self.readout_groups = ReadoutGroup(self.devices)
        self.metadata = LazyAttributeDict(self._load_metadata)
        self._baseline_devices = None
        self._monitored_devices = None
        self._async_devices = None
        self._loaded_devices = False
        self._loaded_metadata = False
        self._info = None
        if file_path is not None:
            self.set_file(file_path)

    def set_file(self, file_path: str):
        """
        Set the file path for the ScanDataContainer.

        Args:
            file_path (str): The path to the HDF5 file.
        """
        self._file_reference = FileReference(file_path)

    def _load_metadata(self) -> None:
        """
        Load the metadata from the HDF5 file.
        """
        if self._loaded_metadata:
            return
        self.metadata.update(self._file_reference.read("entry/collection/metadata"))
        self._loaded_metadata = True

    def _load_devices(self, timeout_time: float = 3) -> None:
        """
        Load the device metadata from the HDF5 file.

        Args:
            timeout_time (float): The time to wait for the file reference to be set.

        Raises:
            ValueError: If the file reference is not set after the timeout time.
        """
        _start = time.time()

        if self._loaded_devices:
            return

        if self._file_reference is None:
            elapsed_time = 0
            while self._file_reference is None and elapsed_time < timeout_time:
                time.sleep(0.1)
                elapsed_time += 0.1
            if self._file_reference is None:
                raise ValueError("File reference not set. Cannot load devices.")

        if self._info is None:
            self._info = self._file_reference.get_hdf5_structure()
        self._load_device_group("baseline", self._info)
        self._load_device_group("monitored", self._info)
        self._load_device_group("async", self._info, grouped_cache=False)
        self._loaded_devices = True
        logger.debug(f"devices loaded in {time.time() - _start:.2f} s")

    def _load_device_group(
        self,
        group: Literal["baseline", "monitored", "async"],
        info: dict,
        grouped_cache: bool = True,
    ) -> None:
        device_group = (
            info.get("entry", {}).get("collection", {}).get("readout_groups", {}).get(group, {})
        )
        base_path = f"entry/collection/readout_groups/{group}"
        for device_name, device_info in device_group.items():
            if device_name.startswith("_"):
                continue
            entry_path = base_path if grouped_cache else f"{base_path}/{device_name}"
            self.devices[device_name] = DeviceDataReference(
                {
                    signal_name: SignalDataReference(
                        file_path=self._file_reference.file_path,
                        entry_path=entry_path,
                        dict_entry=[device_name, signal_name] if grouped_cache else [signal_name],
                    )
                    for signal_name in device_info
                },
                file_path=self._file_reference.file_path,
                entry_path=entry_path,
                dict_entry=device_name if grouped_cache else None,
                info=device_info,
                device_group=group,
            )

    def __repr__(self) -> str:
        """
        Get a string representation of the ScanDataContainer.
        """
        if not self._msg:
            return f"ScanDataContainer: {self._file_reference.file_path}"
        start_time = f"\tStart time: {datetime.datetime.fromtimestamp(self._msg.start_time).strftime('%c')}\n"
        end_time = (
            f"\tEnd time: {datetime.datetime.fromtimestamp(self._msg.end_time).strftime('%c')}\n"
        )
        elapsed_time = f"\tElapsed time: {(self._msg.end_time-self._msg.start_time):.1f} s\n"
        scan_id = f"\tScan ID: {self._msg.scan_id}\n"
        scan_number = f"\tScan number: {self._msg.scan_number}\n"
        scan_name = f"\tScan name: {self._msg.scan_name}\n"
        exit_status = f"\tStatus: {self._msg.exit_status}\n"
        num_points = f"\tNumber of points (monitored): {self._msg.num_points}\n"
        public_file = f"\tFile: {self._msg.file_path}\n"
        details = (
            start_time
            + end_time
            + elapsed_time
            + scan_id
            + scan_number
            + scan_name
            + exit_status
            + num_points
            + public_file
        )
        return f"ScanDataContainer:\n {details}"


if __name__ == "__main__":  # pragma: no cover
    # scan item
    scan_item = ScanDataContainer()
    scan_item.set_file(
        "/Users/wakonig_k/software/work/bec/data/S00000-00999/S00248/S00248_master.h5"
    )
    start = time.time()
    print(scan_item.metadata)
    print(dir(scan_item.readout_groups.baseline_devices))
    print(time.time() - start)
    print(scan_item.devices.aptrx.aptrx.read())
