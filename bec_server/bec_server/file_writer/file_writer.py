from __future__ import annotations

import datetime
import json
import os
import traceback
import typing

import h5py

from bec_lib import messages, plugin_helper
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .default_writer import DefaultFormat as default_NeXus_format
from .merged_dicts import merge_dicts

logger = bec_logger.logger

if typing.TYPE_CHECKING:
    from bec_server.file_writer.file_writer_manager import ScanStorage


class NeXusLayoutError(Exception):
    """
    Exception raised when the NeXus layout is incorrect.
    """


class HDF5Storage:
    """
    The HDF5Storage class is a container used by the HDF5 writer plugins to store data in the correct NeXus format.
    It is used to store groups, datasets, soft links and external links before writing them to the HDF5 file.
    """

    def __init__(self, storage_type: str = "group", data=None) -> None:
        self._storage = {}
        self._storage_type = storage_type
        self.attrs = {}
        self._data = data

    def create_group(self, name: str) -> HDF5Storage:
        """
        Create a group in the HDF5 storage.

        Args:
            name (str): Group name

        Returns:
            HDF5Storage: Group storage
        """
        self._storage[name] = HDF5Storage(storage_type="group")
        return self._storage[name]

    def create_dataset(self, name: str, data: typing.Any) -> HDF5Storage:
        """
        Create a dataset in the HDF5 storage.

        Args:
            name (str): Dataset name
            data (typing.Any): Dataset data

        Returns:
            HDF5Storage: Dataset storage
        """
        self._storage[name] = HDF5Storage(storage_type="dataset", data=data)
        return self._storage[name]

    def create_soft_link(self, name: str, target: str) -> HDF5Storage:
        """
        Create a soft link in the HDF5 storage.

        Args:
            name (str): Link name
            target (str): Link target

        Returns:
            HDF5Storage: Link storage
        """
        self._storage[name] = HDF5Storage(storage_type="softlink", data=target)
        return self._storage[name]

    def create_ext_link(self, name: str, target: str, entry: str) -> HDF5Storage:
        """
        Create an external link in the HDF5 storage.

        Args:
            name (str): Link name
            target (str): Name of the target file
            entry (str): Entry within the target file (e.g. entry/instrument/eiger_4)

        Returns:
            HDF5Storage: Link storage
        """
        data = {"file": target, "entry": entry}
        self._storage[name] = HDF5Storage(storage_type="ext_link", data=data)
        return self._storage[name]


class HDF5StorageWriter:
    """
    The HDF5StorageWriter class is used to write the HDF5Storage object to an HDF5 file.
    """

    device_storage = None
    info_storage = None

    def add_group(self, name: str, container: typing.Any, val: HDF5Storage):
        if name in container:
            group = container[name]
        else:
            group = container.create_group(name)
        self.add_attribute(group, val.attrs)
        self.add_content(group, val._storage)

        data = val._data

        if not data:
            return

        for key, value in data.items():
            if value is None:
                continue
            if isinstance(value, dict):
                sub_storage = HDF5Storage(key)
                dict_to_storage(sub_storage, value)
                self.add_group(key, group, sub_storage)
                # self.add_content(group, sub_storage._storage)
                continue
            if isinstance(value, list) and isinstance(value[0], dict):
                merged_dict = merge_dicts(value)
                sub_storage = HDF5Storage(key)
                dict_to_storage(sub_storage, merged_dict)
                self.add_group(key, group, sub_storage)
                continue

            group.create_dataset(name=key, data=value)

    def add_dataset(self, name: str, container: typing.Any, val: HDF5Storage):
        try:
            if isinstance(val._data, dict):
                self.add_group(name, container, val)
                return

            data = val._data
            if data is None:
                return
            if isinstance(data, list):
                if data and isinstance(data[0], dict):
                    data = json.dumps(data)
                elif not all(isinstance(x, type(data[0])) for x in data):
                    data = json.dumps(data)
            dataset = container.create_dataset(name, data=data)
            self.add_attribute(dataset, val.attrs)
            self.add_content(dataset, val._storage)
            if container.parent.parent.name == "/entry/collection/devices":
                signal_name = container.name.split("/")[-1]
                group_name = container.parent.name.split("/")[-1]
                if signal_name == group_name:
                    container.attrs["NX_class"] = "NXdata"
                    container.attrs["signal"] = "value"
        except Exception:
            content = traceback.format_exc()
            logger.error(f"Failed to write dataset {name}: {content}")
        return

    def add_attribute(self, container: typing.Any, attributes: dict):
        for name, value in attributes.items():
            if value is not None:
                container.attrs[name] = value

    def add_hardlink(self, name, container, val):
        pass

    def add_softlink(self, name, container, val):
        container[name] = h5py.SoftLink(val._data)

    def add_external_link(self, name, container, val):
        container[name] = h5py.ExternalLink(val._data.get("file"), val._data.get("entry"))

    def add_content(self, container, storage):
        for name, val in storage.items():
            # pylint: disable=protected-access
            if val._storage_type == "group":
                self.add_group(name, container, val)
            elif val._storage_type == "dataset":
                self.add_dataset(name, container, val)
            elif val._storage_type == "hardlink":
                self.add_hardlink(name, container, val)
            elif val._storage_type == "softlink":
                self.add_softlink(name, container, val)
            elif val._storage_type == "ext_link":
                self.add_external_link(name, container, val)
            else:
                pass

    @classmethod
    def write(cls, writer_storage, file):
        writer = cls()
        writer.add_content(file, writer_storage)


class HDF5FileWriter:
    """
    The HDF5FileWriter class is used to write data to an HDF5 file. Internally, it uses the HDF5StorageWriter class to
    write the HDF5Storage object to the file.

    Its primary purpose is to prepare the data, select the correct writer plugin and initiate the writing process.
    """

    def __init__(self, file_writer_manager):
        self.file_writer_manager = file_writer_manager

    @staticmethod
    def _create_device_data_storage(data):
        device_storage = {}
        if data.baseline:
            device_storage.update(data.baseline)
        if data.async_data:
            device_storage.update(data.async_data)
        keys = list(data.scan_segments.keys())
        keys.sort()
        for point in keys:
            for dev in data.scan_segments[point]:
                if dev not in device_storage:
                    device_storage[dev] = [data.scan_segments[point][dev]]
                    continue
                device_storage[dev].append(data.scan_segments[point][dev])
        return device_storage

    def write(self, file_path: str, data: ScanStorage, mode="w", file_handle=None):
        """
        Write the data to an HDF5 file.

        Args:
            file_path (str): File path
            data (ScanStorage): Scan data
            mode (str, optional): File mode. Defaults to "w".
            file_handle (h5py.File, optional): File handle. Defaults to None.

        Raises:
            NeXusLayoutError: Raised when the NeXus layout is incorrect.
        """
        device_storage = self._create_device_data_storage(data)
        info_storage = {}
        info_storage["bec"] = data.metadata

        # NeXus needs start_time and end_time in ISO8601 format, so we have to convert it
        if data.start_time is not None:
            info_storage["start_time"] = datetime.datetime.fromtimestamp(
                data.start_time
            ).isoformat()
        if data.end_time is not None:
            info_storage["end_time"] = datetime.datetime.fromtimestamp(data.end_time).isoformat()
        info_storage.update(info_storage["bec"].get("user_metadata", {}))
        info_storage["bec"].pop("user_metadata", None)

        requested_plugin = self.file_writer_manager.file_writer_config.get("plugin")
        if requested_plugin == "default_NeXus_format":
            writer_format_cls = default_NeXus_format
        else:
            plugins = plugin_helper.get_file_writer_plugins()
            if requested_plugin not in plugins:
                logger.error(f"Plugin {requested_plugin} not found. Using default plugin.")
                writer_format_cls = default_NeXus_format
            else:
                writer_format_cls = plugins[requested_plugin]

        for file_ref in data.file_references.values():
            rel_path = os.path.relpath(file_ref.file_path, os.path.dirname(file_path))
            file_ref.file_path = rel_path

        writer_storage = writer_format_cls(
            storage=HDF5Storage(),
            data=device_storage,
            info_storage=info_storage,
            file_references=data.file_references,
            device_manager=self.file_writer_manager.device_manager,
        ).get_storage_format()

        file_data = {}
        for key, val in device_storage.items():
            file_data[key] = val if not isinstance(val, list) else merge_dicts(val)
        msg_data = {"file_path": file_path, "data": file_data, "scan_info": info_storage}
        msg = messages.FileContentMessage(**msg_data)
        self.file_writer_manager.connector.set_and_publish(MessageEndpoints.file_content(), msg)

        file_handle = file_handle or h5py.File(file_path, mode=mode)
        try:
            HDF5StorageWriter.write(writer_storage, file_handle)
        finally:
            file_handle.close()


def dict_to_storage(storage, data):
    for key, val in data.items():
        if isinstance(val, dict):
            sub = storage.create_group(key)
            dict_to_storage(sub, val)
            continue
        storage.create_dataset(key, val)
