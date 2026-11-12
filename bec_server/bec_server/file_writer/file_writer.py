from __future__ import annotations

import datetime
import json
import os
import traceback
import typing
from collections import defaultdict
from copy import deepcopy

import h5py
import numpy as np
from pydantic import BaseModel

from bec_lib import messages, plugin_helper
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.utils.scan_utils import compose_cli_input_from_scan_info

from .default_writer import DefaultFormat as default_NeXus_format
from .merged_dicts import dict_to_storage, merge_dicts

logger = bec_logger.logger

if typing.TYPE_CHECKING:
    from bec_server.file_writer.file_writer_manager import ScanStorage


class NeXusLayoutError(Exception):
    """
    Exception raised when the NeXus layout is incorrect.
    """


class AdditionalScanMetadata(BaseModel):
    start_time: str | None = None
    end_time: str | None = None
    entry_identifier_uuid: str
    deployment_info: messages.DeploymentInfoMessage | None = None
    versions: messages.ServiceVersions


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
        if name in self._storage:
            return self._storage[name]
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

    def __init__(self, entry_name: str = "entry") -> None:
        self._entry_name = entry_name

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
            if isinstance(data, np.ndarray) and data.dtype.kind == "U":
                data = data.astype(h5py.string_dtype(encoding="utf-8"))
            dataset = container.create_dataset(name, data=data)
            self.add_attribute(dataset, val.attrs)
            self.add_content(dataset, val._storage)
            if container.parent.parent.name == f"/{self._entry_name}/collection/devices":
                signal_name = container.name.split("/")[-1]
                group_name = container.parent.name.split("/")[-1]
                if signal_name == group_name:
                    container.attrs["NX_class"] = "NXdata"
                    container.attrs["signal"] = "value"
        except Exception:
            content = traceback.format_exc()
            logger.error(f"Failed to write dataset {container.name}/{name}: {content}")
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
    def write(cls, writer_storage, file, *, entry_name: str = "entry"):
        writer = cls(entry_name=entry_name)
        writer.add_content(file, writer_storage)


class HDF5FileWriter:
    """
    The HDF5FileWriter class is used to write data to an HDF5 file. Internally, it uses the HDF5StorageWriter class to
    write the HDF5Storage object to the file.

    Its primary purpose is to prepare the data, select the correct writer plugin and initiate the writing process.
    """

    def __init__(self, file_writer_manager):
        self.file_writer_manager = file_writer_manager
        self.stored_data_info = defaultdict(dict)

    def _get_deployment_info(self) -> messages.DeploymentInfoMessage | None:
        """
        Fetch the latest deployment info.
        """
        deployment_info = self.file_writer_manager.connector.get_last(
            MessageEndpoints.deployment_info(), "data"
        )
        if deployment_info is None:
            return

        if not isinstance(deployment_info, messages.DeploymentInfoMessage):
            return

        return deployment_info

    @staticmethod
    def _create_device_data_storage(data):
        device_storage = {}
        if data.baseline:
            device_storage.update(data.baseline)
        keys = list(data.scan_segments.keys())
        keys.sort()
        for point in keys:
            for dev in data.scan_segments[point]:
                if dev not in device_storage:
                    device_storage[dev] = [data.scan_segments[point][dev]]
                    continue
                device_storage[dev].append(data.scan_segments[point][dev])
        return device_storage

    def write(
        self,
        file_path: str,
        data: ScanStorage,
        configuration_data: dict[str, dict],
        mode="w",
        file_handle=None,
        written_async_signals: dict[str, list[str]] | None = None,
        *,
        entry_index: int | None = None,
        default_entry: bool = False,
    ):
        """
        Write the data to an HDF5 file.

        Args:
            file_path (str): File path
            data (ScanStorage): Scan data
            configuration_data (dict[str, dict]): Configuration data
            mode (str, optional): File mode for legacy writes. Numbered entries append.
            file_handle (h5py.File, optional): File handle. Defaults to None.
            written_async_signals (dict[str, list[str]], optional): Dictionary of written async signals. Defaults to None.

            entry_index (int, optional): Write to a numbered NXentry rather than /entry.
            default_entry (bool): Mark this numbered entry as the file's default.

        Raises:
            NeXusLayoutError: Raised when the NeXus layout is incorrect.
        """
        if entry_index is not None and (type(entry_index) is not int or entry_index < 0):
            raise ValueError("entry_index must be a non-negative integer.")
        if default_entry and entry_index is None:
            raise ValueError("default_entry requires a numbered entry.")
        entry_name = "entry" if entry_index is None else f"entry_{entry_index}"
        device_storage = self._create_device_data_storage(data)
        info_storage = {}
        bec_metadata = deepcopy(data.metadata)
        bec_metadata["scan_cli_input"] = compose_cli_input_from_scan_info(bec_metadata)
        info_storage["bec"] = bec_metadata

        # NeXus needs start_time and end_time in ISO8601 format, so we have to convert it
        if data.start_time is not None:
            info_storage["start_time"] = datetime.datetime.fromtimestamp(
                data.start_time
            ).isoformat()
        if data.end_time is not None:
            info_storage["end_time"] = datetime.datetime.fromtimestamp(data.end_time).isoformat()

        if "user_metadata" in bec_metadata:
            # Primary path: current scan status messages expose user metadata at the top level.
            info_storage.update(bec_metadata["user_metadata"])
        elif "user_metadata" in bec_metadata.get("metadata", {}):
            # Compatibility fallback for producers that nest user_metadata inside metadata.
            info_storage.update(bec_metadata["metadata"]["user_metadata"])

        additional_scan_metadata = AdditionalScanMetadata(
            start_time=info_storage.get("start_time"),
            end_time=info_storage.get("end_time"),
            entry_identifier_uuid=bec_metadata.get("scan_id") or data.scan_id,
            deployment_info=self._get_deployment_info(),
            versions=messages.ServiceVersions._get_version_numbers(),
        )

        requested_plugin = self.file_writer_manager.file_writer_config.get("plugin")
        plugins = plugin_helper.get_file_writer_plugins()
        if len(plugins) == 0:
            # no plugins defined, use default
            writer_format_cls = default_NeXus_format
        elif len(plugins) == 1:
            # only one plugin defined, use it
            writer_format_cls = list(plugins.values())[0]
        elif requested_plugin in plugins:
            # requested plugin is available, use it
            writer_format_cls = plugins[requested_plugin]
        else:
            logger.error(f"Plugin {requested_plugin} not found. Using default plugin.")
            writer_format_cls = default_NeXus_format

        format_kwargs = {}
        if entry_index is not None:
            if writer_format_cls is not default_NeXus_format:
                raise NeXusLayoutError("Numbered entries require the default writer.")
            format_kwargs["entry_name"] = entry_name

        file_references = self._resolve_file_references(file_path, data.file_references)

        writer_storage = writer_format_cls(
            storage=HDF5Storage(),
            data=device_storage,
            info_storage=info_storage,
            configuration=configuration_data,
            file_references=file_references,
            beamline_states=data.beamline_states,
            device_manager=self.file_writer_manager.device_manager,
            additional_scan_metadata=additional_scan_metadata,
            written_async_signals=written_async_signals,
            **format_kwargs,
        ).get_storage_format()

        if entry_index is None:
            self._publish_file_content(file_path, device_storage, info_storage)

        # Write to temporary file first
        tmp_file_path = file_path.replace(".h5", ".tmp")

        close_handle = entry_index is None or file_handle is None
        file_handle = file_handle or h5py.File(
            tmp_file_path, mode=mode if entry_index is None else "a"
        )
        try:
            logger.info(f"Writing to file {tmp_file_path}.")
            if (
                entry_index is not None
                and entry_name in file_handle
                and written_async_signals is None
            ):
                raise NeXusLayoutError(f"Entry /{entry_name} already exists.")
            if default_entry and "default" in file_handle.attrs:
                raise NeXusLayoutError("A default entry is already set.")
            HDF5StorageWriter.write(writer_storage, file_handle, entry_name=entry_name)
            if default_entry:
                file_handle.attrs["default"] = entry_name
                self.stored_data_info.clear()
            if entry_index is None or default_entry:
                self.update_data_info(file_handle, entry_name)
        finally:
            if close_handle:
                file_handle.close()

        if entry_index is not None:
            return self._build_file_content(file_path, device_storage, info_storage)
        return None

    @staticmethod
    def _resolve_file_references(file_path: str, references: dict) -> dict:
        file_references = {name: ref.model_copy() for name, ref in references.items()}
        file_refs_to_remove = []
        for device_name, file_ref in file_references.items():
            if file_ref.file_path == file_path:
                # avoid self-referencing file links
                file_refs_to_remove.append(device_name)
                continue
            if not os.path.exists(file_ref.file_path):
                logger.warning(f"File reference {file_ref.file_path} does not exist.")
                file_refs_to_remove.append(device_name)
                continue
            rel_path = os.path.relpath(file_ref.file_path, os.path.dirname(file_path))
            file_ref.file_path = rel_path
        for device_name in file_refs_to_remove:
            file_references.pop(device_name, None)

        return file_references

    def update_entry_links(
        self,
        file_handle,
        file_path: str,
        entry: ScanStorage,
        entry_index: int,
        *,
        update_file_references: bool,
    ) -> None:
        """Include detector links and the primary data link after the final async drain."""
        name = f"entry_{entry_index}"
        group = file_handle[name]
        collection = group["collection"]
        if update_file_references:
            storage = HDF5Storage()
            references = self._resolve_file_references(file_path, entry.file_references)
            default_NeXus_format.write_file_references(storage, references)
            del collection["file_references"]
            HDF5StorageWriter.write(storage._storage, collection, entry_name=name)

        report_devices = entry.metadata.get("scan_report_devices") or []
        if report_devices:
            device = report_devices[0]
            target = f"/{name}/collection/devices/{device}/{device}/value"
            if group["data"].get(device, getlink=True) is None and target in file_handle:
                group["data"][device] = h5py.SoftLink(target)

    def _publish_file_content(self, file_path: str, device_storage: dict, info_storage: dict):
        msg = self._build_file_content(file_path, device_storage, info_storage)
        self.file_writer_manager.connector.set_and_publish(
            MessageEndpoints.file_content(), msg, expire=3600
        )

    @staticmethod
    def _build_file_content(file_path: str, device_storage: dict, info_storage: dict):
        file_data = {
            key: merge_dicts(value) if isinstance(value, list) else value
            for key, value in device_storage.items()
        }
        return messages.FileContentMessage(
            file_path=file_path, data=file_data, scan_info=info_storage
        )

    def update_data_info(self, file_handle: h5py.File, entry_name: str = "entry"):
        """
        Update the stored data information in the file handle.

        Args:
            file_handle (h5py.File): The HDF5 file handle to inspect.
            entry_name (str): NeXus entry containing the devices.
        """
        device_group = file_handle.get(f"/{entry_name}/collection/devices")
        for device_name, device_group in device_group.items():
            if not isinstance(device_group, h5py.Group):
                continue
            for signal_name, signal_group in device_group.items():
                if not isinstance(signal_group, h5py.Group):
                    continue
                if "value" in signal_group:
                    value_dset = signal_group["value"]
                    if not isinstance(value_dset, h5py.Dataset):
                        continue
                    value_dset_shape = value_dset.shape
                    if value_dset_shape == ():
                        value_dset_shape = (1,)
                    self.stored_data_info[device_name][signal_name] = {
                        "shape": value_dset_shape,
                        "dtype": value_dset.dtype.name,
                    }
