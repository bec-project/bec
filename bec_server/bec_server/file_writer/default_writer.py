from __future__ import annotations

from typing import TYPE_CHECKING, Any

import h5py
import numpy as np

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_lib.devicemanager import DeviceManagerBase
    from bec_server.file_writer.file_writer import HDF5Storage


class DefaultFormat:
    """
    Default NeXus file format.
    """

    def __init__(
        self,
        storage: HDF5Storage,
        data: dict,
        info_storage: dict,
        configuration: dict,
        file_references: dict[str, messages.FileMessage],
        beamline_states: dict[str, list[messages.BeamlineStateMessage]],
        device_manager: DeviceManagerBase,
    ):
        self.storage = storage
        self.data = data
        self.configuration = configuration
        self.file_references = file_references
        self.device_manager = device_manager
        self.info_storage = info_storage
        self.beamline_states = beamline_states

    def get_storage_format(self) -> dict:
        """
        Internal method to extract the storage format after formatting the data. This method
        should not be called directly.

        Returns:
            dict: The storage format.
        """
        self.write_bec_entries()
        self.format()
        # pylint: disable=protected-access
        return self.storage._storage

    def has_async_signal(self, device_name: str, signal_name: str) -> bool:
        """
        Check if a device has an async signal.

        Args:
            device_name (str): The name of the device.
            signal_name (str): The name of the signal.

        Returns:
            bool: True if the device has an async signal, False otherwise.
        """
        signals = self.device_manager.get_bec_signals(
            ["AsyncMultiSignal", "AsyncSignal", "DynamicSignal"]
        )
        for device_name_, _, signal_info in signals:
            obj_name = signal_info.get("object_name", "")
            obj_name_without_prefix = obj_name.removeprefix("devicename")
            if device_name_ == device_name and (signal_name in [obj_name, obj_name_without_prefix]):
                return True
        return False

    def get_entry(self, name: str, signal: str | None = None, default=None) -> Any:
        """
        Get an entry from the scan data (monitored or baseline) assuming a <device>.<signal>.value structure.

        This method is a helper to extract the device data from the scan data, irrespective of the
        data structure (list of entries or single entry).

        Note: This method does not handle async signals. Use `has_async_signal` to check for async signals.

        Args:
            name (str): Entry name
            signal (str, optional): Signal name. Defaults to None.
            default (Any, optional): Default value. Defaults to None.
        """
        signal = signal or name
        if isinstance(self.data.get(name), list) and isinstance(self.data[name][0], dict):
            return [sub_data.get(signal, {}).get("value", default) for sub_data in self.data[name]]

        return self.data.get(name, {}).get(signal, {}).get("value", default)

    def write_bec_entries(self) -> None:
        """
        Write the BEC entries to the NeXus file format.
        """
        # /entry
        entry = self.storage.create_group("entry")
        entry.attrs["NX_class"] = "NXentry"
        entry.attrs["start_time"] = self.info_storage.get("start_time")
        entry.attrs["end_time"] = self.info_storage.get("end_time")
        entry.attrs["version"] = 1.0

        # /entry/collection
        collection = entry.create_group("collection")
        collection.attrs["NX_class"] = "NXcollection"
        devices = collection.create_dataset("devices", data=self.data)
        devices.attrs["NX_class"] = "NXcollection"
        metadata = collection.create_dataset("metadata", data=self.info_storage)
        metadata.attrs["NX_class"] = "NXcollection"
        readout_groups = collection.create_group("readout_groups")
        readout_groups.attrs["NX_class"] = "NXcollection"
        for priority_name, devices in self.info_storage["bec"]["readout_priority"].items():
            if priority_name not in ["baseline", "monitored", "async"]:
                continue
            group = readout_groups.create_group(priority_name)
            group.attrs["NX_class"] = "NXcollection"
            for device in devices:
                group.create_soft_link(name=device, target=f"/entry/collection/devices/{device}")
        configuration = collection.create_dataset("configuration", data=self.configuration)
        configuration.attrs["NX_class"] = "NXcollection"

        # create file references
        file_references = collection.create_group("file_references")
        file_references.attrs["NX_class"] = "NXcollection"
        for name, msg in self.file_references.items():
            if name == "master":
                continue
            if msg.is_master_file:
                continue
            file_device = file_references.create_group(name=name)
            if msg.hinted_h5_entries:
                for entry_name, entry_path in msg.hinted_h5_entries.items():
                    file_device.create_ext_link(
                        name=entry_name, target=msg.file_path, entry=entry_path
                    )
            else:
                file_device.create_ext_link(name="data", target=msg.file_path, entry="/")

        # create beamline states
        states = {}
        for state_name, state_values in self.beamline_states.items():
            dtype = np.dtype(
                [
                    ("label", h5py.string_dtype("utf-8")),
                    ("status", h5py.string_dtype("utf-8")),
                    ("timestamp", np.float64),
                ]
            )
            states[state_name] = np.array(
                [
                    (state_msg.label, state_msg.status, state_msg.timestamp)
                    for state_msg in state_values
                ],
                dtype=dtype,
            )
        beamline_states_group = collection.create_group("states")
        beamline_states_group.attrs["NX_class"] = "NXcollection"
        for state_name, state_values in states.items():
            state_group = beamline_states_group.create_dataset(name=state_name, data=state_values)
            state_group.attrs["NX_class"] = "NXcollection"

    def safe_dataset(
        self,
        group: HDF5Storage,
        name: str,
        device: str,
        signal: str | None = None,
        units: str | None = None,
        description: str | None = None,
        attributes: dict | None = None,
        softlink: bool = True,
    ) -> None:
        """
        Write a dataset from the BEC scan data dictionary.
        Silently skips if the device was not recorded in this scan
        (e.g. removed from config, readoutPriority=on_request and not triggered,
        or the scan finished before the device responded).

        Args:
            group (HDF5Storage): The HDF5 group to write the dataset to.
            name (str): The name of the dataset.
            device (str): The device name to retrieve the data from.
            attributes (dict, optional): Additional attributes to set on the dataset. Defaults to None.
            units (str, optional): The units of the dataset. Defaults to None.
            description (str, optional): The description of the dataset. Defaults to None.
            softlink (bool, optional): Create a soft link into /entry/collection/devices instead of
                copying the value into a new dataset. Defaults to True. For async signals, this is always True.
        """
        signal = signal or device
        value = self.get_entry(device, signal=signal)
        if self.has_async_signal(device, signal):
            softlink = True
        elif value is None:
            return

        if softlink:
            group.create_soft_link(
                name=name, target=f"/entry/collection/devices/{device}/{signal}/value"
            )
            return
        ds = group.create_dataset(name, data=value)
        if attributes:
            for key, val in attributes.items():
                ds.attrs[key] = val
        if units:
            ds.attrs["units"] = units
        if description:
            ds.attrs["description"] = description

    def _device_shape_matches(self, reference_device: str, candidate_device: str) -> bool:
        """
        Check whether two scan report devices have compatible value shapes for NXdata.
        """
        reference_value = self.get_entry(reference_device)
        candidate_value = self.get_entry(candidate_device)
        if reference_value is None or candidate_value is None:
            return False
        try:
            return np.asarray(reference_value).shape == np.asarray(candidate_value).shape
        except Exception:
            return False

    def _write_scan_report_data(self, entry: HDF5Storage) -> None:
        """
        Write an NXdata group containing soft links to the scan report devices.
        """
        data_group = entry.create_group("data")
        data_group.attrs["NX_class"] = "NXdata"

        scan_report_devices = self.info_storage.get("bec", {}).get("scan_report_devices") or []
        if scan_report_devices:
            data_group.attrs["signal"] = scan_report_devices[0]
        if not scan_report_devices:
            return

        primary_device = scan_report_devices[0]
        compatible_devices = [primary_device]
        auxiliary_signals = []
        for device in scan_report_devices[1:]:
            if self._device_shape_matches(primary_device, device):
                compatible_devices.append(device)
                auxiliary_signals.append(device)
        if auxiliary_signals:
            data_group.attrs["auxiliary_signals"] = auxiliary_signals

        for device in compatible_devices:
            self.safe_dataset(data_group, name=device, device=device, softlink=True)

    def format(self) -> None:
        """
        Prepare the NeXus file format.
        Override this method in file writer plugins to customize the HDF5 file format.

        The class provides access to the following attributes:
        - self.storage: The HDF5Storage object.
        - self.data: The data dictionary.
        - self.file_references: The file references dictionary.
        - self.device_manager: The DeviceManagerBase object.

        See also: :class:`bec_server.file_writer.file_writer.HDF5Storage`.

        """

        entry = self.storage.create_group("entry")

        # /entry/control
        control = entry.create_group("control")
        control.attrs["NX_class"] = "NXmonitor"
        control.create_dataset(name="mode", data="monitor")

        # /entry/data
        self._write_scan_report_data(entry)

        # /entry/sample
        control = entry.create_group("sample")
        control.attrs["NX_class"] = "NXsample"
        control.create_dataset(name="name", data=self.data.get("samplename"))
        control.create_dataset(name="description", data=self.data.get("sample_description"))

        # /entry/instrument
        instrument = entry.create_group("instrument")
        instrument.attrs["NX_class"] = "NXinstrument"

        source = instrument.create_group("source")
        source.attrs["NX_class"] = "NXsource"
        source.create_dataset(name="type", data="Synchrotron X-ray Source")
        source.create_dataset(name="name", data="Swiss Light Source")
        source.create_dataset(name="probe", data="x-ray")
