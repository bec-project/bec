from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
        device_manager: DeviceManagerBase,
    ):
        self.storage = storage
        self.data = data
        self.configuration = configuration
        self.file_references = file_references
        self.device_manager = device_manager
        self.info_storage = info_storage

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

    def get_entry(self, name: str, default=None) -> Any:
        """
        Get an entry from the scan data assuming a <device>.<device>.value structure.

        This method is a helper to extract the device data from the scan data, irrespective of the
        data structure (list of entries or single entry).

        Args:
            name (str): Entry name
            default (Any, optional): Default value. Defaults to None.
        """
        if isinstance(self.data.get(name), list) and isinstance(self.data[name][0], dict):
            return [sub_data.get(name, {}).get("value", default) for sub_data in self.data[name]]

        return self.data.get(name, {}).get(name, {}).get("value", default)

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
        if "eiger_4" in self.device_manager.devices:
            entry.create_soft_link(name="data", target="/entry/instrument/eiger_4")

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
