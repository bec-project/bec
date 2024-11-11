"""
Async writer for writing async device data to a separate nexus file
"""

from __future__ import annotations

import os
import threading
import traceback
from collections import defaultdict
from typing import TYPE_CHECKING, Literal

import h5py
import numpy as np

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.serialization import MsgpackSerialization

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector


class AsyncWriter(threading.Thread):
    """
    Async writer for writing async device data to a separate nexus file
    """

    BASE_PATH = "/entry/collection/devices"

    def __init__(self, file_path: str, scan_id: str, connector: RedisConnector, devices: list[str]):
        """
        Initialize the async writer

        Args:
            file_path (str): The path to the file to write the data to
            scan_id (str): The scan id
            connector (RedisConnector): The redis connector
            devices (list[str]): The list of devices to write data for
        """
        super().__init__(target=self._run, daemon=True, name="AsyncWriter")
        self.file_path = file_path
        self.scan_id = scan_id
        self.devices = devices
        self.connector = connector
        self.stream_keys = {}
        self.shutdown_event = threading.Event()
        self.device_data_replace = {}
        self.append_shapes = {}
        self.written_devices = set()
        self.file_handle = None

    def initialize_stream_keys(self):
        """
        Initialize the stream keys for the devices
        """
        for device in self.devices:
            topic = MessageEndpoints.device_async_readback(
                scan_id=self.scan_id, device=device.name
            ).endpoint
            key = "0-0"
            self.stream_keys[topic] = key

    def poll_data(self, poll_timeout: int | None = 500) -> dict:
        """
        Poll the redis stream for new data.

        Args:
            poll_timeout (int, optional): The time to wait for new data before returning. Defaults to 500. If set to 0,
                it waits indefinitely. If set to None, it returns immediately.
        """
        # pylint: disable=protected-access
        out = self.connector._redis_conn.xread(self.stream_keys, block=poll_timeout)
        return self._decode_stream_messages_xread(out)

    def _decode_stream_messages_xread(self, msg) -> dict:
        out = defaultdict(list)
        for topic, msgs in msg:
            for index, record in msgs:
                device_name = topic.decode().split("/")[-1]
                for _, msg_entry in record.items():
                    device_msg: messages.DeviceMessage = MsgpackSerialization.loads(msg_entry)
                    if device_msg.metadata["async_update"] == "replace":
                        # if the message is a replace message, store the data to be written later
                        self.device_data_replace[device_name] = {
                            "async_update": "replace",
                            "data": self.process_async_data(device_name, [device_msg]),
                        }
                    else:
                        out[device_name].append(device_msg)
                self.stream_keys[topic.decode()] = index
        return out if out else None

    def _get_write_data(self, data: dict, write_replace: bool = False) -> dict:
        out = {}
        if write_replace:
            out.update(self.device_data_replace)
        if not data:
            return out
        for device_name, device_data in data.items():
            out[device_name] = {
                "async_update": device_data[0].metadata["async_update"],
                "data": self.process_async_data(device_name, device_data),
            }
        return out

    def poll_and_write_data(self, final: bool = False) -> None:
        data = self.poll_data(poll_timeout=None if final else 500)
        out = self._get_write_data(data, write_replace=final)
        if out:
            self.write_data(out)

    def _run(self) -> None:
        try:
            self.send_file_message(done=False, successful=False)
            self.shutdown_event.clear()
            self.initialize_stream_keys()
            if not self.devices:
                return
            # self.register_async_callbacks()
            while not self.shutdown_event.is_set():
                self.poll_and_write_data()
            # run one last time to get any remaining data
            self.poll_and_write_data(final=True)
            # self.send_file_message(done=True, successful=True)
            logger.info(f"Finished writing async data file {self.file_path}")
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            # self.send_file_message(done=True, successful=False)
            logger.error(f"Error writing async data file {self.file_path}: {content}")

    def send_file_message(self, done: bool, successful: bool) -> None:
        """
        Send a file message to inform other services about current writing status

        Args:
            done (bool): Whether the writing is done
            successful (bool): Whether the writing was successful
        """
        self.connector.set_and_publish(
            MessageEndpoints.public_file(self.scan_id, "async"),
            messages.FileMessage(
                file_path=self.file_path,
                done=done,
                successful=successful,
                devices=list(self.written_devices),
                hinted_locations={device_name: device_name for device_name in self.written_devices},
                metadata={},
            ),
        )

    def stop(self) -> None:
        """
        Stop the async writer
        """
        self.shutdown_event.set()

    def write_data(self, data: list[dict]) -> None:
        """
        Write data to the file. If write_replace is True, write also async data with
        aggregation set to replace.

        Args:
            data (list[dict]): List of dictionaries containing data from devices
            write_replace (bool, optional): Write data with aggregation set to replace. This is
                typically used only after the scan is complete. Defaults to False.

        """
        if self.file_handle is None:
            self.file_handle = h5py.File(self.file_path, "w")

        f = self.file_handle

        for device_name, data_container in data.items():
            self.written_devices.add(device_name)
            # create the group if it doesn't exist
            async_update = data_container["async_update"]
            signal_data = data_container["data"]
            group_name = f"{self.BASE_PATH}/{device_name}"
            if group_name not in f:
                f.create_group(group_name)
            device_group = f[group_name]
            # create the signal group if it doesn't exist
            for signal_name, signal_data in signal_data.items():
                if signal_name not in device_group:
                    device_group.create_group(signal_name)
                signal_group = device_group[signal_name]
                for key, value in signal_data.items():

                    if key == "value":
                        self._write_value_data(signal_group, value, async_update)
                    elif key == "timestamp":
                        self._write_timestamp_data(signal_group, value)
                    else:
                        raise ValueError(f"Unknown key {key}")
        f.flush()

    def _write_value_data(self, signal_group, value, async_update):

        if isinstance(value, list) and len(value) == 1:
            value = value[0]

        if not isinstance(value, (np.ndarray, list)):
            value = np.array(value)

        if isinstance(value, list):
            shape = value[0].shape if hasattr(value[0], "shape") else (len(value),)
        else:
            shape = value.shape

        shape = shape or (1,)

        value_ndim = value.ndim if isinstance(value, np.ndarray) else len(value)

        if "value" not in signal_group:
            signal_group.attrs["NX_class"] = "NXdata"
            signal_group.attrs["signal"] = "value"
            if async_update == "extend":
                # Create a chunked dataset and allow unlimited resizing
                signal_group.create_dataset(
                    "value",
                    data=value,
                    maxshape=tuple(None for _ in shape),
                    chunks=True,  # Enable chunking
                )
            elif async_update == "append":
                reference_shape = self.append_shapes[signal_group.name]["shape"]
                reference_ndim = self.append_shapes[signal_group.name]["ndim"]

                if reference_ndim == value_ndim:
                    value = value.reshape((1,) + shape)
                signal_group.create_dataset(
                    "value", data=value, maxshape=tuple([None] + list(reference_shape))
                )

            elif async_update == "replace":
                # Create a fixed length dataset
                signal_group.create_dataset("value", data=value)

        else:
            if async_update == "extend":
                # Extend the dataset with the new data
                # we always extend along the first axis
                if not isinstance(value, np.ndarray):
                    value = np.array(value)
                shapes = list(signal_group["value"].shape)
                shapes[0] += value.shape[0]
                signal_group["value"].resize(tuple(shapes))
                signal_group["value"][-value.shape[0] :, ...] = value
            elif async_update == "append":
                # Append the data to the dataset
                append_info = self.append_shapes[signal_group.name]
                fixed_length = append_info["fixed_length"]

                if isinstance(value, list):
                    ndims = len(value)
                else:
                    ndims = 1

                if append_info.pop("rewrite", False):
                    # The shape has changed! - We need to rewrite the dataset
                    append_info["fixed_length"] = False

                    # Read the current data
                    data = signal_group["value"][:]
                    # Delete the dataset
                    del signal_group["value"]

                    # we only have two options here, either the data is a list of arrays or a single array
                    if isinstance(value, list):
                        value_ndim = value[0].ndim
                    else:
                        value_ndim = value.ndim

                    signal_group.create_dataset(
                        "value",
                        shape=(data.shape[0] + ndims,),
                        maxshape=tuple(None for _ in shape),
                        dtype=h5py.vlen_dtype(np.dtype(data[0].dtype)),
                    )
                    # Write the data back to the dataset
                    signal_group["value"][: data.shape[0]] = data
                    signal_group["value"][data.shape[0] :] = value
                elif not fixed_length:
                    # Append the data to the dataset
                    signal_group["value"].resize((len(signal_group["value"]) + ndims,))
                    signal_group["value"][-ndims:] = value
                else:
                    signal_group["value"].resize((len(signal_group["value"]) + ndims, *shape))
                    signal_group["value"][-ndims:, ...] = value

    def _write_timestamp_data(self, signal_group, value):
        """
        Write the timestamp data to the file.
        Timestamp data is always written as a 1D array, irrespective of the async update type.

        Args:
            signal_group (h5py.Group): The group to write the data to
            value (list): The timestamp data to write
        """
        if not isinstance(value, (list, np.ndarray)):
            value = [value]
        if "timestamp" not in signal_group:
            signal_group.create_dataset("timestamp", data=value, maxshape=(None,))
        else:
            signal_group["timestamp"].resize((len(signal_group["timestamp"]) + len(value),))
            signal_group["timestamp"][-len(value) :] = value

    def process_async_data(
        self, device_name: str, msgs: list[messages.DeviceMessage]
    ) -> dict | list[dict]:
        """
        Process the async data.

        Args:
            device_name (str): the device name
            msgs(list[messages.DeviceMessage]): the async data to process

        Returns:
            list: the processed async data
        """
        concat_type = None
        data = []
        async_data = {}
        # merge the msg data into a list of dictionaries
        for msg in msgs:
            if not concat_type:
                concat_type = msg.metadata.get("async_update", "append")
            data.append(msg.content["signals"])

        if concat_type == "extend":
            # concatenate the dictionaries
            for signal in data[0].keys():
                async_data[signal] = {}
                for key in data[0][signal].keys():
                    if hasattr(data[0][signal][key], "__iter__"):
                        async_data[signal][key] = np.concatenate([d[signal][key] for d in data])
                    else:
                        async_data[signal][key] = [d[signal][key] for d in data]
            return async_data

        if concat_type == "append":
            # concatenate the lists
            for key in data[0].keys():
                async_data[key] = {"value": [], "timestamp": []}

                for d in data:
                    if not isinstance(d[key]["value"], np.ndarray):
                        d[key]["value"] = np.array(d[key]["value"])

                    shape = d[key]["value"].shape
                    ndim = d[key]["value"].ndim
                    if os.path.join(self.BASE_PATH, device_name, key) not in self.append_shapes:
                        self.append_shapes[os.path.join(self.BASE_PATH, device_name, key)] = {
                            "shape": shape,
                            "fixed_length": True,
                            "ndim": ndim,
                        }
                    reference = self.append_shapes[os.path.join(self.BASE_PATH, device_name, key)]

                    if reference["fixed_length"] and shape != reference["shape"]:
                        reference["fixed_length"] = False
                        reference["rewrite"] = True

                    async_data[key]["value"].append(d[key]["value"])
                    if "timestamp" in d[key]:
                        async_data[key]["timestamp"].append(d[key]["timestamp"])
            return async_data

        if concat_type == "replace":
            # replace the dictionaries
            async_data = data[-1]
            return async_data

        raise ValueError(f"Unknown async update type: {concat_type}")
