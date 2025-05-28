"""
Async writer for writing async device data to a separate nexus file
"""

from __future__ import annotations

import threading
import traceback
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Literal

import h5py
import numpy as np

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.serialization import MsgpackSerialization

logger = bec_logger.logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.redis_connector import RedisConnector


class AsyncWriter(threading.Thread):
    """
    Async writer for writing async device data during the scan.

    The writer supports the following async update types:
    - add: Appends data to the existing dataset. The data is always appended to the first axis.
    - add_slice: Appends a slice of data to the existing dataset. The slice is defined by the index in the async update metadata.
    - replace: Replaces the existing dataset with the new data. Note that data is only written after the scan is complete.

    To change the async update type, the device must send the async update metadata with the data. The metadata must contain the key 'async_update'
    with the following structure:
    {
        "type": "add" | "add_slice" | "replace",
        "max_shape": [int | None, ...],
        "index": int
    }

    "index" is only required for the 'add_slice' type and specifies the row index to append the data to.
    "max_shape" is required for the 'add' and 'add_slice' types and specifies the maximum shape of the dataset. If the dataset is 1D, 'max_shape' should be [None].

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

        self.cursor = defaultdict(dict)

    def initialize_stream_keys(self):
        """
        Initialize the stream keys for the devices
        """
        for device in self.devices:
            topic = MessageEndpoints.device_async_readback(
                scan_id=self.scan_id, device=device
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
                    out[device_name] = device_msg
                self.stream_keys[topic.decode()] = index
        return out if out else None

    def poll_and_write_data(self, final: bool = False) -> None:
        """
        Poll the data and write it to the file

        Args:
            final (bool, optional): Whether this is the final write. If True, also write data with aggregation set to replace. Defaults to False.
        """
        data = self.poll_data(poll_timeout=None if final else 500)
        if data or final:
            self.write_data(data or {}, write_replace=final)

    def _run(self) -> None:
        try:
            self.send_file_message(done=False, successful=False)
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
            self.connector.raise_alarm(
                severity=Alarms.WARNING,
                alarm_type="AsyncWriterError",
                source={"file_path": self.file_path},
                msg=f"Error writing async data file {self.file_path}: {content}",
                metadata={},
            )

    def send_file_message(self, done: bool, successful: bool) -> None:
        """
        Send a file message to inform other services about current writing status

        Args:
            done (bool): Whether the writing is done
            successful (bool): Whether the writing was successful
        """
        self.connector.set_and_publish(
            MessageEndpoints.public_file(self.scan_id, "master"),
            messages.FileMessage(
                file_path=self.file_path,
                done=done,
                successful=successful,
                hinted_h5_entries={
                    device_name: device_name for device_name in self.written_devices
                },
                metadata={},
            ),
        )

    def stop(self) -> None:
        """
        Stop the async writer
        """
        self.shutdown_event.set()

    def write_data(
        self, data: list[dict[str, messages.DeviceMessage]], write_replace: bool = False
    ) -> None:
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

            # create the device group if it doesn't exist
            # -> /entry/collections/devices/<device_name>
            group_name = f"{self.BASE_PATH}/{device_name}"
            if group_name not in f:
                f.create_group(group_name)
            device_group = f[group_name]

            signals = data_container.signals
            async_update = data_container.metadata["async_update"]

            for signal_name, signal_data in signals.items():
                # create the device signal group if it doesn't exist
                # -> /entry/collections/devices/<device_name>/<signal_name>
                if signal_name not in device_group:
                    signal_group = device_group.create_group(signal_name)
                    signal_group.attrs["NX_class"] = "NXdata"
                    signal_group.attrs["signal"] = "value"
                else:
                    signal_group = device_group[signal_name]

                for key, value in signal_data.items():

                    if key == "value":
                        self.write_value_data(signal_group, value, async_update)
                    elif key == "timestamp":
                        self.write_timestamp_data(signal_group, value)
                    else:  # pragma: no cover
                        # this should never happen as the keys are fixed in the pydantic model
                        self.connector.raise_alarm(
                            severity=Alarms.WARNING,
                            alarm_type="ValueError",
                            source={"device": device_name},
                            msg=f"Unknown key: {key}. Data will not be written.",
                            metadata={},
                        )

        if write_replace:
            for group_name, value in self.device_data_replace.items():
                group = f[group_name]
                group.create_dataset("value", data=value, maxshape=value.shape)

        f.flush()

    def write_value_data(self, signal_group: h5py.Group, value: Any, async_update: dict):

        if isinstance(value, list):
            value = np.array(value)

        # TODO: remove once all devices have been transitioned to the new async update format
        ###############
        if not isinstance(async_update, dict):
            logger.warning(
                f"Invalid async update metadata: {async_update}. Please transition your device to the new async update format."
            )
            if async_update == "extend":
                async_update = {"type": "add", "max_shape": [None]}
            else:
                async_update = {"type": "add", "max_shape": [None] + list(value.shape)}
        ###############

        update_type: Literal["add", "add_slice", "replace"] = async_update["type"]

        if update_type == "add":
            self._write_value_add(async_update, signal_group, value)

        elif update_type == "add_slice":
            self._write_value_add_slice(async_update, signal_group, value)

        elif update_type == "replace":
            # store the data to be written after the scan is complete
            self.device_data_replace[signal_group.name] = value
        else:
            self.connector.raise_alarm(
                severity=Alarms.WARNING,
                alarm_type="ValueError",
                source={"device": signal_group.name},
                msg=f"Unknown async update type: {update_type}. Data will not be written.",
                metadata={},
            )

    def _write_value_add(self, async_update: dict, signal_group: h5py.Group, value: Any):
        """
        Write the value data to the file using the 'add' async update type.

        Args:
            async_update (dict): The async update metadata
            signal_group (h5py.Group): The group to write the data to
            value (Any): The value data to write
        """

        max_shape: tuple = async_update["max_shape"]
        # if max_shape contains more than one None, we have to use a vlen_dtype
        num_undefined = sum(1 for i in max_shape if i is None)
        if num_undefined and not isinstance(value, (np.ndarray, list)):
            value = np.array(value)

        if "value" not in signal_group:
            if num_undefined > 1:
                shape = (1,) if value.ndim < len(max_shape) else (value.shape[0],)
                signal_group.create_dataset(
                    "value",
                    shape=shape,
                    maxshape=tuple(None for _ in value.shape),
                    dtype=h5py.vlen_dtype(np.dtype(value[0].dtype)),
                )
                signal_group["value"][: shape[0]] = value
            else:
                if value.ndim < len(max_shape):
                    value = value.reshape((1,) + value.shape)
                signal_group.create_dataset("value", data=value, maxshape=max_shape)
            return

        # add to the already existing dataset
        if value.ndim < len(max_shape):
            value = value.reshape((1,) + value.shape)
        if len(max_shape) == 1:
            # 1D case: simply append the data
            signal_group["value"].resize((len(signal_group["value"]) + len(value),))
            signal_group["value"][-len(value) :] = value
        elif len(max_shape) == 2 and max_shape[1] is not None:
            # ND case: we resize the first axis and append the data
            current_shape = signal_group["value"].shape
            signal_group["value"].resize((current_shape[0] + value.shape[0], max_shape[1]))
            signal_group["value"][-value.shape[0] :, : min(max_shape[1], value.shape[1])] = value[
                :, : min(max_shape[1], value.shape[1])
            ]
        else:
            current_shape = signal_group["value"].shape
            if max_shape[1] is not None and value.shape[1] > max_shape[1]:
                self.connector.raise_alarm(
                    severity=Alarms.WARNING,
                    alarm_type="ValueError",
                    source={"device": signal_group.name},
                    msg=f"Data for {signal_group.name} exceeds the defined max_shape {max_shape}. Data will not be written.",
                    metadata={},
                )
                return
            signal_group["value"].resize((current_shape[0] + value.shape[0], *current_shape[1:]))
            signal_group["value"][-value.shape[0] :, ...] = value

    def _write_value_add_slice(self, async_update: dict, signal_group: h5py.Group, value: Any):
        """
        Write the value data to the file using the 'add_slice' async update type.

        Args:
            async_update (dict): The async update metadata
            signal_group (h5py.Group): The group to write the data to
            value (Any): The value data to write
        """

        max_shape: tuple = async_update["max_shape"]

        if len(max_shape) != 2:
            # We currently only support 2D datasets for the 'add_slice' async update type
            self.connector.raise_alarm(
                severity=Alarms.WARNING,
                alarm_type="ValueError",
                source={"device": signal_group.name},
                msg=f"Invalid max_shape for async update type 'add_slice': {max_shape}. max_shape cannot exceed two dimensions. Data will not be written.",
                metadata={},
            )
            return

        # if max_shape contains more than one None, we have to use a vlen_dtype
        num_undefined = sum(1 for i in max_shape if i is None)
        row_index = async_update["index"]

        if "value" not in signal_group:
            if num_undefined > 1:
                row_index = 0
                shape = (1,) if value.ndim < len(max_shape) else (value.shape[0],)
                signal_group.create_dataset(
                    "value",
                    shape=shape,
                    maxshape=(None,),
                    dtype=h5py.vlen_dtype(np.dtype(value[0].dtype)),
                )
                signal_group["value"][row_index] = value
            else:
                if value.ndim < len(max_shape):
                    value = value.reshape((1,) + value.shape)
                if value.shape[1] > max_shape[1]:
                    self.connector.raise_alarm(
                        severity=Alarms.WARNING,
                        alarm_type="ValueError",
                        source={"device": signal_group.name, "slice": row_index},
                        msg=f"Data for {signal_group.name} exceeds the defined max_shape {max_shape}. Data will be truncated.",
                        metadata={},
                    )
                    value = value[:, : max_shape[1]]
                signal_group.create_dataset("value", data=value, maxshape=max_shape)
                self.cursor[signal_group.name][row_index] = value.shape[1]
            return

        # add a slice to the already existing dataset
        if num_undefined > 1:
            max_index = signal_group["value"].shape[0]
            if row_index == -1:
                row_index = max_index + 1
            if row_index >= max_index:
                signal_group["value"].resize((row_index + 1,))
                signal_group["value"][row_index] = value
            else:
                value = np.concatenate([signal_group["value"][row_index], value])
                signal_group["value"][row_index] = value
        else:
            col_index = self.cursor[signal_group.name].get(row_index, 0)
            if col_index + len(value) > max_shape[1]:
                self.connector.raise_alarm(
                    severity=Alarms.WARNING,
                    alarm_type="ValueError",
                    source={"device": signal_group.name, "slice": row_index},
                    msg=f"Added data slice for {signal_group.name} exceeds the defined max_shape {max_shape}. Data will be truncated.",
                    metadata={},
                )
                value = value[: max_shape[1] - col_index]
            signal_group["value"].resize((row_index + 1, max_shape[1]))
            signal_group["value"][row_index, col_index : col_index + len(value)] = value
            self.cursor[signal_group.name][row_index] = col_index + len(value)

    def write_timestamp_data(self, signal_group, value):
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
