from __future__ import annotations

import os
import threading
import time
import traceback
from collections import defaultdict, deque
from pathlib import Path

from bec_lib import messages, plugin_helper
from bec_lib.alarm_handler import Alarms
from bec_lib.bec_service import BECService
from bec_lib.callback_handler import CallbackHandler, EventType
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.file_utils import get_full_path, sanitize_relative_subdir
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import MessageObject, RedisConnector
from bec_lib.service_config import ServiceConfig
from bec_server.file_writer.async_writer import AsyncWriter
from bec_server.file_writer.file_writer import HDF5FileWriter

logger = bec_logger.logger

SYNC_SCAN_TYPES = {"step", "software_triggered", "hardware_triggered"}


class ScanStorage:
    def __init__(self, scan_number: int, scan_id: str) -> None:
        """
        Helper class to store scan data until it is ready to be written to file.

        Args:
            scan_number (int): Scan number
            scan_id (str): Scan ID
        """
        self.scan_number = scan_number
        self.scan_id = scan_id
        self.status_msg: messages.ScanStatusMessage | None = None
        self.entries: dict[int, ScanStorage] = {}
        self.written_entries: set[int] = set()
        self.entries_with_new_file_refs: set[int] = set()
        self.entry_content: dict[int, messages.FileContentMessage] = {}
        self.entry_write_error: Exception | None = None
        self.scan_segments = {}
        self.scan_finished = False
        self.num_points: int | None = None
        self.num_monitored_readouts: int | None = None
        self.baseline = {}
        self.async_writer: AsyncWriter | None = None
        self.beamline_states: dict[str, list[messages.BeamlineStateMessage]] = defaultdict(list)
        self.metadata = {}
        self.file_references = {}
        self.start_time = None
        self.end_time = None
        self.enforce_sync = True
        self.forced_finish = False

    def append(self, point_id, data):
        """
        Append data to the scan storage.

        Args:
            point_id (int): Point ID
            data (dict): Data to be stored
        """
        self.scan_segments[point_id] = data

    @property
    def baseline_ready(self) -> bool:
        """Whether no baseline read was requested or its bundled data has arrived."""
        if self.status_msg is None:
            return True
        return not self.status_msg.info.get("baseline_readout_requested", False) or bool(
            self.baseline
        )

    def expected_entries(self) -> list[dict]:
        """Return the entries that must be written for the terminal sequence status."""
        sequence = self.metadata["entry_sequence"]
        if self.metadata["status"] == "closed":
            return sequence
        return sequence[: self.metadata["entries_started"]]

    def ready_to_write(self) -> bool:
        """
        Check if the scan is ready to be written to file.
        """
        if self.metadata.get("entry_sequence"):
            if not self.scan_finished:
                return False
            for plan in self.expected_entries():
                entry = self.entries.get(plan["entry_index"])
                if entry is None or entry.scan_id != plan["scan_id"] or not entry.ready_to_write():
                    return False
            return True
        if self.forced_finish:
            return True
        if "entry_index" in self.metadata and any(
            not reference.done for reference in self.file_references.values()
        ):
            return False
        if not self.baseline_ready:
            return False
        if self.enforce_sync:
            # wait for all points to be received. Since this method will be called for every
            # update of the scan segments, we can also accept to write after the scan is finished
            _ready_to_write = self.scan_finished and (
                self.num_monitored_readouts == len(self.scan_segments)
            )
            if not _ready_to_write:
                if self.status_msg is None or self.status_msg.readout_priority is None:
                    return False
                monitored_devices = self.status_msg.readout_priority.get("monitored")
                if not monitored_devices:
                    logger.info(
                        f"Received number of segments: {len(self.scan_segments)}, Number of monitored readouts (expected): {self.num_monitored_readouts},  Ready to write: {_ready_to_write}"
                    )
                    return self.scan_finished
            return _ready_to_write
        return self.scan_finished and self.scan_number is not None


class FileWriterManager(BECService):
    def __init__(self, config: ServiceConfig, connector_cls: type[RedisConnector]) -> None:
        """
        Service to write scan data to file.

        Args:
            config (ServiceConfig): Service config
            connector_cls (RedisConnector): Connector class
        """
        super().__init__(config, connector_cls, unique_service=True)
        self._lock = threading.RLock()
        self._shutting_down = False
        self.callbacks = CallbackHandler()
        self.callbacks.register(
            event_type=EventType.DEVICE_UPDATE, callback=self._update_available_devices
        )
        self.file_writer_config = self._service_config.config.get("file_writer")
        self._start_device_manager()
        self.device_configuration = {}
        self.available_beamline_states: list[messages.BeamlineStateConfig] = []
        self.beamline_state_subscriptions: set[str] = set()
        self.beamline_states: dict[str, messages.BeamlineStateMessage] = {}
        self.scan_storage: dict[str, ScanStorage] = {}
        self._entry_locations: dict[str, tuple[str, int]] = {}
        self._pending_baseline_writes: dict[str, dict] = {}
        self._pending_file_references: dict[str, dict[str, messages.FileMessage]] = {}
        self._finished_scan_ids: deque[str] = deque(maxlen=1000)
        self.connector.register(MessageEndpoints.scan_segment(), cb=self._scan_segment_callback)
        self.connector.register(MessageEndpoints.scan_status(), cb=self._scan_status_callback)
        self.connector.register(MessageEndpoints.scan_baseline(), cb=self._scan_baseline_callback)
        self.connector.register(
            patterns=MessageEndpoints.public_file("*", "*"), cb=self._file_reference_callback
        )
        self.connector.register(
            patterns=MessageEndpoints.device_read_configuration("*"),  # type: ignore
            cb=self._device_configuration_callback,
        )
        self.connector.register(
            MessageEndpoints.available_beamline_states(),
            cb=self._update_available_beamline_states,
            from_start=True,
        )
        self.connector.register(
            MessageEndpoints.storage_copy_request(), cb=self._storage_copy_request_callback
        )
        self._run_storage_copy_plugin = self._load_storage_copy_plugin()
        self.async_writer = None
        self.file_writer = HDF5FileWriter(self)
        self.status = messages.BECStatus.RUNNING
        self.refresh_device_configs()

    def _start_device_manager(self):
        self.wait_for_service("DeviceServer")
        self.device_manager = DeviceManagerBase(self)
        self.device_manager.initialize([self.bootstrap_server])

    def shutdown(self, per_thread_timeout_s: float | None = None):
        """Stop active sequence writers before closing the Redis connection."""
        with self._lock:
            self._shutting_down = True
            writers = [
                storage.async_writer
                for storage in self.scan_storage.values()
                if storage.metadata.get("entry_sequence") and storage.async_writer is not None
            ]
            for writer in writers:
                writer.stop()
            for writer in writers:
                writer.join(timeout=per_thread_timeout_s)
                if not writer.is_alive() and writer.file_handle is not None:
                    writer.file_handle.close()
        super().shutdown(per_thread_timeout_s=per_thread_timeout_s)

    def _scan_segment_callback(self, msg: MessageObject):
        msgs = msg.value
        for scan_msg in msgs:
            self.insert_to_scan_storage(scan_msg)

    def _scan_status_callback(self, msg):
        msg = msg.value
        if start := msg.sequence_start():
            self.update_scan_storage_with_status(start)
        self.update_scan_storage_with_status(msg)
        for update in msg.sequence_updates():
            self.update_scan_storage_with_status(update)

    def _scan_baseline_callback(self, msg: MessageObject) -> None:
        baseline_msg = msg.value
        with self._lock:
            scan_id = (
                messages.sequence_id_for_acquisition(
                    baseline_msg.scan_id, baseline_msg.metadata.get("entry_index")
                )
                or baseline_msg.scan_id
            )
            if scan_id in self._finished_scan_ids:
                return
            storage = self.scan_storage.get(scan_id)
            if scan_id != baseline_msg.scan_id:
                storage = self._entry_storage(
                    scan_id, baseline_msg.metadata["entry_index"], baseline_msg.scan_id
                )
            if storage is None:
                self._pending_baseline_writes[baseline_msg.scan_id] = baseline_msg.data
                return
            storage.baseline = baseline_msg.data
            self.check_storage_status(scan_id=scan_id)

    def _file_reference_callback(self, msg: MessageObject) -> None:
        _, scan_id, _, name = msg.topic.rsplit("/", 3)
        file_msg = msg.value
        if name == "master" or file_msg.is_master_file:
            return
        with self._lock:
            if scan_id in self._finished_scan_ids:
                return
            location = self._entry_locations.get(scan_id)
            if location is not None:
                parent_id, index = location
                storage = self._entry_storage(parent_id, index, scan_id)
                previous = storage.file_references.get(name)
                if (
                    previous
                    and previous.file_path == file_msg.file_path
                    and not file_msg.hinted_h5_entries
                ):
                    file_msg = file_msg.model_copy(
                        update={"hinted_h5_entries": previous.hinted_h5_entries}
                    )
                storage.file_references[name] = file_msg
                parent = self.scan_storage[parent_id]
                if index in parent.written_entries:
                    parent.entries_with_new_file_refs.add(index)
                self.check_storage_status(scan_id=parent_id)
                return
            storage = self.scan_storage.get(scan_id)
            if storage is None:
                self._pending_file_references.setdefault(scan_id, {})[name] = file_msg
                return
            storage.file_references[name] = file_msg

    def _get_or_create_scan_storage(self, scan_id: str, scan_number: int | None) -> ScanStorage:
        """Create scan storage and transfer any notifications received before its status or data."""
        with self._lock:
            storage = self.scan_storage.get(scan_id)
            if storage is None:
                storage = ScanStorage(scan_number=scan_number, scan_id=scan_id)
                storage.baseline = self._pending_baseline_writes.pop(scan_id, {})
                storage.file_references = self._pending_file_references.pop(scan_id, {})
                self.scan_storage[scan_id] = storage
            return storage

    def _entry_storage(self, parent_id: str, entry_index: int, scan_id: str) -> ScanStorage:
        parent = self._get_or_create_scan_storage(parent_id, None)
        if entry_index not in parent.entries:
            entry = ScanStorage(parent.scan_number, scan_id)
            entry.file_references = self._pending_file_references.pop(scan_id, {})
            parent.entries[entry_index] = entry
        return parent.entries[entry_index]

    def _device_configuration_callback(self, msg):
        topic, msg = msg.topic, msg.value
        device = topic.split("/")[-1]
        self.update_device_configuration(device, msg)

    def _storage_copy_request_callback(self, msg_obj: MessageObject):
        msg = msg_obj.value
        if not isinstance(msg, messages.StorageCopyRequestMessage):
            logger.error(f"Received invalid storage copy request message: {msg}")
            return
        if self._run_storage_copy_plugin is None:
            logger.error("Storage copy request received but no storage copy plugin is installed.")
            return
        self._run_storage_copy_plugin(msg)

    def _load_storage_copy_plugin(self):
        plugin = plugin_helper.get_file_writer_storage_copy_plugin()
        if plugin is None:
            return None

        def _run_storage_copy_plugin(msg: messages.StorageCopyRequestMessage) -> None:
            current_account_msg = self.connector.get_last(MessageEndpoints.account(), "data")
            active_account = ""
            if current_account_msg is not None and isinstance(current_account_msg.value, str):
                active_account = current_account_msg.value
            plugin(msg.source_file, msg.scope, active_account, sanitize_relative_subdir(msg.subdir))

        return _run_storage_copy_plugin

    def _update_available_beamline_states(
        self, msg: dict[str, messages.AvailableBeamlineStatesMessage]
    ):
        info = msg["data"]
        self.available_beamline_states = info.states
        self.update_beamline_state_subscriptions()

    def update_beamline_state_subscriptions(self):
        """
        Update the beamline state subscriptions.
        This method ensures that the file writer is subscribed to the beamline states that are currently available.
        """
        current_subscriptions = set(self.beamline_state_subscriptions)
        needed_subscriptions = set()
        for state in self.available_beamline_states:
            needed_subscriptions.add(state.name)
            if state.name not in current_subscriptions:
                self.connector.register(
                    MessageEndpoints.beamline_state(state.name),
                    cb=self._beamline_state_callback,
                    from_start=True,
                )
                self.beamline_state_subscriptions.add(state.name)
        # Unregister from states that are no longer needed
        for topic in current_subscriptions - needed_subscriptions:
            self.connector.unregister(
                MessageEndpoints.beamline_state(topic), cb=self._beamline_state_callback
            )
            self.beamline_state_subscriptions.remove(topic)
            self.beamline_states.pop(topic, None)

    def _beamline_state_callback(self, msg: dict[str, messages.BeamlineStateMessage]):
        state_msg = msg["data"]
        self.update_beamline_state(state_msg)

    def update_beamline_state(self, state_msg: messages.BeamlineStateMessage):
        """
        Update the beamline state in the file writer.

        Args:
            state_msg (messages.BeamlineStateMessage): Beamline state message
        """
        # We store the latest beamline state messages in the file writer so that
        # they can be added to the file when writing, even if they do not change
        # during the scan
        with self._lock:
            self.beamline_states[state_msg.name] = state_msg
            for storage in self.scan_storage.values():
                acquisitions = storage.entries.values() if storage.entries else (storage,)
                for entry in acquisitions:
                    if entry.metadata.get("status") == "open":
                        entry.beamline_states[state_msg.name].append(state_msg)

    def _update_available_devices(self, *args) -> None:
        """
        Update the available devices.
        """
        remove_devices = []
        for device in self.device_configuration:
            if (
                device not in self.device_manager.devices
                or not self.device_manager.devices[device].enabled
            ):
                remove_devices.append(device)
        for device in remove_devices:
            self.device_configuration.pop(device)

    def refresh_device_configs(self) -> None:
        """
        Refresh the device configurations.
        """
        for device in self.device_manager.devices:
            info = self.connector.get(MessageEndpoints.device_read_configuration(device))
            if info:
                self.update_device_configuration(device, info)

    def update_scan_storage_with_status(self, msg: messages.ScanStatusMessage) -> None:
        """
        Update the scan storage with the scan status.

        Args:
            msg (messages.ScanStatusMessage): Scan status message
        """
        scan_id = msg.content.get("scan_id")
        if scan_id is None:
            return

        entry_index = msg.info.get("entry_index")
        scan_id = messages.sequence_id_for_acquisition(scan_id, entry_index) or scan_id
        with self._lock:
            if self._shutting_down or scan_id in self._finished_scan_ids:
                return
            scan_number = (
                msg.scan_number if msg.scan_number is not None else msg.info.get("scan_number")
            )
            parent = self._get_or_create_scan_storage(scan_id, scan_number)
            storage = parent
            if entry_index is not None:
                storage = self._entry_storage(scan_id, entry_index, msg.scan_id)
            elif msg.status == "open":
                for plan in msg.info.get("entry_sequence", ()):
                    self._entry_locations[plan["scan_id"]] = (scan_id, plan["entry_index"])
            if scan_number is not None:
                storage.scan_number = scan_number
            self._update_acquisition_status(storage, msg, parent)
            if entry_index is not None and msg.status == "closed":
                if msg.info.get("entry_role") == "main":
                    parent.num_points = storage.num_points
                    parent.num_monitored_readouts = storage.num_monitored_readouts
                if msg.closes_sequence():
                    parent.scan_finished = True
                    parent.end_time = msg.timestamp
                    parent.metadata.update(status="closed", reason=msg.reason)
            self.check_storage_status(scan_id=scan_id)

    def _update_acquisition_status(self, scan_storage, msg, parent):
        scan_id = scan_storage.scan_id
        for state in self.beamline_states.values():
            if state.name not in scan_storage.beamline_states:
                scan_storage.beamline_states[state.name].append(state)

        scan_storage.status_msg = msg

        metadata = msg.content.get("info").copy()
        metadata.pop("DIID", None)
        metadata.pop("stream", None)
        metadata.pop("sequence_start", None)
        metadata.pop("sequence_updates", None)

        scan_storage.metadata.update(metadata)
        status = msg.status
        if status:
            scan_storage.metadata["status"] = status
            scan_storage.metadata["reason"] = msg.reason

        if status == "open" and not scan_storage.start_time:
            scan_storage.start_time = msg.timestamp
            entry_index = metadata.get("entry_index")
            devices = (msg.readout_priority or {}).get("async", [])
            async_signals = self.device_manager.get_bec_signals(
                ["AsyncSignal", "DynamicSignal", "AsyncMultiSignal"]
            )
            if entry_index is not None:
                if parent.async_writer:
                    parent.async_writer.add_entry(entry_index, scan_id, devices, async_signals)
                return
            sequence = bool(metadata.get("entry_sequence"))
            scan_storage.async_writer = AsyncWriter(
                get_full_path(scan_status_msg=msg, name="master", log_if_dir_does_not_exist=False),
                scan_id=scan_id,
                scan_number=msg.scan_number,
                connector=self.connector,
                devices=[] if sequence else devices,
                async_signals=[] if sequence else async_signals,
                **({"sequence": True} if sequence else {}),
            )
            scan_storage.async_writer.start()

        if status in ["closed", "aborted", "halted", "user_completed"]:
            if status in ["aborted", "halted", "user_completed"]:
                scan_storage.forced_finish = True
            if not scan_storage.end_time:
                scan_storage.end_time = msg.content.get("timestamp")

            scan_storage.scan_finished = True
            scan_storage.num_points = msg.num_points
            scan_storage.num_monitored_readouts = msg.num_monitored_readouts
            info = msg.content.get("info")
            if info:
                if msg.scan_type in SYNC_SCAN_TYPES:
                    scan_storage.enforce_sync = True
                else:
                    scan_storage.enforce_sync = info["monitor_sync"] == "bec"

            if scan_storage.async_writer and not metadata.get("entry_sequence"):
                scan_storage.async_writer.stop()
                scan_storage.async_writer.join()

    def insert_to_scan_storage(self, msg: messages.ScanMessage) -> None:
        """
        Insert scan data to the scan storage.

        Args:
            msg (messages.ScanMessage): Scan message
        """
        scan_id = (
            messages.sequence_id_for_acquisition(msg.scan_id, msg.metadata.get("entry_index"))
            or msg.scan_id
        )
        if scan_id is None:
            return
        with self._lock:
            if scan_id in self._finished_scan_ids:
                return
            entry_index = msg.metadata.get("entry_index")
            if entry_index is None:
                storage = self._get_or_create_scan_storage(scan_id, msg.metadata.get("scan_number"))
            else:
                storage = self._entry_storage(scan_id, entry_index, msg.scan_id)
            storage.append(point_id=msg.point_id, data=msg.data)
            self.check_storage_status(scan_id=scan_id)

    def update_device_configuration(self, device: str, msg: messages.DeviceMessage) -> None:
        """
        Update the device configuration. Note that this is a global configuration and not specific to a scan.

        Args:
            msg (messages.DeviceMessage): Device message
        """
        self.device_configuration.update({device: msg.signals})

    def check_storage_status(self, scan_id: str) -> None:
        """
        Check if the scan storage is ready to be written to file and write it if it is.

        Args:
            scan_id (str): Scan ID
        """
        with self._lock:
            storage = self.scan_storage.get(scan_id)
            if self._shutting_down or storage is None:
                return
            if storage.metadata.get("entry_sequence"):
                self._write_ready_entries(storage)
            if storage.ready_to_write():
                self.write_file(scan_id)

    def _write_ready_entries(self, parent: ScanStorage) -> None:
        writer = parent.async_writer
        if writer is None or parent.entry_write_error is not None or writer.error_info:
            return
        for plan in parent.metadata["entry_sequence"]:
            index = plan["entry_index"]
            entry = parent.entries.get(index)
            if (
                index in parent.written_entries
                or entry is None
                or entry.scan_id != plan["scan_id"]
                or not entry.ready_to_write()
            ):
                continue
            try:
                with writer.io_lock:
                    writer.flush_pending(entry_index=index)
                    content = self.file_writer.write(
                        file_path=writer.file_path,
                        data=entry,
                        configuration_data=self.device_configuration,
                        file_handle=writer.open_file(),
                        entry_index=index,
                        written_async_signals=writer.entry_signals(index),
                    )
                    writer.file_handle.flush()
                parent.written_entries.add(index)
                if (
                    index == parent.metadata["entry_sequence"][0]["entry_index"]
                    or plan["role"] == "main"
                ):
                    parent.entry_content[index] = content
            except Exception as exc:
                parent.entry_write_error = exc
                return

    def write_file(self, scan_id: str) -> None:
        """
        Write scan data to file.

        Args:
            scan_id (str): Scan ID
        """
        if not self.scan_storage.get(scan_id):
            return
        storage = self.scan_storage[scan_id]
        if storage.scan_number is None:
            return
        sequence = storage.metadata.get("entry_sequence")
        if sequence and not storage.ready_to_write():
            return

        file_path = ""
        file_suffix = "master"

        start_time = time.time()
        successful = storage.async_writer is None or storage.async_writer.error_info is None
        file_handle = None
        file_content = None
        writer = storage.async_writer

        try:
            if sequence and writer is None and not storage.expected_entries():
                successful = False
            elif sequence:
                if writer is None:
                    raise RuntimeError("Sequence ended without opening its master writer.")
                file_path = writer.file_path
                writer.stop()
                writer.join()
                file_handle = writer.file_handle
                if storage.entry_write_error is not None:
                    raise storage.entry_write_error
                expected = storage.expected_entries()
                successful = (
                    writer.error_info is None
                    and bool(expected)
                    and all(plan["entry_index"] in storage.written_entries for plan in expected)
                )
                if successful:
                    for plan in expected:
                        index = plan["entry_index"]
                        self.file_writer.update_entry_links(
                            file_handle,
                            file_path,
                            storage.entries[index],
                            index,
                            update_file_references=index in storage.entries_with_new_file_refs,
                        )
                    default = next(
                        (plan for plan in expected if plan["role"] == "main"), expected[0]
                    )
                    index = default["entry_index"]
                    entry_name = f"entry_{index}"
                    file_handle.attrs["default"] = entry_name
                    self.file_writer.stored_data_info.clear()
                    self.file_writer.update_data_info(file_handle, entry_name)
                    file_content = storage.entry_content.get(index)
                    file_handle.close()
                    os.rename(writer.tmp_file_path, file_path)
            else:
                file_path = get_full_path(
                    scan_status_msg=storage.status_msg,
                    name=file_suffix,
                    log_if_dir_does_not_exist=False,
                )
                written_async_signals = (
                    storage.async_writer.written_signals if storage.async_writer else None
                )

                # If we've already written device data, we need to append to the file
                written_devices = set(written_async_signals or {})
                write_mode = "w" if not written_devices else "a"
                file_handle = storage.async_writer.file_handle if storage.async_writer else None

                if write_mode == "w":
                    # If we are writing a new file, we need to set the file path
                    self.connector.set_and_publish(
                        MessageEndpoints.public_file(scan_id, "master"),
                        messages.FileMessage(
                            file_path=file_path, done=False, successful=False, is_master_file=True
                        ),
                        expire=3600,
                    )

                self.file_writer.write(
                    file_path=file_path,
                    data=storage,
                    configuration_data=self.device_configuration,
                    mode=write_mode,
                    file_handle=file_handle,
                    written_async_signals=written_async_signals,
                )
        # pylint: disable=broad-except
        # pylint: disable=unused-variable
        except Exception as exc:
            content = traceback.format_exc()
            logger.error(f"Failed to write to file {file_path}.")
            error_info = messages.ErrorInfo(
                error_message=content,
                compact_error_message=traceback.format_exc(limit=0),
                exception_type=exc.__class__.__name__,
                device=None,
            )
            self.connector.raise_alarm(
                severity=Alarms.MINOR, info=error_info, metadata=self.scan_storage[scan_id].metadata
            )
            successful = False
        finally:
            # make sure that the file is closed
            if file_handle:
                file_handle.close()
            if sequence and writer is not None:
                writer.stop()
                writer.join()
                if writer.file_handle is not None:
                    writer.file_handle.close()
            tmp_file_path = file_path.replace(".h5", ".tmp")
            if tmp_file_path and os.path.exists(tmp_file_path):
                if not sequence:
                    logger.info(
                        f"Renaming temporary file {tmp_file_path} to final file {file_path}."
                    )
                    os.rename(tmp_file_path, file_path)
                elif not successful:
                    try:
                        Path(tmp_file_path).unlink(missing_ok=True)
                    except OSError:
                        logger.exception(f"Could not remove temporary scan file {tmp_file_path}.")
        if successful and file_content is not None:
            self.connector.set_and_publish(
                MessageEndpoints.file_content(), file_content, expire=3600
            )
        logger.info(f"Writing to file {file_path} took {time.time() - start_time:.2f} seconds.")

        self.scan_storage.pop(scan_id)
        for plan in storage.metadata.get("entry_sequence", ()):
            entry_scan_id = plan["scan_id"]
            self._entry_locations.pop(entry_scan_id, None)
            self._pending_file_references.pop(entry_scan_id, None)
            self._finished_scan_ids.append(entry_scan_id)
        self._finished_scan_ids.append(scan_id)
        self.connector.set_and_publish(
            MessageEndpoints.public_file(scan_id, "master"),
            messages.FileMessage(file_path=file_path, done=True, successful=successful),
            expire=3600,
        )

        if not successful:
            return

        history_msg = messages.ScanHistoryMessage(
            scan_id=scan_id,
            scan_number=storage.scan_number,
            dataset_number=storage.metadata.get("dataset_number"),
            exit_status=storage.metadata.get("status"),
            reason=storage.metadata.get("reason"),
            file_path=file_path,
            start_time=storage.start_time,
            end_time=storage.end_time,
            num_points=storage.num_points,
            num_monitored_readouts=storage.num_monitored_readouts,
            scan_name=storage.metadata.get("scan_name"),
            request_inputs=storage.metadata.get("request_inputs", {}),
            stored_data_info=self.file_writer.stored_data_info or {},
        )
        self.connector.xadd(
            topic=MessageEndpoints.scan_history(), msg_dict={"data": history_msg}, max_size=10000
        )
        logger.success(f"Finished writing file {file_path}.")
