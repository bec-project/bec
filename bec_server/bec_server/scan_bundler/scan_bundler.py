from __future__ import annotations

import collections
import threading
import time
import traceback
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.bec_service import BECService
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .bec_emitter import BECEmitter
from .bluesky_emitter import BlueskyEmitter

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector


logger = bec_logger.logger


class ScanBundler(BECService):
    def __init__(self, config, connector_cls: type[RedisConnector]) -> None:
        super().__init__(config, connector_cls, unique_service=True)

        self.device_manager = None
        self._start_device_manager()
        self.connector.register(
            patterns=MessageEndpoints.device_read("*"),
            cb=self._device_read_callback,
            name="device_read_register",
        )
        self.connector.register(
            MessageEndpoints.scan_queue_status(),
            cb=self._scan_queue_callback,
            group_id="scan_bundler",
            name="scan_queue_register",
        )
        self.connector.register(
            MessageEndpoints.scan_status(),
            cb=self._scan_status_callback,
            group_id="scan_bundler",
            name="scan_status_register",
        )

        self.sync_storage = {}
        self.monitored_devices = {}
        self.baseline_devices = {}
        self.device_storage = {}
        self.scan_motors = {}
        self.readout_priority = {}
        self.storage_initialized = set()
        self.current_queue = None
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.executor_tasks = collections.deque(maxlen=100)
        self.scan_id_history = collections.deque(maxlen=10)
        self._lock = threading.Lock()
        self._emitter = []
        self._initialize_emitters()
        self.status = messages.BECStatus.RUNNING

    def _initialize_emitters(self):
        self._emitter = [BECEmitter(self), BlueskyEmitter(self)]

    def run_emitter(self, emitter_method: Callable, *args, **kwargs):
        for emi in self._emitter:
            try:
                getattr(emi, emitter_method)(*args, **kwargs)
            except Exception:
                content = traceback.format_exc()
                logger.error(f"Failed to run emitter: {content}")

    def _start_device_manager(self):
        self.wait_for_service("DeviceServer")
        self.device_manager = DeviceManagerBase(self)
        self.device_manager.initialize(self.bootstrap_server)

    def _device_read_callback(self, msg, **_kwargs):
        # pylint: disable=protected-access
        dev = msg.topic.split(MessageEndpoints.device_read("").endpoint)[-1]
        msgs = msg.value
        logger.debug(f"Received reading from device {dev}")
        if not isinstance(msgs, list):
            msgs = [msgs]
        task = self.executor.submit(self._add_device_to_storage, msgs, dev)
        self.executor_tasks.append(task)

    def _scan_queue_callback(self, msg, **_kwargs):
        msg = msg.value
        logger.trace(msg)
        self.current_queue = msg.content["queue"]["primary"].get("info")

    def _scan_status_callback(self, msg, **_kwargs):
        msg = msg.value
        self.handle_scan_status_message(msg)

    def handle_scan_status_message(self, msg: messages.ScanStatusMessage) -> None:
        """handle scan status messages"""
        logger.info(f"Received new scan status: {msg}")
        scan_id = msg.content["scan_id"]
        if not scan_id:
            return
        self.cleanup_storage()
        if scan_id not in self.sync_storage:
            self._initialize_scan_container(msg)
            if scan_id not in self.scan_id_history:
                self.scan_id_history.append(scan_id)
        if msg.content.get("status") != "open":
            self._scan_status_modification(msg)
        self.run_emitter("on_scan_status_update", msg)

    def _scan_status_modification(self, msg: messages.ScanStatusMessage):
        status = msg.content.get("status")
        if status not in ["closed", "aborted", "paused", "halted"]:
            logger.error(f"Unknown scan status {status}")
            return
        scan_id = msg.content.get("scan_id")
        if not scan_id:
            logger.warning(f"Received scan status update without scan_id: {msg}")
            return
        if self.sync_storage.get(scan_id):
            self.sync_storage[scan_id]["status"] = status
        else:
            self.sync_storage[scan_id] = {"info": {}, "status": status, "sent": set()}
            self.storage_initialized.add(scan_id)
            if scan_id not in self.scan_id_history:
                self.scan_id_history.append(scan_id)

    def _initialize_scan_container(self, scan_msg: messages.ScanStatusMessage):
        if scan_msg.content.get("status") != "open":
            return

        scan_id = scan_msg.content["scan_id"]
        scan_info = scan_msg.content["info"]
        scan_motors = list(set(self.device_manager.devices[m] for m in scan_info["scan_motors"]))
        self.scan_motors[scan_id] = scan_motors
        self.readout_priority[scan_id] = scan_info["readout_priority"]
        if scan_id not in self.storage_initialized:
            self.sync_storage[scan_id] = {"info": scan_info, "status": "open", "sent": set()}
            self.monitored_devices[scan_id] = {
                "devices": self.device_manager.devices.monitored_devices(
                    readout_priority=self.readout_priority[scan_id]
                ),
                "point_id": {},
            }
            self.baseline_devices[scan_id] = {
                "devices": self.device_manager.devices.baseline_devices(
                    readout_priority=self.readout_priority[scan_id]
                ),
                "done": {
                    dev.name: False
                    for dev in self.device_manager.devices.baseline_devices(
                        readout_priority=self.readout_priority[scan_id]
                    )
                },
            }
            self.storage_initialized.add(scan_id)
            self.run_emitter("on_init", scan_id)
            return

    def _step_scan_update(self, scan_id, device, signal, metadata):
        if "point_id" not in metadata:
            return
        with self._lock:
            dev = {device: signal}
            point_id = metadata["point_id"]
            monitored_devices = self.monitored_devices[scan_id]

            self.sync_storage[scan_id][point_id] = {
                **self.sync_storage[scan_id].get(point_id, {}),
                **dev,
            }

            if monitored_devices["point_id"].get(point_id) is None:
                monitored_devices["point_id"][point_id] = {
                    dev.name: False for dev in monitored_devices["devices"]
                }
            monitored_devices["point_id"][point_id][device] = True

            monitored_devices_completed = list(monitored_devices["point_id"][point_id].values())

            all_monitored_devices_completed = bool(
                all(monitored_devices_completed)
                and (
                    len(monitored_devices_completed)
                    == len(self.monitored_devices[scan_id]["devices"])
                )
            )
            missing_devices = [
                dev for dev, status in monitored_devices["point_id"][point_id].items() if not status
            ]
            if missing_devices:
                logger.debug(
                    f"Waiting for devices {missing_devices} to complete for scan_id {scan_id} at point {point_id}."
                )
            if all_monitored_devices_completed and self.sync_storage[scan_id].get(point_id):
                self._update_monitor_signals(scan_id, point_id)
                self._send_scan_point(scan_id, point_id)

    def _fly_scan_update(self, scan_id, device, signal, metadata):
        if "point_id" not in metadata:
            logger.warning(
                f"Received device message from device {device} without point_id in metadata. {metadata}"
            )
            return
        with self._lock:
            dev = {device: signal}
            point_id = metadata["point_id"]
            logger.info(
                f"Received reading from device {device} for scan_id {scan_id} at point {point_id}."
            )
            if self.sync_storage[scan_id].get("info", {}).get("monitor_sync", "bec") == "bec":
                # For monitor sync with BEC, we use the point_id as the key for the sync_storage.
                monitored_devices = self.monitored_devices[scan_id]

                self.sync_storage[scan_id][point_id] = {
                    **self.sync_storage[scan_id].get(point_id, {}),
                    **dev,
                }

                if monitored_devices["point_id"].get(point_id) is None:
                    monitored_devices["point_id"][point_id] = {
                        dev.name: False for dev in monitored_devices["devices"]
                    }
                monitored_devices["point_id"][point_id][device] = True

                monitored_devices_completed = list(monitored_devices["point_id"][point_id].values())

                all_monitored_devices_completed = bool(
                    all(monitored_devices_completed)
                    and (
                        len(monitored_devices_completed)
                        == len(self.monitored_devices[scan_id]["devices"])
                    )
                )
                if all_monitored_devices_completed and self.sync_storage[scan_id].get(point_id):
                    self._update_monitor_signals(scan_id, point_id)
                    self._send_scan_point(scan_id, point_id)
            else:
                self.sync_storage[scan_id][point_id] = {
                    **self.sync_storage[scan_id].get(point_id, {}),
                    **signal,
                }

                if self.sync_storage[scan_id].get(point_id):
                    self._update_monitor_signals(scan_id, point_id)
                    self._send_scan_point(scan_id, point_id)

    def _baseline_update(self, scan_id, device, signal):
        with self._lock:
            dev = {device: signal}
            baseline_devices_status = self.baseline_devices[scan_id]["done"]
            baseline_devices_status[device] = True

            self.sync_storage[scan_id]["baseline"] = {
                **self.sync_storage[scan_id].get("baseline", {}),
                **dev,
            }

            if not all(status for status in baseline_devices_status.values()):
                return

            logger.info(f"Sending baseline readings for scan_id {scan_id}.")
            logger.debug("Baseline: ", self.sync_storage[scan_id]["baseline"])
            self.run_emitter("on_baseline_emit", scan_id)
            self.baseline_devices[scan_id]["done"] = {
                dev.name: False
                for dev in self.device_manager.devices.baseline_devices(
                    readout_priority=self.readout_priority[scan_id]
                )
            }

    def _wait_for_scan_id(self, scan_id, timeout_time=10):
        elapsed_time = 0
        while scan_id not in self.storage_initialized:
            msg = self.connector.get(MessageEndpoints.public_scan_info(scan_id))
            if msg and msg.content["scan_id"] == scan_id:
                self.handle_scan_status_message(msg)
            if scan_id in self.sync_storage:
                if self.sync_storage[scan_id]["status"] in ["closed", "aborted"]:
                    logger.info(
                        f"Received reading for {self.sync_storage[scan_id]['status']} scan {scan_id}."
                    )
                    return
            time.sleep(0.05)
            elapsed_time += 0.05
            if elapsed_time > timeout_time:
                raise TimeoutError(
                    f"Reached timeout whilst waiting for scan_id {scan_id} to appear."
                )

    def _add_device_to_storage(self, msgs, device, timeout_time=10):
        for msg in msgs:
            metadata = msg.metadata

            scan_id = metadata.get("scan_id")
            if not scan_id:
                logger.info("Received device message without scan_id")
                return

            signal = msg.content.get("signals")

            try:
                self._wait_for_scan_id(scan_id, timeout_time=timeout_time)
            except TimeoutError:
                logger.warning(f"Could not find a matching scan_id {scan_id} in sync_storage.")
                return

            if self.sync_storage[scan_id]["status"] in ["aborted", "closed"]:
                # check if the sync_storage has been initialized properly.
                # In case of post-scan initialization, scan info is not available
                if not self.sync_storage[scan_id]["info"].get("scan_type"):
                    return
            self.device_storage[device] = signal
            readout_priority = metadata.get("readout_priority")
            device_is_monitor_sync = self.sync_storage[scan_id]["info"]["monitor_sync"] == device
            dev_obj = self.device_manager.devices.get(device)
            if dev_obj in self.monitored_devices[scan_id]["devices"] or device_is_monitor_sync:
                if self.sync_storage[scan_id]["info"]["scan_type"] == "step":
                    self._step_scan_update(scan_id, device, signal, metadata)
                elif self.sync_storage[scan_id]["info"]["scan_type"] == "fly":
                    self._fly_scan_update(scan_id, device, signal, metadata)
                else:
                    raise RuntimeError(
                        f"Unknown scan type {self.sync_storage[scan_id]['info']['scan_type']}"
                    )
            elif readout_priority == "baseline":
                self._baseline_update(scan_id, device, signal)
            else:
                logger.warning(f"Received reading from unknown device {device}")

    def _update_monitor_signals(self, scan_id, point_id) -> None:
        if self.sync_storage[scan_id]["info"]["scan_type"] == "fly":
            # for fly scans, take all primary and monitor signals
            devices = self.monitored_devices[scan_id]["devices"]

            readings = self._get_last_device_readback(devices)

            for read, dev in zip(readings, devices):
                self.sync_storage[scan_id][point_id][dev.name] = read

    def _get_last_device_readback(self, devices: list) -> list:
        pipe = self.connector.pipeline()
        for dev in devices:
            self.connector.get(MessageEndpoints.device_readback(dev.name), pipe)
        return [msg.content["signals"] for msg in self.connector.execute_pipeline(pipe)]

    def cleanup_storage(self):
        """remove old scan_ids to free memory"""
        remove_scan_ids = []
        for scan_id, entry in self.sync_storage.items():
            if entry.get("status") not in ["closed", "aborted"]:
                continue
            if scan_id in self.scan_id_history:
                continue
            remove_scan_ids.append(scan_id)

        for scan_id in remove_scan_ids:
            for storage in [
                "sync_storage",
                "monitored_devices",
                "baseline_devices",
                "scan_motors",
                "readout_priority",
            ]:
                try:
                    getattr(self, storage).pop(scan_id)
                except KeyError:
                    logger.warning(f"Failed to remove {scan_id} from {storage}.")
            # self.bluesky_emitter.cleanup_storage(scan_id)
            self.run_emitter("on_cleanup", scan_id)
            self.storage_initialized.remove(scan_id)

    def _send_scan_point(self, scan_id, point_id) -> None:
        logger.info(f"Sending point {point_id} for scan_id {scan_id}.")
        logger.debug(f"{point_id}, {self.sync_storage[scan_id][point_id]}")

        self.run_emitter("on_scan_point_emit", scan_id, point_id)

        if point_id not in self.sync_storage[scan_id]["sent"]:
            self.sync_storage[scan_id]["sent"].add(point_id)
        else:
            logger.warning(f"Resubmitting existing point_id {point_id} for scan_id {scan_id}")

    def shutdown(self):
        self.device_manager.shutdown()
        self.connector.shutdown()
        self.executor.shutdown()
        for emi in self._emitter:
            emi.shutdown()
