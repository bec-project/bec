from __future__ import annotations

import concurrent.futures
import copy
import threading
import traceback
from typing import TYPE_CHECKING, TypedDict

from ophyd_devices import set_registry

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.devicemanager import CancelledError, DeviceConfigError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.plugin_helper import reload_plugin_modules

if TYPE_CHECKING:
    from ophyd import OphydObject

    from bec_server.device_server.devices.devicemanager import DeviceManagerDS, DSDevice

logger = bec_logger.logger


class RequestInfo(TypedDict):
    future: concurrent.futures.Future
    cancel_event: threading.Event
    request_id: str


class ConfigUpdateHandler:
    def __init__(self, device_manager: DeviceManagerDS) -> None:
        self.device_manager = device_manager
        self.connector = self.device_manager.connector
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ConfigUpdateHandler"
        )
        self._active_request: RequestInfo | None = None
        self._lock = threading.Lock()
        self.connector.register(
            MessageEndpoints.device_server_config_request(), cb=self._device_config_callback
        )

    def _device_config_callback(self, msg) -> None:
        logger.info(f"Received request: {msg}")
        config_msg: messages.DeviceConfigMessage = msg.value

        # Handle cancel requests immediately
        if config_msg.action == "cancel":
            self._cancel_config_request(config_msg)
            return

        # Create a cancel event for this request
        cancel_event = threading.Event()

        # Submit to executor and store both future and cancel_event
        future = self.executor.submit(self.parse_config_request, msg.value, cancel_event)

        with self._lock:
            self._active_request = RequestInfo(
                future=future, cancel_event=cancel_event, request_id=msg.value.metadata.get("RID")
            )
            # Add callback to clean up when done
            future.add_done_callback(lambda f: self._remove_active_request())

    def _remove_active_request(self) -> None:
        """Clear the active request."""
        with self._lock:
            self._active_request = None

    def _cancel_config_request(
        self, msg: messages.DeviceConfigMessage, timeout: float = 30.0
    ) -> None:
        """Cancel the active config request.

        Args:
            msg (BECMessage.DeviceConfigMessage): Config message containing the cancel request
        """
        with self._lock:
            request_info = self._active_request
            if request_info is None:
                logger.warning("No active request found to cancel")
                self.send_config_request_reply(
                    accepted=False,
                    error_msg="No active request found to cancel",
                    metadata=msg.metadata,
                )
                return
        # Signal cancellation
        cancel_event = request_info["cancel_event"]
        future = request_info["future"]
        active_request_id = request_info["request_id"]
        cancel_event.set()
        logger.info(f"Cancellation requested for config request {active_request_id}")

        # Wait for the task to actually stop
        try:
            out = concurrent.futures.wait([future], timeout=timeout)
            if future in out.not_done:
                error_msg = "Config cancellation is exceeding the expected time limit. The config will be flushed and you may need to restart the device server."
                self.connector.raise_alarm(
                    severity=Alarms.WARNING,
                    info=messages.ErrorInfo(
                        id="ConfigCancellationTimeout",
                        error_message=error_msg,
                        compact_error_message=error_msg,
                        exception_type="TimeoutError",
                    ),
                )
                self._flush_config()
                concurrent.futures.wait([future])

            logger.info(f"Config request {active_request_id} has completed after cancellation")
            self.send_config_request_reply(accepted=True, error_msg="", metadata=msg.metadata)
        except Exception as exc:
            logger.warning(f"Error waiting for cancellation of {active_request_id}: {exc}")
            self.send_config_request_reply(
                accepted=False, error_msg=f"Error during cancellation: {exc}", metadata=msg.metadata
            )

    def parse_config_request(
        self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event
    ) -> None:
        """Processes a config request. If successful, it emits a config reply

        Args:
            msg (BECMessage.DeviceConfigMessage): Config request
            cancel_event: Event to check for cancellation

        """
        error_msg = ""
        accepted = True
        original_devices = None
        original_session_devices = None
        try:
            self.device_manager.check_request_validity(msg)
            if msg.action == "add":
                original_session_devices = self.device_manager.current_session["devices"].copy()
                configured_devices = {device["name"] for device in original_session_devices}
                for name in msg.config:
                    if name in configured_devices:
                        raise DeviceConfigError(
                            f"Device {name} already exists in the session config."
                        )
                original_devices = set(self.device_manager.devices)
            match msg.action:
                case "update":
                    self._update_config(msg, cancel_event)
                case "add":
                    self._add_config(msg, cancel_event)
                    if self.device_manager.failed_devices:
                        msg.metadata["failed_devices"] = self.device_manager.failed_devices
                case "reload":
                    self._reload_config(cancel_event)
                    if self.device_manager.failed_devices:
                        msg.metadata["failed_devices"] = self.device_manager.failed_devices
                case "remove":
                    self._remove_config(msg, cancel_event)
                case _:
                    pass
            # After any config change, resolve dependencies. It will raise if dependencies are not met.
            self.update_session_config(msg)
            self.device_manager.resolve_device_dependencies(
                self.device_manager.current_session["devices"]
            )
        except CancelledError:
            error_msg = "Request was cancelled"
            accepted = False
            logger.info(
                f"Config request {msg.metadata.get('RID')} was cancelled. The config will be flushed."
            )
            self._flush_config()

        except Exception:
            error_msg = traceback.format_exc()
            accepted = False
            if original_devices is not None:
                self._rollback_added_devices(msg, original_devices, original_session_devices)
        finally:
            self.send_config_request_reply(
                accepted=accepted, error_msg=error_msg, metadata=msg.metadata
            )

    def send_config_request_reply(self, accepted: bool, error_msg: str, metadata: dict) -> None:
        """
        Sends a config request reply

        Args:
            accepted (bool): Whether the request was accepted
            error_msg (str): Error message
            metadata (dict): Metadata of the request
        """
        msg = messages.RequestResponseMessage(
            accepted=accepted, message=error_msg, metadata=metadata
        )
        request_id = metadata.get("RID", "")
        self.device_manager.connector.set(
            MessageEndpoints.device_config_request_response(request_id), msg, expire=60
        )

    def _cleanup_failed_device_init(self, obj: OphydObject, device: DSDevice | None = None) -> None:
        """Best-effort cleanup that does not mask the initialization failure."""
        try:
            self.device_manager.disconnect_device(obj)
        # pylint: disable=broad-except
        except Exception:
            logger.error(
                f"Failed to destroy partially initialized device {obj.name}: "
                f"{traceback.format_exc()}"
            )

        if device is None:
            return
        try:
            self.device_manager.reset_device(device)
        # pylint: disable=broad-except
        except Exception:
            logger.error(
                f"Failed to reset partially initialized device {device.name}: "
                f"{traceback.format_exc()}"
            )

    def _update_config(
        self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event
    ) -> None:
        for dev, dev_config in msg.content["config"].items():
            if cancel_event.is_set():
                raise CancelledError("Config update cancelled")
            device = self.device_manager.devices[dev]
            if "deviceConfig" in dev_config:
                if not device.enabled:
                    raise DeviceConfigError(
                        f"Cannot update deviceConfig for disabled device {dev}. Enable the device first."
                    )
                new_config = dev_config["deviceConfig"] or {}
                # store old config
                old_config = device._config["deviceConfig"].copy()

                # apply config
                try:
                    self.device_manager.update_config(device.obj, new_config)
                except Exception as exc:
                    self.device_manager.update_config(device.obj, old_config)
                    raise DeviceConfigError(f"Error during object update. {exc}")

                if "limits" in dev_config["deviceConfig"]:
                    limits = {
                        "low": {"value": device.obj.low_limit_travel.get()},
                        "high": {"value": device.obj.high_limit_travel.get()},
                    }
                    self.device_manager.connector.set_and_publish(
                        MessageEndpoints.device_limits(device.name),
                        messages.DeviceMessage(signals=limits),
                    )

            if "enabled" in dev_config:
                # pylint: disable=protected-access
                was_enabled = device._config.get("enabled", True)
                if was_enabled and not dev_config["enabled"]:
                    # It was enabled and we want to disable it. Disconnect and reset the device.
                    self.device_manager.disconnect_device(device.obj)
                    self.device_manager.reset_device(device)
                    device._config["enabled"] = False
                elif not was_enabled and dev_config["enabled"]:
                    # It was disabled and we want to enable it. Construct and initialize the device.
                    device._config["enabled"] = True
                    obj = None
                    try:
                        obj, config = self.device_manager.construct_device_obj(
                            device._config, device_manager=self.device_manager
                        )
                        self.device_manager.initialize_device(device._config, config, obj)
                    # pylint: disable=broad-except
                    except Exception:
                        device._config["enabled"] = was_enabled
                        failed_device = self.device_manager.devices.get(dev)
                        if failed_device is not None and failed_device is not device:
                            failed_device._config["enabled"] = was_enabled
                            self.device_manager.devices._add_device(dev, device)
                            self._cleanup_failed_device_init(failed_device.obj, failed_device)
                        elif obj is not None:
                            self._cleanup_failed_device_init(obj)
                        raise

    def _flush_config(self) -> None:
        """Flush all devices from the device manager."""
        objects = [device.obj for device in self.device_manager.devices.values()]
        roots = [obj.root for obj in objects]
        # Cancel every old root before any destroy hook can wait for another
        # device's set. Keep admission closed until the old config is removed.
        with set_registry.stopping_many(roots):
            try:
                for obj in objects:
                    try:
                        self.device_manager.disconnect_device(obj)
                    except Exception as exc:
                        logger.warning(f"Failed to destroy {obj.name}")
                        raise RuntimeError("Failed to flush config") from exc
                self.device_manager.devices.flush()
            finally:
                # A hook may have started cleanup sets on a different old root,
                # including one whose destroy was skipped after an earlier error.
                for root in roots:
                    set_registry.cancel(root)

    def _reload_config(self, cancel_event: threading.Event) -> None:
        self._flush_config()
        reload_plugin_modules()

        self.device_manager._get_config(cancel_event=cancel_event)

    def _add_config(self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event) -> None:
        """
        Adds new devices to the config and initializes them. If a device fails to initialize, it is added to the
        failed_devices dictionary.

        Args:
            msg (BECMessage.DeviceConfigMessage): Config message containing the new devices
            cancel_event: Event to check for cancellation

        """
        # pylint:disable=protected-access
        self.device_manager.failed_devices = {}
        dm: DeviceManagerDS = self.device_manager
        for dev, dev_config in msg.content["config"].items():
            if cancel_event.is_set():
                raise CancelledError("Config add cancelled")
            name = dev_config["name"]
            logger.info(f"Adding device {name}")
            if dev in dm.devices:
                continue  # tbd what to do here: delete and add new device?
            obj, config = dm.construct_device_obj(dev_config, device_manager=dm)
            try:
                dm.initialize_device(dev_config, config, obj)
            # pylint: disable=broad-except
            except Exception:
                error = traceback.format_exc()
                if name not in dm.devices:
                    self._cleanup_failed_device_init(obj)
                    raise
                dm.failed_devices[name] = error
                dev_config["enabled"] = False
                failed_device = dm.devices[name]
                failed_device._config["enabled"] = False
                self._cleanup_failed_device_init(failed_device.obj, failed_device)
                logger.error(f"Failed to initialize device {name}: {error}")

    def _rollback_added_devices(
        self,
        msg: messages.DeviceConfigMessage,
        original_devices: set[str],
        original_session_devices: list[dict],
    ) -> None:
        """Remove a rejected add batch without masking the original failure."""
        dm = self.device_manager
        for name in reversed(msg.config):
            if name in original_devices:
                continue
            if name in dm.devices:
                device = dm.devices[name]
                # Registered initialization failures have already been cleaned up.
                if name not in dm.failed_devices:
                    self._cleanup_failed_device_init(device.obj, device)
                del dm.devices[name]
            try:
                pipe = self.connector.pipeline()
                for endpoint in (
                    MessageEndpoints.device_status,
                    MessageEndpoints.device_read,
                    MessageEndpoints.device_readback,
                    MessageEndpoints.device_read_configuration,
                    MessageEndpoints.device_info,
                    MessageEndpoints.device_limits,
                ):
                    self.connector.delete(endpoint(name), pipe=pipe)
                pipe.execute()
            # pylint: disable=broad-except
            except Exception:
                logger.error(f"Failed to remove device data for {name}: {traceback.format_exc()}")
        dm.current_session["devices"][:] = original_session_devices
        dm.failed_devices = {}
        msg.metadata.pop("failed_devices", None)

    def _remove_config(
        self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event
    ) -> None:
        """
        Removes devices from the config and disconnects them.

        Args:
            msg (BECMessage.DeviceConfigMessage): Config message containing the devices to be removed
            cancel_event: Event to check for cancellation

        """
        for dev in msg.content["config"]:
            if cancel_event.is_set():
                raise CancelledError("Config remove cancelled")
            logger.info(f"Removing device {dev}")
            if dev not in self.device_manager.devices:
                continue
            device = self.device_manager.devices[dev]
            self.device_manager.disconnect_device(device.obj)
            self.device_manager.reset_device(device)
            self.device_manager.devices.pop(dev)

    def update_session_config(self, msg: messages.DeviceConfigMessage) -> None:
        """
        Updates the current session config with the new config from the message.

        Args:
            msg (BECMessage.DeviceConfigMessage): Config message containing the new config

        """
        action = msg.action
        match action:
            case "update":
                # Update the session config
                for dev in msg.content["config"]:
                    dev_config = self.device_manager.devices[dev]._config
                    session_device_config = next(
                        (
                            d
                            for d in self.device_manager.current_session["devices"]
                            if d["name"] == dev
                        ),
                        None,
                    )
                    if session_device_config:
                        session_device_config.update(dev_config)
            case "add":
                # Add new devices to the session config
                for dev, dev_config in msg.content["config"].items():
                    self.device_manager.current_session["devices"].append(dev_config)
            case "remove":
                # Remove devices from the session config
                for dev in msg.content["config"]:
                    self.device_manager.current_session["devices"] = [
                        d
                        for d in self.device_manager.current_session["devices"]
                        if d["name"] != dev
                    ]

    def handle_failed_device_inits(self) -> None:
        """Clean up failed devices and persist their disabled state in the session and Redis."""
        if self.device_manager.failed_devices:
            msg = messages.DeviceConfigMessage(
                action="update",
                config={name: {"enabled": False} for name in self.device_manager.failed_devices},
            )
            for name in self.device_manager.failed_devices:
                device = self.device_manager.devices[name]
                # pylint: disable=protected-access
                device._config["enabled"] = False
                self._cleanup_failed_device_init(device.obj, device)
            self.update_session_config(msg)
            self.force_update_config_in_redis()

    def force_update_config_in_redis(self):
        config = []
        for name, device in self.device_manager.devices.items():
            device_config = copy.deepcopy(device._config)
            device_config["name"] = name
            config.append(device_config)
        msg = messages.AvailableResourceMessage(resource=config)
        self.device_manager.connector.set(MessageEndpoints.device_config(), msg)

    def shutdown(self) -> None:
        """Shutdown the config update handler, canceling any active request."""
        logger.info("Shutting down ConfigUpdateHandler...")

        request_info: RequestInfo | None = None
        with self._lock:
            request_info = self._active_request
            if request_info:
                # Signal cancellation for the active request
                request_info["cancel_event"].set()
                logger.info(
                    f"Cancellation signaled for config request {request_info['request_id']}"
                )

        # Wait for the request to complete
        if request_info:
            logger.info("Waiting for active config request to complete...")
            concurrent.futures.wait([request_info["future"]], timeout=5.0)

        # Shutdown the executor
        self.executor.shutdown(wait=True, cancel_futures=True)
        logger.info("ConfigUpdateHandler shutdown complete")
