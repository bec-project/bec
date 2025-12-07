from __future__ import annotations

import concurrent.futures
import copy
import threading
import traceback
from typing import TYPE_CHECKING, TypedDict

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.devicemanager import CancelledError, DeviceConfigError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

if TYPE_CHECKING:
    from bec_server.device_server.devices.devicemanager import DeviceManagerDS

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
            MessageEndpoints.device_server_config_request(),
            cb=self._device_config_callback,
            parent=self,
        )

    @staticmethod
    def _device_config_callback(msg, *, parent: ConfigUpdateHandler, **_kwargs) -> None:
        logger.info(f"Received request: {msg}")
        config_msg: messages.DeviceConfigMessage = msg.value

        # Handle cancel requests immediately
        if config_msg.action == "cancel":
            parent._cancel_config_request(config_msg)
            return

        # Create a cancel event for this request
        cancel_event = threading.Event()

        # Submit to executor and store both future and cancel_event
        future = parent.executor.submit(parent.parse_config_request, msg.value, cancel_event)

        with parent._lock:
            parent._active_request = RequestInfo(
                future=future, cancel_event=cancel_event, request_id=msg.value.metadata.get("RID")
            )
            # Add callback to clean up when done
            future.add_done_callback(lambda f: parent._remove_active_request())

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
        try:
            self.device_manager.check_request_validity(msg)
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

    def _update_config(
        self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event
    ) -> None:
        for dev, dev_config in msg.content["config"].items():
            if cancel_event.is_set():
                raise CancelledError("Config update cancelled")
            device = self.device_manager.devices[dev]
            if "deviceConfig" in dev_config:
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
                device._config["enabled"] = dev_config["enabled"]
                if dev_config["enabled"]:
                    # pylint:disable=protected-access
                    if device.obj._destroyed:
                        obj, config = self.device_manager.construct_device_obj(
                            device._config, device_manager=self.device_manager
                        )
                        self.device_manager.initialize_device(device._config, config, obj)
                    else:
                        self.device_manager.initialize_enabled_device(device)
                else:
                    self.device_manager.disconnect_device(device.obj)
                    self.device_manager.reset_device(device)

    def _flush_config(self) -> None:
        """Flush all devices from the device manager."""
        for _, obj in self.device_manager.devices.items():
            try:
                obj.obj.destroy()
            except Exception:
                logger.warning(f"Failed to destroy {obj.obj.name}")
                raise RuntimeError("Failed to flush config")
        self.device_manager.devices.flush()

    def _reload_config(self, cancel_event: threading.Event) -> None:
        self._flush_config()
        self.device_manager._get_config(cancel_event=cancel_event)
        if self.device_manager.failed_devices:
            self.handle_failed_device_inits()

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
                msg = traceback.format_exc()
                dm.failed_devices[name] = msg
                logger.error(f"Failed to initialize device {name}: {msg}")

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
            self.device_manager.disconnect_device(device)
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

    def handle_failed_device_inits(self):
        if self.device_manager.failed_devices:
            msg = messages.DeviceConfigMessage(
                action="update",
                config={name: {"enabled": False} for name in self.device_manager.failed_devices},
            )
            # Create a non-cancelled event for internal calls
            cancel_event = threading.Event()
            self._update_config(msg, cancel_event)
            self.force_update_config_in_redis()
        return

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
