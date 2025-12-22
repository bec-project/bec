from __future__ import annotations

import concurrent.futures
import threading
import time
import traceback
import uuid
from typing import TYPE_CHECKING, Tuple, TypedDict

from bec_lib import messages
from bec_lib.atlas_models import Device, DevicePartial
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.config_helper import CONF
from bec_lib.devicemanager import CancelledError
from bec_lib.devicemanager import DeviceManagerBase as DeviceManager
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.device import DeviceBaseWithConfig
    from bec_lib.redis_connector import RedisConnector
    from bec_server.scihub.atlas.atlas_connector import AtlasConnector


logger = bec_logger.logger


class RequestInfo(TypedDict):
    future: concurrent.futures.Future
    cancel_event: threading.Event
    request_id: str


class ConfigHandler:
    """Handles device configuration requests and updates."""

    def __init__(self, atlas_connector: AtlasConnector, connector: RedisConnector) -> None:
        self.atlas_connector = atlas_connector
        self.connector = connector
        self.device_manager = DeviceManager(self.atlas_connector.scihub)
        self.device_manager.initialize(atlas_connector.config.redis)
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ConfigHandler"
        )
        self._active_request: RequestInfo | None = None
        self._lock = threading.Lock()

    def handle_config_request_callback(self, msg: messages.DeviceConfigMessage) -> None:
        """Handle incoming config request messages.

        This method is called by the message broker callback and manages the execution
        of config requests, including cancel handling and async execution.

        Args:
            msg(messages.DeviceConfigMessage): Incoming config request message
        """
        logger.info(f"Received request: {msg}")

        # Handle cancel requests immediately
        if msg.action == "cancel":
            self._cancel_config_request(msg)
            return

        # Create a cancel event for this request
        cancel_event = threading.Event()

        # Submit to executor and store both future and cancel_event
        future = self.executor.submit(self.parse_config_request, msg, cancel_event)

        with self._lock:
            self._active_request = RequestInfo(
                future=future, cancel_event=cancel_event, request_id=msg.metadata.get("RID")
            )
            # Add callback to clean up when done
            future.add_done_callback(lambda f: self._remove_active_request())

    def parse_config_request(
        self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event
    ) -> None:
        """Processes a config request. If successful, it emits a config reply

        Args:
            msg (BMessage.DeviceConfigMessage): Config request
            cancel_event: Event to check for cancellation

        """
        error_msg = ""
        accepted = True
        try:
            self.device_manager.check_request_validity(msg)
            match msg.action:
                case "update":
                    self._update_config(msg, cancel_event)
                case "reload":
                    self._reload_config(msg)
                case "set":
                    self._set_config(msg, cancel_event)
                case "add":
                    self._add_to_config(msg, cancel_event)
                case "remove":
                    self._remove_from_config(msg)
                case "reset":
                    self._reset_config(msg)

        except CancelledError:
            error_msg = "Request was cancelled"
            accepted = False
            logger.info(f"Config request {msg.metadata.get('RID')} was cancelled.")
        except Exception:
            error_msg = traceback.format_exc()
            accepted = False
        finally:
            if not accepted:
                self.send_config_request_reply(
                    accepted=False, error_msg=error_msg, metadata=msg.metadata
                )

    def send_config(self, msg: messages.DeviceConfigMessage) -> None:
        """broadcast a new config"""
        self.connector.send(MessageEndpoints.device_config_update(), msg)

    def send_config_request_reply(self, accepted, error_msg, metadata):
        """send a config request reply"""
        msg = messages.RequestResponseMessage(
            accepted=accepted, message=error_msg, metadata=metadata
        )
        request_id = metadata.get("RID")
        self.connector.set(
            MessageEndpoints.device_config_request_response(request_id), msg, expire=60
        )

    def _remove_active_request(self) -> None:
        """Clear the active request."""
        with self._lock:
            self._active_request = None

    #################################################################
    ############### Config Actions ##################################
    #################################################################

    def _update_config(self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event):
        """
        Update the currently available config with the provided one.
        If the device does not exist, it is skipped.

        Args:
            msg (messages.DeviceConfigMessage): Config update message

        """
        updated = False
        dev_configs = msg.content["config"]

        for dev, config in dev_configs.items():
            if cancel_event.is_set():
                raise CancelledError("Config update cancelled")
            if dev not in self.device_manager.devices:
                continue

            device = self.device_manager.devices[dev]
            updated = self._update_device_config(device, config.copy())
            if updated:
                self.update_config_in_redis(device)

        # send updates to services
        if updated:
            self.send_config(msg)
            self.send_config_request_reply(accepted=True, error_msg=None, metadata=msg.metadata)

    def _reload_config(self, msg: messages.DeviceConfigMessage):
        """
        Reload the config in all services.
        Args:
            msg (messages.DeviceConfigMessage): Config reload message
        """
        self.send_config_request_reply(accepted=True, error_msg=None, metadata=msg.metadata)
        self.send_config(msg)

    def _set_config(self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event):
        """
        Replace the config with the provided one. It will wait for the DeviceServer to accept the new config
        before resolving.

        Args:
            msg (messages.DeviceConfigMessage): Config set message
            cancel_event: Event to check for cancellation
        """
        config = msg.content["config"]
        msg.metadata["updated_config"] = False

        # make sure the config is valid before setting it in redis
        for name, device in config.items():
            if cancel_event.is_set():
                raise CancelledError("Config set cancelled")
            self._convert_to_db_config(name, device)
            Device(**device)
        self.set_config_in_redis(list(config.values()))

        msg.metadata["updated_config"] = True

        # update the devices in the device server
        # if the server fails, the config is wrong
        request_id = str(uuid.uuid4())
        self._update_device_server(request_id, config, action="reload")
        accepted, server_response_msg = self._wait_for_device_server_update(
            request_id, timeout_time=min(300, 30 * len(config))
        )
        if "failed_devices" in server_response_msg.metadata:
            # failed devices indicate that the server was able to initialize them but failed to
            # connect to them
            logger.warning(f"Failed devices: {server_response_msg.metadata['failed_devices']}")
            msg.metadata["failed_devices"] = server_response_msg.metadata["failed_devices"]

        reload_msg = messages.DeviceConfigMessage(action="reload", config={}, metadata=msg.metadata)

        if accepted:
            # inform the user that the config was accepted and inform all services about the new config
            self.send_config_request_reply(accepted=accepted, error_msg=None, metadata=msg.metadata)
            self.send_config(reload_msg)
            return

        # the server failed to update the config and flushed the config
        self.send_config_request_reply(
            accepted=accepted,
            error_msg=f"{server_response_msg.message} Error during loading. The config will be flushed",
            metadata=msg.metadata,
        )
        self.send_config(reload_msg)

    def _add_to_config(self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event):
        """
        Add devices to the current config. If the device already exists, an error is raised.
        The new config is sent to the DeviceServer and if accepted, the config is updated in redis.

        Args:
            msg (messages.DeviceConfigMessage): Config add message
            cancel_event: Event to check for cancellation
        """
        dev_configs = msg.content["config"]

        for dev, config in dev_configs.items():
            if cancel_event.is_set():
                raise CancelledError("Config add cancelled")
            self._convert_to_db_config(dev, config)
            Device(**config)
            if dev in self.device_manager.devices:
                raise DeviceConfigError(f"Device {dev} already exists in the device manager.")

        rid = str(uuid.uuid4())
        self._update_device_server(rid, dev_configs, action="add")
        accepted, server_response_msg = self._wait_for_device_server_update(
            rid, timeout_time=min(300, 30 * len(dev_configs))
        )

        if "failed_devices" in server_response_msg.metadata:
            # failed devices indicate that the server was able to initialize them but failed to
            # connect to them
            logger.warning(f"Failed devices: {server_response_msg.metadata['failed_devices']}")
            msg.metadata["failed_devices"] = server_response_msg.metadata["failed_devices"]

        if accepted:
            # update config in redis
            self.add_devices_to_redis(dev_configs)
            self.send_config_request_reply(accepted=True, error_msg=None, metadata=msg.metadata)
            self.send_config(msg)
            return

        self.send_config_request_reply(
            accepted=False,
            error_msg=f"{server_response_msg.message} Failed to add devices to the server.",
            metadata=msg.metadata,
        )

    def _remove_from_config(self, msg: messages.DeviceConfigMessage):
        """
        Remove devices from the current config. If the device does not exist, an error is raised.
        The new config is sent to the DeviceServer and if accepted, the config is updated in redis.

        Args:
            msg (messages.DeviceConfigMessage): Config remove message

        Raises:
            DeviceConfigError: If the device does not exist in the device manager.
        """
        dev_configs = msg.content["config"]

        for dev in dev_configs:
            if dev not in self.device_manager.devices:
                raise DeviceConfigError(f"Device {dev} not found in the device manager.")

        rid = str(uuid.uuid4())
        self._update_device_server(rid, dev_configs, action="remove")
        accepted, server_response_msg = self._wait_for_device_server_update(rid)
        if accepted:
            # update config in redis
            self.remove_devices_from_redis(dev_configs)
            self.send_config_request_reply(accepted=True, error_msg=None, metadata=msg.metadata)
            self.send_config(msg)
            return

        self.send_config_request_reply(
            accepted=False,
            error_msg=f"{server_response_msg.message} Failed to remove devices from the server.",
            metadata=msg.metadata,
        )

    def _reset_config(self, msg: messages.DeviceConfigMessage):
        """
        Reset the config to an empty one.
        Args:
            msg (messages.DeviceConfigMessage): Config reset message
        """
        # set the config in redis to empty
        self.set_config_in_redis([])

        self.send_config_request_reply(accepted=True, error_msg=None, metadata=msg.metadata)

        # tell all services to reload the config
        reload_msg = messages.DeviceConfigMessage(action="reload", config={}, metadata=msg.metadata)
        self.send_config(reload_msg)

    def _cancel_config_request(self, msg: messages.DeviceConfigMessage):
        """
        Cancel any active config request on the device server and locally.
        Even if there is no active request locally, a cancel is still sent to the device server
        as it may still have an active request.

        Args:
            msg (BECMessage.DeviceConfigMessage): Config message containing the cancel request
        """

        with self._lock:
            request_info = self._active_request
            if request_info is not None:
                # Signal cancellation of local operations
                cancel_event = request_info["cancel_event"]
                future = request_info["future"]
                active_request_id = request_info["request_id"]
                cancel_event.set()
                logger.info(f"Cancellation requested for config request {active_request_id}")

        # Send 'cancel' to device server in case the active request initiated device server updates
        # The device server will handle this gracefully if there's no active request on its side
        try:
            rid = str(uuid.uuid4())
            self._update_device_server(rid, {}, action="cancel")
            accepted, server_response_msg = self._wait_for_device_server_update(rid, timeout_time=5)
            if not accepted:
                logger.warning(
                    f"Failed to cancel device server request: {server_response_msg.message}"
                )
        except TimeoutError:
            logger.warning("Timeout while attempting to cancel device server request")
        except Exception as exc:
            logger.warning(f"Error canceling device server request: {exc}")

        # Wait for the local task to actually stop
        try:
            if request_info is None:
                logger.info("No active config request to cancel locally.")
                self.send_config_request_reply(accepted=True, error_msg="", metadata=msg.metadata)
                return
            concurrent.futures.wait([future])
            logger.info(f"Config request {active_request_id} has completed after cancellation")
            self.send_config_request_reply(accepted=True, error_msg="", metadata=msg.metadata)
        except Exception as exc:
            logger.warning(f"Error waiting for cancellation of {active_request_id}: {exc}")
            self.send_config_request_reply(
                accepted=False, error_msg=f"Error during cancellation: {exc}", metadata=msg.metadata
            )

    ##################################################################
    ############### Device Server Handling ############################
    ##################################################################

    def _update_device_server(self, RID: str, config: dict, action="update") -> None:
        msg = messages.DeviceConfigMessage(action=action, config=config, metadata={"RID": RID})
        self.connector.send(MessageEndpoints.device_server_config_request(), msg)

    def _wait_for_device_server_update(
        self, RID: str, timeout_time=30
    ) -> Tuple[bool, messages.RequestResponseMessage]:
        timeout = timeout_time
        time_step = 0.05
        elapsed_time = 0
        while True:
            msg = self.connector.get(MessageEndpoints.device_config_request_response(RID))
            if msg:
                return msg.content["accepted"], msg

            if elapsed_time > timeout:
                raise TimeoutError(
                    "Reached timeout whilst waiting for a device server config reply."
                )

            time.sleep(time_step)
            elapsed_time += time_step

    def _update_device_config(self, device: DeviceBaseWithConfig, dev_config) -> bool:
        """
        Update a single device config

        Args:
            device (DeviceBaseWithConfig): Device to update
            dev_config (dict): Config to update

        Returns:
            bool: True if the config was updated, False otherwise
        """
        updated = False
        if "deviceConfig" in dev_config:
            request_id = str(uuid.uuid4())
            self._update_device_server(request_id, {device.name: dev_config})
            updated, msg = self._wait_for_device_server_update(request_id)
            if not updated:
                raise DeviceConfigError(f"Failed to update device {device.name}. {msg.message}")
            device._config["deviceConfig"].update(dev_config["deviceConfig"])
            dev_config.pop("deviceConfig")

        if "enabled" in dev_config:
            self._validate_update({"enabled": dev_config["enabled"]})
            device._config["enabled"] = dev_config["enabled"]
            request_id = str(uuid.uuid4())
            self._update_device_server(request_id, {device.name: dev_config})
            updated, msg = self._wait_for_device_server_update(request_id)
            if not updated:
                raise DeviceConfigError(f"Failed to update device {device.name}. {msg.message}")
            dev_config.pop("enabled")

        if not dev_config:
            return updated

        for key in dev_config:
            if key not in CONF.UPDATABLE:
                raise DeviceConfigError(f"Cannot update key {key}!")

            self._validate_update({key: dev_config[key]})
            device._config[key] = dev_config[key]
            updated = True

        return updated

    ###################################################################
    ############### Config Validation and Conversion ##################
    ###################################################################

    def _validate_update(self, update: dict) -> None:
        DevicePartial(**update)

    def _convert_to_db_config(self, name: str, config: dict) -> None:
        if not config.get("deviceConfig"):
            config["deviceConfig"] = {}
        config["name"] = name

    ##################################################################
    ############### Redis Config Handling ############################
    ##################################################################

    def update_config_in_redis(self, device: DeviceBaseWithConfig):
        """
        Update the device config in redis

        Args:
            device (DeviceBaseWithConfig): Device to update
        """
        config = self.get_config_from_redis()
        index = next(
            index for index, dev_conf in enumerate(config) if dev_conf["name"] == device.name
        )
        # pylint: disable=protected-access
        config[index] = device._config
        self.set_config_in_redis(config)

    def add_devices_to_redis(self, dev_configs: dict):
        """
        Add devices to the redis config

        Args:
            dev_configs (dict): Dictionary of device configs
        """
        config = self.get_config_from_redis()
        for dev, dev_config in dev_configs.items():
            config.append(dev_config)
        self.set_config_in_redis(config)

    def remove_devices_from_redis(self, dev_configs: dict):
        """
        Remove devices from the redis config

        Args:
            dev_configs (dict): Dictionary of device configs
        """
        config = self.get_config_from_redis()
        for dev in dev_configs:
            index = next(index for index, dev_conf in enumerate(config) if dev_conf["name"] == dev)
            config.pop(index)
        self.set_config_in_redis(config)

    def get_config_from_redis(self):
        """
        Get the config from redis

        Returns:
            list: List of device configs
        """
        config = self.device_manager.connector.get(MessageEndpoints.device_config())
        return config.content["resource"]

    def set_config_in_redis(self, config):
        """
        Set the config in redis

        Args:
            config (list): List of device configs
        """
        msg = messages.AvailableResourceMessage(resource=config)
        self.device_manager.connector.set(MessageEndpoints.device_config(), msg)

    def shutdown(self) -> None:
        """Shutdown the config handler, canceling any active request."""
        logger.info("Shutting down ConfigHandler...")

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
        logger.info("ConfigHandler shutdown complete")
