from __future__ import annotations

import os
import time
import traceback
import uuid
from typing import TYPE_CHECKING, Tuple

import bec_lib
from bec_lib import messages
from bec_lib.atlas_models import Device, DevicePartial
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.devicemanager import DeviceManagerBase as DeviceManager
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.device import DeviceBase
    from bec_lib.redis_connector import RedisConnector
    from bec_server.scihub.atlas.atlas_connector import AtlasConnector


logger = bec_logger.logger

dir_path = os.path.abspath(os.path.join(os.path.dirname(bec_lib.__file__), "./configs/"))


class ConfigHandler:
    def __init__(self, atlas_connector: AtlasConnector, connector: RedisConnector) -> None:
        self.atlas_connector = atlas_connector
        self.connector = connector
        self.device_manager = DeviceManager(self.atlas_connector.scihub)
        self.device_manager.initialize(atlas_connector.config.redis)

    def parse_config_request(self, msg: messages.DeviceConfigMessage) -> None:
        """Processes a config request. If successful, it emits a config reply

        Args:
            msg (BMessage.DeviceConfigMessage): Config request

        """
        try:
            self.device_manager.check_request_validity(msg)
            if msg.content["action"] == "update":
                self._update_config(msg)
            if msg.content["action"] == "reload":
                self._reload_config(msg)
            if msg.content["action"] == "set":
                self._set_config(msg)
            if msg.content["action"] == "add":
                self._add_to_config(msg)
            if msg.content["action"] == "remove":
                self._remove_from_config(msg)

        except Exception:
            content = traceback.format_exc()
            self.send_config_request_reply(accepted=False, error_msg=content, metadata=msg.metadata)

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

    def _set_config(self, msg: messages.DeviceConfigMessage):
        config = msg.content["config"]
        msg.metadata["updated_config"] = False

        # make sure the config is valid before setting it in redis
        for name, device in config.items():
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

    def _convert_to_db_config(self, name: str, config: dict) -> None:
        if not config.get("deviceConfig"):
            config["deviceConfig"] = {}
        config["name"] = name

    def _reload_config(self, msg: messages.DeviceConfigMessage):
        self.send_config_request_reply(accepted=True, error_msg=None, metadata=msg.metadata)
        self.send_config(msg)

    def _add_to_config(self, msg: messages.DeviceConfigMessage):
        dev_configs = msg.content["config"]

        for dev, config in dev_configs.items():
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

    def _update_config(self, msg: messages.DeviceConfigMessage):
        updated = False
        dev_configs = msg.content["config"]

        for dev, config in dev_configs.items():
            device = self.device_manager.devices[dev]
            updated = self._update_device_config(device, config.copy())
            if updated:
                self.update_config_in_redis(device)

        # send updates to services
        if updated:
            self.send_config(msg)
            self.send_config_request_reply(accepted=True, error_msg=None, metadata=msg.metadata)

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

    def _update_device_config(self, device: DeviceBase, dev_config) -> bool:
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

        available_keys = [
            "readOnly",
            "userParameter",
            "onFailure",
            "deviceTags",
            "readoutPriority",
            "softwareTrigger",
        ]
        for key in dev_config:
            if key not in available_keys:
                raise DeviceConfigError(f"Unknown update key {key}.")

            self._validate_update({key: dev_config[key]})
            device._config[key] = dev_config[key]
            updated = True

        return updated

    def _validate_update(self, update: dict) -> None:
        DevicePartial(**update)

    def update_config_in_redis(self, device: DeviceBase):
        """
        Update the device config in redis

        Args:
            device (DeviceBase): Device to update
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
