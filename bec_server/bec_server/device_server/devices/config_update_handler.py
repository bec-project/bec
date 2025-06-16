from __future__ import annotations

import copy
import traceback
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.devicemanager import DeviceConfigError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

if TYPE_CHECKING:
    from bec_server.device_server.devices.devicemanager import DeviceManagerDS

logger = bec_logger.logger


class ConfigUpdateHandler:
    def __init__(self, device_manager: DeviceManagerDS) -> None:
        self.device_manager = device_manager
        self.connector = self.device_manager.connector
        self.connector.register(
            MessageEndpoints.device_server_config_request(),
            cb=self._device_config_callback,
            parent=self,
        )

    @staticmethod
    def _device_config_callback(msg, *, parent, **_kwargs) -> None:
        logger.info(f"Received request: {msg}")
        parent.parse_config_request(msg.value)

    def parse_config_request(self, msg: messages.DeviceConfigMessage) -> None:
        """Processes a config request. If successful, it emits a config reply

        Args:
            msg (BECMessage.DeviceConfigMessage): Config request

        """
        error_msg = ""
        accepted = True
        try:
            self.device_manager.check_request_validity(msg)
            if msg.content["action"] == "update":
                self._update_config(msg)
            if msg.content["action"] == "add":
                self._add_config(msg)
                if self.device_manager.failed_devices:
                    msg.metadata["failed_devices"] = self.device_manager.failed_devices
            if msg.content["action"] == "reload":
                self._reload_config()
                if self.device_manager.failed_devices:
                    msg.metadata["failed_devices"] = self.device_manager.failed_devices
            if msg.content["action"] == "remove":
                self._remove_config(msg)

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
        RID = metadata.get("RID")
        self.device_manager.connector.set(
            MessageEndpoints.device_config_request_response(RID), msg, expire=60
        )

    def _update_config(self, msg: messages.DeviceConfigMessage) -> None:
        for dev, dev_config in msg.content["config"].items():
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

    def _reload_config(self) -> None:
        for _, obj in self.device_manager.devices.items():
            try:
                obj.obj.destroy()
            except Exception:
                logger.warning(f"Failed to destroy {obj.obj.name}")
                raise RuntimeError
        self.device_manager.devices.flush()
        self.device_manager._get_config()
        if self.device_manager.failed_devices:
            self.handle_failed_device_inits()
        return

    def _add_config(self, msg: messages.DeviceConfigMessage) -> None:
        """
        Adds new devices to the config and initializes them. If a device fails to initialize, it is added to the
        failed_devices dictionary.

        Args:
            msg (BECMessage.DeviceConfigMessage): Config message containing the new devices

        """
        # pylint:disable=protected-access
        self.device_manager.failed_devices = {}
        dm: DeviceManagerDS = self.device_manager
        for dev, dev_config in msg.content["config"].items():
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

    def _remove_config(self, msg: messages.DeviceConfigMessage) -> None:
        """
        Removes devices from the config and disconnects them.

        Args:
            msg (BECMessage.DeviceConfigMessage): Config message containing the devices to be removed

        """
        for dev in msg.content["config"]:
            logger.info(f"Removing device {dev}")
            if dev not in self.device_manager.devices:
                continue
            device = self.device_manager.devices[dev]
            self.device_manager.disconnect_device(device)
            self.device_manager.reset_device(device)
            self.device_manager.devices.pop(dev)

    def handle_failed_device_inits(self):
        if self.device_manager.failed_devices:
            msg = messages.DeviceConfigMessage(
                action="update",
                config={name: {"enabled": False} for name in self.device_manager.failed_devices},
            )
            self._update_config(msg)
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
