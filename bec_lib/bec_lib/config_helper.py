"""
This module provides a helper class for updating and saving the BEC device configuration.
"""

from __future__ import annotations

import datetime
import json
import os
import pathlib
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import yaml

import bec_lib
from bec_lib.bec_errors import DeviceConfigError, ServiceConfigError
from bec_lib.bec_yaml_loader import yaml_load
from bec_lib.endpoints import MessageEndpoints
from bec_lib.file_utils import DeviceConfigWriter
from bec_lib.logger import bec_logger
from bec_lib.messages import ConfigAction
from bec_lib.utils.import_utils import lazy_import_from
from bec_lib.utils.json import ExtendedEncoder

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.messages import DeviceConfigMessage, RequestResponseMessage, ServiceResponseMessage
    from bec_lib.redis_connector import RedisConnector
else:
    # TODO: put back normal import when Pydantic gets faster
    DeviceConfigMessage = lazy_import_from("bec_lib.messages", ("DeviceConfigMessage",))

logger = bec_logger.logger


@dataclass(frozen=True)
class _ConfigConstants:
    NON_UPDATABLE = ("name", "deviceClass")
    UPDATABLE = (
        "description",
        "deviceConfig",
        "deviceTags",
        "enabled",
        "onFailure",
        "readOnly",
        "readoutPriority",
        "softwareTrigger",
        "userParameter",
    )


CONF = _ConfigConstants()


class ConfigHelper:
    """Config Helper"""

    def __init__(self, connector: RedisConnector, service_name: str = None) -> None:
        """Helper class for updating and saving the BEC device configuration.

        Args:
            connector (RedisConnector): Redis connector.
            service_name (str, optional): Name of the service. Defaults to None.
        """
        self.connector = connector
        self._service_name = service_name
        self.writer_mixin = None
        self._base_path_recovery = None

    def update_session_with_file(self, file_path: str, save_recovery: bool = True) -> None:
        """Update the current session with a yaml file from disk.

        Args:
            file_path (str): Full path to the yaml file.
            save_recovery (bool, optional): Save the current session before updating. Defaults to True.
        """
        if save_recovery:
            time_stamp = f"{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}"
            if not self._base_path_recovery:
                self._update_base_path_recovery()
            if not os.path.exists(self._base_path_recovery):
                self.writer_mixin.create_directory(self._base_path_recovery)
            fname = os.path.join(self._base_path_recovery, f"recovery_config_{time_stamp}.yaml")
            success = self._save_config_to_file(fname, raise_on_error=False)
            if success:
                print(f"A recovery config was written to {fname}.")
        config = self._load_config_from_file(file_path)
        self.send_config_request(action="set", config=config)

    def _update_base_path_recovery(self):
        """
        Compile the filepath for the recovery configs.
        """
        # pylint: disable=import-outside-toplevel
        from bec_lib.bec_service import SERVICE_CONFIG

        service_cfg = SERVICE_CONFIG.config["service_config"].get("log_writer", None)
        if not service_cfg:
            raise ServiceConfigError(
                f"ServiceConfig {service_cfg} must at least contain key with 'log_writer'"
            )
        self.writer_mixin = DeviceConfigWriter(service_cfg)
        self._base_path_recovery = self.writer_mixin.get_recovery_directory()
        self.writer_mixin.create_directory(self._base_path_recovery)

    def _load_config_from_file(self, file_path: str) -> dict:
        data = {}
        if pathlib.Path(file_path).suffix not in (".yaml", ".yml"):
            raise NotImplementedError

        with open(file_path, "r", encoding="utf-8") as stream:
            try:
                data = yaml_load(stream)
                logger.trace(
                    f"Loaded new config from disk: {json.dumps(data, sort_keys=True, indent=4, cls=ExtendedEncoder)}"
                )
            except yaml.YAMLError as err:
                logger.error(f"Error while loading config from disk: {repr(err)}")

        return data

    def save_current_session(self, file_path: str):
        """Save the current session as a yaml file to disk.

        Args:
            file_path (str): Full path to the yaml file.
        """
        self._save_config_to_file(file_path)
        print(f"Config was written to {file_path}.")

    def _save_config_to_file(self, file_path: str, raise_on_error: bool = True) -> bool:
        config = self.connector.get(MessageEndpoints.device_config())
        if not config:
            if raise_on_error:
                raise DeviceConfigError("No config found in the session.")
            return False
        config = config.content["resource"]
        out = {}
        for dev in config:
            dev.pop("id", None)
            dev.pop("createdAt", None)
            dev.pop("createdBy", None)
            dev.pop("sessionId", None)
            name = dev.pop("name")
            out[name] = dev

        with open(file_path, "w") as file:
            file.write(yaml.dump(out))
        return True

    def send_config_request(
        self,
        action: ConfigAction = "update",
        config: dict | None = None,
        wait_for_response: bool = True,
        timeout_s: float | None = None,
    ) -> str:
        """
        Send a request to update config
        Args:
            action (ConfigAction): what to do with the config
            config (dict | None): the config
            wait_for_response (bool): whether to wait for the response, default True
            timeout_s (float, optional): how long to wait for a response. Ignored if not waiting. Defaults to best effort calculated value based on message length.
        Returns: request ID (str)

        """
        if action in ["update", "add", "set"] and not config:
            raise DeviceConfigError(f"Config cannot be empty for an {action} request.")
        RID = str(uuid.uuid4())
        self.connector.send(
            MessageEndpoints.device_config_request(),
            DeviceConfigMessage(action=action, config=config, metadata={"RID": RID}),
        )

        if wait_for_response:
            timeout = timeout_s if timeout_s is not None else self.suggested_timeout_s(config)
            logger.info(f"Waiting for reply with timeout {timeout} s")
            reply = self.wait_for_config_reply(RID, timeout=timeout)
            self.handle_update_reply(reply, RID, timeout)
        return RID

    @staticmethod
    def suggested_timeout_s(config: dict):
        return min(300, len(config) * 30) + 2

    def handle_update_reply(self, reply: RequestResponseMessage, RID: str, timeout: int):
        if not reply.content["accepted"] and not reply.metadata.get("updated_config"):
            raise DeviceConfigError(
                f"Failed to update the config: {reply.content['message']}. No devices were updated."
            )
        try:
            if not reply.content["accepted"] and reply.metadata.get("updated_config"):
                raise DeviceConfigError(
                    f"Failed to update the config: {reply.content['message']}. The old config will be kept in the device config history."
                )

            if "failed_devices" in reply.metadata:
                print("Failed to update the config for some devices.")
                for dev in reply.metadata["failed_devices"]:
                    print(
                        f"Device {dev} failed to update:\n {reply.metadata['failed_devices'][dev]}."
                    )
                devices = [dev for dev in reply.metadata["failed_devices"]]

                raise DeviceConfigError(
                    f"Failed to update the config for some devices. The following devices were disabled: {devices}."
                )
        finally:
            # wait for the device server and scan server to acknowledge the config change
            self.wait_for_service_response(RID, timeout)

    def wait_for_service_response(self, RID: str, timeout: float = 60) -> ServiceResponseMessage:
        """
        wait for service response

        Args:
            RID (str): request id
            timeout (float, optional): timeout in seconds. Defaults to 60.

        Returns:
            ServiceResponseMessage: reply message
        """
        start_time = time.monotonic()
        while True:
            elapsed_time = time.monotonic() - start_time
            service_messages = self.connector.lrange(MessageEndpoints.service_response(RID), 0, -1)
            if not service_messages:
                time.sleep(0.005)
            else:
                ack_services = [
                    msg.content["response"]["service"]
                    for msg in service_messages
                    if msg is not None
                ]
                checked_services = set(["DeviceServer", "ScanServer"])
                if self._service_name:
                    checked_services.add(self._service_name)
                if checked_services.issubset(set(ack_services)):
                    break
            if elapsed_time > timeout:  # type: ignore
                if service_messages:
                    raise DeviceConfigError(
                        "Timeout reached whilst waiting for config change to be acknowledged."
                        f" Received {service_messages}."
                    )

                raise DeviceConfigError(
                    "Timeout reached whilst waiting for config change to be acknowledged. No"
                    " messages received."
                )

    def wait_for_config_reply(self, RID: str, timeout: float = 60) -> RequestResponseMessage:
        """
        wait for config reply

        Args:
            RID (str): request id
            timeout (int, optional): timeout in seconds. Defaults to 60.

        Returns:
            RequestResponseMessage: reply message
        """
        start = time.monotonic()
        while True:
            elapsed_time = time.monotonic() - start
            msg = self.connector.get(MessageEndpoints.device_config_request_response(RID))
            if msg is None:
                time.sleep(0.01)
                if elapsed_time > timeout:
                    raise DeviceConfigError("Timeout reached whilst waiting for config reply.")
                continue
            return msg

    def load_demo_config(self):
        """Load BEC device demo_config.yaml for simulation."""
        dir_path = os.path.abspath(os.path.join(os.path.dirname(bec_lib.__file__), "./configs/"))
        fpath = os.path.join(dir_path, "demo_config.yaml")
        self.update_session_with_file(fpath)
