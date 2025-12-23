"""
This module provides a helper class for updating and saving the BEC device configuration.
"""

from __future__ import annotations

import ast
import datetime
import json
import os
import pathlib
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import yaml
from pydantic import ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

import bec_lib
from bec_lib.atlas_models import _DeviceModelCore
from bec_lib.bec_errors import DeviceConfigError, ServiceConfigError
from bec_lib.bec_yaml_loader import yaml_load
from bec_lib.endpoints import MessageEndpoints
from bec_lib.file_utils import DeviceConfigWriter
from bec_lib.logger import bec_logger
from bec_lib.messages import ConfigAction
from bec_lib.utils.import_utils import lazy_import_from
from bec_lib.utils.json import ExtendedEncoder

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.devicemanager import DeviceManagerBase
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


class ConfigHelperUser:
    """
    Thin wrapper around ConfigHelper to expose only selected methods to the user.
    """

    def __init__(self, device_manager: DeviceManagerBase) -> None:
        self._device_manager = device_manager
        self._config_helper = device_manager.config_helper

    def update_session_with_file(
        self, file_path: str, save_recovery: bool = True, force: bool = False, validate: bool = True
    ) -> None:
        """Update the current session with a yaml file from disk.

        Args:
            file_path (str): Full path to the yaml file.
            save_recovery (bool, optional): Save the current session before updating. Defaults to True.
            force (bool, optional): Force update even if there are conflicts. Defaults to False.
            validate (bool, optional): Whether to validate the new config. Defaults to True.
        """
        self._config_helper.update_session_with_file(
            file_path, save_recovery=save_recovery, force=force, validate=validate
        )

    def save_current_session(self, file_path: str):
        """Save the current session as a yaml file to disk.

        Args:
            file_path (str): Full path to the yaml file.
        """
        self._config_helper.save_current_session(file_path)

    def reset_config(self, wait_for_response: bool = True, timeout_s: float | None = None) -> None:
        """
        Send a request to reset config to default
        Args:
            wait_for_response (bool): whether to wait for the response, default True
            timeout_s (float, optional): how long to wait for a response. Ignored if not waiting. Defaults to best effort calculated value based on message length.
        Returns: None
        """
        self._config_helper.reset_config(wait_for_response=wait_for_response, timeout_s=timeout_s)

    def load_demo_config(self, force: bool = False) -> None:
        """
        Load BEC device demo_config.yaml for simulation.
        Args:
            force (bool, optional): Force update even if there are conflicts. Defaults to False.
        Returns: None
        """
        self._config_helper.load_demo_config(force=force)


class ConfigHelper:
    """Config Helper"""

    def __init__(
        self,
        connector: RedisConnector,
        service_name: str | None = None,
        device_manager: DeviceManagerBase | None = None,
    ) -> None:
        """Helper class for updating and saving the BEC device configuration.

        Args:
            connector (RedisConnector): Redis connector.
            service_name (str, optional): Name of the service. Defaults to None.
            device_manager (DeviceManagerBase, optional): Device manager instance. Defaults to None.
        """
        self._connector = connector
        self._service_name = service_name
        self._writer_mixin = None
        self._base_path_recovery = None
        self._device_manager = device_manager

    def update_session_with_file(
        self, file_path: str, save_recovery: bool = True, force: bool = False, validate: bool = True
    ) -> None:
        """Update the current session with a yaml file from disk.

        Args:
            file_path (str): Full path to the yaml file.
            save_recovery (bool, optional): Save the current session before updating. Defaults to True.
            force (bool, optional): Force update even if there are conflicts. Defaults to False.
            validate (bool, optional): Whether to validate the new config. Defaults to True.
        """
        config = self._load_config_from_file(file_path)
        config_conflicts = self._get_config_conflicts(config, validate=validate)
        if config_conflicts and not force:
            try:
                self._merge_conflicts_with_user_input(config, config_conflicts)
            except KeyboardInterrupt:
                print("\nConfiguration update aborted by user. No changes were made.")
                return
        if save_recovery:
            time_stamp = f"{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}"
            if not self._base_path_recovery:
                self._update_base_path_recovery()
            # after update_base_path_recovery, we can
            assert self._base_path_recovery is not None
            assert self._writer_mixin is not None

            if not os.path.exists(self._base_path_recovery):
                self._writer_mixin.create_directory(self._base_path_recovery)
            fname = os.path.join(self._base_path_recovery, f"recovery_config_{time_stamp}.yaml")
            success = self._save_config_to_file(fname, raise_on_error=False)
            if success:
                print(f"A recovery config was written to {fname}.")

        self.send_config_request(action="set", config=config)

    def _update_base_path_recovery(self):
        """
        Compile the filepath for the recovery configs.
        """
        # pylint: disable=import-outside-toplevel
        from bec_lib.bec_service import SERVICE_CONFIG

        service_cfg = SERVICE_CONFIG.config.get("log_writer", None)
        if not service_cfg:
            raise ServiceConfigError(
                f"ServiceConfig {service_cfg} must at least contain key with 'log_writer'"
            )
        self._writer_mixin = DeviceConfigWriter(service_cfg)
        self._base_path_recovery = self._writer_mixin.get_recovery_directory()
        self._writer_mixin.create_directory(self._base_path_recovery)

    def _load_config_from_file(self, file_path: str) -> dict:
        data = {}
        if pathlib.Path(file_path).suffix not in (".yaml", ".yml"):
            raise NotImplementedError

        logger.info(f"Loading config from file: {file_path}")
        with open(file_path, "r", encoding="utf-8") as stream:
            try:
                data = yaml_load(stream)
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

    def _get_config_conflicts(self, new_config: dict, validate: bool = True) -> dict:
        """
        Check if the newly provided config has conflicts with the current session config.
        The current session config is fetched from Redis.

        Args:
            new_config (dict): New config to check.
            validate (bool): Whether to validate the new config. Defaults to True.

        Returns:
            dict: Conflicts found in the config: Device name -> List of conflicting elements.
        """
        if not self._device_manager:
            return {}
        current_config = self._device_manager.get_device_config_cached()
        if not current_config:
            return {}

        output_conflicts = {}
        for dev, config in new_config.items():
            if dev not in current_config:
                continue
            if validate:
                try:
                    config = _DeviceModelCore(**config).model_dump()
                except ValidationError as exc:
                    exc.model = _DeviceModelCore  # type: ignore
                    exc.context = f"the provided device config for device '{dev}'"  # type: ignore
                    raise exc

            for element, value in config.items():
                current_value = current_config[dev].get(element, None)
                if element == "deviceConfig" and value is None:
                    value = {}
                if value == current_value:
                    continue
                if isinstance(value, dict):
                    # only store conflicts for nested dicts if there is a difference
                    # Note: also a missing key in current_value will be detected as a conflict
                    nested_conflicts = {}
                    all_config_keys = set(value.keys()).union(
                        set(current_value.keys() if current_value else [])
                    )
                    for sub_element in all_config_keys:
                        sub_value = value.get(sub_element, None)
                        current_sub_value = (
                            current_value.get(sub_element, None) if current_value else None
                        )
                        if sub_value != current_sub_value:
                            nested_conflicts[sub_element] = {
                                "new": sub_value,
                                "current": current_sub_value,
                            }
                    if nested_conflicts:
                        if dev not in output_conflicts:
                            output_conflicts[dev] = {}
                        output_conflicts[dev][element] = nested_conflicts

                else:
                    if dev not in output_conflicts:
                        output_conflicts[dev] = {}
                    output_conflicts[dev].update(
                        {element: {"new": value, "current": current_value}}
                    )

        return output_conflicts

    def _merge_conflicts_with_user_input(self, config: dict, conflicts: dict) -> None:
        """
        Merge the conflicts found in the config with user input.
        Prompts the user interactively for each conflict to decide whether to accept
        the new value, keep the current value, or manually merge.

        Args:
            config (dict): Config to update.
            conflicts (dict): Conflicts found in the config.
        """
        console = Console()

        # Display initial warning
        text = "Configuration Conflicts Detected"
        device_names = ", ".join(conflicts.keys())
        body = Text(
            f"Found conflicts in {len(conflicts)} device(s). The following devices have conflicts: \n\n{device_names}\n",
            style="bold yellow",
        )
        panel = Panel(body, title=text, expand=False, border_style="red")
        console.print(panel)
        console.print()

        # Ask for global resolution strategy
        console.print("[bold]Please choose:[/bold]")
        console.print("  [green](a)[/green] Accept all new values from file")
        console.print("  [red](k)[/red] Keep all current session values")
        console.print("  [cyan](r)[/cyan] Resolve conflicts individually")
        global_choice = Prompt.ask(
            "Your choice", choices=["a", "k", "r"], show_default=False, console=console
        )
        console.print()

        if global_choice.lower() == "a":
            # Accept all new values
            console.print("[green]Accepting all new values from file...[/green]\n")
            # Config already has new values, nothing to do
            console.print(
                "[bold green]✓ All conflicts resolved - new values accepted![/bold green]\n"
            )
            return
        if global_choice.lower() == "k":
            # Keep all current values
            console.print("[red]Keeping all current session values...[/red]\n")
            self._apply_all_current_values(config, conflicts)
            console.print(
                "[bold green]✓ All conflicts resolved - current values kept![/bold green]\n"
            )
            return

        # Individual resolution (i)
        console.print("[cyan]Resolving conflicts individually...[/cyan]\n")

        # Iterate through each device with conflicts
        for device_name, device_conflicts in conflicts.items():
            console.print(f"\n[bold cyan]Device: {device_name}[/bold cyan]")
            console.rule(style="cyan")

            # Iterate through each conflicting element in the device
            for element, conflict_data in device_conflicts.items():
                # Handle nested conflicts (e.g., deviceConfig)
                if not self._is_nested_conflict(conflict_data):
                    self._resolve_single_conflict(
                        console,
                        config,
                        device_name,
                        element,
                        None,
                        conflict_data["new"],
                        conflict_data["current"],
                    )
                    continue
                # This is a nested conflict (like deviceConfig)
                console.print(f"\n  [bold]Element:[/bold] [yellow]{element}[/yellow]")
                for sub_element, sub_conflict in conflict_data.items():
                    self._resolve_single_conflict(
                        console,
                        config,
                        device_name,
                        element,
                        sub_element,
                        sub_conflict["new"],
                        sub_conflict["current"],
                    )

        console.print("\n[bold green]✓ All conflicts resolved![/bold green]\n")

    def _resolve_single_conflict(
        self,
        console: Console,
        config: dict,
        device_name: str,
        element: str,
        sub_element: str | None,
        new_value,
        current_value,
    ) -> None:
        """
        Resolve a single conflict by prompting the user.

        Args:
            console (Console): Rich console instance.
            config (dict): Config to update.
            device_name (str): Name of the device.
            element (str): Element with conflict.
            sub_element (str | None): Sub-element for nested conflicts (e.g., deviceConfig.key).
            new_value: New value from the file.
            current_value: Current value in the session.
        """
        # Create a table to display the conflict
        table = Table(show_header=True, header_style="bold magenta", padding=(0, 1))
        table.add_column("Element", style="yellow", no_wrap=True)
        table.add_column("New (file)", style="green")
        table.add_column("Current (session)", style="red")

        display_element = f"{element}.{sub_element}" if sub_element else element

        # Format values for display
        new_value_str = self._format_value_for_display(new_value)
        current_value_str = self._format_value_for_display(current_value)

        table.add_row(display_element, new_value_str, current_value_str)
        console.print(table)

        # Prompt user for choice with detailed context
        choice = Prompt.ask(
            "  [bold]Action:[/bold] [green](a)[/green]ccept new  [red](k)[/red]eep current  [cyan](m)[/cyan]anual merge",
            choices=["a", "k", "m"],
            show_default=False,
            console=console,
        )

        if choice.lower() == "a":
            # Accept new value
            self._apply_config_value(config, device_name, element, sub_element, new_value)
            console.print("  [green]→ Accepted new value[/green]")
        elif choice.lower() == "k":
            # Keep current value - do nothing to config since we're loading from file
            # We need to update config dict to keep current value
            self._apply_config_value(config, device_name, element, sub_element, current_value)
            console.print("  [yellow]→ Kept current value[/yellow]")
        elif choice.lower() == "m":
            # Manual merge - prompt for custom value
            console.print(
                "  [cyan]Enter custom value (will be evaluated as Python literal):[/cyan]"
            )
            custom_input = Prompt.ask("  Value")
            try:
                # Try to evaluate as Python literal (handles lists, dicts, numbers, strings, etc.)

                custom_value = ast.literal_eval(custom_input)
            except (ValueError, SyntaxError):
                # If evaluation fails, treat as string
                custom_value = custom_input
            self._apply_config_value(config, device_name, element, sub_element, custom_value)
            console.print(f"  [blue]→ Applied custom value: {custom_value}[/blue]")

    def _format_value_for_display(self, value) -> str:
        """
        Format a value for display in the conflict resolution UI.

        Args:
            value: Value to format.

        Returns:
            str: Formatted value string.
        """
        if value is None:
            return "None"
        if isinstance(value, (list, set)):
            # Format lists and sets on a single line
            return json.dumps(value, cls=ExtendedEncoder)
        if isinstance(value, dict):
            # Format dicts compactly if small, otherwise use indentation
            return json.dumps(value, indent=2, cls=ExtendedEncoder)
        return str(value)

    def _apply_config_value(
        self, config: dict, device_name: str, element: str, sub_element: str | None, value
    ) -> None:
        """
        Apply a resolved value to the config dict.

        Args:
            config (dict): Config to update.
            device_name (str): Name of the device.
            element (str): Element to update.
            sub_element (str | None): Sub-element for nested conflicts.
            value: Value to apply.
        """
        if device_name not in config:
            config[device_name] = {}

        if sub_element:
            # Nested element (e.g., deviceConfig.key)
            if element not in config[device_name]:
                config[device_name][element] = {}
            config[device_name][element][sub_element] = value
        else:
            # Top-level element
            config[device_name][element] = value

    def _apply_all_current_values(self, config: dict, conflicts: dict) -> None:
        """
        Apply all current values to the config for all conflicts.

        Args:
            config (dict): Config to update.
            conflicts (dict): Conflicts found in the config.
        """
        for device_name, device_conflicts in conflicts.items():
            for element, conflict_data in device_conflicts.items():
                # Handle nested conflicts (e.g., deviceConfig)
                if not self._is_nested_conflict(conflict_data):
                    self._apply_config_value(
                        config, device_name, element, None, conflict_data["current"]
                    )
                    continue
                # This is a nested conflict (like deviceConfig)
                for sub_element, sub_conflict in conflict_data.items():
                    self._apply_config_value(
                        config, device_name, element, sub_element, sub_conflict["current"]
                    )

    def _is_nested_conflict(self, conflict_data: dict) -> bool:
        """
        Check if the conflict data represents a nested conflict.

        Args:
            conflict_data (dict): Conflict data to check.

        Returns:
            bool: True if nested conflict, False otherwise.
        """
        return isinstance(conflict_data, dict) and all(
            isinstance(v, dict) and "new" in v and "current" in v for v in conflict_data.values()
        )

    def _save_config_to_file(self, file_path: str, raise_on_error: bool = True) -> bool:
        """Save the current session as a yaml file to disk.
        Args:
            file_path (str): Full path to the yaml file.
            raise_on_error (bool, optional): Whether to raise an error on failure. Defaults to True.
        Returns:
            bool: True if successful, False otherwise.
        """

        header = (
            "# BEC Device Configuration File\n"
            "#\n"
            "# The device config consists of a mapping of device names to their configurations.\n"
            "#\n"
            "# The following fields are required for each device:\n"
            "# - deviceClass: The class of the device (e.g., ophyd_devices.SimPositioner).\n"
            "# - enabled: A boolean indicating if the device is enabled.\n"
            '# - readoutPriority: Determines how the device is read out during a scan. Possible values are ["monitored", "baseline", "async", "on_request", "continuous"].\n'
            "#\n"
            "# Optional fields (defaults shown):\n"
            "# - connectionTimeout: Connection timeout in seconds. Default is 5.\n"
            '# - description: A string description of the device. Default is "" (empty string).\n'
            "# - deviceConfig: A dictionary of configuration parameters specific to the device class. Default is None.\n"
            "# - deviceTags: A list/set of tags associated with the device. Default is an empty list/set.\n"
            '# - onFailure: The action to take on failure. Possible values are ["buffer", "retry", "raise"]. Default is "retry".\n'
            "# - readOnly: A boolean indicating if the device is read-only. Default is false.\n"
            "# - softwareTrigger: A boolean indicating if the device uses software triggering. Default is false.\n"
            "# - userParameter: A dictionary of user-defined parameters. Default is {}.\n"
            "#\n"
            "# Default values may be omitted for brevity.\n"
            "##########################################################################################################################\n"
            "\n\n"
            "# An example device configuration with all fields:\n\n"
            "# device1:\n"
            "#   deviceClass: ophyd_devices.SimPositioner\n"
            "#   description: Sample positioner device\n"
            "#   deviceConfig:\n"
            "#     delay: 1\n"
            "#     update_frequency: 10\n"
            "#   deviceTags: \n"
            "#     - frontend\n"
            "#     - motor\n"
            "#   enabled: true\n"
            "#   connectionTimeout: 20\n"
            "#   readoutPriority: baseline\n"
            "#   onFailure: retry\n"
            "#   readOnly: false\n"
            "#   softwareTrigger: false\n"
            "#   userParameter:\n"
            "#     in: 23.1\n"
            "#     out: -50\n\n"
            "##########################################################################################################################\n\n\n"
        )
        if not self._device_manager:
            if raise_on_error:
                raise DeviceConfigError("Device manager is not available.")
            return False
        config = self._device_manager.get_device_config_cached(exclude_defaults=True)
        if not config:
            if raise_on_error:
                raise DeviceConfigError("No device configuration available to save.")
            return False

        # convert the device tag sets to lists for yaml serialization
        for dev_conf in config.values():
            if "deviceTags" in dev_conf and isinstance(dev_conf["deviceTags"], set):
                dev_conf["deviceTags"] = list(dev_conf["deviceTags"])

        with open(file_path, "w") as file:
            file.write(header)
            file.write(yaml.dump(config))
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
        request_id = str(uuid.uuid4())
        self._connector.send(
            MessageEndpoints.device_config_request(),
            DeviceConfigMessage(action=action, config=config, metadata={"RID": request_id}),
        )

        if wait_for_response:
            timeout = timeout_s if timeout_s is not None else self.suggested_timeout_s(config)
            logger.info(f"Waiting for reply with timeout {timeout} s")
            reply = self.wait_for_config_reply(
                request_id, timeout=timeout, send_cancel_on_interrupt=(action != "cancel")
            )
            if action == "cancel":
                raise DeviceConfigError(
                    "Config update was cancelled by user. The config has been flushed."
                )
            self.handle_update_reply(reply, request_id, timeout)
        return request_id

    def reset_config(self, wait_for_response: bool = True, timeout_s: float | None = None) -> None:
        """
        Send a request to reset config to default
        Args:
            wait_for_response (bool): whether to wait for the response, default True
            timeout_s (float, optional): how long to wait for a response. Ignored if not waiting. Defaults to best effort calculated value based on message length.
        Returns: None
        """
        RID = str(uuid.uuid4())
        self._connector.send(
            MessageEndpoints.device_config_request(),
            DeviceConfigMessage(action="reset", config=None, metadata={"RID": RID}),
        )

        if wait_for_response:
            timeout = timeout_s if timeout_s is not None else 120
            logger.info(f"Waiting for reply with timeout {timeout} s")
            reply = self.wait_for_config_reply(RID, timeout=timeout)
            self.handle_update_reply(reply, RID, timeout)

    @staticmethod
    def suggested_timeout_s(config: dict):
        return min(300, len(config) * 30) + 2

    def handle_update_reply(self, reply: RequestResponseMessage, RID: str, timeout: float):
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

    def wait_for_service_response(self, RID: str, timeout: float = 60) -> None:
        """
        wait for service response

        Args:
            RID (str): request id
            timeout (float, optional): timeout in seconds. Defaults to 60.

        """
        start_time = time.monotonic()
        while True:
            elapsed_time = time.monotonic() - start_time
            service_messages = self._connector.lrange(MessageEndpoints.service_response(RID), 0, -1)
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

    def wait_for_config_reply(
        self, RID: str, timeout: float = 60, send_cancel_on_interrupt: bool = True
    ) -> RequestResponseMessage:
        """
        wait for config reply

        Args:
            RID (str): request id
            timeout (int, optional): timeout in seconds. Defaults to 60.

        Returns:
            RequestResponseMessage: reply message
        """
        try:
            start = time.monotonic()
            while True:
                elapsed_time = time.monotonic() - start
                msg = self._connector.get(MessageEndpoints.device_config_request_response(RID))
                if msg is None:
                    time.sleep(0.01)
                    if elapsed_time > timeout:
                        raise DeviceConfigError("Timeout reached whilst waiting for config reply.")
                    continue
                return msg
        except KeyboardInterrupt:
            if send_cancel_on_interrupt:
                self.send_config_request(
                    action="cancel", config=None, wait_for_response=True, timeout_s=10
                )
            raise

    def load_demo_config(self, force: bool = False) -> None:
        """
        Load BEC device demo_config.yaml for simulation.
        Args:
            force (bool, optional): Force update even if there are conflicts. Defaults to False.
        Returns: None
        """
        dir_path = os.path.abspath(os.path.join(os.path.dirname(bec_lib.__file__), "./configs/"))
        fpath = os.path.join(dir_path, "demo_config.yaml")
        self.update_session_with_file(fpath, force=force)
