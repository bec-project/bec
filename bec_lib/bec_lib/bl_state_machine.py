"""
Module for managing beamline states based on configuration files.

Example of the YAML configuration file:
``` yaml
bl_transition_states:
  bl_state_class: AggregatedState
  config:
    evaluation_method: any
    states:
      alignment:
        devices:
          samx:
            value: 0
            abs_tol: 0.1
            signals:
              velocity:
                value: 5
                abs_tol: 0.1
          bpm4i:
            value: 100
            abs_tol: 10
          samy:
            at: in
            abs_tol: 0.1
samx_within_limits:
  bl_state_class: DeviceWithinLimitsState
  config:
    device: samx
    signal: samx
    low_limit: -1
    high_limit: 1
    tolerance: 0.1
```

"""

from __future__ import annotations

import yaml

from bec_lib import bl_states
from bec_lib.bl_state_manager import BeamlineStateManager, _state_class_for_state_type
from bec_lib.logger import bec_logger

logger = bec_logger.logger


class BeamlineStateMachine:
    """
    A class to manage beamline states based on configuration files or dictionaries.

    Args:
        manager (BeamlineStateManager): An instance of BeamlineStateManager to manage the states.
    """

    def __init__(self, manager: BeamlineStateManager) -> None:
        self._manager = manager
        self._configs: dict[str, bl_states.BeamlineStateConfig] = {}

    def load_from_config(
        self,
        config_path: str | None = None,
        config_dict: dict | None = None,
        flush: bool = True,
        skip_existing: bool = False,
    ) -> None:
        """
        Load a state configuration from a YAML file or a dictionary. If None or both are provided,
        an error will be raised. Configs must adhere to the following structure:

        ``` yaml
        <state_name>:
          bl_state_class: <class_name>
          config:
            <config_key>: <config_value>
        ...
        ```

        Args:
            config_path (str | None): The path to the YAML configuration file.
            config_dict (dict | None): A dictionary containing the configuration.
            flush (bool): If True, existing states in the manager will be cleared before loading new ones.
            skip_existing (bool): If True, existing states in the manager will be skipped during loading.
        """
        self._check_inputs(config_path=config_path, config_dict=config_dict)
        config_dict = self._load_config(config_path=config_path, config_dict=config_dict)
        # Check first if the config is valid before clearing the existing states
        configs = self._parse_config(config_dict=config_dict)
        if flush:
            self._manager.clear_all()
        for config in configs:
            try:
                self._manager.add(config, skip_existing=skip_existing)
            except TimeoutError:
                logger.warning(f"Timeout while waiting for state {config.name} to be active.")

    def _check_inputs(self, config_path: str | None, config_dict: dict | None) -> None:
        """Utility method to check that either config_path or config_dict is provided, but not both."""
        if (config_path is None and config_dict is None) or (
            config_path is not None and config_dict is not None
        ):
            raise ValueError("Either config_path or config_dict must be provided, but not both.")

    def _load_config(self, config_path: str | None, config_dict: dict | None) -> dict:
        """Utility method to load the configuration from a YAML file or return the provided dictionary."""
        if config_path:
            with open(config_path, "r", encoding="utf-8") as f:
                loaded_config = yaml.safe_load(f)
            if loaded_config is None:
                raise ValueError("Config file is empty.")
            return loaded_config
        if config_dict is None:
            raise ValueError("config_dict must be provided when config_path is not set.")
        return config_dict

    def _parse_config(self, config_dict: dict) -> list[bl_states.BeamlineStateConfig]:
        """Parse the configuration dictionary into a list of BeamlineStateConfig instances. Raise if invalid."""
        parsed_configs: list[bl_states.BeamlineStateConfig] = []
        for state_name, entry in config_dict.items():
            state_config_class = self._resolve_config_class(entry["bl_state_class"])
            config = entry["config"]
            if not isinstance(config, dict):
                raise ValueError(f"Config for state {state_name!r} must be a dictionary.")
            if "name" in config and config["name"] != state_name:
                raise ValueError(
                    f"Config name {config['name']!r} does not match top-level state name {state_name!r}."
                )
            config = {**config, "name": state_name}
            parsed_configs.append(state_config_class(**config))
        return parsed_configs

    def _resolve_config_class(
        self, bl_state_class: str | type[bl_states.BeamlineStateConfig]
    ) -> type[bl_states.BeamlineStateConfig]:
        """Resolve the configuration class for a given beamline state class name or type."""
        if isinstance(bl_state_class, str):
            resolved_class = _state_class_for_state_type(bl_state_class)
        else:
            resolved_class = bl_state_class
        config = resolved_class.CONFIG_CLASS
        return config
