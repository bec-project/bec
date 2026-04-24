"""
Module for managing aggregated beamline states based on configuration files.

Example of the YAML configuration file:
``` yaml
alignment:
    devices:
        samx:
        readback:
            value: 0
            abs_tol: 0.1
    measurement:
    devices:
        samx:
        readback:
            value: 19
            abs_tol: 0.1
        velocity:
            value: 5
            abs_tol: 0.1
        samy:
        readback:
            value: 0
            abs_tol: 0.1
    test:
    devices:
        samy:
        readback:
            value: 0
            abs_tol: 0.1
```

"""

from __future__ import annotations

import yaml

from bec_lib.bl_state_manager import BeamlineStateManager
from bec_lib.bl_states import AggregatedStateConfig


class BeamlineStateMachine:

    def __init__(self, manager: BeamlineStateManager) -> None:
        self._manager = manager
        self._configs: dict[str, AggregatedStateConfig] = {}

    def load_from_config(
        self, name: str, config_path: str | None = None, config_dict: dict | None = None
    ) -> None:
        """
        Load a state configuration from a YAML file or a dictionary. If None or both are provided,
        an error will be raised. Config must be states for an AggregatedStateConfig or a dictionary/YAML file that
        can be parsed into one. Please check AggregatedStateConfig state field for the expected format of the configuration.

        Args:
            name (str): The name of the aggregated state to load.
            config_path (str | None): The path to the YAML configuration file.
            config_dict (dict | None): A dictionary containing the configuration. If provided, this will be used instead of loading from a file.
        """
        self._check_inputs(config_path=config_path, config_dict=config_dict)
        if config_path:
            with open(config_path, "r", encoding="utf-8") as f:
                config_dict = yaml.safe_load(f)
        config = AggregatedStateConfig(name=name, states=config_dict)
        self._manager.add(config)

    def update_config(
        self, name: str, config_path: str | None = None, config_dict: dict | None = None
    ) -> None:
        """
        Update a state configuration from a YAML file or a dictionary. If None or both are provided,
        an error will be raised. Config must be states for an AggregatedStateConfig or a dictionary/YAML file that
        can be parsed into one. Please check AggregatedStateConfig state field for the expected format of the configuration.

        Args:
            name (str): The name of the aggregated state to update.
            config_path (str | None): The path to the YAML configuration file.
            config_dict (dict | None): A dictionary containing the configuration. If provided, this will be used instead of loading from a file.
        """
        self._check_inputs(config_path=config_path, config_dict=config_dict)
        if config_path:
            with open(config_path, "r", encoding="utf-8") as f:
                config_dict = yaml.safe_load(f)
        # Load the new state
        config = AggregatedStateConfig(name=name, states=config_dict)
        self._manager._update_state(config)

    def _check_inputs(self, config_path: str | None, config_dict: dict | None) -> None:
        if (config_path is None and config_dict is None) or (
            config_path is not None and config_dict is not None
        ):
            raise ValueError("Either config_path or config_dict must be provided, but not both.")
