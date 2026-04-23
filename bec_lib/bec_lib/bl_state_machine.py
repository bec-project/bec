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
        self, name: str, config_path: str | None, config_dict: dict | None = None
    ) -> None:
        """
        Load an aggregated state configuration from a YAML file or a dictionary. If None or both are provided,
        and error will be raised.

        Args:
            name (str): The name of the aggregated state to create.
            config_path (str | None): The path to the YAML configuration file.
            config_dict (dict | None): A dictionary containing the configuration. If provided, this will be used instead of loading from a file.

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
        self._check_inputs(config_path=config_path, config_dict=config_dict)
        if config_path:
            with open(config_path, "r", encoding="utf-8") as f:
                config_dict = yaml.safe_load(f)

        config = AggregatedStateConfig(name=name, states=config_dict)
        self._manager.add(config)

    def update_config(
        self,
        name: str,
        config_path: str | None,
        config_dict: dict | AggregatedStateConfig | None = None,
    ) -> None:
        """
        Update an existing aggregated state configuration from a YAML file or a dictionary.
        If None or both are provided, and error will be raised.
        It will update the state based on the configuration and update it in the state_manager.

        Args:
            name (str): The name of the aggregated state to update.
            config_path (str | None): The path to the YAML configuration file.
            config_dict (dict | None): A dictionary containing the configuration. If provided, this will
                be used instead of loading from a file.
        """
        self._check_inputs(config_path=config_path, config_dict=config_dict)
        # pylint: disable=protected-access
        if name not in self._manager._states:
            raise ValueError(f"Configuration for name {name} not found.")
        if config_path:
            with open(config_path, "r", encoding="utf-8") as f:
                config_dict = yaml.safe_load(f)
        # Load the new state
        config = AggregatedStateConfig(name=name, states=config_dict)
        self._manager.update(config)

    def _check_inputs(
        self, config_path: str | None, config_dict: dict | AggregatedStateConfig | None
    ) -> None:
        if (config_path is None and config_dict is None) or (
            config_path is not None and config_dict is not None
        ):
            raise ValueError("Either config_path or config_dict must be provided, but not both.")
