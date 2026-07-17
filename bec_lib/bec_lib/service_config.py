"""
This module provides a class to handle the service configuration.
"""

import json
import os
import re
from getpass import getuser
from pathlib import Path
from typing import Any, ClassVar, Self

import yaml
from pydantic import BaseModel, Field, PositiveInt, model_validator

from bec_lib.logger import bec_logger

logger = bec_logger.logger

DEFAULT_BASE_PATH = (
    str(Path(__file__).resolve().parent.parent.parent) if "site-packages" not in __file__ else "./"
)


class RedisConfig(BaseModel):
    """Redis configuration model."""

    host: str = Field(default_factory=lambda: os.environ.get("BEC_REDIS_HOST", "localhost"))
    port: int = 6379

    @property
    def url(self) -> str:
        """Return the Redis URL."""
        return f"{self.host}:{self.port}"


class FileWriterConfig(BaseModel):
    """File writer configuration model."""

    plugin: str = "default_NeXus_format"
    base_path: str = Field(default_factory=lambda: os.path.join(DEFAULT_BASE_PATH, "data"))


class LogFilePolicy(BaseModel):
    """Optional per-service log file policy overrides."""

    max_file_size_mb: PositiveInt | None = None
    max_files: PositiveInt | None = None
    max_file_age_days: PositiveInt | None = None


class LogWriterConfig(BaseModel):
    """Log writer configuration model."""

    base_path: str = Field(default_factory=lambda: os.path.join(DEFAULT_BASE_PATH, "logs"))
    max_file_size_mb: PositiveInt = 50
    max_files: PositiveInt = 3
    max_file_age_days: PositiveInt = 14
    service_overrides: dict[str, LogFilePolicy] = Field(default_factory=dict)


class UserMacrosConfig(BaseModel):
    """User macros configuration model."""

    base_path: str = Field(default_factory=lambda: os.path.join(DEFAULT_BASE_PATH, "macros"))


class UserScriptsConfig(BaseModel):
    """User scripts configuration model."""

    base_path: str = Field(default_factory=lambda: os.path.join(DEFAULT_BASE_PATH, "scripts"))


class BecWidgetsSettings(BaseModel):
    """BEC widgets settings configuration model."""

    base_path: str = Field(
        default_factory=lambda: os.path.join(DEFAULT_BASE_PATH, "widgets_settings")
    )


class AtlasConfig(BaseModel):
    """Atlas configuration model."""

    env_file: str = Field(default_factory=lambda: os.path.join(DEFAULT_BASE_PATH, ".atlas.env"))


class SciLogConfig(BaseModel):
    """SciLog configuration model."""

    env_file: str = Field(default_factory=lambda: os.path.join(DEFAULT_BASE_PATH, ".scilog.env"))


class ACLConfig(BaseModel):
    """ACL configuration model."""

    env_file: str = Field(default_factory=lambda: os.path.join(DEFAULT_BASE_PATH, ".bec_acl.env"))
    user: str | None = None


class ProcedureConfig(BaseModel):
    """Procedure config model."""

    enable_procedures: bool = True
    use_subprocess_worker: bool = False


class ServiceConfigModel(BaseModel):
    """Service configuration model."""

    _CMDLINE_ARGS: ClassVar[dict[str, tuple[str, str]]] = {
        # A mapping from CLI args to service config model fields
        "use_subprocess_proc_worker": ("procedures", "use_subprocess_worker")
    }

    redis: RedisConfig = Field(default_factory=RedisConfig)
    file_writer: FileWriterConfig = Field(default_factory=FileWriterConfig)
    log_writer: LogWriterConfig = Field(default_factory=LogWriterConfig)
    user_macros: UserMacrosConfig = Field(default_factory=UserMacrosConfig)
    user_scripts: UserScriptsConfig = Field(default_factory=UserScriptsConfig)
    widgets_settings: BecWidgetsSettings = Field(default_factory=BecWidgetsSettings)
    atlas: AtlasConfig = Field(default_factory=AtlasConfig)
    scilog: SciLogConfig = Field(default_factory=SciLogConfig)
    acl: ACLConfig = Field(default_factory=ACLConfig)
    abort_on_ctrl_c: bool = True
    procedures: ProcedureConfig = Field(default_factory=ProcedureConfig)

    @model_validator(mode="before")
    def apply_cmdline_args(self, data: Any) -> Self:
        if isinstance(data, dict):
            if cmdline_args := data.get("cmdline_args"):
                for arg in cmdline_args.items():
                    self._update_data_for_arg(arg, data)
        return self

    def _update_data_for_arg(self, arg: tuple[str, Any], data: dict):
        argname, argval = arg
        if argname not in self._CMDLINE_ARGS:
            return data
        subconfig, variable = self._CMDLINE_ARGS[argname]
        if subconfig not in data:
            data[subconfig] = {}
        data[subconfig][variable] = argval
        return data


class ServiceConfig:
    """Service configuration handler using Pydantic models."""

    def __init__(
        self,
        config_path: str | None = None,
        config: dict | None = None,
        config_name: str = "server",
        **kwargs,
    ) -> None:
        self.config_path = config_path
        self.config_name = config_name

        # Load raw config dict first
        raw_config = config if config else {}
        if not raw_config:
            raw_config = self._load_config()

        # Update with provided overrides
        self._update_raw_config(raw_config, **kwargs)

        # Convert to Pydantic model
        self._config_model = ServiceConfigModel(**raw_config)

        self.config = self._config_model.model_dump()

    def _update_raw_config(self, config: dict, **kwargs):
        """Update raw config with provided overrides."""
        for key, val in kwargs.items():
            if val is not None:
                config[key] = val

    def _load_config(self) -> dict:
        """
        Load the base configuration. There are four possible sources:
        1. A file specified by `config_path`.
        2. An environment variable `BEC_SERVICE_CONFIG` containing a JSON string.
        3. The config stored in the deployment_configs directory, matching the defined config name.
        4. The default configuration.
        """
        if self.config_path:
            if not os.path.isfile(self.config_path):
                raise FileNotFoundError(f"Config file {repr(self.config_path)} not found.")
            with open(self.config_path, "r", encoding="utf-8") as stream:
                config = yaml.safe_load(stream)
                logger.info(
                    "Loaded new config from disk:"
                    f" {json.dumps(config, sort_keys=True, indent=4)}"
                )
            config = self._parse_config_from_file(config)
            return config

        _env_config = os.environ.get("BEC_SERVICE_CONFIG")
        if _env_config and isinstance(_env_config, str):
            config = json.loads(_env_config)
            logger.info(
                "Loaded new config from environment:"
                f" {json.dumps(config, sort_keys=True, indent=4)}"
            )
            return config

        if self.config_name:
            path_candidates = [
                os.path.join(DEFAULT_BASE_PATH, "deployment_configs", f"{self.config_name}.yaml"),
                os.path.join(
                    os.path.dirname(DEFAULT_BASE_PATH),
                    "deployment_configs",
                    f"{self.config_name}.yaml",
                ),
            ]
            deployment_config_path = next(
                (path for path in path_candidates if os.path.exists(path)), None
            )
            if deployment_config_path is not None:
                with open(deployment_config_path, "r", encoding="utf-8") as stream:
                    config = yaml.safe_load(stream)
                    logger.info(
                        "Loaded new config from deployment_configs:"
                        f" {json.dumps(config, sort_keys=True, indent=4)}"
                    )
                config = self._parse_config_from_file(config)
                return config

        return {}

    @staticmethod
    def _username_to_pgroup(username: str) -> str | None:
        """Convert PSI eaccounts like ``e12345`` to proposal groups like ``p12345``."""
        if re.match(r"^e\d{5}$", username):
            return f"p{username[1:]}"
        return None

    def _expand_path_variables(self, path: str, username: str) -> str:
        """Expand supported path variables for service config base paths."""
        expanded_path = path.replace("$username", username)
        pgroup = self._username_to_pgroup(username)
        if pgroup is not None:
            expanded_path = expanded_path.replace("$p_username", pgroup)
        return expanded_path

    def _parse_config_from_file(self, config: dict) -> dict:
        """
        Parse the configuration loaded from a file, by checking for username-specific
        base paths and expanding supported path variables.

        Possible path variables include:
        - $username: The current user's username.
        - $p_username: The proposal group corresponding to the current user's username (if applicable). If
            the username is not an eaccount, this variable will not be replaced.
        - $account: The current account set in BEC. It will be replaced in runtime on the server side, so it is not expanded here.

        Args:
            config (dict): The raw configuration dictionary.
        Returns:
            dict: The parsed configuration dictionary with user-specific paths.
        """
        username = getuser()

        for _, val in config.items():
            if not isinstance(val, dict):
                continue
            if "base_path" not in val or not isinstance(val["base_path"], dict):
                continue
            default = val["base_path"].pop("*", None)
            for username_regex, path in val["base_path"].items():
                regex = re.compile(username_regex)
                if regex.match(username):
                    val["base_path"] = self._expand_path_variables(path, username)

                    break
            else:
                if default:
                    val["base_path"] = self._expand_path_variables(default, username)
                else:
                    raise ValueError(
                        f"No matching base_path for user {username} and no default (*) provided."
                    )
        return config

    @property
    def redis(self):
        """Get Redis URL."""
        return self.model.redis.url

    @property
    def service_config(self) -> dict:
        """
        Backward compatibility method to access the service configuration.
        Deprecated in favor of using the Pydantic model directly.

        See issue https://github.com/bec-project/bec/issues/572 for details.
        """
        logger.warning(
            "Accessing service_config directly is deprecated. Use the Pydantic model instead."
        )
        return self.config

    @property
    def abort_on_ctrl_c(self):
        """Get abort_on_ctrl_c setting."""
        return self.model.abort_on_ctrl_c

    @property
    def model(self) -> ServiceConfigModel:
        """Get the Pydantic model."""
        return self._config_model

    def is_default(self):
        """Return whether config is the default configuration."""
        return self.config == ServiceConfigModel().model_dump()
