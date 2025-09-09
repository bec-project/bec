"""
This module provides a class to handle the service configuration.
"""

import json
import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

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


class LogWriterConfig(BaseModel):
    """Log writer configuration model."""

    base_path: str = Field(default_factory=lambda: os.path.join(DEFAULT_BASE_PATH, "logs"))


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


class ServiceConfigModel(BaseModel):
    """Service configuration model."""

    redis: RedisConfig = Field(default_factory=RedisConfig)
    file_writer: FileWriterConfig = Field(default_factory=FileWriterConfig)
    log_writer: LogWriterConfig = Field(default_factory=LogWriterConfig)
    user_macros: UserMacrosConfig = Field(default_factory=UserMacrosConfig)
    user_scripts: UserScriptsConfig = Field(default_factory=UserScriptsConfig)
    bec_widgets_settings: BecWidgetsSettings = Field(default_factory=BecWidgetsSettings)
    atlas: AtlasConfig = Field(default_factory=AtlasConfig)
    scilog: SciLogConfig = Field(default_factory=SciLogConfig)
    acl: ACLConfig = Field(default_factory=ACLConfig)
    abort_on_ctrl_c: bool = True


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
                return config

        return {}

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
