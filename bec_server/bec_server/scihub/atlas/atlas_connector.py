from __future__ import annotations

import os
from typing import TYPE_CHECKING

from dotenv import dotenv_values

from bec_lib.connector import ConnectorBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector

from .atlas_forwarder import AtlasForwarder
from .atlas_metadata_handler import AtlasMetadataHandler
from .config_handler import ConfigHandler

if TYPE_CHECKING:
    from bec_server.scihub import SciHub

logger = bec_logger.logger


class AtlasConnector:

    def __init__(self, scihub: SciHub, connector: ConnectorBase) -> None:
        self.scihub = scihub
        self.connector = connector

        self.connected_to_atlas = False
        self.redis_atlas: RedisConnector = None
        self.host = None
        self.deployment_name = None
        self.atlas_key = None
        self._env_configured = False
        self._config_request_handler = None
        self.config_handler = None
        self.metadata_handler = None
        self._start(connector)

    def _start(self, connector: ConnectorBase):
        self.connect_to_atlas()
        self.config_handler = ConfigHandler(self, connector)
        self._start_config_request_handler()
        self.metadata_handler = AtlasMetadataHandler(self)
        self.atlas_forwarder = AtlasForwarder(self)

    @property
    def config(self):
        """get the current service config"""
        return self.scihub.config

    def connect_to_atlas(self):
        """
        Connect to Atlas
        """
        self._load_environment()
        if not self._env_configured:
            logger.warning("No environment file found. Cannot connect to Atlas.")
            return

        try:
            self.redis_atlas = RedisConnector(
                self.host, username=f"ingestor_{self.deployment_name}", password=self.atlas_key
            )
            # pylint: disable=protected-access

            # use authenticate method once merged.
            # self.redis_atlas.authenticate(self.atlas_key, username=f"ingestor_{self.deployment_name}")

            # self.redis_atlas._redis_conn.auth(
            #     self.atlas_key, username=f"ingestor_{self.deployment_name}"
            # )
            # self.redis_atlas._redis_conn.connection_pool.connection_kwargs["username"] = (
            #     f"ingestor_{self.deployment_name}"
            # )
            # self.redis_atlas._redis_conn.connection_pool.connection_kwargs["password"] = (
            #     self.atlas_key
            # )

            self.redis_atlas._redis_conn.ping()
            logger.success("Connected to Atlas")
        # pylint: disable=broad-except
        except Exception as exc:
            logger.error(f"Failed to connect to Atlas: {exc}")
        else:
            self.connected_to_atlas = True

    def ingest_data(self, data: dict) -> None:
        """
        Ingest data into Atlas
        """
        if not self.connected_to_atlas:
            logger.warning("Not connected to Atlas. Cannot ingest data.")
            return

        self.redis_atlas.xadd(
            f"internal/deployment/{self.deployment_name}/ingest", data, max_size=1000
        )

    def _load_environment(self):
        env_base = self.scihub.config.service_config.get("atlas", {}).get("env_file", "")
        env_file = os.path.join(env_base, ".env")
        if not os.path.exists(env_file):
            # check if there is an env file in the parent directory
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            env_file = os.path.join(current_dir, ".env")
            if not os.path.exists(env_file):
                return

        config = dotenv_values(env_file)
        self._update_config(**config)

    # pylint: disable=invalid-name
    def _update_config(
        self, ATLAS_HOST: str = None, ATLAS_DEPLOYMENT: str = None, ATLAS_KEY: str = None, **kwargs
    ) -> None:
        self.host = ATLAS_HOST
        self.deployment_name = ATLAS_DEPLOYMENT
        self.atlas_key = ATLAS_KEY

        if self.host and self.atlas_key:
            self._env_configured = True

    def _start_config_request_handler(self) -> None:
        self._config_request_handler = self.connector.register(
            MessageEndpoints.device_config_request(),
            cb=self._device_config_request_callback,
            parent=self,
        )

    @staticmethod
    def _device_config_request_callback(msg, *, parent, **_kwargs) -> None:
        logger.info(f"Received request: {msg}")
        parent.config_handler.parse_config_request(msg.value)

    def shutdown(self):
        """
        Shutdown the Atlas connector
        """
        if self._config_request_handler:
            self._config_request_handler.shutdown()
        if self.metadata_handler:
            self.metadata_handler.shutdown()
