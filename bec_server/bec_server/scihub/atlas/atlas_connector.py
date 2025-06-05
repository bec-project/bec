from __future__ import annotations

import inspect
import os
from typing import TYPE_CHECKING

from dotenv import dotenv_values

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_lib.signature_serializer import signature_to_dict

from .atlas_forwarder import AtlasForwarder
from .atlas_metadata_handler import AtlasMetadataHandler
from .config_handler import ConfigHandler

if TYPE_CHECKING:  # pragma: no cover
    from bec_server.scihub import SciHub

logger = bec_logger.logger


class AtlasConnector:

    def __init__(
        self, scihub: SciHub, connector: RedisConnector, redis_atlas: RedisConnector | None = None
    ) -> None:
        self.scihub = scihub
        self.connector = connector
        self.redis_atlas = redis_atlas

        self.connected_to_atlas = False
        self.host = None
        self.deployment_name = None
        self.atlas_key = None
        self._env_configured = False
        self._config_request_handler = None
        self.config_handler = None
        self.metadata_handler = None
        self.atlas_forwarder = None

    def start(self):
        self.connect_to_atlas()
        self.config_handler = ConfigHandler(self, self.connector)
        self._start_config_request_handler()
        if self.connected_to_atlas:
            self.metadata_handler = AtlasMetadataHandler(self)
            self.atlas_forwarder = AtlasForwarder(self)
        self.update_acls()
        self.update_available_endpoints()

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
            if not self.host:
                return  # no host configured
            if self.redis_atlas is None:
                self.redis_atlas = RedisConnector(self.host)
                self.redis_atlas.authenticate(
                    username=f"ingestor_{self.deployment_name}", password=self.atlas_key
                )
            # pylint: disable=protected-access
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

        if self.redis_atlas is None:
            logger.error("Redis Atlas connection is not initialized.")
            return

        self.redis_atlas.xadd(
            f"internal/deployment/{self.deployment_name}/ingest", data, max_size=1000
        )

    def update_acls(self):
        """
        Update the ACLs from Atlas. This is done by reading the ACLs from the Atlas
        Redis instance and writing them to BEC's Redis instance.
        If there is no connection to Atlas, it will populate the default ACLs.
        """
        if self.connected_to_atlas:
            # TODO: Implement this
            return

        # Populate default ACLs
        self._populate_default_acls()

    def update_available_endpoints(self):
        """
        Update the available endpoints in the connector.
        """

        endpoints = {}

        for endpoint in dir(MessageEndpoints):
            if endpoint.startswith("_"):
                continue
            endpoint_func = getattr(MessageEndpoints, endpoint)
            if not callable(endpoint_func):
                continue
            endpoints[endpoint] = {
                "signature": signature_to_dict(endpoint_func),
                "doc": endpoint_func.__doc__,
            }
        self.connector.set(
            MessageEndpoints.endpoint_info(), messages.AvailableResourceMessage(resource=endpoints)
        )

    def _populate_default_acls(self):
        """
        Populate default ACLs
        """
        # get the list of all ACLs
        acls = self.connector._redis_conn.acl_list()
        if not acls:
            return
        user_accounts = {}
        for acl in acls:
            acl_name = acl.split(" ")[1]
            user_info: dict = self.connector._redis_conn.acl_getuser(acl_name)
            if not user_info["enabled"] or acl_name in ["default", "bec"]:
                continue
            user_accounts[acl_name] = {
                key: val
                for key, val in user_info.items()
                if key in ["categories", "keys", "channels", "commands", "profile"]
            }

            # patch the profile for test accounts "user" and "admin"
            if acl_name == "user":
                user_accounts[acl_name]["profile"] = "user_write"
            elif acl_name == "admin":
                user_accounts[acl_name]["profile"] = "su_write"

        self.connector.set(
            MessageEndpoints.acl_accounts(), messages.ACLAccountsMessage(accounts=user_accounts)
        )
        self.connector.set(
            MessageEndpoints.login_info(),
            messages.LoginInfoMessage(
                host="",
                deployment="",
                available_accounts=list(user_accounts.keys()),
                atlas_login=False,
            ),
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
        if self.atlas_forwarder:
            self.atlas_forwarder.shutdown()
        if self.redis_atlas:
            self.redis_atlas.shutdown()
