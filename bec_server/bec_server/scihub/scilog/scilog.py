from __future__ import annotations

from typing import TYPE_CHECKING

import requests
from dotenv import dotenv_values

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.scihub.repeated_timer import RepeatedTimer

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_server.scihub import SciHub


class SciLogConnector:
    token_expiration_time = 86400  # one day

    def __init__(self, scihub: SciHub, connector: RedisConnector) -> None:
        self.scihub = scihub
        self.connector = connector
        self.host = None
        self.user = None
        self.user_secret = None
        self._configured = False
        self._scilog_thread = None
        self._load_environment()
        self._start_scilog_update()

    def get_token(self) -> str | None:
        """get a new scilog token"""
        response = requests.post(
            f"{self.host}/users/login",
            json={"principal": self.user, "password": self.user_secret},
            timeout=5,
        )
        if response.ok:
            return response.json()["token"]
        return

    def set_bec_token(self, token: str) -> None:
        """set the scilog token in redis"""
        self.connector.set(
            MessageEndpoints.logbook(),
            messages.CredentialsMessage(
                credentials={"url": self.host, "token": f"Bearer {token}", "user": self.user}
            ),
        )

    def _start_scilog_update(self) -> None:
        if not self._configured:
            logger.warning("No environment file found. Cannot connect to SciLog.")
            return
        self._scilog_update()
        self._scilog_thread = RepeatedTimer(self.token_expiration_time, self._scilog_update)

    def _scilog_update(self):
        logger.info("Updating SciLog token.")
        token = self.get_token()
        if token:
            self.set_bec_token(token)

    def _load_environment(self):
        env_file = self.scihub.config.config.get("scilog", {}).get("env_file", "")
        config = dotenv_values(env_file)
        if isinstance(config, dict):
            self._update_config(**config)

    # pylint: disable=invalid-name
    def _update_config(
        self,
        SCILOG_DEFAULT_HOST: str | None = None,
        SCILOG_USER: str | None = None,
        SCILOG_USER_SECRET: str | None = None,
        **kwargs,
    ) -> None:
        self.host = SCILOG_DEFAULT_HOST
        self.user = SCILOG_USER
        self.user_secret = SCILOG_USER_SECRET

        if self.host and self.user and self.user_secret:
            self._configured = True

    def shutdown(self):
        if self._scilog_thread:
            self._scilog_thread.stop()
