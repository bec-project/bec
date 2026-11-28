"""Connect the optional Slurm manager to BEC's active cluster credentials."""

from __future__ import annotations

from threading import RLock
from typing import TYPE_CHECKING

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import SlurmCredentialsMessage

if TYPE_CHECKING:
    from slurm_manager.slurm_manager import SlurmManager

    from bec_lib.redis_connector import MessageObject, RedisConnector

logger = bec_logger.logger


class SlurmClient:
    """Maintain one lazily constructed manager as the server rotates credentials."""

    def __init__(self, connector: RedisConnector) -> None:
        """Subscribe before reading the current credentials.

        Args:
            connector (RedisConnector): Authenticated BEC connection.
        """
        self._connector = connector
        self._lock = RLock()
        self._credentials: SlurmCredentialsMessage | None = None
        self._manager: SlurmManager | None = None
        self._closed = False
        endpoint = MessageEndpoints.slurm_credentials()
        self._connector.register(topics=endpoint, cb=self._credentials_callback)
        try:
            self._update_credentials(self._connector.get(endpoint))
        except BaseException:
            self._connector.unregister(topics=endpoint, cb=self._credentials_callback)
            raise

    def _credentials_callback(self, msg: MessageObject) -> None:
        self._update_credentials(msg.value)

    def _update_credentials(self, credentials: SlurmCredentialsMessage | None) -> None:
        if not isinstance(credentials, SlurmCredentialsMessage):
            return
        with self._lock:
            if self._closed:
                return
            previous = self._credentials
            if previous is not None and credentials.generation <= previous.generation:
                return
            if self._manager is not None:
                if credentials.cluster != previous.cluster:
                    logger.error("Slurm cluster changed; restart the client to switch.")
                    return
                self._manager.set_credentials(
                    username=credentials.username, token=credentials.token
                )
            self._credentials = credentials

    @property
    def manager(self) -> SlurmManager:
        """Return the manager configured with the active server-provided credentials.

        Returns:
            SlurmManager: Manager exposing submission and job event subscriptions.

        Raises:
            RuntimeError: Credentials are unavailable or this client was shut down.
            ImportError: The optional Slurm dependency is not installed.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("The BEC Slurm client has been shut down.")
            if self._manager is None:
                credentials = self._credentials
                if credentials is None:
                    raise RuntimeError("The BEC server has not provided Slurm credentials.")
                try:
                    # Keep Slurm optional and avoid starting its threads during BEC startup.
                    # pylint: disable=import-outside-toplevel
                    from slurm_manager.slurm_manager import SlurmManager
                except ImportError as exc:
                    raise ImportError("Install bec_lib[slurm] to use bec.slurm.") from exc
                self._manager = SlurmManager.for_cluster(
                    credentials.cluster, username=credentials.username, token=credentials.token
                )
            return self._manager

    def shutdown(self) -> None:
        """Unsubscribe and disconnect monitoring without cancelling submitted jobs."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            manager = self._manager
        try:
            self._connector.unregister(
                topics=MessageEndpoints.slurm_credentials(), cb=self._credentials_callback
            )
        finally:
            if manager is not None:
                manager.shutdown(timeout=None)
