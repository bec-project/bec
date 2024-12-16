from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

if TYPE_CHECKING:
    from bec_server.scihub.atlas.atlas_connector import AtlasConnector

logger = bec_logger.logger


class AtlasForwarder:
    def __init__(self, atlas_connector: AtlasConnector):
        self.atlas_connector = atlas_connector
        self._deployment_register = None
        self.active_subscriptions = set()
        self._deployment_cleanup_thread = None
        self._shutdown_event = threading.Event()
        self._start_deployment_subscription()
        self._start_deployment_cleanup_loop()

    def _start_deployment_subscription(self):
        self._deployment_register = self.atlas_connector.redis_atlas.register(
            patterns=f"internal/deployment/{self.atlas_connector.deployment_name}/*/state",
            # patterns="*",
            cb=self._update_deployment_subscriptions,
            parent=self,
        )

    def _start_deployment_cleanup_loop(self):
        self._deployment_cleanup_thread = threading.Thread(
            target=self._deployment_cleanup_loop, name="DeploymentCleanup"
        )
        self._deployment_cleanup_thread.start()

    def _deployment_cleanup_loop(self):
        while not self._shutdown_event.is_set():
            endpoints = self.atlas_connector.redis_atlas.keys(
                f"internal/deployment/{self.atlas_connector.deployment_name}/*/state"
            )
            if endpoints:
                data = self.atlas_connector.redis_atlas.mget(endpoints)
                for msg in data:
                    self.update_deployment_state(msg)
            self._shutdown_event.wait(60)

    @staticmethod
    def _update_deployment_subscriptions(msg_obj, parent):
        msg = msg_obj.value
        parent.update_deployment_state(msg)

    def update_deployment_state(self, msg: messages.RawMessage):
        requested_subscriptions = {
            sub for user_info in msg.data.values() for sub in user_info.get("subscriptions", [])
        }

        # Add new subscriptions
        new_subscriptions = requested_subscriptions - self.active_subscriptions
        for subscription in new_subscriptions:
            self.active_subscriptions.add(subscription)
            self.atlas_connector.connector.register(
                subscription, cb=self.forward_message, parent=self
            )

        # Remove subscriptions that are no longer needed
        stale_subscriptions = self.active_subscriptions - requested_subscriptions
        for subscription in stale_subscriptions:
            self.active_subscriptions.remove(subscription)
            self.atlas_connector.connector.unregister(subscription)

        logger.info(f"Active subscriptions: {self.active_subscriptions}")

    @staticmethod
    def forward_message(msg, parent):
        if isinstance(msg, MessageObject):
            data = msg.value
            endpoint = msg.topic
        else:
            data = msg
            endpoint = None
        parent.atlas_connector.redis_atlas.xadd(
            f"internal/deployment/{parent.atlas_connector.deployment_name}/data/{endpoint}",
            data if isinstance(data, dict) else {"pubsub_data": data},
            max_size=10,
            expire=60,
        )

    def shutdown(self):
        self._shutdown_event.set()
        if self._deployment_cleanup_thread:
            self._deployment_cleanup_thread.join()
