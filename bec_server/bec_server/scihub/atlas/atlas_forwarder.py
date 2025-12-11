from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.logger import bec_logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.redis_connector import RedisConnector
    from bec_server.scihub.atlas.atlas_connector import AtlasConnector

logger = bec_logger.logger


class AtlasForwarder:
    def __init__(self, atlas_connector: AtlasConnector):
        self.atlas_connector = atlas_connector
        self.active_subscriptions = set()
        self._deployment_cleanup_thread = None
        self._shutdown_event = threading.Event()
        self._start_deployment_subscription()
        self._start_deployment_cleanup_loop()

    def _start_deployment_subscription(self):
        if not self.atlas_connector.deployment_name:
            return
        if not self.atlas_connector.redis_atlas:
            return
        self.atlas_connector.redis_atlas.register(
            patterns=MessageEndpoints.atlas_websocket_state(
                self.atlas_connector.deployment_name, "*"
            ),
            cb=self._update_deployment_subscriptions,
            parent=self,
        )
        self.atlas_connector.redis_atlas.register(
            patterns=MessageEndpoints.atlas_deployment_request(
                self.atlas_connector.deployment_name
            ),
            cb=self._on_redis_request,
            parent=self,
        )

    def _start_deployment_cleanup_loop(self):
        self._deployment_cleanup_thread = threading.Thread(
            target=self._deployment_cleanup_loop, name="DeploymentCleanup"
        )
        self._deployment_cleanup_thread.start()

    def _deployment_cleanup_loop(self):
        if not self.atlas_connector.deployment_name:
            return
        if not self.atlas_connector.redis_atlas:
            return
        while not self._shutdown_event.is_set():
            endpoints = self.atlas_connector.redis_atlas.keys(
                MessageEndpoints.atlas_websocket_state(self.atlas_connector.deployment_name, "*")
            )
            if endpoints:
                data = self.atlas_connector.redis_atlas.mget(endpoints)
                for msg in data:  # type: ignore
                    self.update_deployment_state(msg)
            self._shutdown_event.wait(60)

    @staticmethod
    def _on_redis_request(msg_obj, parent):
        msg: messages.RawMessage = msg_obj.value
        redis_atlas: RedisConnector = parent.atlas_connector.redis_atlas
        redis_bec: RedisConnector = parent.atlas_connector.connector
        if msg.data.get("action") == "get":
            key = msg.data.get("key")

            out = redis_bec.get(key)
            topic = msg.data.get("response_endpoint")
            if out is None:
                out = json.dumps({"out": None})
            redis_atlas.send(topic, out)

    @staticmethod
    def _update_deployment_subscriptions(msg_obj, parent):
        msg = msg_obj.value
        parent.update_deployment_state(msg)

    def update_deployment_state(self, msg: messages.RawMessage):
        requested_subscriptions = {
            sub[1] for user_info in msg.data.values() for sub in user_info.get("subscriptions", [])
        }

        # Add new subscriptions
        new_subscriptions = requested_subscriptions - self.active_subscriptions
        for subscription in new_subscriptions:
            self.active_subscriptions.add(subscription)
            endpoint = self._get_endpoint_from_subscription(subscription)
            self.atlas_connector.connector.register(
                endpoint, cb=self.forward_message, parent=self, endpoint=endpoint
            )

        # Remove subscriptions that are no longer needed
        stale_subscriptions = self.active_subscriptions - requested_subscriptions
        for subscription in stale_subscriptions:
            self.active_subscriptions.remove(subscription)
            endpoint = self._get_endpoint_from_subscription(subscription)
            self.atlas_connector.connector.unregister(subscription)

        logger.info(f"Active subscriptions: {self.active_subscriptions}")

    def _get_endpoint_from_subscription(self, subscription: str) -> EndpointInfo:
        endpoint_info = json.loads(subscription)
        func = getattr(MessageEndpoints, endpoint_info["endpoint"])
        if endpoint_info.get("args"):
            endpoint = func(*endpoint_info["args"])
        else:
            endpoint = func()
        return endpoint

    @staticmethod
    def forward_message(msg, parent, endpoint):
        endpoint = endpoint.endpoint
        if isinstance(msg, MessageObject):
            data = msg.value
        else:
            data = msg
        parent.atlas_connector.redis_atlas.xadd(
            MessageEndpoints.atlas_deployment_data(
                parent.atlas_connector.deployment_name, endpoint
            ),
            data if isinstance(data, dict) else {"pubsub_data": data},
            max_size=10,
            expire=60,
        )

    def shutdown(self):
        self._shutdown_event.set()
        if self._deployment_cleanup_thread:
            self._deployment_cleanup_thread.join()
