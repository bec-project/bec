"""Actors can autonomously respond to changes in beamline states."""

import time
from abc import ABC, abstractmethod
from threading import Event
from typing import Iterable

from bec_lib.actors import ActorActionTable
from bec_lib.client import BECClient
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.logger import bec_logger

logger = bec_logger.logger


class ActorBase(ABC):
    client: BECClient
    action_table: ActorActionTable

    def __init__(self, client: BECClient, exec_id: str):
        self.client = client
        self.stop_event = Event()
        self.client.connector.register(MessageEndpoints.actor_stop(exec_id), cb=self.stop)

    def evaluate(self, *_, **__):
        for condition, action in self.action_table.items():
            if condition(self.client):
                logger.info(
                    f"{self.__class__.__name__} triggered, executing action for condition: {condition}"
                )
                action(self.client)

    @abstractmethod
    def run(self):
        """Run forever until self.stop_event is set"""

    def stop(self, *_):
        self.stop_event.set()


class SubscriptionActor(ActorBase):
    """An actor which subscribes to a list of redis endpoints, and evaluates on any message to any
    of those endpoints."""

    def __init__(
        self,
        client: BECClient,
        exec_id: str,
        endpoints: Iterable[EndpointInfo],
        min_delay_s: float = 0.01,
    ):
        super().__init__(client, exec_id)
        self.min_delay = min_delay_s
        self.last_evaluated = time.monotonic()
        for endpoint in set(endpoints):
            client.connector.register(endpoint, cb=self.evaluate)

    def evaluate(self, *_, **__):
        if (now := time.monotonic()) < self.last_evaluated + self.min_delay:
            return
        self.last_evaluated = now
        return super().evaluate(*_, **__)

    def run(self):
        self.stop_event.wait()


class PollingActor(ActorBase):
    """An actor which evaluates its conditions after a certain time interval."""

    def __init__(self, client: BECClient, exec_id: str, poll_interval_s: float = 0.1):
        super().__init__(client, exec_id)
        self.poll_interval = poll_interval_s

    def run(self):
        while not self.stop_event.wait(timeout=0.1):
            self.evaluate()
