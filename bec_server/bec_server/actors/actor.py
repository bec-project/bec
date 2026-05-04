"""Actors can autonomously respond to changes in beamline states."""

import time
from abc import ABC, abstractmethod
from threading import Event
from typing import Callable

from bec_lib.actors import ActorActionTable
from bec_lib.client import BECClient
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import ProcedureWorkerStatus
from bec_server.procedures.oop_worker_base import push_status

logger = bec_logger.logger


class ActorBase(ABC):
    client: BECClient
    action_table: ActorActionTable

    def __init__(self, client: BECClient, name: str, exec_id: str):
        self.client = client
        self.stop_event = Event()
        self.name = name
        self.exec_id = exec_id
        self.client.connector.register(MessageEndpoints.actor_stop(exec_id), cb=self.stop)

    def push_status(self, st: ProcedureWorkerStatus):
        exec = self.exec_id if st == ProcedureWorkerStatus.RUNNING else None
        push_status(self.client.connector, self.name, st, exec)

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

    min_delay_s: float = 0.01

    def __init__(self, client: BECClient, name: str, exec_id: str):
        super().__init__(client, name, exec_id)
        self._endpoints = self.default_monitor_endpoints()
        self.last_evaluated = 0

        logger.info(f"Setting up {self.__class__.__name__}: {self._endpoints}.")
        for endpoint in self._endpoints:
            logger.info(f"Connecting {self.__class__.__name__} to '{endpoint.endpoint}'")
            for cb in self.default_monitor_callbacks():
                client.connector.register(endpoint, cb=cb)

    def default_monitor_endpoints(self) -> set[EndpointInfo]:
        return set()

    def default_monitor_callbacks(self) -> list[Callable]:
        return [self.evaluate]

    def evaluate(self, *_, **__):
        logger.info(f"{self.__class__.__name__} triggered")
        if (now := time.monotonic()) < self.last_evaluated + self.min_delay_s:
            logger.info("too little time elapsed since last trigger")
            return
        self.last_evaluated = now
        return super().evaluate(*_, **__)

    def run(self):
        self.push_status(ProcedureWorkerStatus.RUNNING)
        try:
            self.stop_event.wait()
        except KeyboardInterrupt:
            self.push_status(ProcedureWorkerStatus.IDLE)


class PollingActor(ActorBase):
    """An actor which evaluates its conditions after a certain time interval."""

    def __init__(self, client: BECClient, name: str, exec_id: str, poll_interval_s: float = 0.1):
        super().__init__(client, name, exec_id)
        self.poll_interval = float(poll_interval_s)

    def run(self):
        self.push_status(ProcedureWorkerStatus.RUNNING)
        try:
            while not self.stop_event.wait(timeout=0.1):
                self.evaluate()
        except KeyboardInterrupt:
            self.push_status(ProcedureWorkerStatus.IDLE)
