"""Actors can autonomously respond to changes in beamline states."""

import time
from abc import ABC, abstractmethod
from threading import Event, RLock
from typing import Callable

from bec_lib.actors import ActorActionTable
from bec_lib.client import BECClient
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import BeamlineStateMessage, BlStateStatus, ProcedureWorkerStatus
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
        """
        For each `(condition, action)` entry in the `action_table` dictionary, call `condition`, and
        if it returns `True`, call `action`. It accepts any args and kwargs but does not use any of
        them - subclasses may use this as a callback for a function which returns anything without
        overriding it, to trigger evaluation of the actor as a response. Each condition may be a
        simple function which evaluates to a boolean, or a `bec_lib.actors.ActorConditionSet` with
        a set of such functions and an operation to combine them.

        This is called in a loop by the `PollingActor`, and called as a callback to any Redis event
        it is subscribed to in the `SubscriptionActor`.
        """
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
        self._stopped = False
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
        if self._stopped:
            return
        if (now := time.monotonic()) < self.last_evaluated + self.min_delay_s:
            return
        logger.debug(f"{self.__class__.__name__} evaluated")
        self.last_evaluated = now
        return super().evaluate(*_, **__)

    def run(self):
        self.push_status(ProcedureWorkerStatus.RUNNING)
        try:
            self.stop_event.wait()
            self.stop()
        except KeyboardInterrupt:
            self.push_status(ProcedureWorkerStatus.IDLE)

    def stop(self, *_):
        self._stopped = True
        for endpoint in self._endpoints:
            for cb in self.default_monitor_callbacks():
                try:
                    self.client.connector.unregister(endpoint, cb=cb)
                except Exception as e:
                    logger.error(
                        f"{self.__class__} {self.__qualname__} failed to unregister {cb} from {endpoint}: {e}"
                    )


class BlStateActor(SubscriptionActor):
    """
    Base for actors which respond to changes in beamline states.

    If all current values of states in state_table match the value in the table,
    self.all_match_action() is called. If not, self.some_mismatch_action() is called.
    """

    state_table: dict[str, BlStateStatus]

    def __init__(self, client: BECClient, name: str, exec_id: str):
        self.state_table_lock = RLock()
        self.action_table = {
            self.all_states_match: self.all_match_action,
            self.not_all_states_match: self.some_mismatch_action,
        }
        super().__init__(client, name, exec_id)
        self.state_cache: dict[str, BlStateStatus] = {}

    def _update_cache(self):
        with self.state_table_lock:
            to_remove = []
            for state in self.state_table:
                status = self.client.beamline_states.get_status_by_name(state)
                if status is None:
                    logger.warning(f"Beamline state actor could not get the status of {state}!")
                    to_remove.append(state)
                    continue
                self.state_cache[state] = status
            for state in to_remove:
                logger.warning(
                    f"Removing {state} from watched states because it no longer seems to exist."
                )
                del self.state_table[state]

    def all_states_match(self, client: BECClient):
        with self.state_table_lock:
            for state, status in self.state_table.items():
                if self.state_cache.get(state) != status:
                    logger.info(f"Beamline state {state} out of bounds: expected={status}")
                    return False
            return True

    def not_all_states_match(self, client: BECClient):
        return not self.all_states_match(client)

    def all_match_action(self, client: BECClient):
        pass

    def some_mismatch_action(self, client: BECClient):
        pass

    def run(self):
        while not self.client.beamline_states.ready:
            logger.warning(f"{self.__class__.__name__} waiting for beamline states to become ready")
            time.sleep(0.1)
        self._update_cache()
        self.evaluate()
        return super().run()

    def default_monitor_endpoints(self) -> set[EndpointInfo]:
        return {MessageEndpoints.beamline_state(state) for state in self.state_table}

    def evaluate(self, msg_dict: dict | None = None):
        """If evaluate is triggered as a callback to a received beamline state stream message, it
        will update the cache before executing the evaluation. If it is called without an argument
        it will evaluate based on the current cache of beamline states."""
        if msg_dict is not None:
            msg: BeamlineStateMessage = msg_dict["data"]
            self.state_cache[msg.name] = msg.status
        return super().evaluate()


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
