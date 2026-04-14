from __future__ import annotations

import queue
import threading
import uuid
from typing import Callable, ClassVar, Literal, Protocol

from pydantic import BaseModel

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_lib.scan_manager import ScanManager

logger = bec_logger.logger


class ScanModifications:

    def __init__(self, connector: RedisConnector, target_queue: str = "primary"):
        self.connector = connector
        self.target_queue = target_queue

    def add_queue_lock(self, reason: str, identifier: str):
        """Lock the queue with a reason and identifier."""
        self.connector.send(
            MessageEndpoints.scan_queue_modification_request(),
            messages.ScanQueueModificationMessage(
                scan_id=None,
                action="lock",
                queue=self.target_queue,
                parameter={"reason": reason, "identifier": identifier},
            ),
        )

    def remove_queue_lock(self, identifier: str):
        """Release the queue lock with a reason and identifier."""
        self.connector.send(
            MessageEndpoints.scan_queue_modification_request(),
            messages.ScanQueueModificationMessage(
                scan_id=None,
                action="release_lock",
                queue=self.target_queue,
                parameter={"identifier": identifier},
            ),
        )

    def restart_scan(self):
        """Restart the current scan in the queue."""
        self.connector.send(
            MessageEndpoints.scan_queue_modification_request(),
            messages.ScanQueueModificationMessage(
                scan_id=None,
                action="restart",
                queue=self.target_queue,
                parameter={"position": "replace", "RID": str(uuid.uuid4())},
            ),
        )


class ActorProtocol(Protocol):

    def set_enabled(self, enabled: bool): ...

    def on_disabled(self): ...

    def config_update(self, config: ActorConfig): ...

    def run(self): ...

    def shutdown(self): ...


class ActorConfig(BaseModel):
    actor_type: ClassVar[str] = "ActorConfig"

    enabled: bool = True
    name: str | None = None


class ScanInterlockConfig(ActorConfig):
    actor_type: ClassVar[str] = "ScanInterlockConfig"
    conditions: list[str] = []
    target_queue: str = "primary"


class MessagingActorService(BaseModel):
    service_name: Literal["signal", "scilog", "teams"]
    scope: str | None = None


class MessagingActorConfig(ActorConfig):
    actor_type: ClassVar[str] = "MessagingActorConfig"

    services: list[MessagingActorService] = []
    conditions: list[str] = []


class Actor(ActorProtocol):
    """Base class for beamline actors, implementing the ActorProtocol"""

    def __init__(self, name: str, scan_manager: ScanManager):
        self.name = name
        self.scan_manager = scan_manager
        self.config: ActorConfig | None = None
        self.enabled = False
        self.shutdown_event = threading.Event()
        self._action_queue = queue.Queue()

    def set_enabled(self, enabled: bool):
        """Enable or disable the actor."""
        self.enabled = enabled
        if not enabled:
            self.on_disabled()

    def on_disabled(self):
        """Hook called when the actor is disabled."""

    def config_update(self, config: ActorConfig):
        """Update the actor's configuration."""
        self.config = config

    def _enqueue_action(self, action: Callable, *args, **kwargs):
        """Enqueue an action to be executed on the main thread."""
        self._action_queue.put((action, args, kwargs))

    def run(self):
        """Run the actor's main logic."""
        while not self.shutdown_event.is_set():
            try:
                # Wait for actions with a timeout to allow shutdown check
                action, args, kwargs = self._action_queue.get(timeout=0.1)
                try:
                    action(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error executing action in actor {self.name}: {e}")
                finally:
                    self._action_queue.task_done()
            except queue.Empty:
                continue

    def shutdown(self):
        """Shutdown the actor."""
        self.shutdown_event.set()


class InProcessActor(ActorProtocol):
    """
    Base class for actors that run in a separate thread within the same process,
    primarily the scan server process. It implements the ActorProtocol but does
    not have its own process. Instead, it runs in a thread managed by the scan
    server. This is useful for very lightweight actors that do not require a separate
    process and can run safely within the scan server without risking stability.
    These actors should not perform any blocking operations.
    """

    def __init__(self, name: str, connector: RedisConnector):
        self.name = name
        self.connector = connector
        self.scan_modifications = ScanModifications(connector)
        self.enabled = False
        self.config: ActorConfig | None = None

    def set_enabled(self, enabled: bool):
        """Enable or disable the actor."""
        self.enabled = enabled
        if not enabled:
            self.on_disabled()

    def on_disabled(self):
        """Hook called when the actor is disabled."""

    def config_update(self, config: ActorConfig):
        """Update the actor's configuration."""
        self.config = config

    def run(self):
        """Run the actor's main logic."""

    def shutdown(self):
        """Shutdown the actor."""


class ScanInterlock(InProcessActor):
    """
    An actor that locks the queue when the specified beamline condition is not met.
    Once the condition is met, it unlocks the queue and restarts any paused scans.
    """

    def __init__(self, name: str, connector: RedisConnector):
        super().__init__(name, connector)
        self.conditions: list[str] = []
        self.custom_action: Callable | None = None
        self.states: dict[str, bool] = {}
        self.in_alarm_state = False
        self.target_queue = "primary"

    ################################################
    ####### Protocol method implementations ########
    ################################################

    def config_update(self, config: ScanInterlockConfig):
        """Update the actor's configuration."""
        super().config_update(config)
        self.target_queue = config.target_queue

        conditions_to_add = set(config.conditions) - set(self.conditions)
        conditions_to_remove = set(self.conditions) - set(config.conditions)

        for condition in conditions_to_add:
            self.add_condition(condition)

        for condition in conditions_to_remove:
            self.remove_condition(condition)

    def add_condition(self, condition_name: str):
        """Add a beamline condition to monitor."""
        self.conditions.append(condition_name)
        self._register_condition(condition_name)

    def remove_condition(self, condition_name: str):
        """Remove a beamline condition from monitoring."""
        if condition_name in self.conditions:
            self.conditions.remove(condition_name)
            self._unregister_condition(condition_name)

    def on_enter_alarm_state(self):
        """Hook called when entering alarm state."""
        self.scan_modifications.add_queue_lock(
            reason=f"Actor {self.name} locked the queue due to beamline condition.",
            identifier=self.name,
        )

    def on_exit_alarm_state(self):
        """Hook called when exiting alarm state."""
        self.scan_modifications.remove_queue_lock(identifier=self.name)
        self.scan_modifications.restart_scan()

    def on_disabled(self):
        self.scan_modifications.remove_queue_lock(identifier=self.name)

    def shutdown(self):
        """Shutdown the actor."""
        self.on_disabled()

    ################################################
    ####### Internal helper methods ################
    ################################################

    def _register_condition(self, condition_name: str):
        self.connector.register(
            MessageEndpoints.beamline_state(condition_name),
            cb=self._condition_update_callback,
            parent=self,
            from_start=True,
        )

    def _unregister_condition(self, condition_name: str):
        self.connector.unregister(
            MessageEndpoints.beamline_state(condition_name),
            cb=self._condition_update_callback,
        )

    @staticmethod
    def _condition_update_callback(
        msg_dict: dict[str, messages.BeamlineStateMessage],
        parent: ScanInterlock,
        **_kwargs,
    ):
        parent._evaluate_conditions(msg_dict["data"])

    def _evaluate_conditions(self, condition: messages.BeamlineStateMessage):
        if not self.enabled:
            return
        self.states[condition.name] = condition.status == "invalid"
        in_alarm_state = any(not state for state in self.states.values())
        if in_alarm_state == self.in_alarm_state:
            # no change in alarm state, do nothing
            return
        self.in_alarm_state = in_alarm_state
        if in_alarm_state:
            logger.info(
                f"Actor {self.name} entering alarm state due to condition change."
            )
            self.on_enter_alarm_state()
        else:
            logger.info(f"Actor {self.name} exiting alarm state as conditions are met.")
            self.on_exit_alarm_state()
