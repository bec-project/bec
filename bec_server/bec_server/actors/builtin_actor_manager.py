from threading import Event, Thread
from typing import TypeVar

from bec_lib.client import BECClient, ServiceConfig
from bec_lib.config_values import RedisConfigValue
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import (
    AvailableBeamlineStatesMessage,
    BuiltinActorStateChangeNotification,
    BuiltinActorStateUpdatedNotification,
    InterlockTargetState,
    ScanInterlockModifyStateTableMessage,
    ScanInterlockStateTableContent,
)
from bec_server.actors.actor import ActorBase
from bec_server.actors.scan_interlock import ScanInterlockActor

logger = bec_logger.logger

ActorType = TypeVar("ActorType", bound=ActorBase)


class ActorDict(dict):
    def __setitem__(self, key: type[ActorType], value: tuple[ActorType, Thread, Event], /) -> None:
        return super().__setitem__(key, value)

    def __getitem__(self, key: type[ActorType], /) -> tuple[ActorType, Thread, Event]:
        return super().__getitem__(key)

    def get(  # type: ignore
        self, key: type[ActorType], default: tuple[ActorType, Thread, Event] | None = None
    ) -> tuple[ActorType, Thread, Event]:
        return super().get(key)  # type: ignore


class BuiltinActorManager:
    """A simple manager for builtin actors which are always available - only handles enabling and
    disabling"""

    def __init__(self, bootstrap_server: str) -> None:
        host, port = bootstrap_server.split(":")
        self._client = BECClient(
            config=ServiceConfig(config={"redis": {"host": host, "port": port}}),
            name="BuiltinActors",
        )
        self._client.start()
        self._actors_threads_and_stops = ActorDict()

        self._scan_interlock_enabled = RedisConfigValue(
            connector=self._client.connector, endpoint=MessageEndpoints.scan_interlock_enabled()
        )
        self._scan_interlock_enabled.subscribe(self._interlock_enabled_changed)

        self._start_all()
        self._client.connector.register(
            MessageEndpoints.modify_interlock_table(), cb=self._modify_interlock_table
        )
        self._client.connector.register(
            MessageEndpoints.available_beamline_states(), cb=self._handle_state_update
        )

    def _ping_clients(self, actor_name: str):
        self._client.connector.send(
            MessageEndpoints.builtin_actor_update_notif(actor_name),
            BuiltinActorStateUpdatedNotification(actor_name=actor_name),
        )

    def _interlock_enabled_changed(self, enabled: bool):
        self._start_actor(ScanInterlockActor) if enabled else self._stop_actor(ScanInterlockActor)

    def _start_all(self):
        if self._scan_interlock_enabled.value:
            self._start_actor(ScanInterlockActor)

    def _start_actor(self, actor_class: type[ActorBase]):
        name = actor_class.__name__
        logger.info(f"Starting {name}")
        if actor_class in self._actors_threads_and_stops:
            logger.warning(f"Actor {name} is already active!")
            return
        actor = actor_class(self._client, name=name, exec_id=name)
        t = Thread(target=actor.run)
        self._actors_threads_and_stops[actor_class] = (actor, t, actor.stop_event)
        t.start()

    def _stop_actor(self, actor_class: type[ActorBase]):
        logger.info(f"Stopping {actor_class.__name__}")
        if (entry := self._actors_threads_and_stops.get(actor_class)) is None:
            logger.warning(f"Actor {actor_class.__name__} is not active!")
            return
        actor, t, event = entry
        event.set()
        t.join()
        del self._actors_threads_and_stops[actor_class]
        del actor

    def shutdown(self):
        for actor in self._actors_threads_and_stops:
            self._stop_actor(actor)
        self._client.shutdown()

    # Actor specific management methods:
    def _set_interlock_states_in_redis(self, states: dict[str, InterlockTargetState]):
        self._client.connector.set(
            MessageEndpoints.scan_interlock_states(),
            ScanInterlockStateTableContent(states_watched=states),
        )

    def _current_watched_states(self) -> dict[str, InterlockTargetState]:
        states: ScanInterlockStateTableContent | None = self._client.connector.get(
            MessageEndpoints.scan_interlock_states()
        )
        return states.states_watched if states is not None else {}

    def _handle_state_update(self, msg_dict: dict):
        msg: AvailableBeamlineStatesMessage = msg_dict["data"]
        state_names = [state.name for state in msg.states]
        for watched_state in self._current_watched_states():
            if watched_state not in state_names:
                logger.info(
                    "Removing watched interlock state because it is not in available beamline "
                    f"states: {watched_state}"
                )
                self._modify_interlock_table(
                    {
                        "data": ScanInterlockModifyStateTableMessage(
                            action="remove", state_name=watched_state
                        )
                    }
                )

    def _modify_interlock_table(self, msg_dict):
        """Update the watched states for ScanInterlockActor - handled by the actor itself if it is
        active, otherwise just the config in redis is updated."""
        msg: ScanInterlockModifyStateTableMessage = msg_dict["data"]
        if (ats := self._actors_threads_and_stops.get(ScanInterlockActor)) is not None:
            actor, _, _ = ats
            actor._on_state_modification(msg)
        else:
            current_watched = self._current_watched_states()
            if msg.action == "add" and msg.state_name not in current_watched:
                logger.info(f"Adding {msg.state_name} to the scan interlock actor")
                current_watched[msg.state_name] = msg.status
                self._set_interlock_states_in_redis(current_watched)
            elif msg.action == "remove_all":
                self._set_interlock_states_in_redis({})
            else:
                logger.info(f"Removing {msg.state_name} from the scan interlock actor")
                current_watched.pop(msg.state_name, None)
                self._set_interlock_states_in_redis(current_watched)
            self._ping_clients("ScanInterlockActor")
