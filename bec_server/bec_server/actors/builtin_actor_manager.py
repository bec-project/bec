from threading import Event, Thread

from bec_lib.client import BECClient, ServiceConfig
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import BuiltinActorStateChangeMessage
from bec_server.actors.actor import ActorBase
from bec_server.actors.scan_interlock import ScanInterlockActor

logger = bec_logger.logger


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
        self._actors_threads_and_stops: dict[str, tuple[ActorBase, Thread, Event]] = {}
        self._builtin_actors = {cls.__name__: cls for cls in (ScanInterlockActor,)}
        self._start_all()
        self._client.connector.register(
            MessageEndpoints.builtin_actor_notification(), cb=self._on_state_changed
        )

    def _on_state_changed(self, msg_obj: MessageObject):
        msg: BuiltinActorStateChangeMessage = msg_obj.value  # type: ignore
        logger.info(f"Received state change notification {msg.actor_name}")
        if msg.actor_name not in self._builtin_actors:
            logger.error(f"Actor {msg.actor_name} does not exist!")
            return
        if self._client.builtin_actors.check_enabled(msg.actor_name):
            self._start_actor(self._builtin_actors[msg.actor_name])
        else:
            self._stop_actor(msg.actor_name)

    def _start_all(self):
        for actor_class in self._builtin_actors.values():
            if self._client.builtin_actors.check_enabled(actor_class.__name__):
                self._start_actor(actor_class)

    def _start_actor(self, actor_class: type[ActorBase]):
        name = actor_class.__name__
        logger.info(f"Starting {name}")
        if name in self._actors_threads_and_stops:
            logger.warning(f"Actor {name} is already active!")
            return
        actor = actor_class(self._client, name=name, exec_id=name)
        t = Thread(target=actor.run)
        self._actors_threads_and_stops[name] = (actor, t, actor.stop_event)
        t.start()

    def _stop_actor(self, actor_name: str):
        logger.info(f"Stopping {actor_name}")
        if (entry := self._actors_threads_and_stops.get(actor_name)) is None:
            logger.warning(f"Actor {actor_name} is not active!")
            return
        actor, t, event = entry
        event.set()
        t.join()
        del self._actors_threads_and_stops[actor_name]
        del actor

    def shutdown(self):
        for actor in self._actors_threads_and_stops:
            self._stop_actor(actor)
        self._client.shutdown()
