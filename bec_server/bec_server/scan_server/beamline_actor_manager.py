from __future__ import annotations

import traceback

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import ErrorInfo
from bec_lib.redis_connector import RedisConnector

from .beamline_actors import ActorConfig, InProcessActor, ScanInterlock, ScanInterlockConfig


class BeamlineActorManager:
    """Container for beamline actor instances."""

    def __init__(self, connector: RedisConnector):
        self.connector = connector
        self.actors: dict[str, InProcessActor] = {}

        self.connector.register(
            MessageEndpoints.available_actors(),
            cb=self._available_actors_callback,
            parent=self,
            from_start=True,
        )

    @staticmethod
    def _available_actors_callback(
        msg_dict: dict[str, messages.AvailableActorsMessage],
        parent: BeamlineActorManager,
        **_kwargs,
    ):
        """Callback to update the list of available actors."""
        available_actors_msg = msg_dict["data"]
        try:
            parent.update_available_actors(available_actors_msg.actors)
        except Exception as exc:
            content = traceback.format_exc()
            info = ErrorInfo(
                exception_type=type(exc).__name__,
                error_message=content,
                compact_error_message="Error updating beamline actors.",
            )
            parent.connector.raise_alarm(severity=Alarms.WARNING, info=info)

    def update_available_actors(self, actor_configs: list[ActorConfig]):
        """Update the list of available actors based on the received message."""
        actor_type_to_actor_class: dict[str, type[InProcessActor]] = {
            ScanInterlockConfig.actor_type: ScanInterlock
        }

        requested_actor_names = {actor_config.name for actor_config in actor_configs}
        remove_actor_names = set(self.actors) - requested_actor_names

        for actor_name in remove_actor_names:
            self.remove_actor(actor_name)

        for actor_config in actor_configs:
            if actor_config.name is None:
                raise ValueError("Actor configuration missing actor name.")

            actor_class = actor_type_to_actor_class.get(actor_config.actor_type)
            if actor_class is None:
                raise ValueError(f"Unsupported actor type: {actor_config.actor_type}")

            existing_actor = self.get_actor(actor_config.name)
            if existing_actor is None or not isinstance(existing_actor, actor_class):
                if existing_actor is not None:
                    self.remove_actor(actor_config.name)
                actor_instance = actor_class(
                    name=actor_config.name, connector=self.connector
                )
                self.add_actor(actor_config.name, actor_instance)
                existing_actor = actor_instance

            current_config = (
                existing_actor.config.model_dump() if existing_actor.config else None
            )
            new_config = actor_config.model_dump()
            if current_config != new_config:
                existing_actor.config_update(actor_config)

            existing_actor.set_enabled(actor_config.enabled)

    def add_actor(self, name: str, actor: InProcessActor):
        """Add a beamline actor to the container."""
        self.actors[name] = actor

    def get_actor(self, name: str) -> InProcessActor | None:
        """Retrieve a beamline actor by name."""
        return self.actors.get(name)

    def remove_actor(self, name: str):
        """Remove a beamline actor from the container."""
        if name in self.actors:
            self.actors[name].shutdown()
            del self.actors[name]
