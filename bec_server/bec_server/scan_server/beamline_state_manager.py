from __future__ import annotations

import traceback

from bec_lib import bl_states, messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import ErrorInfo
from bec_lib.redis_connector import RedisConnector


class BeamlineStateManager:
    """Manager for beamline states."""

    def __init__(self, connector: RedisConnector) -> None:
        self.connector = connector
        self._states: dict[str, bl_states.BeamlineState] = {}
        self.connector.register(
            MessageEndpoints.available_beamline_states(),
            cb=self._handle_state_update,
            parent=self,
            from_start=True,
        )

    @staticmethod
    def _handle_state_update(msg_dict: dict, *, parent: BeamlineStateManager, **_kwargs) -> None:

        msg: messages.AvailableBeamlineStatesMessage = msg_dict["data"]  # type: ignore ; we know it's a AvailableBeamlineStatesMessage
        try:
            parent.update_states(msg)
        except Exception as exc:
            content = traceback.format_exc()
            info = ErrorInfo(
                exception_type=type(exc).__name__,
                error_message=content,
                compact_error_message="Error updating beamline states.",
            )
            parent.connector.raise_alarm(severity=Alarms.WARNING, info=info)

    def update_states(self, msg: messages.AvailableBeamlineStatesMessage) -> None:
        """
        Update the beamline states based on the received update message.

        Args:
            msg (messages.AvailableBeamlineStatesMessage): The update message containing state updates.
        """

        # get the states that we need to remove
        remove_state_names = set(self._states) - set(state.name for state in msg.states)

        added_state_names = set(state.name for state in msg.states) - set(self._states)
        added_states = {
            state.name: state for state in msg.states if state.name in added_state_names
        }

        for state_name in remove_state_names:
            if hasattr(self, state_name):
                delattr(self, state_name)
            self._states.pop(state_name, None)

        for state_name, state in added_states.items():
            state_class = getattr(bl_states, state.state_type)
            if not issubclass(state_class, bl_states.BeamlineState):
                raise ValueError(f"State type {state.state_type} not found in beamline states.")
            model_cls = state_class.CONFIG_CLASS
            model_instance = model_cls(**state.parameters)
            state_instance = state_class(config=model_instance, redis_connector=self.connector)
            state_instance.start()
            self._states[state.name] = state_instance

        # Check if the config has changed for existing states and update them if needed
        for state_msg in msg.states:
            state = self._states.get(state_msg.name)
            if state is None:
                continue
            if state.config.model_dump() != state_msg.parameters:
                # The config has changed, we need to update the state
                state.update_parameters(**state_msg.parameters)
                state.restart()
