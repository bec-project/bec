from __future__ import annotations

from bec_lib import bl_states, messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector


class BeamlineStateManager:
    """Manager for beamline states."""

    def __init__(self, connector: RedisConnector) -> None:
        self.connector = connector
        self.states: list[bl_states.BeamlineState] = []
        self.connector.register(
            MessageEndpoints.available_beamline_states(),
            cb=self._handle_state_update,
            parent=self,
            from_start=True,
        )

    @staticmethod
    def _handle_state_update(msg_dict: dict, *, parent: BeamlineStateManager, **_kwargs) -> None:

        msg: messages.AvailableBeamlineStatesMessage = msg_dict["data"]  # type: ignore ; we know it's a AvailableBeamlineStatesMessage
        parent.update_states(msg)

    def update_states(self, msg: messages.AvailableBeamlineStatesMessage) -> None:
        """
        Update the beamline states based on the received update message.

        Args:
            msg (messages.AvailableBeamlineStatesMessage): The update message containing state updates.
        """

        # get the states that we need to remove
        states_in_msg = {state.name for state in msg.states}
        current_states = {state.name for state in self.states}
        states_to_remove = current_states - states_in_msg
        # remove states that are no longer needed
        for state_name in states_to_remove:
            state = next((s for s in self.states if s.name == state_name), None)
            if state:
                state.stop()
                self.states.remove(state)
        # filter out existing states from the message
        new_states = [state for state in msg.states if state.name not in current_states]
        # add new states
        for state in new_states:
            self.states.append(self.create_state_from_message(state))

    def create_state_from_message(
        self, state_info: messages.BeamlineStateConfig
    ) -> bl_states.BeamlineState:
        """
        Create a BeamlineState instance from a BeamlineStateConfig message.

        Args:
            state_info (messages.BeamlineStateConfig): The state config message.
        Returns:
            BeamlineState: The created BeamlineState instance.
        """
        try:
            cls = getattr(bl_states, state_info.state_type, None)
            if cls is None or not issubclass(cls, bl_states.BeamlineState):
                raise ValueError(
                    f"State type {state_info.state_type} not found in beamline states."
                )
            state = cls(
                name=state_info.name, redis_connector=self.connector, title=state_info.title
            )
            state.configure(**state_info.parameters)
            state.start()
        except Exception as exc:
            self.connector.raise_alarm(
                severity=Alarms.WARNING,
                info=messages.ErrorInfo(
                    error_message=f"Failed to create beamline state {state_info.name}: {exc}",
                    compact_error_message=f"Failed to create beamline state {state_info.name}",
                    exception_type=type(exc).__name__,
                ),
            )
        return state
