from __future__ import annotations

from bec_lib import bl_conditions as bl_states
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector


class BeamlineConditionManager:
    """Manager for beamline conditions."""

    def __init__(self, connector: RedisConnector) -> None:
        self.connector = connector
        self.conditions: list[bl_states.BeamlineCondition] = []
        self.connector.register(
            MessageEndpoints.available_beamline_conditions(),
            cb=self._handle_condition_update,
            parent=self,
            from_start=True,
        )

    @staticmethod
    def _handle_condition_update(
        msg_dict: dict, *, parent: BeamlineConditionManager, **_kwargs
    ) -> None:

        msg: messages.AvailableBeamlineConditionsMessage = msg_dict["data"]  # type: ignore ; we know it's a AvailableBeamlineConditionsMessage
        parent.update_conditions(msg)

    def update_conditions(self, msg: messages.AvailableBeamlineConditionsMessage) -> None:
        """
        Update the beamline conditions based on the received update message.

        Args:
            msg (messages.AvailableBeamlineConditionsMessage): The update message containing condition updates.
        """

        # get the conditions that we need to remove
        conditions_in_msg = {cond.name for cond in msg.conditions}
        current_conditions = {cond.name for cond in self.conditions}
        conditions_to_remove = current_conditions - conditions_in_msg
        # remove conditions that are no longer needed
        for cond_name in conditions_to_remove:
            cond = next((c for c in self.conditions if c.name == cond_name), None)
            if cond:
                cond.stop()
                self.conditions.remove(cond)
        # filter out existing conditions from the message
        new_conditions = [cond for cond in msg.conditions if cond.name not in current_conditions]

        # add new conditions
        for cond in new_conditions:
            self.conditions.append(self.create_condition_from_message(cond))

    def create_condition_from_message(
        self, cond_info: messages.BeamlineConditionUpdateEntry
    ) -> bl_states.BeamlineCondition:
        """
        Create a BeamlineCondition instance from a BeamlineConditionUpdateEntry message.

        Args:
            cond_info (messages.BeamlineConditionUpdateEntry): The condition update entry message.
        Returns:
            BeamlineCondition: The created BeamlineCondition instance.
        """
        cls = getattr(bl_states, cond_info.condition_type, None)
        if cls is None:
            raise ValueError(
                f"Condition type {cond_info.condition_type} not found in beamline conditions."
            )
        condition = cls(name=cond_info.name, redis_connector=self.connector, title=cond_info.title)
        condition.configure(**cond_info.parameters)
        condition.start()
        return condition
