from typing import TYPE_CHECKING

from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import (
    BlStateStatus,
    BuiltinActorStateChangeNotification,
    InterlockTargetState,
    ScanInterlockModifyStateTableMessage,
)

if TYPE_CHECKING:
    from bec_lib.client import BECClient

VAR_PREFIX = "_BuiltinActors"


def builtin_actor_enabled_var(actor_name: str):
    return f"{VAR_PREFIX}/enabled/{actor_name}"


class ScanInterlockHli:
    def __init__(self, client: "BECClient", parent: "BuiltinActorHli") -> None:
        self._client = client
        self._parent = parent
        self._actor_name = "ScanInterlockActor"

    @property
    def enabled(self):
        return self._parent.check_enabled(self._actor_name)

    @enabled.setter
    def enabled(self, enabled: bool):
        if enabled:
            self._parent.set_enabled(self._actor_name)
        else:
            self._parent.set_disabled(self._actor_name)

    @property
    def states_watched(self) -> dict[str, InterlockTargetState]:
        """Return the table of beamline states currently watched by the scan interlock actor"""
        if msg := self._client.connector.get(MessageEndpoints.scan_interlock_states()):
            return msg.states_watched
        return {}

    def add_state_to_interlock(
        self, state_name: str, required_value: BlStateStatus | InterlockTargetState | None = None
    ):
        """
        Add a beamline state and its status to watch to the ScanInterlockActor. If the state no
        longer has one of these statuses, an interlock will be placed on the primary scan queue.
        Args:
            state_name (str): the state to watch
            required_value (Literal["valid","invalid","warning","unknown"] | list[...]):
                the accepted status or statuses. Defaults to `["valid", "warning"]`.
        """
        if required_value is None:
            required_value = ["valid", "warning"]
        elif isinstance(required_value, str):
            required_value = [required_value]
        elif isinstance(required_value, (tuple, set)):
            required_value = list(required_value)
        self._client.connector.xadd(
            MessageEndpoints.modify_interlock_table(),
            {
                "data": ScanInterlockModifyStateTableMessage(
                    action="add", state_name=state_name, status=required_value
                )
            },
        )

    def remove_state_from_interlock(self, state_name: str):
        """
        No longer watch the given state for the scan interlock.
        Args:
            state_name (str): the state to watch
        """
        self._client.connector.xadd(
            MessageEndpoints.modify_interlock_table(),
            {"data": ScanInterlockModifyStateTableMessage(action="remove", state_name=state_name)},
        )

    def clear_all(self):
        """
        Remove all beamline states from the interlock watch table
        Args:
            state_name (str): the state to watch
        """
        self._client.connector.xadd(
            MessageEndpoints.modify_interlock_table(),
            {"data": ScanInterlockModifyStateTableMessage(action="remove_all")},
        )


class BuiltinActorHli:
    def __init__(self, client: "BECClient") -> None:
        self._client = client
        self.scan_interlock = ScanInterlockHli(self._client, self)

    def _notify(self, actor_name):
        self._client.connector.send(
            MessageEndpoints.builtin_actor_update_req_notif(),
            BuiltinActorStateChangeNotification(actor_name=actor_name),
        )

    def check_enabled(self, actor_name: str):
        return bool(self._client.get_global_var(builtin_actor_enabled_var(actor_name)))

    def set_enabled(self, actor_name: str):
        self._client.set_global_var(builtin_actor_enabled_var(actor_name), True)
        self._notify(actor_name)

    def set_disabled(self, actor_name: str):
        self._client.set_global_var(builtin_actor_enabled_var(actor_name), False)
        self._notify(actor_name)
