from typing import TYPE_CHECKING

from bec_lib.config_values import RedisConfigValue
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import (
    BlStateStatus,
    InterlockTargetState,
    ScanInterlockModifyStateTableMessage,
    ScanInterlockTriggerSetting,
)

if TYPE_CHECKING:
    from bec_lib.client import BECClient

VAR_PREFIX = "_BuiltinActors"


class ScanInterlockHli:
    def __init__(self, client: "BECClient", parent: "BuiltinActorHli") -> None:
        self._client = client
        self._parent = parent
        self._actor_name = "ScanInterlockActor"
        self._enabled = RedisConfigValue(
            connector=self._client.connector, endpoint=MessageEndpoints.scan_interlock_enabled()
        )
        self._trigger_setting = RedisConfigValue(
            connector=self._client.connector,
            endpoint=MessageEndpoints.scan_interlock_trigger_setting(),
        )

    @property
    def enabled(self):
        return self._enabled.value

    @enabled.setter
    def enabled(self, enabled: bool):
        self._enabled.value = enabled

    @property
    def trigger_setting(self):
        return self._trigger_setting.value

    @trigger_setting.setter
    def trigger_setting(self, trigger_setting: str | ScanInterlockTriggerSetting):
        accepted_values = [str(v) for v in ScanInterlockTriggerSetting]
        if isinstance(trigger_setting, str) and trigger_setting not in accepted_values:
            raise ValueError(f"Scan interlock trigger setting must be one of {accepted_values}!")
        self._trigger_setting.value = ScanInterlockTriggerSetting(trigger_setting)

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
