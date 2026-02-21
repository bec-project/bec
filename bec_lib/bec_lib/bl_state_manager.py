from __future__ import annotations

import inspect
from inspect import Parameter, Signature
from typing import TYPE_CHECKING, TypedDict

from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

from bec_lib import bl_states, messages
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib.bl_states import BeamlineStateConfig
    from bec_lib.client import BECClient


def build_signature_from_model(model: BaseModel) -> Signature:
    """
    Build a function signature from a Pydantic model. The parameters of the signature will match the fields of the model.
    """
    parameters = []

    for name, field in model.model_fields.items():
        annotation = field.annotation or inspect.Parameter.empty
        parameters.append(
            Parameter(
                name=name, kind=Parameter.KEYWORD_ONLY, default=field.default, annotation=annotation
            )
        )

    return Signature(parameters)


class BeamlineStateGet(TypedDict):
    """
    TypedDict for the return value of the get method of a beamline state client.
    """

    status: str
    label: str


class BeamlineStateClientBase:
    """Base class for beamline state clients."""

    def __init__(self, manager: BeamlineStateManager, state: BeamlineStateConfig) -> None:
        self._manager = manager
        self._connector = manager._connector
        self._state = state

        # pylint: disable=unnecessary-lambda
        self._run = lambda **kwargs: self._run_update(**kwargs)
        self._update_signature()

    def _update_signature(self) -> None:
        # Dynamically update the signature of the update_parameters method to match the parameters of the state config
        setattr(self, "update_parameters", self._run)
        setattr(
            getattr(self, "update_parameters"),
            "__signature__",
            build_signature_from_model(self._state),
        )

    def _run_update(self, **kwargs) -> None:
        self._state = self._state.model_copy(update=kwargs)
        self._manager._update_state(self._state)  # pylint: disable=protected-access

    def get(self) -> BeamlineStateGet:
        """
        Get the current status of the beamline state. Returns a dictionary with keys "status" and "label".

        Returns:
            BeamlineStateGet: A dictionary containing the status and label of the beamline state.
        """
        msg_container: dict[str, messages.BeamlineStateMessage] = self._connector.get_last(
            MessageEndpoints.beamline_state(self._state.name)
        )
        if not msg_container:
            return {"status": "unknown", "label": "No state information available."}
        msg = msg_container["data"]
        return {"status": msg.status, "label": msg.label}

    def delete(self) -> None:
        """
        Delete the current beamline state.
        """
        self._manager.remove(self._state.name)


class BeamlineStateManager:
    """Manager for beamline states."""

    def __init__(self, client: BECClient) -> None:
        self._client = client
        self._connector = client.connector
        self._states: dict[str, BeamlineStateConfig] = {}
        self._connector.register(
            MessageEndpoints.available_beamline_states(),
            cb=self._on_state_update,
            parent=self,
            from_start=True,
        )

    @staticmethod
    def _on_state_update(msg_dict: dict, *, parent: BeamlineStateManager, **_kwargs) -> None:
        # type: ignore ; we know it's an AvailableBeamlineStatesMessage
        msg: messages.AvailableBeamlineStatesMessage = msg_dict["data"]
        parent._update_states(msg.states)  # pylint: disable=protected-access

    def _update_state(self, state: BeamlineStateConfig) -> None:
        if state.name in self._states:
            self._states[state.name] = state
            self._publish_states()
            return
        raise ValueError(f"State with name {state.name} not found")

    def _update_states(self, states: list[messages.BeamlineStateConfig]) -> None:
        remove_state_names = set(self._states) - set(state.name for state in states)

        added_state_names = set(state.name for state in states) - set(self._states)
        added_states = {state.name: state for state in states if state.name in added_state_names}

        for state_name in remove_state_names:
            if hasattr(self, state_name):
                delattr(self, state_name)
                self._states.pop(state_name, None)

        for state_name, state in added_states.items():
            state_class = getattr(bl_states, state.state_type)
            model_cls = state_class.CONFIG_CLASS
            model_instance = model_cls(**state.parameters)
            instance = BeamlineStateClientBase(manager=self, state=model_instance)
            setattr(self, state.name, instance)
            self._states[state.name] = model_instance

    def _publish_states(self) -> None:
        bl_states_container = [
            messages.BeamlineStateConfig(
                name=state.name,
                title=state.title if state.title else state.name,
                state_type=state.state_type,
                parameters=state.model_dump(),
            )
            for state in self._states.values()
        ]
        msg = messages.AvailableBeamlineStatesMessage(states=bl_states_container)
        self._connector.xadd(
            MessageEndpoints.available_beamline_states(),
            {"data": msg},
            max_size=1,
            approximate=False,
        )

    ##########################
    ##### Public API #########
    ##########################

    def add(self, state: bl_states.BeamlineStateConfig) -> None:
        """
        Add a new beamline state to the manager.
        Args:
            state (BeamlineStateConfig): The beamline state to add.
        """

        self._states[state.name] = state
        self._publish_states()

    def remove(self, state_name: str) -> None:
        """
        Remove a beamline state by name.
        Args:
            state_name (str): The name of the state to remove.
        """
        if state_name in self._states:
            del self._states[state_name]
            self._publish_states()

    def show_all(self):
        """
        Pretty print all beamline states using rich.
        """

        def _format_parameters(state_config: bl_states.BeamlineStateConfig) -> str:
            parameter_dict = state_config.model_dump(exclude={"name", "title"}, exclude_none=True)
            if not parameter_dict:
                return "-"
            return "\n".join(f"{key}={value}" for key, value in parameter_dict.items())

        def _status_style(status_value: str) -> str:
            status_styles = {"valid": "green3", "invalid": "red3", "warning": "yellow3"}
            return status_styles.get(status_value.lower(), "grey50")

        console = Console()
        table = Table(title="Beamline States", padding=(0, 1, 1, 1))
        table.add_column("Name", style="magenta", no_wrap=True)
        table.add_column("Type", style="grey70")
        table.add_column("Parameters", style="grey70")
        table.add_column("Status")
        table.add_column("Label")

        for state in self._states.values():
            params = _format_parameters(state)
            status = (
                getattr(self, state.name).get()
                if hasattr(self, state.name)
                else {"status": "unknown", "label": "No state information available."}
            )
            status_value = str(status.get("status", ""))
            status_style = _status_style(status_value)
            table.add_row(
                str(state.name),
                str(state.state_type),
                str(params),
                f"[{status_style}]{status_value}[/{status_style}]",
                f"[{status_style}]{str(status.get('label', ''))}[/{status_style}]",
            )

        console.print(table)
