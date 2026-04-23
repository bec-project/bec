from __future__ import annotations

import inspect
import time
from collections.abc import Mapping, Sequence
from inspect import Parameter, Signature
from typing import TYPE_CHECKING, Any, TypedDict

from pydantic import BaseModel
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from bec_lib import bl_states, messages
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib.bl_states import BeamlineStateConfig
    from bec_lib.client import BECClient


def build_signature_from_model(model: BaseModel, skip: set[str] | None = None) -> Signature:
    """
    Build a function signature from a Pydantic model. The parameters of the signature will
    match the fields of the model.

    Args:
        model (BaseModel): The Pydantic model to build the signature from.
        skip (set[str], optional): A set of field names to skip when building the signature. Defaults to None.
    Returns:
        Signature: The built function signature.
    """
    parameters = []
    skip = skip or set()

    for name, field in type(model).model_fields.items():
        if name in skip:
            continue
        annotation = field.annotation or inspect.Parameter.empty
        parameters.append(
            Parameter(
                name=name, kind=Parameter.KEYWORD_ONLY, default=field.default, annotation=annotation
            )
        )

    return Signature(parameters)


def _state_class_for_state_type(state_type: str) -> type[bl_states.BeamlineState]:
    """
    Resolve and validate a serialized beamline state type.

    The state type identifies the concrete runtime state class that will be
    started by the scan server.
    """
    state_class = getattr(bl_states, state_type, None)
    if (
        not inspect.isclass(state_class)
        or not issubclass(state_class, bl_states.BeamlineState)
        or inspect.isabstract(state_class)
    ):
        raise ValueError(f"State type {state_type!r} is not a concrete beamline state.")
    if getattr(state_class, "CONFIG_CLASS", None) is None:
        raise ValueError(f"State type {state_type!r} does not define a config class.")

    return state_class


class BeamlineStateGet(TypedDict):
    """
    TypedDict for the return value of the get method of a beamline state client.
    """

    status: messages.BlStateStatus
    label: str


def _add_parameter_to_tree(parent: Tree, name: str, value: Any) -> None:
    """Add a configuration value to a Rich tree without interpreting it as markup."""
    if isinstance(value, Mapping):
        branch = parent.add(Text(str(name), style="cyan"))
        if not value:
            branch.add(Text("{}", style="grey70"))
            return
        for child_name, child_value in value.items():
            _add_parameter_to_tree(branch, str(child_name), child_value)
        return

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        branch = parent.add(Text(str(name), style="cyan"))
        if not value:
            branch.add(Text("[]", style="grey70"))
            return
        for index, child_value in enumerate(value):
            _add_parameter_to_tree(branch, f"[{index}]", child_value)
        return

    line = Text()
    line.append(f"{name}: ", style="cyan")
    line.append(repr(value), style="grey70")
    parent.add(line)


def _truncate_text(value: str, max_length: int) -> str:
    """Truncate text to a deterministic maximum length."""
    if len(value) <= max_length:
        return value
    return f"{value[: max_length - 1]}…"


def _summarize_label(label: str, max_labels: int = 3, max_length: int = 64) -> str:
    """Bound the potentially large set of matching labels shown by ``show_all``."""
    labels = label.split("|")
    if len(labels) > max_labels:
        label = f"{'|'.join(labels[:max_labels])}|… (+{len(labels) - max_labels})"
    return _truncate_text(label.replace("\n", " "), max_length=max_length)


class BeamlineStateClientBase:
    """Base class for beamline state clients."""

    def __init__(self, manager: BeamlineStateManager, state: BeamlineStateConfig) -> None:
        self._manager = manager
        self._connector = manager._connector
        self._state = state
        self._skip_parameters = {"name"}

        # pylint: disable=unnecessary-lambda
        self._run = lambda **kwargs: self._run_update(**kwargs)
        self._update_signature()

    def _update_signature(self) -> None:
        # Dynamically update the signature of the update_parameters method to match the parameters of the state config
        setattr(self, "update_parameters", self._run)
        setattr(
            getattr(self, "update_parameters"),
            "__signature__",
            build_signature_from_model(self._state, skip=self._skip_parameters),
        )

    def _run_update(self, **kwargs) -> None:
        if not kwargs:
            return
        if self._skip_parameters.intersection(kwargs):
            raise ValueError(f"Invalid parameters: {self._skip_parameters.intersection(kwargs)}")
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

    def describe(self) -> None:
        """Pretty print the complete, validated configuration for this state."""
        title = Text(self._state.name, style="bold magenta")
        title.append(f" ({self._state.state_type})", style="grey70")
        tree = Tree(title)
        parameters = self._state.model_dump(exclude={"name"}, exclude_none=True)
        if (
            isinstance(self._state, bl_states.AggregatedStateConfig)
            and self._state.evaluation_method is None
        ):
            parameters = {"evaluation_method": None, **parameters}
        if not parameters:
            tree.add(Text("No parameters", style="grey70"))
        else:
            for name, value in parameters.items():
                _add_parameter_to_tree(tree, name, value)
        Console().print(tree)

    def remove(self) -> None:
        """
        Remove the current beamline state.
        """
        self._manager.delete(self._state.name)


class BeamlineStateManager:
    """Manager for beamline states."""

    def __init__(self, client: BECClient) -> None:
        self._client = client
        self._connector = client.connector
        self._states: dict[str, BeamlineStateConfig] = {}
        self._ready = False
        if msg := self._connector.get_last(MessageEndpoints.available_beamline_states()):
            self._on_state_update(msg)
        else:
            # No beamline-state stream exists yet: treat that as "ready with zero states".
            self._ready = True
        self._connector.register(
            MessageEndpoints.available_beamline_states(), cb=self._on_state_update
        )

    @property
    def ready(self) -> bool:
        """Returns true after beamline states have been loaded from Redis."""
        return self._ready

    def _on_state_update(self, msg_dict: dict, **_kwargs) -> None:
        # type: ignore ; we know it's an AvailableBeamlineStatesMessage
        msg: messages.AvailableBeamlineStatesMessage = msg_dict["data"]
        self._update_states(msg.states)  # pylint: disable=protected-access
        self._ready = True

    def _update_state(self, state: BeamlineStateConfig) -> None:
        if state.name in self._states:
            self._states[state.name] = state
            self._publish_states()
            return
        raise ValueError(f"State with name {state.name} not found")

    def _update_states(self, states: list[messages.BeamlineStateConfig]) -> None:
        incoming_states = {state.name: state for state in states}
        remove_state_names = self._states.keys() - incoming_states.keys()

        for state_name in remove_state_names:
            self._delete_state(state_name)

        for state_name, state in incoming_states.items():
            self._add_state(state)

    def _add_state(
        self, state: messages.BeamlineStateConfig | bl_states.BeamlineStateConfig
    ) -> None:
        if isinstance(state, messages.BeamlineStateConfig):
            state_class = _state_class_for_state_type(state.state_type)
            model_cls = state_class.CONFIG_CLASS
            model_instance = model_cls(**state.parameters)
        else:
            _state_class_for_state_type(state.state_type)
            model_instance = state
        instance = BeamlineStateClientBase(manager=self, state=model_instance)
        setattr(self, state.name, instance)
        self._states[state.name] = model_instance

    def _delete_state(self, state_name: str) -> None:
        if state_name in self._states:
            del self._states[state_name]
            if hasattr(self, state_name):
                delattr(self, state_name)

    def _publish_states(self) -> None:
        bl_states_container = [
            messages.BeamlineStateConfig(
                name=state.name, state_type=state.state_type, parameters=state.model_dump()
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

    def _wait_for_initial_state(self, state_name: str, timeout_s: float = 5.0) -> None:
        deadline = time.monotonic() + timeout_s
        endpoint = MessageEndpoints.beamline_state(state_name)

        while time.monotonic() < deadline:
            state_msg = self._connector.get_last(endpoint)
            if state_msg and state_msg["data"].status != "unknown":
                return
            time.sleep(0.05)

        raise TimeoutError(f"Beamline state {state_name} did not publish an initial status.")

    ##########################
    ##### Public API #########
    ##########################

    def add(self, state: bl_states.BeamlineStateConfig, skip_existing: bool = False) -> None:
        """
        Add a new beamline state to the manager.
        Args:
            state (BeamlineStateConfig): The beamline state to add.
            skip_existing (bool): If True, existing states in the manager will be skipped during loading.
        """
        if skip_existing and state.name in self._states:
            return
        self._add_state(state)
        self._publish_states()
        self._wait_for_initial_state(state.name)

    def clear_all(self) -> None:
        """
        Clear all beamline states from the manager.
        """
        for state_name in list(self._states.keys()):
            self._delete_state(state_name)
        self._publish_states()

    def delete(self, state_name: str) -> None:
        """
        Delete a beamline state from the manager.
        Args:
            state_name (str): The name of the state to delete.
        """
        if state_name in self._states:
            self._delete_state(state_name)
            self._publish_states()

    def get_status_by_name(self, name: str) -> messages.BlStateStatus | None:
        """
        Get current value of a given state, or None if it does not exist.
        Args:
            state_name (str): The name of the state for which to get the value.
        """
        if not isinstance(state := getattr(self, name, None), BeamlineStateClientBase):
            return
        return state.get()["status"]

    def show_all(self):
        """
        Pretty print all beamline states using rich.
        """

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
            state_class = _state_class_for_state_type(state.state_type)
            params = state_class.format_config_summary(state)
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
                Text(status_value, style=status_style),
                Text(_summarize_label(str(status.get("label", ""))), style=status_style),
            )

        console.print(table)
