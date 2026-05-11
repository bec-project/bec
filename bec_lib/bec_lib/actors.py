"""Definitions and protocols for the classes in bec_server.actors and in plugins."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from typing_extensions import TypeAliasType  # only for 3.11

from bec_lib.client import BECClient


class ConditionCombination(StrEnum):
    Any = "Any"
    All = "All"


class ActorCondition(Protocol):
    def __call__(self, client: BECClient) -> Any:
        """A callable which returns True if the condition is met and False if it is not."""


@dataclass
class ActorConditionSet:
    """
    An actor condition combines a set of conditions to act as one. The combination mode defines
    how they are combined. For example:
        ```
        ActorConditionSet(
            conditions = {lambda client: True, lambda client: False},
            combination_mode = ConditionCombination.Any
        )
        ```
    would evaluate to `True` when called, while with `ConditionCombination.All` it would be `False`.
    """

    conditions: set[ActorCondition]
    combination_mode: ConditionCombination

    def __call__(self, client: BECClient) -> bool:
        if self.combination_mode == ConditionCombination.Any:
            return any(condition(client) for condition in self.conditions)
        return all(condition(client) for condition in self.conditions)


class ActorAction(Protocol):
    def __call__(self, client: BECClient) -> None:
        """A callable which an actor calls if it has met the associated condition"""


ActorActionTable = TypeAliasType(
    "ActorActionTable", dict[ActorConditionSet | ActorCondition, ActorAction]
)
"""Mapping of action conditions to their actions. Keys are condition callables
which evaluate to booleans, or combinations of such callables. Values are
Actions, callables which accept a BECClient as their only argument, and specify
what to do if a condition returns `True`."""
