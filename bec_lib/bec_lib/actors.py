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
