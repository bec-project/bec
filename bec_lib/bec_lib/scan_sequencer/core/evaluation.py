"""Scoped expression evaluation and checked JSON/boolean values."""

from __future__ import annotations

import json
import operator
from dataclasses import dataclass, field
from typing import Any, Callable

from pydantic import JsonValue, TypeAdapter

from ..expressions import (
    Boolean,
    Compare,
    LiteralValue,
    Not,
    Parameter,
    Reference,
    StatusField,
    expression,
)


def validate_json(value: JsonValue) -> None:
    """Require portable JSON literals, including finite numbers and string keys.

    Args:
        value (JsonValue): Value to validate, resolve or encode.
    """
    TypeAdapter(JsonValue).validate_python(value, strict=True)
    json.dumps(value, allow_nan=False)


def boolean(value: object) -> bool:
    """Conditions never silently coerce values to truthiness.

    Args:
        value (object): Value to validate, resolve or encode.

    Returns:
        bool: The validated boolean value without truthiness coercion.
    """
    if not isinstance(value, bool):
        raise ValueError("Condition must resolve to a boolean")
    return value


UNKNOWN = object()
OPERATORS = {
    "eq": operator.eq,
    "ne": operator.ne,
    "lt": operator.lt,
    "le": operator.le,
    "gt": operator.gt,
    "ge": operator.ge,
}


@dataclass
class Scope:
    """Separate variable, parameter and status namespaces.

    Args:
        variables (dict): Visible loop-variable bindings.
        parameters (dict): Visible reusable component parameter bindings.
        statuses (dict): Visible status handles or symbolic status names.
    """

    variables: dict = field(default_factory=dict)
    parameters: dict = field(default_factory=dict)
    statuses: dict = field(default_factory=dict)

    def child(self) -> Scope:
        """Copy lexical bindings; inner writes cannot escape.

        Returns:
            Scope: A child scope with independent binding mappings.
        """
        return Scope(self.variables.copy(), self.parameters.copy(), self.statuses.copy())


# Structured expression variants each have their own return semantics.
# pylint: disable=too-many-return-statements,too-many-branches
def evaluate(
    value: JsonValue, scope: Scope, status_done: Callable[[object], bool] | None = None
) -> Any:
    """Resolve expressions without reinterpreting values obtained from bindings.

    Args:
        value (JsonValue): Value to validate, resolve or encode.
        scope (Scope): Lexical bindings used while visiting or evaluating this component.
        status_done (Callable[[object], bool] | None): Optional checked status observer; absent
            during static evaluation.

    Returns:
        Any: Resolved data, or UNKNOWN for values unavailable during static evaluation.
    """
    expr = expression(value)
    if isinstance(expr, LiteralValue):
        return expr.value
    if isinstance(expr, (Reference, Parameter)):
        name = expr.ref if isinstance(expr, Reference) else expr.param
        bindings = scope.variables if isinstance(expr, Reference) else scope.parameters
        if name not in bindings:
            raise ValueError(f"Undefined {type(expr).__name__}: {name}")
        return bindings[name]
    if isinstance(expr, StatusField):
        if expr.status_ref not in scope.statuses:
            raise ValueError(f"Status binding is not guaranteed: {expr.status_ref}")
        return status_done(scope.statuses[expr.status_ref]) if status_done else UNKNOWN
    if isinstance(expr, Not):
        result = evaluate(expr.operand, scope, status_done)
        return UNKNOWN if result is UNKNOWN else not boolean(result)
    if isinstance(expr, Boolean):
        # Validate every operand statically, even if runtime short circuiting skips it.
        results = []
        for operand in expr.operands:
            result = evaluate(operand, scope, status_done)
            if result is not UNKNOWN:
                result = boolean(result)
            results.append(result)
            if status_done and result is not UNKNOWN:
                if expr.type == "and" and not result:
                    return False
                if expr.type == "or" and result:
                    return True
        if any(result is UNKNOWN for result in results):
            return UNKNOWN
        return all(results) if expr.type == "and" else any(results)
    if isinstance(expr, Compare):
        left = evaluate(expr.left, scope, status_done)
        right = evaluate(expr.right, scope, status_done)
        return (
            UNKNOWN
            if left is UNKNOWN or right is UNKNOWN
            else OPERATORS[expr.operator](left, right)
        )
    if isinstance(value, list):
        return [evaluate(item, scope, status_done) for item in value]
    if isinstance(value, dict):
        return {key: evaluate(item, scope, status_done) for key, item in value.items()}
    return value


def known(value: Any) -> bool:
    """Whether all parts of a statically resolved value are known.

    Args:
        value (Any): Value to validate, resolve or encode.

    Returns:
        bool: True when the value contains no statically unknown parts.
    """
    if value is UNKNOWN:
        return False
    if isinstance(value, dict):
        return all(known(item) for item in value.values())
    if isinstance(value, list):
        return all(known(item) for item in value)
    return True
