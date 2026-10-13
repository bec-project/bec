"""Serializable expressions used in component values, arguments and conditions.

Expressions describe references and logical operations without executing Python source.
Their models define the accepted JSON shapes; core evaluation resolves them against
the current variable, parameter and status scopes. Logical expressions can be nested.
"""

from __future__ import annotations

from typing import Literal

from pydantic import JsonValue

from .components import Model


class Reference(Model):
    """An expression that references a visible loop variable by name.

    It resolves to the current value bound by a ForEach component. References
    can appear in component values or nested argument mappings. The binding is
    visible within its loop body and does not escape that scope.

    Args:
        ref (str): Name of a visible loop variable.
    """

    ref: str


class Parameter(Model):
    """An expression that references a reusable component parameter by name.

    It resolves to a validated argument or schema default in the current
    definition's parameter scope. A ComponentCall resolves its arguments in the
    caller's scope before binding these parameters in the callee's isolated scope.

    Args:
        param (str): Name of a parameter in the current reusable component scope.
    """

    param: str


class LiteralValue(Model):
    """An expression wrapper that holds literal data.

    Its value is returned unchanged. Nested dictionaries are not interpreted as
    references or logical expressions, even when their shape matches an expression.
    Use this wrapper when supplying expression-shaped data as a literal value.

    Args:
        type (Literal['literal']): Expression discriminator.
        value (JsonValue): Literal data returned unchanged without interpreting nested
            expression shapes.
    """

    type: Literal["literal"]
    value: JsonValue


class StatusField(Model):
    """An expression that observes a field of a bound runtime status.

    It holds the status binding name and the field to observe. The status must
    be visible in the current scope. Only done is supported: it is false while
    the operation runs and true after successful completion. Failed completion
    raises an execution error instead of producing successful termination.

    Args:
        type (Literal['status_field']): Expression discriminator.
        status_ref (str): Name of a visible runtime status binding.
        field (Literal['done']): Checked status field to observe. Only done is supported.
    """

    type: Literal["status_field"]
    status_ref: str
    field: Literal["done"]


class Not(Model):
    """An expression that holds and negates one boolean operand.

    The operand can be a boolean literal or a nested expression. It is resolved
    before negation and must produce a boolean; other values are not converted
    through Python truthiness. A Not expression can be nested in other conditions.

    Args:
        type (Literal['not']): Expression discriminator.
        operand (JsonValue): Structured boolean expression to negate.
    """

    type: Literal["not"]
    operand: JsonValue


class Boolean(Model):
    """An expression that combines an ordered list of boolean operands.

    The and variant is true when all operands are true; the or variant is true
    when any operand is true. Operands can contain nested expressions and must
    resolve to booleans. Runtime evaluation stops once the result is determined,
    while static validation checks every operand. An empty and is true; an empty
    or is false.

    Args:
        type (Literal['and', 'or']): Expression discriminator.
        operands (list[JsonValue]): Structured boolean expressions evaluated in order.
    """

    type: Literal["and", "or"]
    operands: list[JsonValue]


class Compare(Model):
    """An expression that holds two operands and a comparison operator.

    Each operand can be literal data or a nested expression. Both operands are
    resolved before comparison. Equality and ordering comparisons produce a
    boolean that can be used by While or nested logical expressions. Operand
    values must support the selected comparison.

    Args:
        type (Literal['compare']): Expression discriminator.
        operator (Literal['eq', 'ne', 'lt', 'le', 'gt', 'ge']): Equality, inequality,
            less-than, less-than-or-equal, greater-than or greater-than-or-equal.
        left (JsonValue): Literal or expression on the left of the comparison.
        right (JsonValue): Literal or expression on the right of the comparison.
    """

    type: Literal["compare"]
    operator: Literal["eq", "ne", "lt", "le", "gt", "ge"]
    left: JsonValue
    right: JsonValue


_EXPRESSIONS = {
    "literal": LiteralValue,
    "status_field": StatusField,
    "not": Not,
    "and": Boolean,
    "or": Boolean,
    "compare": Compare,
}


def expression(value: JsonValue) -> Model | None:
    """Parse a supported expression shape into its typed model.

    Reference and Parameter require exact single-key mappings. Other expression
    models are selected by their reserved type discriminator. Parsing validates
    the shape without resolving bindings or observing statuses. Literal data is
    left for the caller to handle, and malformed reserved expressions are rejected.

    Args:
        value (JsonValue): JSON-compatible value to inspect for a reserved expression shape.

    Returns:
        Model | None: Parsed expression model, or None when the value is literal data.

    Raises:
        ValidationError: A reserved expression has malformed fields or unsupported values.
    """
    if not isinstance(value, dict):
        return None
    if set(value) == {"ref"}:
        return Reference.model_validate(value)
    if set(value) == {"param"}:
        return Parameter.model_validate(value)
    kind = value.get("type")
    if isinstance(kind, str) and kind in _EXPRESSIONS:
        return _EXPRESSIONS[kind].model_validate(value)
    return None
