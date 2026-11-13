"""Deterministic procedural descriptions over the resolved sequence tree."""

from __future__ import annotations

import json

from .. import expressions as expr
from ..components import ComponentCall, ForEach, Repeat, ScanCall, Sequence, Set, While
from .evaluation import UNKNOWN, Scope, evaluate
from .resolver import Resolved, partial_parameters

# Structured expression variants each have a distinct rendering template.
# pylint: disable=too-many-return-statements


def _show(value, scope):
    """Render a value or expression as procedural description text.

    Args:
        value (object): Value to validate, resolve or encode.
        scope (Scope): Lexical bindings used while visiting or evaluating this component.

    Returns:
        str: Description text for the value or structured expression.
    """
    expression_model = expr.expression(value)
    if isinstance(expression_model, expr.Reference):
        return f"the current value of {expression_model.ref}"
    if isinstance(expression_model, expr.Parameter):
        resolved = scope.parameters.get(expression_model.param, UNKNOWN)
        return (
            f"parameter {expression_model.param}"
            if resolved is UNKNOWN
            else _show_literal(resolved)
        )
    if isinstance(expression_model, expr.LiteralValue):
        return _show_literal(expression_model.value)
    if isinstance(expression_model, expr.StatusField):
        return f"checked {expression_model.status_ref}.done"
    if isinstance(expression_model, expr.Not):
        return f"not ({_show(expression_model.operand, scope)})"
    if isinstance(expression_model, expr.Boolean):
        return f" {expression_model.type} ".join(
            f"({_show(v, scope)})" for v in expression_model.operands
        )
    if isinstance(expression_model, expr.Compare):
        return f"{_show(expression_model.left, scope)} {expression_model.operator} {_show(expression_model.right, scope)}"
    if isinstance(value, dict):
        return (
            "{"
            + ", ".join(f"{_show_literal(k)}: {_show(v, scope)}" for k, v in sorted(value.items()))
            + "}"
        )
    if isinstance(value, list):
        return "[" + ", ".join(_show(v, scope) for v in value) + "]"
    return _show_literal(value)


def _show_literal(value):
    """Render a literal value as deterministic JSON text.

    Args:
        value (object): Value to validate, resolve or encode.

    Returns:
        str: Deterministic JSON text, with placeholders for unknown values.
    """
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, default=lambda _value: "<dynamic value>"
    )


def _instruction(item, scope):
    """Render the instruction for one component.

    Args:
        item (Resolved): Resolved component or structured value to visit.
        scope (Scope): Lexical bindings used while visiting or evaluating this component.

    Returns:
        str: One procedural instruction.
    """
    node = item.node
    if isinstance(node, Sequence):
        return "Sequence:" if item.children else "Empty sequence."
    if isinstance(node, Repeat):
        return f"Repeat {node.count} times:"
    if isinstance(node, ForEach):
        return f"For each {node.variable} in {_show(node.values, scope)} (in order):"
    if isinstance(node, While):
        return f"While {_show(node.condition, scope)}:"
    if isinstance(node, Set):
        text = f"Set {node.device} to {_show(node.value, scope)}"
        text += " and wait for completion." if node.wait else "; continue asynchronously."
        if node.bind_status:
            text += f" Bind status as {node.bind_status}."
        return text
    if isinstance(node, ScanCall):
        return f"Run {node.name}, positional {_show(node.positional, scope)}, arguments {_show(node.arguments, scope)}; wait for completion."
    definition = item.definition
    return f"Call {definition.id} v{definition.version}: {definition.description} Parameters {_show(node.arguments, scope)}."


def _description_scope(item, scope):
    """Select the lexical scope for nested description components.

    Args:
        item (Resolved): Resolved component or structured value to visit.
        scope (Scope): Lexical bindings used while visiting or evaluating this component.

    Returns:
        Scope: Scope to use for nested instructions.
    """
    node = item.node
    if isinstance(node, ComponentCall):
        return Scope(
            parameters=partial_parameters(item.definition, evaluate(node.arguments, scope))
        )
    if isinstance(node, (ForEach, Repeat, While)):
        inner = scope.child()
        if isinstance(node, ForEach):
            inner.variables[node.variable] = UNKNOWN
        return inner
    if isinstance(node, Set) and node.bind_status:
        scope.statuses[node.bind_status] = UNKNOWN
    return scope


def describe(tree: Resolved) -> str:
    """Render ordered procedural instructions from the resolved tree.

    Args:
        tree (Resolved): Resolved component tree.

    Returns:
        str: Ordered, indented procedural description.
    """
    lines = []

    def visit(item, depth, scope, prefix=""):
        """Append an instruction and visit its nested components.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            depth (int): Indentation depth in the generated output.
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            prefix (str): Optional instruction-number prefix.
        """
        lines.append("  " * depth + prefix + _instruction(item, scope))
        inner = _description_scope(item, scope)
        for index, child in enumerate(item.children, 1):
            visit(child, depth + 1, inner, f"{index}. " if isinstance(item.node, Sequence) else "")

    visit(tree, 0, Scope())
    return "\n".join(lines)
