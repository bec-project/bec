"""Deterministic native IPython export over the resolved sequence tree."""

from __future__ import annotations

import json
import keyword
from dataclasses import dataclass, field

from pydantic import JsonValue

from bec_lib.device import Signal
from bec_lib.devicemanager import DeviceContainer

from .. import expressions as expr
from ..components import ComponentCall, ForEach, Repeat, ScanCall, Sequence, Set, While
from .evaluation import validate_json
from .resolver import Resolved


def literal(value: JsonValue) -> str:
    """Encode JSON values as safe built-in Python literals.

    Args:
        value (JsonValue): Value to validate, resolve or encode.

    Returns:
        str: Safe Python source containing only built-in JSON-compatible literals.
    """
    validate_json(value)
    return repr(json.loads(json.dumps(value, allow_nan=False)))


def _identifier(name: str) -> bool:
    """Check whether a name can be emitted as a Python identifier.

    Args:
        name (str): Device, signal or scan name to look up.

    Returns:
        bool: True when the name is an identifier and is not a Python keyword.
    """
    return name.isidentifier() and not keyword.iskeyword(name)


@dataclass
class _Names:
    """Args:
    variables (dict[str, str]): Loop-variable names mapped to generated Python names.
    parameters (str | None): Generated name of the callee parameter mapping, or None.
    statuses (dict[str, str]): Visible status handles or symbolic status names.

    Args:
        variables (dict[str, str]): Loop-variable names mapped to generated Python names.
        parameters (str | None): Generated name of the callee parameter mapping, or None.
        statuses (dict[str, str]): Visible status handles or symbolic status names.
    """

    variables: dict[str, str] = field(default_factory=dict)
    parameters: str | None = None
    statuses: dict[str, str] = field(default_factory=dict)

    def child(self) -> _Names:
        """Copy lexical names for an isolated loop body.

        Returns:
            _Names: Independent symbolic bindings for a nested body.
        """
        return _Names(self.variables.copy(), self.parameters, self.statuses.copy())


# These visitors explicitly cover every discriminated node/expression variant.
# pylint: disable=too-many-statements,too-many-branches,too-many-locals,too-many-return-statements
def export_python(tree: Resolved, device_manager=None) -> str:
    """Expand a validated tree into native IPython calls and scoped helper functions.

    Live proxy metadata selects signal set/wait versus device umv/mv. Offline exports
    use the same small adapter for set targets whose kind cannot be known statically.

    Args:
        tree (Resolved): Resolved component tree.
        device_manager (DeviceManagerBase | None): Optional live proxy manager for resolving
            device names.

    Returns:
        str: Statements for the existing BEC IPython namespace.
    """
    lines = []
    imports = set()
    counter = 0
    used_names = {"bec", "dev", "scans"}
    async_operations = False

    def emit(text, depth):
        """Append an indented line of Python source.

        Args:
            text (str): Source line to append to the generated output.
            depth (int): Indentation depth in the generated output.
        """
        lines.append("    " * depth + text)

    def value(item, names):
        """Render a literal or expression as Python source.

        Args:
            item (JsonValue): Literal or structured expression to emit.
            names (_Names): Symbolic Python names visible in the current scope.

        Returns:
            str: Python source for the value or expression.
        """
        expression = expr.expression(item)
        if isinstance(expression, expr.Reference):
            return names.variables[expression.ref]
        if isinstance(expression, expr.Parameter):
            return f"{names.parameters}[{literal(expression.param)}]"
        if isinstance(expression, expr.LiteralValue):
            return literal(expression.value)
        if isinstance(expression, expr.StatusField):
            imports.add("from bec_lib.scan_sequencer import BECAdapter")
            return f"_adapter.done({names.statuses[expression.status_ref]})"
        if isinstance(expression, expr.Not):
            imports.add("from bec_lib.scan_sequencer.core.evaluation import boolean")
            return f"(not boolean({value(expression.operand, names)}))"
        if isinstance(expression, expr.Boolean):
            imports.add("from bec_lib.scan_sequencer.core.evaluation import boolean")
            operands = [f"boolean({value(v, names)})" for v in expression.operands]
            return (
                "(" + f" {expression.type} ".join(operands) + ")"
                if operands
                else literal(expression.type == "and")
            )
        if isinstance(expression, expr.Compare):
            operator = {"eq": "==", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}[
                expression.operator
            ]
            return f"({value(expression.left, names)} {operator} {value(expression.right, names)})"
        if isinstance(item, dict):
            return (
                "{" + ", ".join(f"{literal(k)}: {value(v, names)}" for k, v in item.items()) + "}"
            )
        if isinstance(item, list):
            return "[" + ", ".join(value(v, names) for v in item) + "]"
        return literal(item)

    def target(name):
        """Render a device or signal lookup as Python source.

        Args:
            name (str): Device, signal or scan name to look up.

        Returns:
            str: Python source resolving the dotted target name.
        """
        parts = name.split(".")
        if all(_identifier(part) and not part.startswith("_") for part in parts) and not hasattr(
            DeviceContainer, parts[0]
        ):
            return "dev." + name
        return f"dev[{literal(name)}]"

    def uses_reference(item, name):
        expression = expr.expression(item)
        if isinstance(expression, expr.Reference):
            return expression.ref == name
        if isinstance(expression, expr.LiteralValue):
            return False
        if isinstance(item, dict):
            return any(uses_reference(child, name) for child in item.values())
        if isinstance(item, list):
            return any(uses_reference(child, name) for child in item)
        return False

    def uses_variable(item, name):
        node = item.node
        values = node.model_dump(mode="json", exclude={"id", "type", "body", "children"})
        if uses_reference(values, name):
            return True
        # Component calls bind their arguments in the caller, but isolate their bodies.
        if isinstance(node, ComponentCall) or (isinstance(node, ForEach) and node.variable == name):
            return False
        return any(uses_variable(child, name) for child in item.children)

    def visit(item, depth, names):
        """Emit Python statements for a component and its nested components.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            depth (int): Indentation depth in the generated output.
            names (_Names): Symbolic Python names visible in the current scope.
        """
        nonlocal counter, async_operations
        node = item.node
        index = counter
        counter += 1
        if isinstance(node, Sequence):
            if not item.children:
                emit("pass", depth)
            for child in item.children:
                visit(child, depth, names)
        elif isinstance(node, Repeat):
            emit(f"for _ in range({node.count}):", depth)
            visit(item.children[0], depth + 1, names.child())
        elif isinstance(node, ForEach):
            inner = names.child()
            variable = node.variable if uses_variable(item.children[0], node.variable) else "_"
            if variable != "_" and (
                not _identifier(variable) or variable in used_names or variable.startswith("_")
            ):
                variable = f"_value_{index}"
            used_names.add(variable)
            inner.variables[node.variable] = variable
            emit(f"for {variable} in {value(node.values, names)}:", depth)
            visit(item.children[0], depth + 1, inner)
        elif isinstance(node, While):
            imports.add("from bec_lib.scan_sequencer.core.evaluation import boolean")
            emit(f"while boolean({value(node.condition, names)}):", depth)
            visit(item.children[0], depth + 1, names.child())
        elif isinstance(node, ComponentCall):
            imports.add("from bec_lib.scan_sequencer.core.resolver import bind_parameters")
            parameter = f"_parameters_{index}"
            emit(f"def _component_{index}({parameter}):", depth)
            visit(item.children[0], depth + 1, _Names(parameters=parameter))
            parameters = literal(item.definition.parameters)
            emit(
                f"_component_{index}(bind_parameters({parameters}, "
                f"{value(node.arguments, names)}))",
                depth,
            )
        elif isinstance(node, ScanCall):
            function = (
                f"scans.{node.name}"
                if _identifier(node.name)
                else f"getattr(scans, {literal(node.name)})"
            )
            arguments = [value(v, names) for v in node.positional]
            if isinstance(node.arguments, dict) and expr.expression(node.arguments) is None:
                if all(_identifier(k) for k in node.arguments):
                    arguments.extend(f"{k}={value(v, names)}" for k, v in node.arguments.items())
                else:
                    arguments.append(f"**{value(node.arguments, names)}")
            else:
                arguments.append(f"**{value(node.arguments, names)}")
            emit(f"{function}({', '.join(arguments)})", depth)
        elif isinstance(node, Set):
            proxy = device_manager.devices[node.device] if device_manager is not None else None
            destination, resolved_value = target(node.device), value(node.value, names)
            status = f"_status_{index}"
            if proxy is None:
                imports.add("from bec_lib.scan_sequencer import BECAdapter")
                operation = (
                    f"_adapter.set({literal(node.device)}, {resolved_value}, wait={node.wait})"
                )
            elif isinstance(proxy, Signal):
                operation = f"{destination}.set({resolved_value})" + (
                    ".wait()" if node.wait else ""
                )
            else:
                function = "umv" if node.wait else "mv"
                operation = f"scans.{function}({destination}, {resolved_value}, relative=False)"
            if not node.wait:
                async_operations = True
                emit(f"{status} = {operation}", depth)
                emit(f"_pending.append({status})", depth)
            else:
                emit(operation, depth)
                if node.bind_status:
                    # A successfully returned blocking call is already checked and complete.
                    emit(f"{status} = None", depth)
            if node.bind_status:
                names.statuses[node.bind_status] = status
        else:
            raise ValueError(f"Node {node.id}: unsupported Python export")

    visit(tree, 0, _Names())
    if async_operations:
        # Drain on success, matching the interpreter; cancel supported handles on errors.
        imports.update(
            [
                "from collections import deque",
                "from time import sleep",
                "from bec_lib.scan_sequencer import BECAdapter",
            ]
        )
        lines = ["_pending = deque()", "try:"] + ["    " + line for line in lines]
        lines += [
            "    while _pending:",
            "        _status = _pending[0]",
            "        while not _adapter.done(_status):",
            "            sleep(0.02)",
            "        _pending.popleft()",
            "except BaseException:",
            "    for _status in _pending:",
            "        try:",
            "            _adapter.cancel(_status)",
            "        except Exception:",
            "            pass  # Preserve the original failure during cleanup.",
            "    raise",
        ]
    if "from bec_lib.scan_sequencer import BECAdapter" in imports:
        lines.insert(0, "_adapter = BECAdapter(bec)")
    return "\n".join(sorted(imports) + ([""] if imports else []) + lines) + "\n"
