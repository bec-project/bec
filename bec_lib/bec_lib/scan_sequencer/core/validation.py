"""Static lexical validation and metadata checks before execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from bec_lib.bec_errors import DeviceConfigError
from bec_lib.signature_serializer import dict_to_signature

from .catalog import ScanCatalog
from .evaluation import UNKNOWN, Scope, boolean, evaluate, known
from .resolver import Resolved, partial_parameters

if TYPE_CHECKING:
    from bec_lib.devicemanager import DeviceManagerBase


def resolve_set_target(device_manager: DeviceManagerBase, name: str) -> object:
    """Resolve a device or signal using DeviceContainer's dotted-name lookup.

    Args:
        device_manager (DeviceManagerBase): Live device proxy manager.
        name (str): Device, signal or scan name to look up.

    Returns:
        object: Settable device or signal proxy.
    """
    try:
        target = device_manager.devices[name]
    except (KeyError, AttributeError, DeviceConfigError) as exc:
        raise ValueError(f"Unknown device or signal: {name}") from exc
    if not callable(getattr(target, "set", None)):
        raise ValueError(f"Non-settable device or signal: {name}")
    if getattr(target, "read_only", False):
        raise ValueError(f"Read-only device or signal: {name}")
    return target


# The visitor has one public entry point; component handlers are internal.
# pylint: disable=too-few-public-methods
class _Validator:
    """Component visitors implementing lexical and metadata validation.

    Args:
        catalog (ScanCatalog): Catalog used for advertised scan validation.
        device_manager (DeviceManagerBase | None): Optional live device proxy manager.
    """

    def __init__(self, catalog, device_manager):
        """Initialize _Validator parameters.

        Args:
            catalog (ScanCatalog): Catalog used for advertised scan validation.
            device_manager (DeviceManagerBase | None): Optional live device proxy manager.
        """
        self.catalog = catalog
        self.device_manager = device_manager

    def visit(self, item, scope, path):
        """Validate one component and recursively visit its resolved children.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            path (tuple[str, ...]): Node and call/iteration path used for contextual errors.
        """
        node = item.node
        path = path + (node.id,)
        try:
            getattr(self, f"_{node.type}")(item, scope, path)
        except Exception as exc:
            if isinstance(exc, ValueError) and str(exc).startswith("Node "):
                raise
            raise ValueError(f"Node {'/'.join(path)}: {exc}") from exc

    def _sequence(self, item, scope, path):
        """Validate children in order within the current scope.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            path (tuple[str, ...]): Node and call/iteration path used for contextual errors.
        """
        for child in item.children:
            self.visit(child, scope, path)

    def _repeat(self, item, scope, path):
        """Validate the repeated body in a child scope.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            path (tuple[str, ...]): Node and call/iteration path used for contextual errors.
        """
        self.visit(item.children[0], scope.child(), path)

    def _for_each(self, item, scope, path):
        """Validate the body for every supplied value in a child scope.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            path (tuple[str, ...]): Node and call/iteration path used for contextual errors.
        """
        node = item.node
        values = [evaluate(value, scope) for value in node.values]
        for value in values or [UNKNOWN]:
            inner = scope.child()
            inner.variables[node.variable] = value
            self.visit(item.children[0], inner, path)

    def _while(self, item, scope, path):
        """Validate the condition and the body in a child scope.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            path (tuple[str, ...]): Node and call/iteration path used for contextual errors.
        """
        condition = evaluate(item.node.condition, scope)
        if condition is not UNKNOWN:
            boolean(condition)
        self.visit(item.children[0], scope.child(), path)

    def _component_call(self, item, scope, path):
        """Validate parameters and enter the reusable definition scope.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            path (tuple[str, ...]): Node and call/iteration path used for contextual errors.
        """
        params = partial_parameters(item.definition, evaluate(item.node.arguments, scope))
        self.visit(item.children[0], Scope(parameters=params), path)

    def _scan_call(self, item, scope, _path):
        """Validate all statically available scan arguments.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            _path (tuple[str, ...]): Current contextual node path.
        """
        node = item.node
        info = self.catalog.info(node.name)
        args = evaluate(node.positional, scope)
        kwargs = evaluate(node.arguments, scope)
        if kwargs is not UNKNOWN and not isinstance(kwargs, dict):
            raise ValueError("Scan arguments must resolve to a mapping")
        if known(args) and known(kwargs):
            self.catalog.validate(node.name, args, kwargs)
        elif isinstance(kwargs, dict) and info.get("signature"):
            signature = dict_to_signature(info["signature"])
            if info.get("arg_bundle_size", {}).get("bundle", 0):
                signature.bind(**kwargs)
            else:
                signature.bind(*args, **kwargs)

    def _set(self, item, scope, _path):
        """Validate the target, value and optional status binding.

        Args:
            item (Resolved): Resolved component or structured value to visit.
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            _path (tuple[str, ...]): Current contextual node path.
        """
        node = item.node
        evaluate(node.value, scope)
        if self.device_manager is not None:
            resolve_set_target(self.device_manager, node.device)
        if node.bind_status:
            if node.bind_status in scope.statuses:
                raise ValueError(f"Status binding already exists: {node.bind_status}")
            scope.statuses[node.bind_status] = UNKNOWN


def validate(
    tree: Resolved, catalog: ScanCatalog, device_manager: DeviceManagerBase | None = None
) -> None:
    """Validate every body, including unreachable empty-loop bodies, without running it.

    Args:
        tree (Resolved): Resolved component tree.
        catalog (ScanCatalog): Catalog used for advertised scan validation.
        device_manager (DeviceManagerBase | None): Optional live device proxy manager.
    """
    _Validator(catalog, device_manager).visit(tree, Scope(), ())
