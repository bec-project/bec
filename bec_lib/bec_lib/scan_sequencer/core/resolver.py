"""One transitive resolver shared by validation, interpretation and exports."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

import fastjsonschema

from ..components import Component, ComponentCall, Definition, ForEach, Repeat, Sequence, While
from .evaluation import UNKNOWN, known, validate_json


@dataclass(frozen=True)
class Resolved:
    """A resolved tree retaining call boundaries and local identities.

    Args:
        node (Component): Original component represented by this resolved tree.
        children (tuple[Resolved, ...]): Child components to execute in order.
        definition (Definition | None): Resolved reusable definition for a component call, or
            None.
    """

    node: Component
    children: tuple[Resolved, ...] = ()
    definition: Definition | None = None


class DefinitionLibrary:
    """Caller-supplied definitions indexed by ID and version.

    Args:
        definitions (list[Definition | dict] | None): Supplied reusable definitions. Defaults to
            no definitions.
    """

    def __init__(self, definitions: list[Definition | dict] | None = None) -> None:
        """Initialize DefinitionLibrary parameters.

        Args:
            definitions (list[Definition | dict] | None): Supplied reusable definitions. Defaults to
                no definitions.
        """
        self._definitions: dict[tuple[str, int], Definition] = {}
        for definition in definitions or []:
            self.add(Definition.model_validate(definition))

    def add(self, definition: Definition) -> None:
        """Register a definition under its declared ID and version.

        Args:
            definition (Definition): Reusable definition to register or validate.
        """
        key = (definition.id, definition.version)
        if key in self._definitions:
            raise ValueError(f"Definition version already exists: {key}")
        if definition.parameters.get("type") != "object":
            raise ValueError("Component parameter schemas must describe objects")
        validate_json(definition.model_dump())
        _validate_schema(definition.parameters)
        fastjsonschema.compile(definition.parameters)
        self._definitions[key] = definition

    def get(self, key: tuple[str, int]) -> Definition:
        """Look up a registered definition without copying it.

        Args:
            key (tuple[str, int]): Reusable definition ID and exact version.

        Returns:
            Definition: The registered definition object.
        """
        if key not in self._definitions:
            raise ValueError(f"Missing definition version: {key}")
        return self._definitions[key]

    def resolve(self, root: Component) -> tuple[Resolved, list[Definition]]:
        """Reject cycles and duplicate local IDs, resolving the full dependency closure.

        Args:
            root (Component): Root component whose transitive definition references are resolved.

        Returns:
            tuple[Resolved, list[Definition]]: Resolved tree and deterministically ordered transitive
                definitions.
        """
        closure: dict[tuple[str, int], Definition] = {}

        def visit(node, ids, stack):
            """Resolve one component while tracking local IDs and dependencies.

            Args:
                node (Component): Component to resolve.
                ids (set[str]): Node IDs already seen in the current document or definition.
                stack (tuple[tuple[str, int], ...]): Active reusable definition dependencies for cycle
                    detection.

            Returns:
                Resolved: Resolved component with its call boundary and nested children.
            """
            if node.id in ids:
                raise ValueError(f"Duplicate node ID: {node.id}")
            ids.add(node.id)
            if isinstance(node, ComponentCall):
                key = (node.definition.id, node.definition.version)
                if key in stack:
                    raise ValueError(f"Definition dependency cycle: {stack + (key,)}")
                definition = self.get(key)
                closure[key] = definition
                body = visit(definition.body, set(), stack + (key,))
                return Resolved(node, (body,), definition)
            if isinstance(node, Sequence):
                children = tuple(visit(child, ids, stack) for child in node.children)
            elif isinstance(node, (Repeat, ForEach, While)):
                children = (visit(node.body, ids, stack),)
            else:
                children = ()
            return Resolved(node, children)

        tree = visit(root, set(), ())
        return tree, [closure[key] for key in sorted(closure)]


def bind_parameters(parameters: dict, arguments: dict) -> dict:
    """Validate callee arguments and insert schema defaults into a fresh mapping.

    Args:
        parameters (dict): JSON Schema for reusable component parameters.
        arguments (dict): Resolved keyword arguments.

    Returns:
        dict: Validated callee parameters with schema defaults applied.
    """
    if not isinstance(arguments, dict):
        raise ValueError("Component arguments must resolve to a mapping")
    return fastjsonschema.compile(parameters)(deepcopy(arguments))


def _schema_children(schema: dict):
    """Yield nested schemas supported by parameter validation.

    Args:
        schema (dict): JSON Schema object to inspect.

    Yields:
        dict: Each nested parameter schema.
    """
    for key in ("properties", "patternProperties", "definitions", "$defs", "dependentSchemas"):
        yield from (child for child in schema.get(key, {}).values() if isinstance(child, dict))
    for key in (
        "additionalProperties",
        "additionalItems",
        "items",
        "contains",
        "not",
        "if",
        "then",
        "else",
        "propertyNames",
    ):
        value = schema.get(key)
        if isinstance(value, dict):
            yield value
        elif isinstance(value, list):
            yield from (child for child in value if isinstance(child, dict))
    for key in ("allOf", "anyOf", "oneOf", "prefixItems"):
        yield from (child for child in schema.get(key, []) if isinstance(child, dict))


def _validate_schema(schema: dict, root: dict | None = None) -> None:
    """Check defaults and allow only local schema references, without network access.

    Args:
        schema (dict): JSON Schema object to inspect.
        root (dict | None): Root schema used for local references. Defaults to schema.
    """
    root = schema if root is None else root
    reference = schema.get("$ref")
    if reference is not None and not reference.startswith("#"):
        raise ValueError("Component schemas only support local $ref references")
    if "default" in schema:
        context = {key: root[key] for key in ("definitions", "$defs") if key in root}
        fastjsonschema.compile({**context, **schema})(deepcopy(schema["default"]))
    for child in _schema_children(schema):
        _validate_schema(child, root)


def partial_parameters(definition: Definition, args: object) -> dict:
    """Validate known inputs/defaults and defer unknown constraints until runtime.

    Args:
        definition (Definition): Reusable definition to register or validate.
        args (object): Resolved arguments, possibly containing statically unknown values.

    Returns:
        dict: Known parameter values and placeholders for unknown values.
    """
    if known(args):
        return bind_parameters(definition.parameters, args)
    schema = definition.parameters
    properties = schema.get("properties", {})
    if args is UNKNOWN:
        return {name: prop.get("default", UNKNOWN) for name, prop in properties.items()}
    if not isinstance(args, dict):
        raise ValueError("Component arguments must resolve to a mapping")
    missing = set(schema.get("required", [])) - set(args)
    if missing:
        raise ValueError(f"Missing component parameters: {sorted(missing)}")
    if schema.get("additionalProperties") is False:
        extra = set(args) - set(properties)
        if extra:
            raise ValueError(f"Unknown component parameters: {sorted(extra)}")
    params = args.copy()
    for name, prop in properties.items():
        if name not in params and "default" in prop:
            params[name] = prop["default"]
        if name in params and known(params[name]):
            fastjsonschema.compile(prop)(params[name])
    return params
