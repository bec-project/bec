"""One transitive resolver shared by validation, interpretation and exports."""

from __future__ import annotations

import re
from copy import deepcopy
from dataclasses import dataclass
from urllib.parse import unquote

import fastjsonschema

from ..components import Component, ComponentCall, Definition, ForEach, Repeat, Sequence, While
from .evaluation import UNKNOWN, known, validate_json


class _OfflineSchemaHandlers(dict):
    """Intercept every URI scheme when the compiler attempts external resolution."""

    def __contains__(self, _scheme: object) -> bool:
        return True

    def __getitem__(self, scheme: str):
        raise ValueError(
            f"Component schemas only support local $ref references (scheme: {scheme!r})"
        )


def _compile_schema(schema: dict):
    return fastjsonschema.compile(schema, handlers=_OfflineSchemaHandlers())


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
        _compile_schema(definition.parameters)
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
    return _compile_schema(parameters)(deepcopy(arguments))


def _schema_children(schema: dict):
    """Yield nested schemas supported by parameter validation.

    Args:
        schema (dict): JSON Schema object to inspect.

    Yields:
        tuple[tuple[str, ...], dict]: Relative path and nested parameter schema.
    """
    for key in (
        "properties",
        "patternProperties",
        "definitions",
        "$defs",
        "dependencies",
        "dependentSchemas",
    ):
        for name, child in schema.get(key, {}).items():
            if isinstance(child, dict):
                yield (key, name), child
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
            yield (key,), value
        elif isinstance(value, list):
            for index, child in enumerate(value):
                if isinstance(child, dict):
                    yield (key, str(index)), child
    for key in ("allOf", "anyOf", "oneOf", "prefixItems"):
        for index, child in enumerate(schema.get(key, [])):
            if isinstance(child, dict):
                yield (key, str(index)), child


def _validate_schema(schema: dict, root: dict | None = None, path: tuple[str, ...] = ()) -> None:
    """Check defaults and allow only local schema references, without network access.

    Args:
        schema (dict): JSON Schema object to inspect.
        root (dict | None): Root schema used for local references. Defaults to schema.
        path (tuple[str, ...]): Path to the current subschema within the root.
    """
    root = schema if root is None else root
    reference = schema.get("$ref")
    if reference is not None and not reference.startswith("#"):
        raise ValueError("Component schemas only support local $ref references")
    if "default" in schema:
        pointer = "#/" + "/".join(token.replace("~", "~0").replace("/", "~1") for token in path)
        context = {**root, "$ref": pointer} if path else root
        _compile_schema(context)(deepcopy(schema["default"]))
    for child_path, child in _schema_children(schema):
        _validate_schema(child, root, path + child_path)


def _schema_at(root, path):
    """Look up a schema by decoded JSON Pointer tokens."""
    for part in path:
        root = root[int(part)] if isinstance(root, list) else root[part]
    return root


def _parameter_schemas(root, schema, path=(), conditional=False, active=frozenset()):
    """Visit object constraints in native order, retaining their original reference paths."""
    if isinstance(schema, bool):
        yield schema, path, conditional
        return
    if "$ref" in schema:
        reference = unquote(schema["$ref"][1:])
        target_path = (
            tuple(part.replace("~1", "/").replace("~0", "~") for part in reference[1:].split("/"))
            if reference
            else ()
        )
        if target_path in active:
            return
        yield from _parameter_schemas(
            root, _schema_at(root, target_path), target_path, conditional, active | {target_path}
        )
        return
    for index, child in enumerate(schema.get("allOf", [])):
        yield from _parameter_schemas(
            root, child, path + ("allOf", str(index)), conditional, active
        )
    for keyword in ("anyOf", "oneOf"):
        for index, child in enumerate(schema.get(keyword, [])):
            yield from _parameter_schemas(root, child, path + (keyword, str(index)), True, active)
    for keyword in ("then", "else"):
        if keyword in schema:
            yield from _parameter_schemas(root, schema[keyword], path + (keyword,), True, active)
    for name, child in schema.get("dependencies", {}).items():
        if isinstance(child, dict):
            yield from _parameter_schemas(root, child, path + ("dependencies", name), True, active)
    yield schema, path, conditional


def _partial_array(root, schema, path, value):
    """Validate array items against their original homogeneous or tuple schemas."""
    items = schema["items"]
    for index, item in enumerate(value):
        if isinstance(items, list):
            if index >= len(items) and "additionalItems" not in schema:
                continue
            child = items[index] if index < len(items) else schema["additionalItems"]
            suffix = ("items", str(index)) if index < len(items) else ("additionalItems",)
        else:
            child, suffix = items, ("items",)
        value[index] = _partial_value(root, child, path + suffix, item)


def _partial_value(root, schema, path, value):
    """Validate known values and container shapes without interpreting unknown leaves."""
    if known(value) or schema is False:
        pointer = "#/" + "/".join(token.replace("~", "~0").replace("/", "~1") for token in path)
        return _compile_schema({**root, "$ref": pointer})(
            deepcopy(value) if known(value) else value
        )
    if value is UNKNOWN:
        return value
    result = value.copy()
    for current, current_path, conditional in _parameter_schemas(root, schema, path):
        if conditional:
            continue
        if current is False:
            raise ValueError("Component value is forbidden by its schema")
        if current is True:
            continue
        if "type" in current:
            _compile_schema({"type": current["type"]})(result)
        if isinstance(result, list) and "items" in current:
            _partial_array(root, current, current_path, result)
    if isinstance(result, dict):
        return _partial_parameters(root, schema, path, result)
    return result


def _parameter_checks(schema, name):
    """Select all property constraints that apply to a concrete argument name."""
    checks = [("properties", name)] if name in schema.get("properties", {}) else []
    checks.extend(
        ("patternProperties", pattern)
        for pattern in schema.get("patternProperties", {})
        if re.search(pattern, name)
    )
    if not checks:
        extra = schema.get("additionalProperties", True)
        if extra is False:
            raise ValueError(f"Unknown component parameters: {[name]}")
        if isinstance(extra, dict):
            checks.append(("additionalProperties",))
    return checks


def _partial_parameters(root, schema, path, args):
    """Check unconditional known fields; defer branch-dependent constraints and defaults."""
    if args is not UNKNOWN and not isinstance(args, dict):
        raise ValueError("Component arguments must resolve to a mapping")
    params = {} if args is UNKNOWN else args.copy()
    deferred = set()
    for current, current_path, conditional in _parameter_schemas(root, schema, path):
        if isinstance(current, bool):
            if not conditional and not current:
                raise ValueError("Component parameters are forbidden by their schema")
            continue
        properties = current.get("properties", {})
        if conditional or args is UNKNOWN:
            deferred.update(properties)
            continue
        if "type" in current:
            _compile_schema({"type": current["type"]})(params)
        missing = set(current.get("required", [])) - set(params)
        if missing:
            raise ValueError(f"Missing component parameters: {sorted(missing)}")
        for name, prop in properties.items():
            if name not in params and isinstance(prop, dict) and "default" in prop:
                params[name] = deepcopy(prop["default"])
        for name in list(params):
            for suffix in _parameter_checks(current, name):
                child_path = current_path + suffix
                params[name] = _partial_value(
                    root, _schema_at(root, child_path), child_path, params[name]
                )
    for name in deferred:
        params.setdefault(name, UNKNOWN)
    return params


def partial_parameters(definition: Definition, args: object) -> dict:
    """Validate known inputs/defaults and defer unknown constraints until runtime.

    Args:
        definition (Definition): Reusable definition to register or validate.
        args (object): Resolved arguments, possibly containing statically unknown values.

    Returns:
        dict: Known parameter values and placeholders for unknown values. Defaults and constraints
            in alternative or conditional schemas are deferred until runtime.
    """
    if known(args):
        return bind_parameters(definition.parameters, args)
    return _partial_parameters(definition.parameters, definition.parameters, (), args)
