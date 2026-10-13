"""JSON contracts for sequence documents and reusable components."""

from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    StrictInt,
    TypeAdapter,
    field_validator,
)


class Model(BaseModel):
    """Strict serializable plan model; runtime state lives elsewhere.

    Args:
        **data (Any): Fields validated according to the concrete model schema.
    """

    model_config = ConfigDict(
        extra="forbid", allow_inf_nan=False, validate_assignment=True, revalidate_instances="always"
    )


class Node(Model):
    """A definition-local stable identity.

    Args:
        id (str): Stable identity within the document or reusable definition.
    """

    id: str = Field(min_length=1)


class Sequence(Node):
    """A component that holds child components.

    Children are executed in order. A sequence can be nested inside another
    sequence or used as the body of a loop or reusable definition.

    Args:
        id (str): Stable identity within the document or reusable definition.
        type (Literal['sequence']): Component or document discriminator.
        children (list[Component]): Child components to execute in order.
    """

    type: Literal["sequence"] = "sequence"
    children: list[Component] = Field(default_factory=list)


class Repeat(Node):
    """A component that holds a body and a fixed repetition count.

    The body is executed count times. The body can be any component, including
    a sequence or another loop. The count must be a positive integer.

    Args:
        id (str): Stable identity within the document or reusable definition.
        type (Literal['repeat']): Component or document discriminator.
        count (StrictInt): Positive integer number of body executions; booleans are rejected.
        body (Component): Nested component executed by this loop or reusable definition.
    """

    type: Literal["repeat"] = "repeat"
    count: StrictInt = Field(gt=0)
    body: Component


class ForEach(Node):
    """A component that holds an ordered list of values, a variable name and a body.

    The body is executed once for each value. The current value is bound to the
    variable within the body's scope. The body can contain nested components,
    and the variable binding does not escape the loop.

    Args:
        id (str): Stable identity within the document or reusable definition.
        type (Literal['for_each']): Component or document discriminator.
        variable (str): Name bound to the current value within each iteration.
        values (list[JsonValue]): Ordered values supplied to successive iterations.
        body (Component): Nested component executed by this loop or reusable definition.
    """

    type: Literal["for_each"] = "for_each"
    variable: str = Field(min_length=1)
    values: list[JsonValue]
    body: Component


class While(Node):
    """A component that holds a structured condition and a body.

    The condition is checked before each iteration. The body is executed while
    the condition is true and can contain nested components. A started iteration
    finishes before the next condition check unless execution is interrupted.

    Args:
        id (str): Stable identity within the document or reusable definition.
        type (Literal['while']): Component or document discriminator.
        condition (JsonValue): Structured boolean condition checked before each iteration.
        body (Component): Nested component executed by this loop or reusable definition.
    """

    type: Literal["while"] = "while"
    condition: JsonValue
    body: Component


class ScanCall(Node):
    """A component that invokes an existing scan advertised by BEC.

    It holds the scan name, positional arguments and keyword arguments.
    Arguments are resolved in the current scope before invocation, preserving
    positional and keyword calling semantics. Scans execute synchronously.

    Args:
        id (str): Stable identity within the document or reusable definition.
        type (Literal['scan_call']): Component or document discriminator.
        name (str): Name of an existing advertised BEC scan.
        arguments (JsonValue): Keyword arguments or an expression resolving to an argument
            mapping.
        positional (list[JsonValue]): Positional scan arguments in calling order.
    """

    type: Literal["scan_call"] = "scan_call"
    name: str = Field(min_length=1)
    arguments: JsonValue = Field(default_factory=dict)
    positional: list[JsonValue] = Field(default_factory=list)


class Set(Node):
    """A component that sets a device or signal to a value.

    It holds the target's dotted name, a value and whether to wait for completion.
    Device targets use umv or mv; signal targets use set and optionally wait.
    An optional status binding makes completion observable within the current scope.

    Args:
        id (str): Stable identity within the document or reusable definition.
        type (Literal['set']): Component or document discriminator.
        device (str): Dotted name of the device or signal to set.
        value (JsonValue): Literal value or structured expression to resolve in the current scope.
        wait (StrictBool): Whether to wait for completion before continuing. Defaults to True.
        bind_status (str | None): Optional name for the checked runtime status in the current
            scope.
    """

    type: Literal["set"] = "set"
    device: str = Field(
        min_length=1, description="Dotted device or signal name, e.g. samx.velocity"
    )
    value: JsonValue
    wait: StrictBool = True
    bind_status: str | None = Field(default=None, min_length=1)


class DefinitionRef(Model):
    """A reference to a reusable definition's ID and exact version.

    A ComponentCall uses this reference to select its body and parameter schema.

    Args:
        id (str): Identity of the reusable definition.
        version (StrictInt): Exact positive integer version of a reusable definition.
    """

    id: str = Field(min_length=1)
    version: StrictInt = Field(ge=1)


class ComponentCall(Node):
    """A component that references a reusable definition by ID and exact version.

    It holds arguments for the definition's parameters. Arguments are resolved
    in the caller's scope before the definition's body executes in an isolated
    scope. Definitions can contain nested components and further component calls.

    Args:
        id (str): Stable identity within the document or reusable definition.
        type (Literal['component_call']): Component or document discriminator.
        definition (DefinitionRef): Exact reusable definition reference.
        arguments (JsonValue): Keyword arguments or an expression resolving to an argument
            mapping.
    """

    type: Literal["component_call"] = "component_call"
    definition: DefinitionRef
    arguments: JsonValue = Field(default_factory=dict)


Component = Annotated[
    Union[Sequence, Repeat, ForEach, While, ScanCall, Set, ComponentCall],
    Field(discriminator="type"),
]


class Schedule(Model):
    """A sequence document that holds a root component and document metadata.

    The root can be any supported component. A sequence root holds multiple
    ordered children. The interface supplies the document ID and revision;
    the backend validates and executes the root.

    Args:
        schema_version (Literal[1]): Sequence document schema version. Only integer 1 is
            supported.
        type (Literal['sequence']): Component or document discriminator.
        id (str): Document identity supplied by the calling interface.
        revision (StrictInt): Positive document revision supplied by the calling interface.
        root (Component): Root component of the received sequence document.
    """

    schema_version: Literal[1] = 1
    type: Literal["sequence"] = "sequence"
    id: str = Field(min_length=1)
    revision: StrictInt = Field(ge=1)
    root: Component

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value):
        """Require the supported integer schema version.

        Args:
            value (object): Value to validate, resolve or encode.

        Returns:
            int: The supported schema version, 1.
        """
        if not isinstance(value, int) or isinstance(value, bool) or value != 1:
            raise ValueError("schema_version must be integer 1")
        return value


class Definition(Model):
    """A named, versioned reusable component definition.

    It holds a parameter schema, a description and a body component.
    ComponentCall nodes reference it by ID and version. Its body can contain
    nested components and calls to other definitions.

    Args:
        id (str): Identity of the reusable definition.
        version (StrictInt): Exact positive integer version of a reusable definition.
        description (str): Human-readable description of a reusable definition.
        parameters (dict[str, JsonValue]): JSON Schema defining the accepted component parameters
            and defaults.
        body (Component): Nested component executed by this loop or reusable definition.
    """

    id: str = Field(min_length=1)
    version: StrictInt = Field(ge=1)
    description: str = ""
    parameters: dict[str, JsonValue] = Field(
        default_factory=lambda: {"type": "object", "additionalProperties": False}
    )
    body: Component


def available_components() -> dict:
    """Return JSON Schema for all supported component types and their fields.

    The discriminator mapping lists component names. The referenced definitions
    include descriptions, required fields and nested component schemas.

    Returns:
        dict: Component JSON Schema, including discriminator names and field definitions.
    """
    return TypeAdapter(Component).json_schema()
