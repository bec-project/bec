"""Expression discovery describes the actual parser and expression-capable fields."""

from __future__ import annotations

import json

import fastjsonschema
import pytest

from bec_lib.scan_sequencer import available_components, available_expressions
from bec_lib.scan_sequencer.core.evaluation import Scope, evaluate
from bec_lib.scan_sequencer.expressions import expression


def test_expression_schema_examples_match_parser_and_evaluator():
    schema = available_expressions()
    assert json.loads(json.dumps(schema)) == schema
    assert {branch["$ref"].rsplit("/", 1)[-1] for branch in schema["anyOf"]} == {
        "Reference",
        "Parameter",
        "LiteralValue",
        "StatusField",
        "Not",
        "Boolean",
        "Compare",
    }
    validate = fastjsonschema.compile(schema)
    scope = Scope(variables={"position": 2}, parameters={"exposure": 0.1}, statuses={"ramp": None})
    expected = [2, 0.1, {"ref": "position"}, True, False, False, True, True]
    for example, result in zip(schema["examples"], expected, strict=True):
        validate(example)
        assert expression(example) is not None
        assert evaluate(example, scope, lambda _status: True) == result
    assert expression("${position}") is None
    assert evaluate("${position}", scope) == "${position}"


@pytest.mark.parametrize(
    "value",
    [
        {"ref": 1},
        {"param": "position", "extra": True},
        {"type": "status_field", "status_ref": "ramp", "field": "success"},
        {"type": "compare", "operator": "unknown", "left": 1, "right": 0},
        {"type": "and", "operands": True},
        "${position}",
    ],
)
def test_expression_schema_rejects_malformed_expression_shapes(value):
    with pytest.raises(fastjsonschema.JsonSchemaException):
        fastjsonschema.compile(available_expressions())(value)


@pytest.mark.parametrize(
    "component, field",
    [
        ("Set", "value"),
        ("ForEach", "values"),
        ("While", "condition"),
        ("ScanCall", "arguments"),
        ("ScanCall", "positional"),
        ("ComponentCall", "arguments"),
    ],
)
def test_component_schema_identifies_expression_capable_fields(component, field):
    schema = available_components()
    property_schema = schema["$defs"][component]["properties"][field]
    assert "available_expressions()" in property_schema["description"]
    assert "literal wrapper" in property_schema["description"]
    assert property_schema["examples"]
