from io import StringIO

import pytest
from pydantic import ValidationError

from bec_lib.macros.config_model import FileImportMacro, ModuleImportMacro, parse_macro_config

TEST_VALID_YAML = """
macros:
  test_macro: pxii_bec.macros.test::run_test
  other_test_macro: /tmp/macros.py::run_test

  alignment:
    align_x: mx_bec.macros.alignment::x
    align_y: mx_bec.macros.alignment::y

  check_stuff: mx_bec.macros.checks
  move_robot: /tmp/macros/robot.py

add_to_builtins:
    - test_macro
    - alignment.align_x
"""


def test_parse_valid():
    config = parse_macro_config(StringIO(TEST_VALID_YAML))
    assert "test_macro" in config.macros
    assert config.macros.get("test_macro") == ModuleImportMacro(
        func_name="run_test", reference="pxii_bec.macros.test"
    )
    assert isinstance(config.macros.get("alignment"), dict)
    assert isinstance(config.macros.get("check_stuff"), ModuleImportMacro)
    assert isinstance(config.macros.get("move_robot"), FileImportMacro)


def test_parse_invalid_two_idents():
    TEST_INVALID_YAML = """
macros:
  test: test.macro::a::b
"""
    with pytest.raises(AssertionError) as e:
        _ = parse_macro_config(StringIO(TEST_INVALID_YAML))
    e.match(r"Invalid macro reference: 'test.macro::a::b'.")


def test_parse_invalid_add_builtin_top_level_doesnt_exist():
    TEST_INVALID_YAML = TEST_VALID_YAML + "    - nonexistent"
    with pytest.raises(ValidationError) as e:
        _ = parse_macro_config(StringIO(TEST_INVALID_YAML))
    e.match("No macro nonexistent")


def test_parse_invalid_add_builtin_category_doesnt_exist():
    TEST_INVALID_YAML = TEST_VALID_YAML + "    - nonexistent.category"
    with pytest.raises(ValidationError) as e:
        _ = parse_macro_config(StringIO(TEST_INVALID_YAML))
    e.match("No category nonexistent in macro namespace macros")


def test_parse_invalid_add_builtin_not_a_category():
    TEST_INVALID_YAML = TEST_VALID_YAML + "    - alignment.align_y.something"
    with pytest.raises(ValidationError) as e:
        _ = parse_macro_config(StringIO(TEST_INVALID_YAML))
    e.match(r"macros.alignment\.align_y is not a macro category")
