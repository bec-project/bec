from io import StringIO

import pytest
from pydantic import ValidationError

from bec_lib.macros.config_model import parse_macro_config

TEST_VALID_YAML = """
macros:
  test_macro: pxii_bec.macros.test.run_test

  alignment:
    align_x: mx_bec.macros.alignment.x
    align_y: mx_bec.macros.alignment.y

add_to_builtins:
    - test_macro
    - alignment.align_x
"""


def test_parse_valid():
    config = parse_macro_config(StringIO(TEST_VALID_YAML))
    assert "test_macro" in config.macros
    assert config.macros.get("test_macro") == ["pxii_bec", "macros", "test", "run_test"]
    assert isinstance(config.macros.get("alignment"), dict)


def test_parse_invalid_not_enough_levels():
    TEST_INVALID_YAML = """
macros:
  test: test.macro
"""
    with pytest.raises(ValueError) as e:
        _ = parse_macro_config(StringIO(TEST_INVALID_YAML))
    e.match("test.macro is too short to identify")


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
    e.match("macros.alignment.align_y is not a macro category")
