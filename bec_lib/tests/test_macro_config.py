from io import StringIO

import pytest
from pydantic import ValidationError

from bec_lib.macros.config_model import (
    PLUGIN_MACROS_DIR_PLACEHOLDER,
    USER_MACROS_DIR_PLACEHOLDER,
    DirImportMacro,
    FileImportMacro,
    MacroConfig,
    ModuleImportMacro,
    parse_macro_config,
)

TEST_VALID_YAML = """
macros:
  test_macro: pxii_bec.macros.test::run_test
  other_test_macro: /tmp/macros.py::run_test

  alignment:
    align_x: mx_bec.macros.alignment::x
    align_y: mx_bec.macros.alignment::y

  check_stuff: mx_bec.macros.checks
  move_robot: /tmp/macros/robot.py

global_in_interactive_shell:
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


TEST_DIR_YAML = f"""
macros:
  local: {USER_MACROS_DIR_PLACEHOLDER}
  shared: {PLUGIN_MACROS_DIR_PLACEHOLDER}
  absolute: /home/user/bec/macros
  relative: ./macros
  explicit: pxii_bec.macros.test::run_test

global_in_interactive_shell:
    - explicit
    - macro_test.macro_test
"""


def test_parse_directory_references():
    config = parse_macro_config(StringIO(TEST_DIR_YAML))
    for name in ("local", "shared", "absolute", "relative"):
        assert isinstance(config.macros[name], DirImportMacro), name
    assert config.macros["local"] == DirImportMacro(
        reference=USER_MACROS_DIR_PLACEHOLDER, func_name=None
    )
    # kept as text, so that e.g. './macros' is not normalized into something that looks like a module
    assert config.macros["relative"].reference == "./macros"
    assert isinstance(config.macros["explicit"], ModuleImportMacro)


def test_directory_reference_cannot_select_a_function():
    with pytest.raises(ValidationError, match="cannot select a single function"):
        parse_macro_config(StringIO("macros:\n  shared: /home/user/macros::func\n"))


def test_directory_references_survive_a_json_roundtrip():
    config = parse_macro_config(StringIO(TEST_DIR_YAML))
    restored = MacroConfig.model_validate_json(config.model_dump_json())
    assert restored == config
    assert isinstance(restored.macros["shared"], DirImportMacro)
    assert isinstance(restored.macros["explicit"], ModuleImportMacro)


def test_globals_may_name_macros_provided_by_a_directory_reference():
    # 'macro_test' is not a key: it can only be checked once the directories are loaded
    config = parse_macro_config(StringIO(TEST_DIR_YAML))
    assert config.global_in_interactive_shell == ["explicit", "macro_test.macro_test"]
