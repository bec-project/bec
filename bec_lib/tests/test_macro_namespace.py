from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

import pytest
from IPython.core.interactiveshell import InteractiveShell

from bec_lib.macros.config_model import parse_macro_config
from bec_lib.macros.macro_namespace import BecMacroNamespace

# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access

_MODULE = "bec_macro_unit_test_module"

_CONFIG = f"""
macros:
  test_macro: {_MODULE}::run_test
  everything: {_MODULE}
  alignment:
    align_x: {_MODULE}::align_x

global_in_interactive_shell:
    - test_macro
    - alignment.align_x
"""


def _write_config(tmp_path: Path, config: str) -> Path:
    config_path = tmp_path / "macro_config.yaml"
    config_path.write_text(config)
    return config_path


@pytest.fixture
def macro_module():
    module = ModuleType(_MODULE)

    def run_test():
        return "test"

    def align_x():
        return "align-x"

    def _private():
        return "private"

    for func in (run_test, align_x, _private):
        func.__module__ = _MODULE  # as if the functions had been defined in this module
        setattr(module, func.__name__, func)
    module.parse_macro_config = parse_macro_config

    sys.modules[_MODULE] = module
    yield module
    del sys.modules[_MODULE]


@pytest.fixture
def deep_reload():
    """Patch out the deep reload: the synthetic test module cannot be reloaded by the import
    machinery. Reloading real modules is covered in the bec_ipython_client macro tests."""
    with mock.patch(
        "bec_lib.macros.macro_namespace._deep_reload", side_effect=lambda module: module
    ) as patched_deep_reload:
        yield patched_deep_reload


@pytest.fixture
def shell():
    return mock.MagicMock(spec=InteractiveShell)


@pytest.fixture
def macro_namespace(tmp_path, macro_module, deep_reload):
    macro_namespace = BecMacroNamespace()
    macro_namespace.load_config(_write_config(tmp_path, _CONFIG))
    return macro_namespace


def test_macros_are_loaded_into_the_namespace(macro_namespace):
    assert macro_namespace.test_macro() == "test"
    assert isinstance(macro_namespace.alignment, SimpleNamespace)
    assert macro_namespace.alignment.align_x() == "align-x"


def test_module_without_a_function_name_becomes_a_namespace(macro_namespace):
    everything = macro_namespace.everything
    assert isinstance(everything, SimpleNamespace)
    assert everything.run_test() == "test"
    assert everything.align_x() == "align-x"
    assert not hasattr(everything, "_private")
    # functions which the module only imported are not macros
    assert not hasattr(everything, "parse_macro_config")


def test_macros_sharing_a_module_are_loaded_from_it_once(macro_namespace, deep_reload):
    # all three entries of the config reference the same module
    assert deep_reload.call_count == 1
    assert macro_namespace.test_macro.__globals__ is macro_namespace.alignment.align_x.__globals__


def test_file_macros_are_loaded_from_disk(tmp_path):
    macro_file = tmp_path / "file_macros.py"
    macro_file.write_text('def from_file():\n    return "from-file"\n')
    config = f"macros:\n  file_macro: {macro_file}::from_file\n"

    macro_namespace = BecMacroNamespace()
    macro_namespace.load_config(_write_config(tmp_path, config))

    assert macro_namespace.file_macro() == "from-file"


def test_missing_function_in_module_raises(tmp_path, macro_module, deep_reload):
    config = f"macros:\n  test_macro: {_MODULE}::does_not_exist\n"

    with pytest.raises(ValueError, match="No callable 'does_not_exist'"):
        BecMacroNamespace().load_config(_write_config(tmp_path, config))


def test_macro_name_clashing_with_an_attribute_raises(tmp_path, macro_module, deep_reload):
    config = f"macros:\n  reload: {_MODULE}::run_test\n"

    with pytest.raises(ValueError, match="'reload' is not a valid macro"):
        BecMacroNamespace().load_config(_write_config(tmp_path, config))


def test_loading_macros_without_a_config_raises():
    with pytest.raises(ValueError, match="Macro config must be loaded"):
        BecMacroNamespace()._load_macros()


def test_configured_macros_are_pushed_into_the_shell(macro_namespace, shell):
    macro_namespace.populate_globals_in_ipython(shell)

    # macros in a nested namespace are pushed under their own name
    shell.push.assert_called_once_with(
        {"test_macro": macro_namespace.test_macro, "align_x": macro_namespace.alignment.align_x}
    )


def test_globals_are_dropped_and_pushed_again_on_reload(macro_namespace, shell, deep_reload):
    macro_namespace.populate_globals_in_ipython(shell)
    pushed = shell.push.call_args.args[0]

    macro_namespace.reload()

    shell.drop_by_id.assert_called_once_with(pushed)
    assert shell.push.call_count == 2
    assert deep_reload.call_count == 2
    assert macro_namespace.test_macro() == "test"


def test_unload_removes_macros_and_globals(macro_namespace, shell):
    macro_namespace.populate_globals_in_ipython(shell)
    pushed = shell.push.call_args.args[0]

    macro_namespace._unload_macros()

    shell.drop_by_id.assert_called_once_with(pushed)
    assert not macro_namespace._loaded
    for name in ("test_macro", "everything", "alignment"):
        assert not hasattr(macro_namespace, name)


def test_loading_another_config_replaces_the_loaded_macros(
    macro_namespace, tmp_path, macro_module, deep_reload
):
    other_config = f"macros:\n  other_macro: {_MODULE}::align_x\n"

    macro_namespace.load_config(_write_config(tmp_path, other_config))

    assert macro_namespace.other_macro() == "align-x"
    for name in ("test_macro", "everything", "alignment"):
        assert not hasattr(macro_namespace, name)
