from __future__ import annotations

import sys
from io import StringIO
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

import pytest
from IPython.core.interactiveshell import InteractiveShell

from bec_lib.endpoints import MessageEndpoints
from bec_lib.macros.config_model import MacroConfig, parse_macro_config
from bec_lib.macros.macro_namespace import BecMacroNamespace
from bec_lib.messages import MacroConfigMessage
from bec_lib.tests.utils import wait_until

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

_OTHER_CONFIG = f"macros:\n  other_macro: {_MODULE}::align_x\n"


def _write_config(tmp_path: Path, config: str) -> Path:
    config_path = tmp_path / "macro_config.yaml"
    config_path.write_text(config)
    return config_path


def _config_message(config: str) -> MacroConfigMessage:
    """The message another client would publish after loading the given config."""
    return MacroConfigMessage(config=parse_macro_config(StringIO(config)).model_dump_json())


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
def macro_namespace(tmp_path, macro_module, deep_reload, connected_connector):
    macro_namespace = BecMacroNamespace(connected_connector)
    macro_namespace.load_config(_write_config(tmp_path, _CONFIG))
    yield macro_namespace
    macro_namespace.shutdown()


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


def test_file_macros_are_loaded_from_disk(tmp_path, connected_connector):
    macro_file = tmp_path / "file_macros.py"
    macro_file.write_text('def from_file():\n    return "from-file"\n')
    config = f"macros:\n  file_macro: {macro_file}::from_file\n"

    macro_namespace = BecMacroNamespace(connected_connector)
    macro_namespace.load_config(_write_config(tmp_path, config))

    assert macro_namespace.file_macro() == "from-file"


def test_missing_function_in_module_raises(
    tmp_path, macro_module, deep_reload, connected_connector
):
    config = f"macros:\n  test_macro: {_MODULE}::does_not_exist\n"

    with pytest.raises(ValueError, match="No callable 'does_not_exist'"):
        BecMacroNamespace(connected_connector).load_config(_write_config(tmp_path, config))


def test_macro_name_clashing_with_an_attribute_raises(
    tmp_path, macro_module, deep_reload, connected_connector
):
    config = f"macros:\n  reload: {_MODULE}::run_test\n"

    with pytest.raises(ValueError, match="'reload' is not a valid macro"):
        BecMacroNamespace(connected_connector).load_config(_write_config(tmp_path, config))


def test_loading_macros_without_a_config_raises():
    with pytest.raises(ValueError, match="Macro config must be loaded"):
        BecMacroNamespace(mock.MagicMock())._load_macros()


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


def test_loading_another_config_replaces_the_loaded_macros(macro_namespace, tmp_path):
    macro_namespace.load_config(_write_config(tmp_path, _OTHER_CONFIG))

    assert macro_namespace.other_macro() == "align-x"
    for name in ("test_macro", "everything", "alignment"):
        assert not hasattr(macro_namespace, name)


def test_the_namespace_listens_for_config_updates_until_shutdown():
    connector = mock.MagicMock()

    macro_namespace = BecMacroNamespace(connector)
    connector.register.assert_called_once_with(
        MessageEndpoints.macro_config(), cb=macro_namespace._on_config_message, from_start=True
    )

    macro_namespace.shutdown()
    connector.unregister.assert_called_once_with(
        MessageEndpoints.macro_config(), cb=macro_namespace._on_config_message
    )


def test_loading_a_config_publishes_it(macro_namespace, connected_connector):
    published = connected_connector.xread(MessageEndpoints.macro_config(), from_start=True)

    assert len(published) == 1
    assert MacroConfig.model_validate_json(published[0]["data"].config) == macro_namespace._config


def test_a_new_client_picks_up_the_published_config(macro_namespace, connected_connector):
    other_client = BecMacroNamespace(connected_connector)
    try:
        wait_until(lambda: other_client._loaded, timeout_s=1)
        assert other_client.test_macro() == "test"
        assert other_client.alignment.align_x() == "align-x"
    finally:
        other_client.shutdown()


def test_a_config_published_by_another_client_is_applied(macro_namespace, connected_connector):
    connected_connector.xadd(
        MessageEndpoints.macro_config(), {"data": _config_message(_OTHER_CONFIG)}, max_size=1
    )

    wait_until(lambda: hasattr(macro_namespace, "other_macro"), timeout_s=1)
    assert macro_namespace.other_macro() == "align-x"
    for name in ("test_macro", "everything", "alignment"):
        assert not hasattr(macro_namespace, name)


def test_the_own_published_config_is_not_applied_again(macro_namespace, deep_reload):
    macro_namespace._on_config_message({"data": _config_message(_CONFIG)})

    assert deep_reload.call_count == 1


def test_a_received_config_which_fails_to_load_is_logged_not_raised(macro_namespace):
    broken_config = f"macros:\n  broken: {_MODULE}::does_not_exist\n"

    with mock.patch("bec_lib.macros.macro_namespace.logger") as logger:
        macro_namespace._on_config_message({"data": _config_message(broken_config)})

    logger.error.assert_called_once()
    assert "does_not_exist" in logger.error.call_args.args[0]
