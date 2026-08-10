from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable, Generator

import pytest
from IPython.core.interactiveshell import InteractiveShell
from traitlets.config import Config

from bec_lib.macros.macro_namespace import BecMacroNamespace

# pylint: disable=protected-access

_PKG = "bec_macro_test_pkg"

_HELPER = """def helper():
    return "helper-{version}"
"""

_MACRO_MODULE = """from {pkg}.helper import helper


def run_test():
    return f"test-{{helper()}}"


def align_x():
    return "align-x-{version}"
"""

_FILE_MACRO = """def from_file():
    return "from-file-{version}"
"""

_CONFIG = """
macros:
  test_macro: {pkg}.macros::run_test
  file_macro: {file_macro_path}::from_file
  alignment:
    align_x: {pkg}.macros::align_x
    also_from_file: {file_macro_path}::from_file

global_in_interactive_shell:
    - test_macro
    - alignment.align_x
"""


@pytest.fixture
def macro_sources(
    tmp_path, monkeypatch
) -> Generator[tuple[Path, Callable[[str], None]], None, None]:
    """Write a macro package, a standalone macro file and a macro config into a temporary directory."""
    monkeypatch.syspath_prepend(tmp_path)
    # Don't write bytecode: the source mtime stored in a .pyc has one-second resolution, so an
    # edit made within the same second as the import could otherwise be masked by stale bytecode.
    monkeypatch.setattr(sys, "dont_write_bytecode", True)

    package = tmp_path / _PKG
    package.mkdir()
    (package / "__init__.py").touch()
    file_macro_path = tmp_path / "file_macros.py"

    def write_macro_sources(version: str):
        (package / "helper.py").write_text(_HELPER.format(version=version))
        (package / "macros.py").write_text(_MACRO_MODULE.format(pkg=_PKG, version=version))
        file_macro_path.write_text(_FILE_MACRO.format(version=version))

    write_macro_sources("one")
    config_path = tmp_path / "macro_config.yaml"
    config_path.write_text(_CONFIG.format(pkg=_PKG, file_macro_path=file_macro_path))

    yield config_path, write_macro_sources

    for module_name in [
        name for name in sys.modules if name.startswith((_PKG, "_bec_macros_file_macros"))
    ]:
        del sys.modules[module_name]


@pytest.fixture
def ipython_shell() -> Generator[InteractiveShell, None, None]:
    config = Config()
    config.HistoryAccessor.enabled = False
    shell = InteractiveShell.instance(config=config)
    yield shell
    InteractiveShell.clear_instance()


@pytest.fixture
def macros(
    macro_sources, ipython_shell
) -> tuple[BecMacroNamespace, InteractiveShell, Callable[[str], None]]:
    config_path, write_macro_sources = macro_sources
    macro_namespace = BecMacroNamespace()
    macro_namespace.load_config(config_path)
    macro_namespace.populate_globals_in_ipython(ipython_shell)
    return macro_namespace, ipython_shell, write_macro_sources


def test_macros_are_loaded_from_modules_and_files(macros):
    macro_namespace, _, _ = macros
    assert macro_namespace.test_macro() == "test-helper-one"
    assert macro_namespace.alignment.align_x() == "align-x-one"
    assert macro_namespace.file_macro() == "from-file-one"
    assert macro_namespace.alignment.also_from_file() == "from-file-one"
    # macros which share a module must come from the same, once-loaded, module object
    assert macro_namespace.test_macro.__globals__ is macro_namespace.alignment.align_x.__globals__
    assert (
        macro_namespace.file_macro.__globals__
        is macro_namespace.alignment.also_from_file.__globals__
    )


def test_configured_macros_are_pushed_into_the_shell(macros):
    _, shell, _ = macros
    assert shell.user_ns["test_macro"]() == "test-helper-one"
    # macros in a nested namespace are pushed under their own name
    assert shell.user_ns["align_x"]() == "align-x-one"
    # macros which are not configured as globals are not pushed
    assert "file_macro" not in shell.user_ns


def test_reload_picks_up_edited_macro_sources(macros):
    macro_namespace, shell, write_macro_sources = macros
    bec_messages_before = sys.modules["bec_lib.messages"]

    write_macro_sources("number-two")
    macro_namespace.reload()

    # 'test_macro' only changes if the helper module it imports is reloaded as well
    assert macro_namespace.test_macro() == "test-helper-number-two"
    assert macro_namespace.alignment.align_x() == "align-x-number-two"
    assert macro_namespace.file_macro() == "from-file-number-two"
    assert shell.user_ns["test_macro"]() == "test-helper-number-two"
    assert shell.user_ns["align_x"]() == "align-x-number-two"
    # deep reloading macro modules must not reload BEC itself, or live objects held elsewhere in
    # the session would no longer be instances of the classes they were created from
    assert sys.modules["bec_lib.messages"] is bec_messages_before


def test_unload_removes_macros_and_globals(macros):
    macro_namespace, shell, _ = macros

    macro_namespace._unload_macros()

    assert not hasattr(macro_namespace, "test_macro")
    assert not hasattr(macro_namespace, "alignment")
    assert "test_macro" not in shell.user_ns
    assert "align_x" not in shell.user_ns


def test_unload_keeps_globals_which_were_rebound_by_the_user(macros):
    macro_namespace, shell, _ = macros
    shell.user_ns["align_x"] = "something else"

    macro_namespace._unload_macros()

    assert shell.user_ns["align_x"] == "something else"
