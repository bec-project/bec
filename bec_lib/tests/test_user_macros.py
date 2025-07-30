import builtins
from unittest import mock

import pytest

from bec_lib.callback_handler import EventType
from bec_lib.user_macros import UserMacros

# pylint: disable=no-member
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access


def dummy_func():
    pass


def dummy_func2():
    pass


@pytest.fixture
def macros():
    yield UserMacros(mock.MagicMock())


def test_user_macros_forget(macros):
    mock_run = macros._client.callbacks.run
    macros._macros = {"test": {"cls": dummy_func, "file": "path_to_my_file.py"}}
    builtins.test = dummy_func
    macros.forget_all_user_macros()
    assert mock_run.call_count == 1
    assert mock_run.call_args == mock.call(
        EventType.NAMESPACE_UPDATE, action="remove", ns_objects={"test": dummy_func}
    )
    assert "test" not in builtins.__dict__
    assert len(macros._macros) == 0


def test_user_macro_forget(macros):
    mock_run = macros._client.callbacks.run
    macros._macros = {"test": {"cls": dummy_func, "file": "path_to_my_file.py"}}
    builtins.test = dummy_func
    macros.forget_user_macro("test")
    assert mock_run.call_count == 1
    assert mock_run.call_args == mock.call(
        EventType.NAMESPACE_UPDATE, action="remove", ns_objects={"test": dummy_func}
    )
    assert "test" not in builtins.__dict__


def test_load_user_macro(macros):
    mock_run = macros._client.callbacks.run
    builtins.__dict__["dev"] = macros
    dummy_func.__module__ = "macros"
    with mock.patch.object(macros, "_run_linter_on_file") as linter:
        with mock.patch.object(
            macros,
            "_load_macro_module",
            return_value=[("test", dummy_func), ("wrong_test", dummy_func2)],
        ) as load_macro:
            macros.load_user_macro("dummy")
            assert load_macro.call_count == 1
            assert load_macro.call_args == mock.call("dummy")
            assert "test" in macros._macros
            assert mock_run.call_count == 1
            assert mock_run.call_args == mock.call(
                EventType.NAMESPACE_UPDATE, action="add", ns_objects={"test": dummy_func}
            )
            assert "wrong_test" not in macros._macros
        # linter.assert_called_once_with("dummy") #TODO: re-enable this test once issue #298 is fixed


def test_user_macros_with_executable_code(macros, tmpdir):
    """Test that user macros with executable code are not loaded."""
    macro_file = tmpdir.join("macro_with_code.py")
    macro_file.write("print('This should not run')\n\ndef my_macro(): pass")

    # Mock run to capture namespace updates
    mock_run = macros._client.callbacks.run

    # This should not load the macro because it has executable code
    with mock.patch("builtins.print") as mock_print:
        macros.load_user_macro(str(macro_file))
        # Ensure that the print statement was not executed
        mock_print.assert_not_called()

    # Should not have loaded any macros due to executable code
    assert len(macros._macros) == 0
    assert mock_run.call_count == 0


def test_user_macros_with_safe_code(macros, tmpdir):
    """Test that user macros with only imports, functions, and classes are loaded correctly."""
    macro_file = tmpdir.join("safe_macro.py")
    macro_file.write(
        """
import os
from typing import List

# This is a comment
def my_function():
    '''A safe function'''
    return "hello"

def another_function(x: int) -> int:
    '''Another safe function with type hints'''
    return x * 2

class MyClass:
    '''A safe class'''
    def __init__(self):
        self.value = 42
    
    def method(self):
        return self.value

# Module-level variable assignment (should be allowed for constants)
MY_CONSTANT = "constant_value"
"""
    )

    # Mock run to capture namespace updates
    mock_run = macros._client.callbacks.run

    # This should load the macros successfully
    macros.load_user_macro(str(macro_file))

    # Should have loaded the functions and class
    assert len(macros._macros) == 3  # my_function, another_function, MyClass
    assert "my_function" in macros._macros
    assert "another_function" in macros._macros
    assert "MyClass" in macros._macros

    # Should have made 3 callback calls (one for each loaded item)
    assert mock_run.call_count == 3

    # Verify the functions work correctly
    assert macros._macros["my_function"]["cls"]() == "hello"
    assert macros._macros["another_function"]["cls"](5) == 10
    assert macros._macros["MyClass"]["cls"]().value == 42
