from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.callback_handler import EventType
from bec_lib.endpoints import MessageEndpoints
from bec_lib.macro_update_handler import MacroUpdateHandler
from bec_lib.user_macros import UserMacros

# pylint: disable=no-member
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access


def dummy_func():
    pass


def dummy_func2():
    pass


class MockBuiltins:
    def __init__(self, *args, **kwargs):
        self.__dict__ = {}

    def __contains__(self, item):
        return item in self.__dict__


@pytest.fixture
def user_macros():
    with mock.patch("bec_lib.macro_update_handler.builtins", new_callable=MockBuiltins) as builtins:
        macros = UserMacros(mock.MagicMock())
        macros.forget_all_user_macros()
        yield macros, builtins
        macros.forget_all_user_macros()


def test_user_macros_forget(user_macros):
    macros, builtins = user_macros
    mock_run = macros._client.callbacks.run

    macros._update_handler._add_macro("test", {"cls": dummy_func, "file": "path_to_my_file.py"})
    macros.forget_all_user_macros()
    assert mock_run.call_count == 1
    assert mock_run.call_args == mock.call(
        EventType.NAMESPACE_UPDATE, action="remove", ns_objects={"test": dummy_func}
    )
    assert "test" not in builtins.__dict__
    assert len(macros._update_handler.macros) == 0


def test_user_macro_forget(user_macros):
    macros, builtins = user_macros
    mock_run = macros._client.callbacks.run
    macros._update_handler._add_macro("test", {"cls": dummy_func, "file": "path_to_my_file.py"})
    macros.forget_user_macro("test")
    assert mock_run.call_count == 1
    assert mock_run.call_args == mock.call(
        EventType.NAMESPACE_UPDATE, action="remove", ns_objects={"test": dummy_func}
    )
    assert "test" not in builtins.__dict__


def test_load_user_macro(user_macros):
    macros, _ = user_macros
    mock_run = macros._client.callbacks.run
    mock_run.reset_mock()
    dummy_func.__module__ = "macros_dummy_file"
    with mock.patch.object(
        macros._update_handler,
        "load_macro_module",
        return_value=[("test", dummy_func), ("wrong_test", dummy_func2)],
    ) as load_macro:
        macros.load_user_macro("dummy")
        assert load_macro.call_count == 1
        assert load_macro.call_args == mock.call("dummy")
        assert "test" in macros._update_handler.macros
        assert mock_run.call_count == 1
        assert mock_run.call_args == mock.call(
            EventType.NAMESPACE_UPDATE, action="add", ns_objects={"test": dummy_func}
        )
        assert "wrong_test" not in macros._update_handler.macros


def test_user_macros_with_executable_code(user_macros, tmpdir):
    """Test that user macros with executable code are not loaded."""
    macros, _ = user_macros
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
    assert len(macros._update_handler.macros) == 0
    assert mock_run.call_count == 0


def test_user_macros_with_safe_code(user_macros, tmpdir):
    """Test that user macros with only imports, functions, and classes are loaded correctly."""
    macros, _ = user_macros
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

"""
    )

    # Mock run to capture namespace updates
    mock_run = macros._client.callbacks.run

    # This should load the macros successfully
    macros.load_user_macro(str(macro_file))

    # Should have loaded the functions and class
    assert len(macros._update_handler.macros) == 3  # my_function, another_function, MyClass
    assert "my_function" in macros._update_handler.macros
    assert "another_function" in macros._update_handler.macros
    assert "MyClass" in macros._update_handler.macros

    # Should have made 3 callback calls (one for each loaded item)
    assert mock_run.call_count == 3

    # Verify the functions work correctly
    assert macros._update_handler.macros["my_function"]["cls"]() == "hello"
    assert macros._update_handler.macros["another_function"]["cls"](5) == 10
    assert macros._update_handler.macros["MyClass"]["cls"]().value == 42


def test_on_macro_update_add_case(user_macros, tmpdir):
    """Test on_macro_update with 'add' action."""
    macros, _ = user_macros
    # Create a test macro file
    macro_file = tmpdir.join("test_add_macro.py")
    macro_file.write("def test_add_function(): return 'added'")

    # Mock the load_user_macro method
    with mock.patch.object(macros._update_handler, "load_user_macro") as mock_load:
        msg = messages.MacroUpdateMessage(
            update_type="add", macro_name="test_add_macro", file_path=str(macro_file)
        )
        macros._update_handler.on_macro_update(msg)

        # Verify load_user_macro was called with ignore_existing=True
        mock_load.assert_called_once_with(str(macro_file), ignore_existing=True)


def test_on_macro_update_remove_case(user_macros):
    """Test on_macro_update with 'remove' action."""
    macros, _ = user_macros
    # Mock the forget_user_macro method
    with mock.patch.object(macros._update_handler, "forget_user_macro") as mock_forget:
        msg = messages.MacroUpdateMessage(update_type="remove", macro_name="test_macro")
        macros._update_handler.on_macro_update(msg)

        # Verify forget_user_macro was called with the correct name
        mock_forget.assert_called_once_with("test_macro")


def test_on_macro_update_reload_case(user_macros, tmpdir):
    """Test on_macro_update with 'reload' action."""
    macros, _ = user_macros
    # Create a test macro file
    macro_file = tmpdir.join("test_reload_macro.py")
    macro_file.write("def test_reload_function(): return 'reloaded'")

    # Mock both forget and reload methods
    with mock.patch.object(macros._update_handler, "reload_user_macro") as mock_reload:

        msg = messages.MacroUpdateMessage(
            update_type="reload", macro_name="test_macro", file_path=str(macro_file)
        )
        macros._update_handler.on_macro_update(msg)

        # Verify both methods were called
        mock_reload.assert_called_once_with("test_macro", str(macro_file))


def test_on_macro_update_reload_all_case(user_macros):
    """Test on_macro_update with 'reload_all' action."""
    macros, _ = user_macros
    # Mock the load_all_user_macros method
    with mock.patch.object(macros._update_handler, "load_all_user_macros") as mock_load_all:
        msg = messages.MacroUpdateMessage(update_type="reload_all")
        macros._update_handler.on_macro_update(msg)

        # Verify load_all_user_macros was called
        mock_load_all.assert_called_once()


def test_on_macro_update_unknown_type(user_macros):
    """Test on_macro_update with unknown update_type."""
    macros, _ = user_macros
    # Mock the logger
    with mock.patch("bec_lib.macro_update_handler.logger.error") as mock_logger:
        # Create a message with an invalid update_type by bypassing validation
        msg = mock.MagicMock()
        msg.update_type = "unknown_action"

        macros._update_handler.on_macro_update(msg)

        # Verify error was logged
        mock_logger.assert_called_once_with("Unknown macro update type: unknown_action")


def test_on_macro_update_with_complete_message_fields(user_macros, tmpdir):
    """Test on_macro_update with all message fields populated."""
    macros, _ = user_macros
    macro_file = tmpdir.join("complete_test_macro.py")
    macro_file.write("def complete_function(): return 'complete'")

    with mock.patch.object(macros._update_handler, "load_user_macro") as mock_load:
        msg = messages.MacroUpdateMessage(
            update_type="add",
            macro_name="complete_test_macro",
            file_path=str(macro_file),
            metadata={"test": "data"},
        )
        macros._update_handler.on_macro_update(msg)

        # Verify the method was called regardless of extra fields
        mock_load.assert_called_once_with(str(macro_file), ignore_existing=True)


def test_on_macro_update_integration_with_real_message(user_macros, tmpdir):
    """Test on_macro_update integration with actual MacroUpdateMessage instance."""
    macros, _ = user_macros
    # Test with a real MacroUpdateMessage instance
    macro_file = tmpdir.join("integration_macro.py")
    macro_file.write(
        """
def integration_test():
    '''Integration test function'''
    return 'integration_success'
"""
    )

    # Add a mock macro to the handler first
    macros._update_handler._add_macro(
        "existing_macro",
        {"cls": dummy_func, "fname": "/some/path.py", "source": "def dummy_func(): pass"},
    )

    with (
        mock.patch.object(macros._update_handler, "load_user_macro") as mock_load,
        mock.patch.object(macros._update_handler, "forget_user_macro") as mock_forget,
        mock.patch.object(macros._update_handler, "reload_user_macro") as mock_reload,
        mock.patch.object(macros._update_handler, "load_all_user_macros") as mock_load_all,
    ):

        # Test add
        add_msg = messages.MacroUpdateMessage(
            update_type="add", macro_name="integration_test", file_path=str(macro_file)
        )
        macros._update_handler.on_macro_update(add_msg)
        mock_load.assert_called_with(str(macro_file), ignore_existing=True)

        # Test remove
        remove_msg = messages.MacroUpdateMessage(update_type="remove", macro_name="existing_macro")
        macros._update_handler.on_macro_update(remove_msg)
        mock_forget.assert_called_with("existing_macro")

        # Test reload
        reload_msg = messages.MacroUpdateMessage(
            update_type="reload", macro_name="existing_macro", file_path=str(macro_file)
        )
        macros._update_handler.on_macro_update(reload_msg)
        mock_reload.assert_called_with("existing_macro", str(macro_file))

        # Test reload_all
        reload_all_msg = messages.MacroUpdateMessage(update_type="reload_all")
        macros._update_handler.on_macro_update(reload_all_msg)
        mock_load_all.assert_called_once()


def test_macro_update_message_validation():
    """Test MacroUpdateMessage validation rules."""
    # Valid messages should work
    valid_add = messages.MacroUpdateMessage(
        update_type="add", macro_name="test_macro", file_path="/path/to/file.py"
    )
    assert valid_add.update_type == "add"

    valid_remove = messages.MacroUpdateMessage(update_type="remove", macro_name="test_macro")
    assert valid_remove.update_type == "remove"

    valid_reload = messages.MacroUpdateMessage(
        update_type="reload", macro_name="test_macro", file_path="/path/to/file.py"
    )
    assert valid_reload.update_type == "reload"

    valid_reload_all = messages.MacroUpdateMessage(update_type="reload_all")
    assert valid_reload_all.update_type == "reload_all"

    # Invalid messages should raise ValidationError
    with pytest.raises(Exception):  # ValidationError from pydantic
        messages.MacroUpdateMessage(update_type="add")  # Missing macro_name and file_path

    with pytest.raises(Exception):  # ValidationError from pydantic
        messages.MacroUpdateMessage(update_type="remove")  # Missing macro_name

    with pytest.raises(Exception):  # ValidationError from pydantic
        messages.MacroUpdateMessage(
            update_type="add", macro_name="test_macro"
        )  # Missing file_path for add action


def test_broadcast_method(user_macros):
    """Test the broadcast method sends correct MacroUpdateMessage."""

    macros, _ = user_macros
    # Mock the client connector
    mock_connector = macros._update_handler.client.connector

    # Test broadcast for 'add' action
    macros._update_handler.broadcast(action="add", name="test_macro", file_path="/path/to/test.py")

    # Verify send was called with correct parameters
    mock_connector.send.assert_called()
    call_args = mock_connector.send.call_args
    endpoint = call_args[0][0]  # First positional argument
    message = call_args[0][1]  # Second positional argument

    # Check that the endpoint is correct
    assert endpoint == MessageEndpoints.macro_update()

    # Check that the message is correct
    assert isinstance(message, messages.MacroUpdateMessage)
    assert message.update_type == "add"
    assert message.macro_name == "test_macro"
    assert message.file_path == "/path/to/test.py"


def test_broadcast_different_actions(user_macros):
    """Test broadcast method with different action types."""

    macros, _ = user_macros
    mock_connector = macros._update_handler.client.connector

    # Test remove action
    macros._update_handler.broadcast(action="remove", name="test_macro")
    call_args = mock_connector.send.call_args
    message = call_args[0][1]
    assert message.update_type == "remove"
    assert message.macro_name == "test_macro"
    assert message.file_path is None

    # Test reload action
    macros._update_handler.broadcast(action="reload", name="test_macro", file_path="/new/path.py")
    call_args = mock_connector.send.call_args
    message = call_args[0][1]
    assert message.update_type == "reload"
    assert message.macro_name == "test_macro"
    assert message.file_path == "/new/path.py"

    # Test reload_all action
    macros._update_handler.broadcast(action="reload_all")
    call_args = mock_connector.send.call_args
    message = call_args[0][1]
    assert message.update_type == "reload_all"
    assert message.macro_name is None
    assert message.file_path is None


def test_get_existing_macros_method(user_macros):
    """Test the get_existing_macros method."""

    macros, _ = user_macros
    # Setup test data
    macros._update_handler._add_macro(
        "macro1", {"cls": dummy_func, "fname": "/path/to/file1.py", "source": "code1"}
    )
    macros._update_handler._add_macro(
        "macro2", {"cls": dummy_func2, "fname": "/path/to/file2.py", "source": "code2"}
    )
    macros._update_handler._add_macro(
        "macro3", {"cls": dummy_func, "fname": "/path/to/file1.py", "source": "code3"}
    )
    macros._update_handler._add_macro(
        "macro4", {"cls": dummy_func2, "fname": "/path/to/file3.py", "source": "code4"}
    )

    # Test getting macros from file1.py
    result = macros._update_handler.get_existing_macros("/path/to/file1.py")
    expected = {
        "macro1": {"cls": dummy_func, "fname": "/path/to/file1.py", "source": "code1"},
        "macro3": {"cls": dummy_func, "fname": "/path/to/file1.py", "source": "code3"},
    }
    assert result == expected

    # Test getting macros from file2.py
    result = macros._update_handler.get_existing_macros("/path/to/file2.py")
    expected = {"macro2": {"cls": dummy_func2, "fname": "/path/to/file2.py", "source": "code2"}}
    assert result == expected

    # Test getting macros from non-existent file
    result = macros._update_handler.get_existing_macros("/nonexistent/file.py")
    assert result == {}


def test_get_existing_macros_with_missing_fname(user_macros):
    """Test get_existing_macros with macros that don't have fname attribute."""
    macros, _ = user_macros
    # Setup test data with some macros missing fname
    macros._update_handler._add_macro(
        "macro1", {"cls": dummy_func, "fname": "/path/to/file1.py", "source": "code1"}
    )
    macros._update_handler._add_macro(
        "macro2", {"cls": dummy_func2, "source": "code2"}  # Missing fname
    )
    macros._update_handler._add_macro(
        "macro3", {"cls": dummy_func, "fname": "/path/to/file1.py", "source": "code3"}
    )
    # Should only return macros with matching fname
    result = macros._update_handler.get_existing_macros("/path/to/file1.py")
    expected = {
        "macro1": {"cls": dummy_func, "fname": "/path/to/file1.py", "source": "code1"},
        "macro3": {"cls": dummy_func, "fname": "/path/to/file1.py", "source": "code3"},
    }
    assert result == expected


def test_macro_update_callback_valid_message(user_macros):
    """Test _macro_update_callback with valid MacroUpdateMessage."""
    macros, _ = user_macros
    # Create a mock message object
    mock_msg = mock.MagicMock()
    mock_msg.value = messages.MacroUpdateMessage(
        update_type="add", macro_name="test_macro", file_path="/path/to/test.py"
    )

    # Mock the on_macro_update method
    with mock.patch.object(macros._update_handler, "on_macro_update") as mock_on_update:
        # Call the callback
        macros._update_handler._macro_update_callback(mock_msg, macros._update_handler)

        # Verify on_macro_update was called with the correct message
        mock_on_update.assert_called_once_with(mock_msg.value)


def test_macro_update_callback_invalid_message_type(user_macros):
    """Test _macro_update_callback with invalid message type."""
    macros, _ = user_macros
    # Create a mock message with wrong type
    mock_msg = mock.MagicMock()
    mock_msg.value = "not_a_macro_update_message"

    # Mock the logger
    with mock.patch("bec_lib.macro_update_handler.logger.error") as mock_logger:
        # Mock on_macro_update to ensure it's not called
        with mock.patch.object(macros._update_handler, "on_macro_update") as mock_on_update:
            # Call the callback
            macros._update_handler._macro_update_callback(mock_msg, macros._update_handler)

            # Verify error was logged and on_macro_update was not called
            mock_logger.assert_called_once_with("Received invalid message type: <class 'str'>")
            mock_on_update.assert_not_called()


def test_macro_update_callback_with_different_message_types(user_macros):
    """Test _macro_update_callback with various message types."""
    macros, _ = user_macros
    with mock.patch("bec_lib.macro_update_handler.logger.error") as mock_logger:
        # Test with None
        mock_msg = mock.MagicMock()
        mock_msg.value = None
        macros._update_handler._macro_update_callback(mock_msg, macros._update_handler)
        mock_logger.assert_called_with("Received invalid message type: <class 'NoneType'>")

        mock_logger.reset_mock()

        # Test with integer
        mock_msg.value = 123
        macros._update_handler._macro_update_callback(mock_msg, macros._update_handler)
        mock_logger.assert_called_with("Received invalid message type: <class 'int'>")

        mock_logger.reset_mock()

        # Test with dict
        mock_msg.value = {"not": "a_message"}
        macros._update_handler._macro_update_callback(mock_msg, macros._update_handler)
        mock_logger.assert_called_with("Received invalid message type: <class 'dict'>")


def test_macro_update_callback_static_method():
    """Test that _macro_update_callback is properly decorated as staticmethod."""
    # Verify it's a static method by calling it directly on the class
    mock_msg = mock.MagicMock()
    mock_msg.value = messages.MacroUpdateMessage(update_type="reload_all")

    mock_parent = mock.MagicMock()

    # This should work without creating an instance
    MacroUpdateHandler._macro_update_callback(mock_msg, mock_parent)

    # Verify the parent's on_macro_update method was called
    mock_parent.on_macro_update.assert_called_once_with(mock_msg.value)


def test_reload_user_macro_basic(user_macros):
    """Test the basic reload_user_macro functionality."""
    macros, _ = user_macros
    # Setup - add a macro to the handler
    macros._update_handler._add_macro(
        "test_macro",
        {"cls": dummy_func, "fname": "/test/path.py", "source": "def dummy_func(): pass"},
    )

    # Mock the methods that reload_user_macro calls
    with (
        mock.patch.object(macros._update_handler, "forget_user_macro") as mock_forget,
        mock.patch.object(macros._update_handler, "load_user_macro") as mock_load,
    ):

        # Call reload_user_macro
        macros._update_handler.reload_user_macro("test_macro", "/test/path.py")

        # Verify the correct sequence of calls
        mock_forget.assert_called_once_with("test_macro")
        mock_load.assert_called_once_with("/test/path.py", ignore_existing=True)


def test_reload_user_macro_with_real_files(user_macros, tmpdir):
    """Test reload_user_macro with actual file operations."""
    macros, _ = user_macros
    # Create initial macro file
    macro_file = tmpdir.join("test_reload_macro.py")
    macro_file.write(
        """
def test_function():
    '''Initial version'''
    return 'version_1'

def helper_function():
    '''Helper function'''
    return 'helper'
"""
    )

    # Load initial macro
    macros.load_user_macro(str(macro_file))

    # Verify initial state
    assert "test_function" in macros._update_handler.macros
    assert "helper_function" in macros._update_handler.macros
    assert macros._update_handler.macros["test_function"]["cls"]() == "version_1"

    # Update the file content
    macro_file.write(
        """
def test_function():
    '''Updated version'''
    return 'version_2'

def helper_function():
    '''Helper function'''
    return 'helper'

def new_function():
    '''New function added'''
    return 'new'
"""
    )

    # Reload only the test_function
    macros._update_handler.reload_user_macro("test_function", str(macro_file))

    # Verify the reload worked
    assert macros._update_handler.macros["test_function"]["cls"]() == "version_2"
    assert macros._update_handler.macros["helper_function"]["cls"]() == "helper"
    assert "new_function" in macros._update_handler.macros
    assert macros._update_handler.macros["new_function"]["cls"]() == "new"


def test_reload_user_macro_nonexistent_macro(user_macros):
    """Test reloading a macro that doesn't exist."""
    macros, _ = user_macros
    # Mock forget_user_macro to not raise an error (it should handle this gracefully)
    with (
        mock.patch.object(macros._update_handler, "forget_user_macro") as mock_forget,
        mock.patch.object(macros._update_handler, "load_user_macro") as mock_load,
    ):

        # Try to reload a non-existent macro
        macros._update_handler.reload_user_macro("nonexistent_macro", "/test/path.py")

        # Should still try to forget and then load
        mock_forget.assert_called_once_with("nonexistent_macro")
        mock_load.assert_called_once_with("/test/path.py", ignore_existing=True)


def test_reload_user_macro_error_handling(user_macros):
    """Test reload_user_macro error handling."""
    macros, _ = user_macros
    # Setup initial macro
    macros._update_handler._add_macro(
        "test_macro",
        {"cls": dummy_func, "fname": "/test/path.py", "source": "def dummy_func(): pass"},
    )

    # Mock forget_user_macro to raise an exception
    with mock.patch.object(
        macros._update_handler, "forget_user_macro", side_effect=Exception("Forget failed")
    ) as mock_forget:
        with pytest.raises(Exception, match="Forget failed"):
            macros._update_handler.reload_user_macro("test_macro", "/test/path.py")

        mock_forget.assert_called_once_with("test_macro")


def test_reload_user_macro_integration_with_callbacks(user_macros, tmpdir):
    """Test that reload_user_macro properly triggers callbacks."""
    macros, _ = user_macros
    macro_file = tmpdir.join("callback_test_macro.py")
    macro_file.write(
        """
def callback_test():
    '''Test function for callbacks'''
    return 'original'
"""
    )

    # Load initial macro
    macros.load_user_macro(str(macro_file))

    # Reset the mock to count only reload-related calls
    macros._client.callbacks.run.reset_mock()

    # Update file content
    macro_file.write(
        """
def callback_test():
    '''Updated test function for callbacks'''
    return 'updated'
"""
    )

    # Reload the macro
    macros._update_handler.reload_user_macro("callback_test", str(macro_file))

    # Verify callbacks were triggered (remove + add)
    assert macros._client.callbacks.run.call_count == 2

    # Check the remove callback
    remove_call = macros._client.callbacks.run.call_args_list[0]
    assert remove_call[1]["action"] == "remove"
    assert "callback_test" in remove_call[1]["ns_objects"]

    # Check the add callback
    add_call = macros._client.callbacks.run.call_args_list[1]
    assert add_call[1]["action"] == "add"
    assert "callback_test" in add_call[1]["ns_objects"]


def test_on_macro_update_reload_case_updated(user_macros, tmpdir):
    """Test the updated on_macro_update reload case with new content."""
    macros, _ = user_macros
    macro_file = tmpdir.join("test_reload_macro.py")
    macro_file.write("def test_reload_function(): return 'reloaded'")

    macros.load_user_macro(str(macro_file))
    assert "test_reload_function" in macros._update_handler.macros

    macro_file.write("def test_reload_function(): return 'reloaded_v2'")
    macros._update_handler.reload_user_macro("test_reload_function", str(macro_file))
    assert macros._update_handler.macros["test_reload_function"]["cls"]() == "reloaded_v2"


def test_reload_user_macro_with_different_file(user_macros, tmpdir):
    """Test reloading a macro from a different file."""
    macros, _ = user_macros
    # Create two different files
    original_file = tmpdir.join("original.py")
    original_file.write("def shared_macro(): return 'original_file'")

    new_file = tmpdir.join("new_location.py")
    new_file.write("def shared_macro(): return 'new_location'")

    # Load from original file
    macros.load_user_macro(str(original_file))
    assert macros._update_handler.macros["shared_macro"]["cls"]() == "original_file"
    assert macros._update_handler.macros["shared_macro"]["fname"] == str(original_file)

    # Reload from new location
    macros._update_handler.reload_user_macro("shared_macro", str(new_file))

    # Verify it now loads from the new location
    assert macros._update_handler.macros["shared_macro"]["cls"]() == "new_location"
    assert macros._update_handler.macros["shared_macro"]["fname"] == str(new_file)


def test_load_macro_overwrite_builtin(user_macros, tmpdir):
    """Test that loading a macro with the same name as a built-in function is skipped."""
    macros, builtins = user_macros
    builtins.__dict__["print"] = print  # Ensure built-in print is present
    macro_file = tmpdir.join("test_macro.py")
    macro_file.write("def print(): return 'This should not overwrite built-in'")

    # Load the macro
    macros.load_user_macro(str(macro_file))

    # Verify that the built-in print function was not overwritten
    assert macros._update_handler.macros.get("print") is None
    assert builtins.__dict__["print"] is print  # Ensure built-in print is unchanged


def test_load_all_user_macros_basic(user_macros, tmpdir, monkeypatch):
    """Test the load_all_user_macros method with mocked directories."""
    macros, builtins = user_macros
    # Create temporary directories and files
    user_macro_dir = tmpdir.mkdir("bec").mkdir("macros")
    config_macro_dir = tmpdir.mkdir("config_macros")

    # Create test macro files
    user_macro1 = user_macro_dir.join("user_macro1.py")
    user_macro1.write(
        """
def user_function1():
    '''User macro function 1'''
    return 'user1'
"""
    )

    user_macro2 = user_macro_dir.join("user_macro2.py")
    user_macro2.write(
        """
def user_function2():
    '''User macro function 2'''
    return 'user2'
"""
    )

    config_macro1 = config_macro_dir.join("config_macro1.py")
    config_macro1.write(
        """
def config_function1():
    '''Config macro function 1'''
    return 'config1'
"""
    )

    # Mock the path expansion and existence checks
    def mock_expanduser(path):
        if path == "~":
            return str(tmpdir)
        elif path.startswith("~/"):
            return str(tmpdir.join(path[2:]))
        return path

    def mock_exists(path):
        if "bec/macros" in path:
            return True
        elif str(config_macro_dir) in path:
            return True
        return False

    # Mock the _macro_path to point to our test config directory
    macros._update_handler._macro_path = str(config_macro_dir)

    # Apply mocks
    monkeypatch.setattr("os.path.expanduser", mock_expanduser)
    monkeypatch.setattr("os.path.exists", mock_exists)

    # Mock importlib.metadata.entry_points to return no plugins
    mock_entry_points = mock.MagicMock()
    mock_entry_points.return_value = []
    monkeypatch.setattr("importlib.metadata.entry_points", mock_entry_points)

    # Mock the forget_all_user_macros method to track if it's called
    with mock.patch.object(macros._update_handler, "forget_all_user_macros") as mock_forget:
        # Call the method under test
        macros._update_handler.load_all_user_macros()

        # Verify forget_all_user_macros was called first
        mock_forget.assert_called_once()

    # Verify all expected macros were loaded
    assert "user_function1" in macros._update_handler.macros
    assert "user_function2" in macros._update_handler.macros
    assert "config_function1" in macros._update_handler.macros

    # Verify the functions work correctly
    assert macros._update_handler.macros["user_function1"]["cls"]() == "user1"
    assert macros._update_handler.macros["user_function2"]["cls"]() == "user2"
    assert macros._update_handler.macros["config_function1"]["cls"]() == "config1"

    # Verify functions are added to builtins
    assert hasattr(builtins, "user_function1")
    assert hasattr(builtins, "user_function2")
    assert hasattr(builtins, "config_function1")
    assert getattr(builtins, "user_function1")() == "user1"
    assert getattr(builtins, "user_function2")() == "user2"
    assert getattr(builtins, "config_function1")() == "config1"
