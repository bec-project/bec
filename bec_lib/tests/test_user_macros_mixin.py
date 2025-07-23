import builtins
from unittest import mock

import pytest

from bec_lib.callback_handler import EventType
from bec_lib.user_macros_mixin import UserMacrosMixin

# pylint: disable=no-member
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access


def dummy_func():
    pass


def dummy_func2():
    pass


class client_user_macros_mixin(UserMacrosMixin):
    def __init__(self):
        self.callbacks = None
        super().__init__()
        self._macros = {}


@pytest.fixture
def macros():
    yield client_user_macros_mixin()


def test_user_macros_forget(macros):
    macros.callbacks = mock.MagicMock()
    mock_run = macros.callbacks.run
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
    macros.callbacks = mock.MagicMock()
    mock_run = macros.callbacks.run
    macros._macros = {"test": {"cls": dummy_func, "file": "path_to_my_file.py"}}
    builtins.test = dummy_func
    macros.forget_user_macro("test")
    assert mock_run.call_count == 1
    assert mock_run.call_args == mock.call(
        EventType.NAMESPACE_UPDATE, action="remove", ns_objects={"test": dummy_func}
    )
    assert "test" not in builtins.__dict__


def test_load_user_macro(macros):
    macros.callbacks = mock.MagicMock()
    mock_run = macros.callbacks.run
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


# def test_user_macro_linter():
#     macros = UsermacrosMixin()
#     current_path = pathlib.Path(__file__).parent.resolve()
#     macro_path = os.path.join(current_path, "test_data", "user_macro_with_bug.py")
#     builtins.__dict__["dev"] = macros
#     with mock.patch("bec_lib.user_macros_mixin.logger") as logger:
#         macros._run_linter_on_file(macro_path)
#         logger.error.assert_called_once()
