"""Load the default macro config shipped with bec_testing_plugin, which is installed in CI."""

from pathlib import Path
from unittest import mock

import pytest
from IPython.core.interactiveshell import InteractiveShell

from bec_lib.macros.macro_namespace import BecMacroNamespace
from bec_lib.plugin_helper import default_macro_config_path, plugin_macros_dir

# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name

bec_testing_plugin = pytest.importorskip(
    "bec_testing_plugin", reason="bec_testing_plugin is not installed"
)

PLUGIN_MACROS_DIR = Path(bec_testing_plugin.__file__).parent / "macros"


@pytest.fixture
def installed_plugin():
    """Make BEC treat bec_testing_plugin as the installed plugin, whatever the environment has."""
    with mock.patch("bec_lib.plugin_helper.plugin_package_name", return_value="bec_testing_plugin"):
        yield


@pytest.fixture
def user_macros_dir(tmp_path):
    """A user macro directory as configured in the service config, with one macro file."""
    (tmp_path / "my_macros.py").write_text('def hello():\n    return "hello from the user dir"\n')
    return tmp_path


@pytest.fixture
def macros(installed_plugin, user_macros_dir):
    macro_namespace = BecMacroNamespace(mock.MagicMock(), user_macros_dir=user_macros_dir)
    macro_namespace.load_default()
    return macro_namespace


def test_default_config_is_found_in_the_plugin_macros_dir(installed_plugin):
    assert plugin_macros_dir() == PLUGIN_MACROS_DIR
    assert default_macro_config_path() == PLUGIN_MACROS_DIR / "config.yaml"
    assert default_macro_config_path().is_file()


def test_shared_macros_are_loaded_from_the_plugin(macros):
    assert macros.macro_test.macro_test(1, 2) == 3
    assert macros.alignment.x.align_x(0, 2, 3) == {"start": 0, "stop": 2, "steps": 3}
    assert macros.alignment.y.align_y()["steps"] == 11
    assert macros.alignment.fine.center_beam.center_beam(0.5) == 0.5


def test_local_macros_are_loaded_from_the_user_dir(macros):
    assert macros.my_macros.hello() == "hello from the user dir"


def test_config_and_readme_are_not_loaded_as_macros(macros):
    assert not hasattr(macros, "config")
    assert not hasattr(macros, "README")


def test_configured_globals_are_pushed_into_the_shell(macros):
    shell = mock.MagicMock(spec=InteractiveShell)

    macros.populate_globals_in_ipython(shell)

    shell.push.assert_called_once_with({"macro_test": macros.macro_test.macro_test})
