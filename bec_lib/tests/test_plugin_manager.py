import os
import subprocess
import sys
from importlib import reload
from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from bec_lib.utils.plugin_manager._constants import ANSWER_KEYS
from bec_lib.utils.plugin_manager._util import (
    _goto_dir,
    existing_data,
    git_stage_files,
    make_commit,
)

# Too complicated for import mechanics - tests "without" BW present should run first, then with
pytestmark = pytest.mark.random_order(disabled=True)


def mocked_app(module_patch):
    with patch.dict(sys.modules, dict(module_patch)):
        from bec_lib.utils.plugin_manager import main

        reload(main)
        yield main._app, module_patch[0][1]

        loaded_package_modules = [
            key
            for key, value in sys.modules.items()
            if "bec_lib.utils.plugin_manager.main" in str(value) or "bec_widgets" in str(value)
        ]
        for key in loaded_package_modules:
            del sys.modules[key]


@pytest.fixture(scope="module")
def app_none():
    # hide bec_widgets when running these tests
    yield from mocked_app([("bec_widgets", None)])


@pytest.fixture(scope="module")
def app_with_bw():
    # hide bec_widgets when running these tests
    mock_module = MagicMock()
    yield from mocked_app(
        [("bec_widgets.utils.bec_plugin_manager.edit_ui", mock_module), ("bec_widgets", None)]
    )


@pytest.fixture(scope="module")
def create_app():
    # hide bec_widgets when running these tests
    with patch.dict(sys.modules, {"bec_widgets": None}):
        from bec_lib.utils.plugin_manager import create

        reload(create)
        yield create._app


@pytest.fixture
def runner():
    return CliRunner()


def test_plugin_manager_doesnt_add_missing_command(app_none):
    app, _ = app_none
    assert len(app.registered_commands) == 0


def test_plugin_manager_create_subapp_needs_extra_command(runner, app_none):
    app, _ = app_none
    result = runner.invoke(app, ["create"])
    assert result.exit_code == 2
    assert "Missing command." in result.output


def test_plugin_manager_create_subapp_wo_bw(runner, create_app, plugin_repo):
    with (
        patch(
            "bec_lib.utils.plugin_manager.create.scan.plugin_repo_path",
            return_value=str(plugin_repo),
        ),
        patch("bec_lib.utils.plugin_manager.create.scan.run_formatters"),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.select",
            return_value=MagicMock(ask=MagicMock(return_value="SOFTWARE_TRIGGERED")),
        ),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.checkbox",
            return_value=MagicMock(ask=MagicMock(return_value=[])),
        ),
    ):
        result = runner.invoke(
            create_app,
            ["scan", "test"],
            input="V4 scan implementation.\nDescribe the scan here.\nn\nn\n",
        )
    assert result.exit_code == 0

    result = runner.invoke(create_app, ["device", "test"])
    assert result.exit_code == 1
    assert type(result.exception) is NotImplementedError


@pytest.fixture
def plugin_repo(tmp_path):
    repo = tmp_path / "example_plugin"
    scans_dir = repo / repo.name / "scans"
    scans_dir.mkdir(parents=True)
    (scans_dir / "__init__.py").write_text("", encoding="utf-8")
    return repo


def test_plugin_manager_create_scan(runner, create_app, plugin_repo):
    with (
        patch(
            "bec_lib.utils.plugin_manager.create.scan.plugin_repo_path",
            return_value=str(plugin_repo),
        ),
        patch("bec_lib.utils.plugin_manager.create.scan.run_formatters") as run_formatters,
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.select",
            side_effect=[
                MagicMock(ask=MagicMock(return_value="SOFTWARE_TRIGGERED")),
                MagicMock(ask=MagicMock(return_value="float")),
            ],
        ),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.checkbox",
            return_value=MagicMock(ask=MagicMock(return_value=[])),
        ),
    ):
        result = runner.invoke(
            create_app,
            ["scan"],
            input=(
                "example_scan\n"
                "Example scan description\n"
                "Example scan summary\n"
                "n\n"
                "y\n"
                "start_pos\n"
                "None\n"
                "Start position.\n"
                "n\n"
            ),
        )

    assert result.exit_code == 0
    scan_file = plugin_repo / plugin_repo.name / "scans" / "example_scan.py"
    init_file = plugin_repo / plugin_repo.name / "scans" / "__init__.py"
    scan_content = scan_file.read_text(encoding="utf-8")
    assert scan_file.exists()
    assert "Example scan description" in scan_content
    assert "Example scan summary" in scan_content
    assert "class ExampleScan(ScanBase):" in scan_content
    assert 'scan_name = "_v4_example_scan"' in scan_content
    assert "scan_type = ScanType.SOFTWARE_TRIGGERED" in scan_content
    assert '"Scan Parameters": ["start_pos"]' in scan_content
    assert "start_pos: Annotated[float, ScanArgument(" in scan_content
    assert "display_name='Start Pos'" in scan_content
    assert "description='Start position.'" in scan_content
    assert "num_points=1," in scan_content
    assert "num_monitored_readouts=1," in scan_content
    assert "points=self.scan_info.num_monitored_readouts," in scan_content
    assert "self.actions.pre_scan_all_devices()" in scan_content
    assert init_file.read_text(encoding="utf-8") == "from .example_scan import ExampleScan\n"
    run_formatters.assert_called_once_with(
        plugin_repo,
        [
            str(Path(plugin_repo.name) / "scans" / "example_scan.py"),
            str(Path(plugin_repo.name) / "scans" / "__init__.py"),
        ],
    )


def test_plugin_manager_create_scan_rejects_duplicate(runner, create_app, plugin_repo):
    existing_scan = plugin_repo / plugin_repo.name / "scans" / "example_scan.py"
    existing_scan.write_text("existing", encoding="utf-8")

    with (
        patch(
            "bec_lib.utils.plugin_manager.create.scan.plugin_repo_path",
            return_value=str(plugin_repo),
        ),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.select",
            return_value=MagicMock(ask=MagicMock(return_value="SOFTWARE_TRIGGERED")),
        ),
    ):
        result = runner.invoke(create_app, ["scan", "example_scan"], input="n\n")

    assert result.exit_code == 1
    assert existing_scan.read_text(encoding="utf-8") == "existing"


def test_plugin_manager_create_scan_reprompts_for_invalid_argument_name(
    runner, create_app, plugin_repo
):
    with (
        patch(
            "bec_lib.utils.plugin_manager.create.scan.plugin_repo_path",
            return_value=str(plugin_repo),
        ),
        patch("bec_lib.utils.plugin_manager.create.scan.run_formatters"),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.select",
            side_effect=[
                MagicMock(ask=MagicMock(return_value="SOFTWARE_TRIGGERED")),
                MagicMock(ask=MagicMock(return_value="float")),
            ],
        ),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.checkbox",
            return_value=MagicMock(ask=MagicMock(return_value=[])),
        ),
    ):
        result = runner.invoke(
            create_app,
            ["scan", "example_scan"],
            input=(
                "Example scan description\n"
                "Example scan summary\n"
                "n\n"
                "y\n"
                "asdlkj lkjasd \n"
                "start_pos\n"
                "None\n"
                "Start position.\n"
                "n\n"
            ),
        )

    assert result.exit_code == 0
    scan_content = (plugin_repo / plugin_repo.name / "scans" / "example_scan.py").read_text(
        encoding="utf-8"
    )
    assert "start_pos: Annotated[float, ScanArgument(" in scan_content


def test_plugin_manager_create_scan_skips_units_for_device_argument(
    runner, create_app, plugin_repo
):
    with (
        patch(
            "bec_lib.utils.plugin_manager.create.scan.plugin_repo_path",
            return_value=str(plugin_repo),
        ),
        patch("bec_lib.utils.plugin_manager.create.scan.run_formatters"),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.select",
            side_effect=[
                MagicMock(ask=MagicMock(return_value="SOFTWARE_TRIGGERED")),
                MagicMock(ask=MagicMock(return_value="DeviceBase")),
            ],
        ),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.checkbox",
            return_value=MagicMock(ask=MagicMock(return_value=[])),
        ),
    ):
        result = runner.invoke(
            create_app,
            ["scan", "device_scan"],
            input=(
                "Device scan description\n"
                "Device scan summary\n"
                "n\n"
                "y\n"
                "motor\n"
                "Motor device.\n"
                "n\n"
            ),
        )

    assert result.exit_code == 0
    scan_content = (plugin_repo / plugin_repo.name / "scans" / "device_scan.py").read_text(
        encoding="utf-8"
    )
    assert "motor: Annotated[DeviceBase, ScanArgument(" in scan_content
    assert "units=Units." not in scan_content


def test_plugin_manager_create_scan_renders_builtin_arguments_after_custom_inputs(
    runner, create_app, plugin_repo
):
    from bec_lib.utils.plugin_manager.create.scan import _BUILTIN_ARGUMENTS

    exp_time_argument = next(
        argument for argument in _BUILTIN_ARGUMENTS if argument.name == "exp_time"
    )

    with (
        patch(
            "bec_lib.utils.plugin_manager.create.scan.plugin_repo_path",
            return_value=str(plugin_repo),
        ),
        patch("bec_lib.utils.plugin_manager.create.scan.run_formatters"),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.select",
            side_effect=[
                MagicMock(ask=MagicMock(return_value="SOFTWARE_TRIGGERED")),
                MagicMock(ask=MagicMock(return_value="float")),
            ],
        ),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.checkbox",
            return_value=MagicMock(ask=MagicMock(return_value=[exp_time_argument])),
        ),
    ):
        result = runner.invoke(
            create_app,
            ["scan", "ordered_scan"],
            input=(
                "Ordered scan description\n"
                "Ordered scan summary\n"
                "y\n"
                "y\n"
                "start_pos\n"
                "None\n"
                "Start position.\n"
                "n\n"
            ),
        )

    assert result.exit_code == 0
    scan_content = (plugin_repo / plugin_repo.name / "scans" / "ordered_scan.py").read_text(
        encoding="utf-8"
    )
    assert scan_content.index("start_pos: Annotated[float, ScanArgument(") < scan_content.index(
        "exp_time: Annotated[float | None, ScanArgument("
    )


def test_plugin_manager_create_scan_accepts_compound_pint_unit(runner, create_app, plugin_repo):
    with (
        patch(
            "bec_lib.utils.plugin_manager.create.scan.plugin_repo_path",
            return_value=str(plugin_repo),
        ),
        patch("bec_lib.utils.plugin_manager.create.scan.run_formatters"),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.select",
            side_effect=[
                MagicMock(ask=MagicMock(return_value="SOFTWARE_TRIGGERED")),
                MagicMock(ask=MagicMock(return_value="float")),
            ],
        ),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.checkbox",
            return_value=MagicMock(ask=MagicMock(return_value=[])),
        ),
    ):
        result = runner.invoke(
            create_app,
            ["scan", "unit_scan"],
            input=(
                "Unit scan description\n"
                "Unit scan summary\n"
                "n\n"
                "y\n"
                "ramp_rate\n"
                "T/min\n"
                "Ramp rate.\n"
                "n\n"
            ),
        )

    assert result.exit_code == 0
    scan_content = (plugin_repo / plugin_repo.name / "scans" / "unit_scan.py").read_text(
        encoding="utf-8"
    )
    assert "units=Units.T / Units.min" in scan_content


def test_plugin_manager_create_scan_accepts_compound_pint_unit_with_power(
    runner, create_app, plugin_repo
):
    with (
        patch(
            "bec_lib.utils.plugin_manager.create.scan.plugin_repo_path",
            return_value=str(plugin_repo),
        ),
        patch("bec_lib.utils.plugin_manager.create.scan.run_formatters"),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.select",
            side_effect=[
                MagicMock(ask=MagicMock(return_value="SOFTWARE_TRIGGERED")),
                MagicMock(ask=MagicMock(return_value="float")),
            ],
        ),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.checkbox",
            return_value=MagicMock(ask=MagicMock(return_value=[])),
        ),
    ):
        result = runner.invoke(
            create_app,
            ["scan", "unit_power_scan"],
            input=(
                "Unit power scan description\n"
                "Unit power scan summary\n"
                "n\n"
                "y\n"
                "ramp_rate\n"
                "T/min**2\n"
                "Ramp rate.\n"
                "n\n"
            ),
        )

    assert result.exit_code == 0
    scan_content = (plugin_repo / plugin_repo.name / "scans" / "unit_power_scan.py").read_text(
        encoding="utf-8"
    )
    assert "units=Units.T / Units.min ** 2" in scan_content


def test_plugin_manager_create_scan_builtin_relative_is_scan_parameter(
    runner, create_app, plugin_repo
):
    from bec_lib.utils.plugin_manager.create.scan import _BUILTIN_ARGUMENTS

    relative_argument = next(
        argument for argument in _BUILTIN_ARGUMENTS if argument.name == "relative"
    )

    with (
        patch(
            "bec_lib.utils.plugin_manager.create.scan.plugin_repo_path",
            return_value=str(plugin_repo),
        ),
        patch("bec_lib.utils.plugin_manager.create.scan.run_formatters"),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.select",
            return_value=MagicMock(ask=MagicMock(return_value="SOFTWARE_TRIGGERED")),
        ),
        patch(
            "bec_lib.utils.plugin_manager.create.scan.questionary.checkbox",
            return_value=MagicMock(ask=MagicMock(return_value=[relative_argument])),
        ),
    ):
        result = runner.invoke(
            create_app,
            ["scan", "relative_scan"],
            input="Built-in relative scan.\nRelative scan summary.\ny\nn\n",
        )

    assert result.exit_code == 0
    scan_content = (plugin_repo / plugin_repo.name / "scans" / "relative_scan.py").read_text(
        encoding="utf-8"
    )
    assert '"Scan Parameters": ["relative"]' in scan_content
    assert '"Acquisition Parameters"' not in scan_content


def test_plugin_manager_create_scan_sorts_exports(plugin_repo):
    from bec_lib.utils.plugin_manager.create.scan import _ensure_scan_export

    init_file = plugin_repo / plugin_repo.name / "scans" / "__init__.py"
    init_file.write_text(
        "from .z_scan import ZScan\nfrom .alpha_scan import AlphaScan\n", encoding="utf-8"
    )

    _ensure_scan_export(init_file, "middle_scan")

    assert init_file.read_text(encoding="utf-8") == (
        "from .alpha_scan import AlphaScan\n"
        "from .middle_scan import MiddleScan\n"
        "from .z_scan import ZScan\n"
    )


def test_plugin_manager_adds_found_command(runner, app_with_bw):
    app, mock_edit_ui = app_with_bw
    assert len(app.registered_commands) == 1
    result = runner.invoke(app, ["edit-ui", "widget_to_edit"])
    assert result.exit_code == 0
    mock_edit_ui.open_and_watch_ui_editor.assert_called_with("widget_to_edit")


def test_existing_data(tmp_path):
    with open(tmp_path / ".copier-answers.yml", "w") as f:
        f.write(dedent("""
                # Do not edit this file!
                # It is needed to track the repo template version, and editing may break things.
                # This file will be overwritten by copier on template updates.

                _commit: v1.0.5
                _src_path: https://github.com/bec-project/plugin_copier_template.git
                make_commit: true
                project_name: bec_testing_plugin
                widget_plugins_input:
                -   name: example_widget_plugin
                    use_ui: true
                """))
    result = existing_data(tmp_path, [ANSWER_KEYS.WIDGETS])
    assert result == {"widget_plugins_input": [{"name": "example_widget_plugin", "use_ui": True}]}


def test_goto_dir(tmp_path):
    os.makedirs(tmp_path / "test_dir")
    original_path = os.getcwd()
    with _goto_dir(tmp_path / "test_dir"):
        assert os.getcwd() == str(tmp_path / "test_dir")
    assert os.getcwd() == original_path


@pytest.fixture
def git_repo(tmp_path):
    with _goto_dir(tmp_path):
        subprocess.run(["git", "init", "-b", "main"])
        subprocess.run(["git", "config", "user.email", "test"])
        subprocess.run(["git", "config", "user.name", "test"])
        with open(tmp_path / "test.txt", "w") as f:
            f.write("test\n")
        with open(tmp_path / "test2.txt", "w") as f:
            f.write("test\n")
        yield tmp_path


def test_git_stage_files(git_repo):

    git_stage_files(git_repo, ["test.txt"])

    result = subprocess.run(["git", "diff", "--name-only", "--cached"], stdout=subprocess.PIPE)
    assert result.stdout.decode() == "test.txt\n"

    git_stage_files(git_repo)
    result = subprocess.run(["git", "diff", "--name-only", "--cached"], stdout=subprocess.PIPE)
    assert result.stdout.decode() == "test.txt\ntest2.txt\n"


def test_make_commit(git_repo):
    git_stage_files(git_repo)
    make_commit(git_repo, "test commit")
    result = subprocess.run(["git", "log"], stdout=subprocess.PIPE)
    assert "test commit" in result.stdout.decode()
