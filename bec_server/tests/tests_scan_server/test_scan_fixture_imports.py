"""Check fixture imports in an isolated downstream pytest session."""

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("fixture_name", ["scan_assembler", "v4_scan_assembler"])
def test_scan_assembler_named_import(tmp_path, fixture_name):
    """Both fixture names work without importing the other assembler fixture."""
    (tmp_path / "conftest.py").write_text(
        "from bec_server.scan_server.tests.scan_fixtures import (\n"
        "    device_manager, readout_priority, nth_done_status_mock,\n"
        f"    {fixture_name},\n"
        ")\n",
        encoding="utf-8",
    )
    (tmp_path / "test_downstream.py").write_text(
        "import pytest\n\n"
        "def test_assembler(request):\n"
        + (
            "    with pytest.warns(DeprecationWarning, match='v4_scan_assembler is deprecated'):\n"
            "        assembler = request.getfixturevalue('v4_scan_assembler')\n"
            if fixture_name == "v4_scan_assembler"
            else "    assembler = request.getfixturevalue('scan_assembler')\n"
        )
        + "    assert callable(assembler)\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-p",
            "bec_lib.tests.fixtures",
            str(tmp_path / "test_downstream.py"),
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
