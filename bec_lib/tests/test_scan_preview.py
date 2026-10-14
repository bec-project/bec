"""Tests for device-independent resolution of advertised preview mappings."""

import numpy as np
import pytest

from bec_lib.scan_preview import generate_scan_positions

LINE_INFO = {
    "position_preview": "bec_lib.position_generators.line_scan_positions",
    "position_preview_parameter_mapping": {"axes": {"bundle": [1, 2]}, "steps": "steps"},
    "arg_bundle_size": {"bundle": 3},
}


def test_multiple_bundles_preserve_motor_order():
    positions = generate_scan_positions(LINE_INFO, {"args": ["x", 0, 2, "y", 5, 3], "steps": 3})
    np.testing.assert_allclose(positions, [[0, 5], [1, 4], [2, 3]])


@pytest.mark.parametrize("args", [["x", 0], ["x", 0, 1, "y"]])
def test_incomplete_bundles_are_rejected(args):
    with pytest.raises(ValueError, match="bundles"):
        generate_scan_positions(LINE_INFO, {"args": args, "steps": 3})


def test_scan_without_generator_is_not_previewable():
    with pytest.raises(ValueError, match="does not advertise"):
        generate_scan_positions({"position_preview": None}, {})


def test_missing_scan_input_is_reported():
    with pytest.raises(KeyError, match="steps"):
        generate_scan_positions(LINE_INFO, {"args": ["x", 0, 1]})


def test_plugin_preview_is_imported_from_its_advertised_module(tmp_path, monkeypatch):
    package = tmp_path / "preview_test_plugin"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "positions.py").write_text(
        "import numpy as np\n"
        "def trajectory(start, stop):\n"
        "    return np.array([[start], [stop]])\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    info = {
        "position_preview": "preview_test_plugin.positions.trajectory",
        "position_preview_parameter_mapping": {"start": "begin", "stop": "end"},
    }
    positions = generate_scan_positions(info, {"begin": 2, "end": 7})
    np.testing.assert_array_equal(positions, [[2], [7]])


@pytest.mark.parametrize("path", ["line_scan_positions", ".function", "module."])
def test_preview_requires_fully_qualified_import_path(path):
    with pytest.raises(ValueError, match="fully qualified"):
        generate_scan_positions({"position_preview": path}, {})


def test_preview_must_be_callable():
    with pytest.raises(TypeError, match="not callable"):
        generate_scan_positions({"position_preview": "numpy.pi"}, {})
