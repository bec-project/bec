import io
import os
from unittest import mock

import pytest
import yaml

from bec_lib.bec_yaml_loader import yaml_load


@pytest.fixture
def test_file1():
    return "eiger:\n  readoutPriority: monitored\n  deviceClass: ophyd_devices.SimCamera\n  deviceConfig:\n    device_access: true\n  deviceTags:\n    - detector\n  enabled: true\n  readOnly: false\n  softwareTrigger: true"


@pytest.fixture
def test_file2():
    return "samx:\n  readoutPriority: monitored\n  deviceClass: ophyd_devices.SimCamera\n  deviceConfig:\n    device_access: true\n  deviceTags:\n    - detector\n  enabled: true\n  readOnly: false\n  softwareTrigger: true"


@pytest.fixture
def test_file3():
    return "samy:\n  readoutPriority: monitored\n  deviceClass: ophyd_devices.SimCamera\n  deviceConfig:\n    device_access: true\n  deviceTags:\n    - detector\n  enabled: true\n  readOnly: false\n  softwareTrigger: true"


def _remove_files(files):
    for file in files:
        if os.path.exists(file):
            os.remove(file)


def test_load_yaml_without_include(test_file1):
    # sastt:
    #   - !include /Users/wakonig_k/software/work/csaxs-bec/csaxs_bec/device_configs/bec_device_config_sastt.yaml
    output_file_1 = test_file1
    with open("test_file_1.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_1)
    try:
        out = yaml_load("test_file_1.yaml")
    finally:
        _remove_files(["test_file_1.yaml"])

    assert "eiger" in out
    assert len(out) == 1


def test_load_yaml_single_include(test_file1, test_file2):
    # sastt:
    #   - !include /Users/wakonig_k/software/work/csaxs-bec/csaxs_bec/device_configs/bec_device_config_sastt.yaml
    include_str = "sastt: !include ./test_file2.yaml"
    output_file_1 = test_file1 + "\n" + include_str
    output_file_2 = test_file2
    with open("test_file_1.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_1)
    with open("test_file2.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_2)
    try:
        out = yaml_load("test_file_1.yaml")
    finally:
        _remove_files(["test_file_1.yaml", "test_file2.yaml"])

    assert "samx" in out
    assert "eiger" in out
    assert len(out) == 2


def test_load_yaml_single_include_with_conflict(capfd, test_file1):
    # sastt:
    #   - !include /Users/wakonig_k/software/work/csaxs-bec/csaxs_bec/device_configs/bec_device_config_sastt.yaml
    include_str = "sastt: !include ./test_file2.yaml"
    output_file_1 = test_file1 + "\n" + include_str
    output_file_2 = test_file1
    output_file_1.replace(
        "deviceClass: ophyd_devices.SimCamera", "deviceClass: ophyd_devices.Eiger"
    )
    with open("test_file_1.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_1)
    with open("test_file2.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_2)
    # capture stdout
    try:
        out = yaml_load("test_file_1.yaml")
    finally:
        _remove_files(["test_file_1.yaml", "test_file2.yaml"])

    assert "eiger" in out
    assert len(out) == 1
    assert out["eiger"]["deviceClass"] == "ophyd_devices.SimCamera"
    out, _ = capfd.readouterr()
    assert "Warning: Multiple definitions for key eiger. Using the one from" in out


def test_load_yaml_multi_include(test_file1, test_file2, test_file3):
    # sastt:
    #   - !include /Users/wakonig_k/software/work/csaxs-bec/csaxs_bec/device_configs/bec_device_config_sastt.yaml
    include_str = "sastt:\n  - !include ./test_file2.yaml\n  - !include ./test_file3.yaml"
    output_file_1 = test_file1 + "\n" + include_str
    output_file_2 = test_file2
    output_file_3 = test_file3
    with open("test_file_1.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_1)
    with open("test_file2.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_2)
    with open("test_file3.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_3)
    try:
        out = yaml_load("test_file_1.yaml")
    finally:
        _remove_files(["test_file_1.yaml", "test_file2.yaml", "test_file3.yaml"])

    assert "samx" in out
    assert "samy" in out
    assert "eiger" in out
    assert len(out) == 3


def test_load_yaml_skip_includes(test_file1, test_file2, test_file3):
    # sastt:
    #   - !include /Users/wakonig_k/software/work/csaxs-bec/csaxs_bec/device_configs/bec_device_config_sastt.yaml
    include_str = "sastt:\n  - !include ./test_file2.yaml\n  - !include ./test_file3.yaml"
    output_file_1 = test_file1 + "\n" + include_str
    output_file_2 = test_file2
    output_file_3 = test_file3
    with open("test_file_1.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_1)
    with open("test_file2.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_2)
    with open("test_file3.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_3)
    try:
        out = yaml_load("test_file_1.yaml", process_includes=False)
    finally:
        _remove_files(["test_file_1.yaml", "test_file2.yaml", "test_file3.yaml"])

    assert len(out) == 2
    assert "samx" not in out
    assert "__include__" not in out


def test_load_yaml_comment_only_file():
    with open("test_file_1.yaml", "w", encoding="utf-8") as file:
        file.write("# comment-only config\n# still intentionally empty\n")
    try:
        out = yaml_load("test_file_1.yaml")
    finally:
        _remove_files(["test_file_1.yaml"])

    assert out == {}


def test_load_yaml_comment_only_include(test_file1):
    include_str = "sastt: !include ./test_file2.yaml"
    output_file_1 = test_file1 + "\n" + include_str
    with open("test_file_1.yaml", "w", encoding="utf-8") as file:
        file.write(output_file_1)
    with open("test_file2.yaml", "w", encoding="utf-8") as file:
        file.write("# comment-only config\n# nothing to merge\n")
    try:
        out = yaml_load("test_file_1.yaml")
    finally:
        _remove_files(["test_file_1.yaml", "test_file2.yaml"])

    assert "eiger" in out
    assert "sastt" not in out
    assert len(out) == 1


@pytest.mark.parametrize("process_includes", [True, False])
@pytest.mark.parametrize("document", ["[]", "[samx]", "0", "42", "false", "samx"])
def test_load_yaml_rejects_non_mapping_root(document, process_includes):
    with pytest.raises(yaml.YAMLError, match="mapping at its root"):
        yaml_load(io.StringIO(document), process_includes=process_includes)


@pytest.mark.parametrize("process_includes", [True, False])
@pytest.mark.parametrize(
    "value", [None, 0, False, "samx", "__include__", [], [None], [1, "__include__", None], {}]
)
def test_load_yaml_preserves_ordinary_mapping_values(value, process_includes):
    document = yaml.safe_dump({"samx": value})

    assert yaml_load(io.StringIO(document), process_includes=process_includes) == {"samx": value}


@pytest.mark.parametrize("process_includes", [True, False])
def test_load_yaml_include_list_with_ordinary_values(tmp_path, process_includes):
    included = tmp_path / "included.yaml"
    included.write_text("samx: {}\n", encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text(
        "group:\n  - null\n  - __include__\n  - !include included.yaml\n", encoding="utf-8"
    )

    out = yaml_load(str(config), process_includes=process_includes)

    assert out == ({"samx": {}} if process_includes else {"group": [None, "__include__"]})


@pytest.mark.parametrize("path_type", ["directory", "fifo"])
def test_load_yaml_rejects_non_regular_include(tmp_path, path_type):
    included = tmp_path / "included.yaml"
    if path_type == "directory":
        included.mkdir()
    else:
        if not hasattr(os, "mkfifo"):
            pytest.skip("Named pipes are unavailable on this platform.")
        os.mkfifo(included)

    with mock.patch(
        "bec_lib.bec_yaml_loader.open", side_effect=AssertionError("Cannot open a non-regular file")
    ) as open_file:
        with pytest.raises(yaml.YAMLError, match="included.yaml.*not a regular file"):
            yaml_load(io.StringIO(f"group: !include {included}\n"))

    open_file.assert_not_called()


def test_load_yaml_allows_symlink_include(tmp_path):
    target = tmp_path / "target.yaml"
    target.write_text("samx: {}\n", encoding="utf-8")
    included = tmp_path / "included.yaml"
    included.symlink_to(target)
    config = tmp_path / "config.yaml"
    config.write_text("group: !include included.yaml\n", encoding="utf-8")

    assert yaml_load(str(config)) == {"samx": {}}


def test_load_yaml_preserves_missing_include_error(tmp_path):
    included = tmp_path / "missing.yaml"

    with pytest.raises(FileNotFoundError) as exc:
        yaml_load(io.StringIO(f"group: !include {included}\n"))

    assert exc.value.filename == str(included)


@pytest.mark.parametrize("in_list", [True, False])
@pytest.mark.parametrize(
    "marker",
    [
        None,
        "included.yaml",
        [],
        {},
        {"data": None, "filename": "included.yaml"},
        {"data": [], "filename": "included.yaml"},
        {"data": {}},
        {"filename": "included.yaml"},
    ],
)
def test_load_yaml_rejects_malformed_include_marker(marker, in_list):
    value = {"__include__": marker}
    document = yaml.safe_dump({"group": [value] if in_list else value})

    with pytest.raises(yaml.YAMLError, match="Invalid include marker.*Use !include"):
        yaml_load(io.StringIO(document))


@pytest.mark.parametrize("in_list", [True, False])
def test_load_yaml_strips_malformed_include_marker_when_disabled(in_list):
    value = {"__include__": None}
    document = yaml.safe_dump({"group": [value] if in_list else value})

    assert yaml_load(io.StringIO(document), process_includes=False) == {}
