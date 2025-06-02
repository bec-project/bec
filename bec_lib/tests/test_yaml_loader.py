import os

import pytest

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
