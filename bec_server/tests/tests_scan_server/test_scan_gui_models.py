from typing import Literal

import pytest
from pydantic import ValidationError

from bec_server.scan_server.scan_gui_models import GUIConfig
from bec_server.scan_server.scans import ScanArgType, ScanBase


class GoodScan(ScanBase):  # pragma: no cover
    scan_name = "good_scan"
    required_kwargs = ["steps", "relative"]
    arg_input = {
        "device": ScanArgType.DEVICE,
        "start": ScanArgType.FLOAT,
        "stop": ScanArgType.FLOAT,
    }
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": None}
    gui_config = {
        "Scan Parameters": [
            "steps",
            "exp_time",
            "relative",
            "burst_at_each_point",
            "optim_trajectory",
        ]
    }

    def __init__(
        self,
        *args,
        exp_time: float = 0,
        steps: int = None,
        relative: bool = False,
        burst_at_each_point: int = 1,
        optim_trajectory: Literal["path", None] = None,
        **kwargs,
    ):
        """
        A good scan for one or more motors.

        Args:
            *args (Device, float, float): pairs of device / start position / end position
            exp_time (float): exposure time in s. Default: 0
            steps (int): number of steps. Default: 10
            relative (bool): if True, the start and end positions are relative to the current position. Default: False
            burst_at_each_point (int): number of acquisition per point. Default: 1

        Returns:
            ScanReport


        """
        super().__init__(
            exp_time=exp_time, relative=relative, burst_at_each_point=burst_at_each_point, **kwargs
        )
        self.steps = steps

    def doing_something_good(self):
        pass


class ExtraKwarg(ScanBase):  # pragma: no cover
    scan_name = "wrong_name"
    required_kwargs = ["steps", "relative"]

    gui_config = {"Device 1": ["motor1", "start_motor1", "stop_motor1"]}

    def __init__(self, motor1: str, stop_motor1: float, **kwargs):
        """
        A scan following Fermat's spiral.

        Args:
            motor1 (DeviceBase): first motor
            start_motor1 (float): start position motor 1
            stop_motor1 (float): end position motor 1

        Returns:
            ScanReport
        """

    print("I am a wrong scan")


class WrongDocs(ScanBase):  # pragma: no cover
    scan_name = "wrong_name"
    required_kwargs = ["steps", "relative"]

    gui_config = {"Device 1": ["motor1", "start_motor1", "stop_motor1"]}

    def __init__(self, motor1: str, start_motor1: float, stop_motor1: float, **kwargs):
        """
        A scan following Fermat's spiral.

        Args:
            motor1 (DeviceBase): first motor

        Returns:
            ScanReport
        """

    print("I am a scan with wrong docs.")


def test_gui_config_good_scan_dump():
    gui_config = GUIConfig.from_dict(GoodScan)
    expected_config = {
        "scan_class_name": "GoodScan",
        "arg_group": {
            "name": "Scan Arguments",
            "bundle": 3,
            "arg_inputs": {
                "device": ScanArgType.DEVICE,
                "start": ScanArgType.FLOAT,
                "stop": ScanArgType.FLOAT,
            },
            "inputs": [
                {
                    "arg": True,
                    "name": "device",
                    "display_name": "Device",
                    "type": "device",
                    "tooltip": None,
                    "default": None,
                    "expert": False,
                },
                {
                    "arg": True,
                    "name": "start",
                    "display_name": "Start",
                    "type": "float",
                    "tooltip": None,
                    "default": None,
                    "expert": False,
                },
                {
                    "arg": True,
                    "name": "stop",
                    "display_name": "Stop",
                    "type": "float",
                    "tooltip": None,
                    "default": None,
                    "expert": False,
                },
            ],
            "min": 1,
            "max": None,
        },
        "kwarg_groups": [
            {
                "name": "Scan Parameters",
                "inputs": [
                    {
                        "arg": False,
                        "name": "steps",
                        "display_name": "Steps",
                        "type": "int",
                        "tooltip": "Number of steps",
                        "default": None,
                        "expert": False,
                    },
                    {
                        "arg": False,
                        "name": "exp_time",
                        "display_name": "Exp Time",
                        "type": "float",
                        "tooltip": "Exposure time in s",
                        "default": 0,
                        "expert": False,
                    },
                    {
                        "arg": False,
                        "name": "relative",
                        "display_name": "Relative",
                        "type": "bool",
                        "tooltip": "If True, the start and end positions are relative to the current position",
                        "default": False,
                        "expert": False,
                    },
                    {
                        "arg": False,
                        "name": "burst_at_each_point",
                        "display_name": "Burst At Each Point",
                        "type": "int",
                        "tooltip": "Number of acquisition per point",
                        "default": 1,
                        "expert": False,
                    },
                    {
                        "arg": False,
                        "default": None,
                        "display_name": "Optim Trajectory",
                        "expert": False,
                        "name": "optim_trajectory",
                        "tooltip": None,
                        "type": {"Literal": ("path", None)},
                    },
                ],
            }
        ],
    }
    assert gui_config.model_dump() == expected_config


def test_gui_config_extra_kwarg():
    with pytest.raises(ValidationError) as excinfo:
        GUIConfig.from_dict(ExtraKwarg)
    errors = excinfo.value.errors()
    assert len(errors) == 5
    assert errors[0]["type"] == ("wrong argument name")
    assert errors[1]["type"] == ("missing argument name")
    assert errors[2]["type"] == ("missing argument name")
    assert errors[3]["type"] == ("missing argument name")
    assert errors[4]["type"] == ("missing argument name")


def test_gui_config_wrong_docs():
    gui_config = GUIConfig.from_dict(WrongDocs)
    expected = {
        "scan_class_name": "WrongDocs",
        "arg_group": {
            "name": "Scan Arguments",
            "bundle": 0,
            "arg_inputs": {},
            "inputs": [],
            "min": None,
            "max": None,
        },
        "kwarg_groups": [
            {
                "name": "Device 1",
                "inputs": [
                    {
                        "arg": False,
                        "name": "motor1",
                        "type": "str",
                        "display_name": "Motor 1",
                        "tooltip": "First motor",
                        "default": "_empty",
                        "expert": False,
                    },
                    {
                        "arg": False,
                        "name": "start_motor1",
                        "type": "float",
                        "display_name": "Start Motor 1",
                        "tooltip": None,
                        "default": "_empty",
                        "expert": False,
                    },
                    {
                        "arg": False,
                        "name": "stop_motor1",
                        "type": "float",
                        "display_name": "Stop Motor 1",
                        "tooltip": None,
                        "default": "_empty",
                        "expert": False,
                    },
                ],
            }
        ],
    }
    assert gui_config.model_dump() == expected
