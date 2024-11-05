from unittest import mock

import pytest

from bec_server.scan_server.blissdata_utils import (
    _dev_desc_to_channels,
    bec_scan_info_to_blissdata_scan_info,
)

devs_desc = {
    "samx": {
        "device_attr_name": "",
        "device_dotted_name": "",
        "device_base_class": "positioner",
        "signals": {
            "readback": {
                "component_name": "readback",
                "obj_name": "samx",
                "kind_int": 5,
                "kind_str": "Kind.hinted",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": False,
                    "timestamp": 1724751187.3775752,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "setpoint": {
                "component_name": "setpoint",
                "obj_name": "samx_setpoint",
                "kind_int": 1,
                "kind_str": "Kind.normal",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.377618,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "motor_is_moving": {
                "component_name": "motor_is_moving",
                "obj_name": "samx_motor_is_moving",
                "kind_int": 1,
                "kind_str": "Kind.normal",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3776505,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "velocity": {
                "component_name": "velocity",
                "obj_name": "samx_velocity",
                "kind_int": 2,
                "kind_str": "Kind.config",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3776772,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "acceleration": {
                "component_name": "acceleration",
                "obj_name": "samx_acceleration",
                "kind_int": 2,
                "kind_str": "Kind.config",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3776987,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "tolerance": {
                "component_name": "tolerance",
                "obj_name": "samx_tolerance",
                "kind_int": 2,
                "kind_str": "Kind.config",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3777187,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "high_limit_travel": {
                "component_name": "high_limit_travel",
                "obj_name": "samx_high_limit_travel",
                "kind_int": 0,
                "kind_str": "Kind.omitted",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3777895,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "low_limit_travel": {
                "component_name": "low_limit_travel",
                "obj_name": "samx_low_limit_travel",
                "kind_int": 0,
                "kind_str": "Kind.omitted",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3778143,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "unused": {
                "component_name": "unused",
                "obj_name": "samx_unused",
                "kind_int": 0,
                "kind_str": "Kind.omitted",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3778307,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
        },
        "hints": {"fields": ["samx"]},
        "describe": {
            "samx": {"source": "SIM:samx", "dtype": "float64", "shape": [], "precision": 3},
            "samx_setpoint": {
                "source": "SIM:samx_setpoint",
                "dtype": "float64",
                "shape": [],
                "precision": 3,
            },
            "samx_motor_is_moving": {
                "source": "SIM:samx_motor_is_moving",
                "dtype": "int64",
                "shape": [],
                "precision": 3,
            },
        },
        "describe_configuration": {
            "samx_velocity": {
                "source": "SIM:samx_velocity",
                "dtype": "integer",
                "shape": [],
                "precision": 3,
            },
            "samx_acceleration": {
                "source": "SIM:samx_acceleration",
                "dtype": "integer",
                "shape": [],
                "precision": 3,
            },
            "samx_tolerance": {
                "source": "SIM:samx_tolerance",
                "dtype": "number",
                "shape": [],
                "precision": 3,
            },
        },
        "sub_devices": [],
        "custom_user_access": {
            "dummy_controller": {
                "_func_with_args": {"type": "func", "doc": None},
                "_func_with_args_and_kwargs": {"type": "func", "doc": None},
                "_func_with_kwargs": {"type": "func", "doc": None},
                "_func_without_args_kwargs": {"type": "func", "doc": None},
                "controller_show_all": {
                    "type": "func",
                    "doc": "dummy controller show all\n\n        Raises:\n            in: _description_\n            LimitError: _description_\n\n        Returns:\n            _type_: _description_\n        ",
                },
                "some_var": {"type": "int"},
                "some_var_property": {"type": "NoneType"},
            },
            "registered_proxies": {
                "type": "func",
                "doc": "Dictionary of registered signal_names and proxies.",
            },
            "sim": {
                "get_models": {
                    "type": "func",
                    "doc": "\n        Method to get the all available simulation models.\n        ",
                },
                "params": {"type": "dict"},
                "select_model": {
                    "type": "func",
                    "doc": "\n        Method to select the active simulation model.\n        It will initiate the model_cls and parameters for the model.\n\n        Args:\n            model (str): Name of the simulation model to select.\n\n        ",
                },
                "show_all": {
                    "type": "func",
                    "doc": "Returns a summary about the active simulation and available methods.",
                },
                "sim_params": {"type": "dict"},
                "sim_select_model": {"type": "func", "doc": "Select the active simulation model."},
            },
        },
    },
    "bpm4i": {
        "device_attr_name": "",
        "device_dotted_name": "",
        "device_base_class": "device",
        "signals": {
            "readback": {
                "component_name": "readback",
                "obj_name": "bpm4i",
                "kind_int": 5,
                "kind_str": "Kind.hinted",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": False,
                    "timestamp": 1724751187.417852,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            }
        },
        "hints": {"fields": ["bpm4i"]},
        "describe": {
            "bpm4i": {"source": "SIM:bpm4i", "dtype": "uint32", "shape": [], "precision": 3}
        },
        "describe_configuration": {},
        "sub_devices": [],
        "custom_user_access": {
            "registered_proxies": {"type": "dict"},
            "sim": {
                "get_models": {
                    "type": "func",
                    "doc": "\n        Method to get the all available simulation models.\n        ",
                },
                "params": {"type": "dict"},
                "select_model": {
                    "type": "func",
                    "doc": "\n        Method to select the active simulation model.\n        It will initiate the model_cls and parameters for the model.\n\n        Args:\n            model (str): Name of the simulation model to select.\n\n        ",
                },
                "show_all": {
                    "type": "func",
                    "doc": "Returns a summary about the active simulation and available methods.",
                },
                "sim_params": {"type": "dict"},
                "sim_select_model": {"type": "func", "doc": "Select the active simulation model."},
            },
        },
    },
    "mca": {
        "device_attr_name": "",
        "device_dotted_name": "",
        "device_base_class": "device",
        "signals": {
            "exp_time": {
                "component_name": "exp_time",
                "obj_name": "mca_exp_time",
                "kind_int": 2,
                "kind_str": "Kind.config",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3253682,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "frames": {
                "component_name": "frames",
                "obj_name": "mca_frames",
                "kind_int": 2,
                "kind_str": "Kind.config",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3254642,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "shape": {
                "component_name": "shape",
                "obj_name": "mca_shape",
                "kind_int": 2,
                "kind_str": "Kind.config",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": True,
                    "timestamp": 1724751187.3255262,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
            "readback": {
                "component_name": "readback",
                "obj_name": "mca_readback",
                "kind_int": 5,
                "kind_str": "Kind.hinted",
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": False,
                    "timestamp": 1724751187.3255718,
                    "status": None,
                    "severity": None,
                    "precision": None,
                },
            },
        },
        "hints": {"fields": ["mca_readback"]},
        "describe": {
            "mca_readback": {
                "source": "SIM:mca_readback",
                "dtype": "uint16",
                "shape": [1000],
                "precision": 3,
            }
        },
        "describe_configuration": {
            "mca_exp_time": {
                "source": "SIM:mca_exp_time",
                "dtype": "float64",
                "shape": [],
                "precision": 3,
            },
            "mca_frames": {
                "source": "SIM:mca_frames",
                "dtype": "int64",
                "shape": [],
                "precision": 3,
            },
            "mca_shape": {
                "source": "SIM:mca_shape",
                "dtype": "array",
                "shape": [1],
                "precision": 3,
            },
        },
        "sub_devices": [],
        "custom_user_access": {
            "registered_proxies": {"type": "dict"},
            "sim": {
                "get_models": {
                    "type": "func",
                    "doc": "\n        Method to get the all available simulation models.\n        ",
                },
                "params": {"type": "dict"},
                "select_model": {
                    "type": "func",
                    "doc": "\n        Method to select the active simulation model.\n        It will initiate the model_cls and parameters for the model.\n\n        Args:\n            model (str): Name of the simulation model to select.\n\n        ",
                },
                "show_all": {
                    "type": "func",
                    "doc": "Returns a summary about the active simulation and available methods.",
                },
                "sim_params": {"type": "dict"},
                "sim_select_model": {"type": "func", "doc": "Select the active simulation model."},
            },
        },
    },
    "ring_current_sim": {
        "device_attr_name": "",
        "device_dotted_name": "",
        "device_base_class": "signal",
        "signals": {
            "ring_current_sim": {
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": False,
                    "timestamp": 1724751187.900322,
                    "status": None,
                    "severity": None,
                    "precision": None,
                }
            }
        },
        "hints": {"fields": []},
        "describe": {
            "ring_current_sim": {
                "source": "SIM:ring_current_sim",
                "dtype": "float64",
                "shape": [],
                "precision": 3,
            }
        },
        "describe_configuration": {
            "ring_current_sim": {
                "source": "SIM:ring_current_sim",
                "dtype": "float64",
                "shape": [],
                "precision": 3,
            }
        },
        "sub_devices": [],
        "custom_user_access": {},
    },
}

bec_scan_info = {
    "readout_priority": {
        "monitored": ["mca", "ring_current_sim", "bpm4i", "samx"],
        "baseline": ["samy"],
        "async": ["eiger"],
        "continuous": [],
        "on_request": [],
    },
    "DIID": 3,
    "file_suffix": None,
    "file_directory": None,
    "user_metadata": {},
    "RID": "433ad817-ee3e-4d60-9da1-6ce4b784f523",
    "scan_id": "b3e68152-1f75-4787-a080-8291e56aff83",
    "queue_id": "017d0c4f-8e92-480e-b497-1fbbeeba7131",
    "scan_motors": ["samx"],
    "num_points": 10,
    "positions": [
        [0.0],
        [0.1111111111111111],
        [0.2222222222222222],
        [0.3333333333333333],
        [0.4444444444444444],
        [0.5555555555555556],
        [0.6666666666666666],
        [0.7777777777777777],
        [0.8888888888888888],
        [1.0],
    ],
    "scan_name": "line_scan",
    "scan_type": "step",
    "scan_number": 18,
    "dataset_number": 18,
    "exp_time": 0.01,
    "frames_per_trigger": 1,
    "settling_time": 0,
    "readout_time": 0,
    "acquisition_config": {"default": {"exp_time": 0.01, "readout_time": 0}},
    "scan_report_devices": ["samx"],
    "monitor_sync": "bec",
    "scan_msgs": [
        "metadata={'file_suffix': None, 'file_directory': None, 'user_metadata': {}, 'RID': '433ad817-ee3e-4d60-9da1-6ce4b784f523'} scan_type='line_scan' parameter={'args': {'samx': [0, 1]}, 'kwargs': {'exp_time': 0.01, 'steps': 10, 'relative': False, 'system_config': {'file_suffix': None, 'file_directory': None}}} queue='primary'"
    ],
    "args": {"samx": [0, 1]},
    "kwargs": {
        "exp_time": 0.01,
        "steps": 10,
        "relative": False,
        "system_config": {"file_suffix": None, "file_directory": None},
    },
}


def test_flint_scan_info():
    """Ensure devices description is properly turned into Blissdata scan_info structure"""
    connector = mock.Mock()

    def get_device_info(endpoint):
        dev_name = endpoint.endpoint.split("/")[-1]
        dev_info = mock.Mock()
        dev_info.info = {"device_info": devs_desc[dev_name]}
        return dev_info

    connector.get.side_effect = get_device_info

    devname_channels, scan_info = bec_scan_info_to_blissdata_scan_info(bec_scan_info, connector)

    axis_chans = devname_channels["samx"]
    for channel_name in ["axis:samx", "axis:samx:setpoint", "axis:samx:motor_is_moving"]:
        assert channel_name in [chan.name for chan in axis_chans]
    assert len(devname_channels["bpm4i"]) == 1
    assert devname_channels["bpm4i"][0].name == "bpm4i"
    assert len(devname_channels["mca"]) == 1
    assert devname_channels["mca"][0].name == "mca:readback"
    assert len(devname_channels["ring_current_sim"]) == 1
    assert devname_channels["ring_current_sim"][0].name == "ring_current_sim"
    assert scan_info["devices"]["axis"]
