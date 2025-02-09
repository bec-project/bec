from __future__ import annotations

import builtins
import copy
import os
import time
from typing import TYPE_CHECKING

import bec_lib
from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_lib.scans import Scans

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.alarm_handler import Alarms

dir_path = os.path.dirname(bec_lib.__file__)

logger = bec_logger.logger

# pylint: disable=no-member
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access


def queue_is_empty(queue) -> bool:  # pragma: no cover
    if not queue:
        return True
    if not queue["primary"].get("info"):
        return True
    return False


def get_queue(bec):  # pragma: no cover
    return bec.queue.connector.get(MessageEndpoints.scan_queue_status())


def wait_for_empty_queue(bec):  # pragma: no cover
    while not get_queue(bec):
        time.sleep(1)
    while not queue_is_empty(get_queue(bec).content["queue"]):
        time.sleep(1)
        logger.info(bec.queue)
    while get_queue(bec).content["queue"]["primary"]["status"] != "RUNNING":
        time.sleep(1)
        logger.info(bec.queue)


class ScansMock(Scans):
    def _import_scans(self):
        pass

    def open_scan_def(self):
        pass

    def close_scan_def(self):
        pass

    def close_scan_group(self):
        pass

    def umv(self):
        pass


class ClientMock(BECClient):
    def _load_scans(self):
        self.scans = ScansMock(self)
        builtins.scans = self.scans

    # def _start_services(self):
    #     pass

    def _start_metrics_emitter(self):
        pass

    def _start_update_service_info(self):
        pass


def get_device_info_mock(device_name, device_class) -> messages.DeviceInfoMessage:

    positioner_info = {
        "device_info": {
            "device_base_class": "positioner",
            "device_class": "SimPositioner",
            "signals": {
                "readback": {
                    "component_name": "readback",
                    "obj_name": device_name,
                    "kind_int": 5,
                    "kind_str": "hinted",
                    "doc": "readback doc string",
                    "describe": {
                        "source": f"SIM:{device_name}",
                        "dtype": "integer",
                        "shape": [],
                        "precision": 3,
                    },
                    "metadata": {
                        "connected": True,
                        "read_access": True,
                        "write_access": False,
                        "timestamp": 0,
                        "status": None,
                        "severity": None,
                        "precision": None,
                    },
                },
                "setpoint": {
                    "component_name": "setpoint",
                    "obj_name": f"{device_name}_setpoint",
                    "kind_int": 1,
                    "kind_str": "normal",
                    "doc": "setpoint doc string",
                    "describe": {
                        "source": f"SIM:{device_name}_setpoint",
                        "dtype": "integer",
                        "shape": [],
                        "precision": 3,
                    },
                    "metadata": {
                        "connected": True,
                        "read_access": True,
                        "write_access": True,
                        "timestamp": 0,
                        "status": None,
                        "severity": None,
                        "precision": None,
                    },
                },
                "motor_is_moving": {
                    "component_name": "motor_is_moving",
                    "obj_name": f"{device_name}_motor_is_moving",
                    "kind_int": 1,
                    "kind_str": "normal",
                    "doc": "motor_is_moving doc string",
                    "describe": {
                        "source": f"SIM:{device_name}_motor_is_moving",
                        "dtype": "integer",
                        "shape": [],
                        "precision": 3,
                    },
                    "metadata": {
                        "connected": True,
                        "read_access": True,
                        "write_access": True,
                        "timestamp": 0,
                        "status": None,
                        "severity": None,
                        "precision": None,
                    },
                },
                "velocity": {
                    "component_name": "velocity",
                    "obj_name": f"{device_name}_velocity",
                    "kind_int": 2,
                    "kind_str": "config",
                    "doc": "velocity doc string",
                    "describe": {
                        "source": f"SIM:{device_name}_velocity",
                        "dtype": "integer",
                        "shape": [],
                        "precision": 3,
                    },
                    "metadata": {
                        "connected": True,
                        "read_access": True,
                        "write_access": True,
                        "timestamp": 0,
                        "status": None,
                        "severity": None,
                        "precision": None,
                    },
                },
                "acceleration": {
                    "component_name": "acceleration",
                    "obj_name": f"{device_name}_acceleration",
                    "kind_int": 2,
                    "kind_str": "config",
                    "doc": "acceleration doc string",
                    "describe": {
                        "source": f"SIM:{device_name}_acceleration",
                        "dtype": "integer",
                        "shape": [],
                        "precision": 3,
                    },
                    "metadata": {
                        "connected": True,
                        "read_access": True,
                        "write_access": True,
                        "timestamp": 0,
                        "status": None,
                        "severity": None,
                        "precision": None,
                    },
                },
                "high_limit_travel": {
                    "component_name": "high_limit_travel",
                    "obj_name": f"{device_name}_high_limit_travel",
                    "kind_int": 2,
                    "kind_str": "config",
                    "doc": "",
                    "describe": {
                        "source": f"SIM:{device_name}_high_limit_travel",
                        "dtype": "integer",
                        "shape": [],
                        "precision": 3,
                    },
                    "metadata": {
                        "connected": True,
                        "read_access": True,
                        "write_access": True,
                        "timestamp": 0,
                        "status": None,
                        "severity": None,
                        "precision": None,
                    },
                },
                "low_limit_travel": {
                    "component_name": "low_limit_travel",
                    "obj_name": f"{device_name}_low_limit_travel",
                    "kind_int": 2,
                    "kind_str": "config",
                    "doc": "",
                    "describe": {
                        "source": f"SIM:{device_name}_low_limit_travel",
                        "dtype": "integer",
                        "shape": [],
                        "precision": 3,
                    },
                    "metadata": {
                        "connected": True,
                        "read_access": True,
                        "write_access": True,
                        "timestamp": 0,
                        "status": None,
                        "severity": None,
                        "precision": None,
                    },
                },
                "unused": {
                    "component_name": "unused",
                    "obj_name": f"{device_name}_unused",
                    "kind_int": 0,
                    "kind_str": "omitted",
                    "doc": "",
                    "describe": {
                        "source": f"SIM:{device_name}_unused",
                        "dtype": "integer",
                        "shape": [],
                        "precision": 3,
                    },
                    "metadata": {
                        "connected": True,
                        "read_access": True,
                        "write_access": True,
                        "timestamp": 0,
                        "status": None,
                        "severity": None,
                        "precision": None,
                    },
                },
            },
            "hints": {"fields": [device_name]},
            "describe": {
                device_name: {
                    "source": f"SIM:{device_name}",
                    "dtype": "integer",
                    "shape": [],
                    "precision": 3,
                },
                f"{device_name}_setpoint": {
                    "source": f"SIM:{device_name}_setpoint",
                    "dtype": "integer",
                    "shape": [],
                    "precision": 3,
                },
                f"{device_name}_motor_is_moving": {
                    "source": f"SIM:{device_name}_motor_is_moving",
                    "dtype": "integer",
                    "shape": [],
                    "precision": 3,
                },
            },
            "describe_configuration": {
                f"{device_name}_velocity": {
                    "source": f"SIM:{device_name}_velocity",
                    "dtype": "integer",
                    "shape": [],
                },
                f"{device_name}_acceleration": {
                    "source": f"SIM:{device_name}_acceleration",
                    "dtype": "integer",
                    "shape": [],
                },
            },
            "sub_devices": [],
            "custom_user_access": {},
        }
    }
    positioner_info_with_user_access = copy.deepcopy(positioner_info)
    positioner_info_with_user_access["device_info"]["custom_user_access"].update(
        {
            "dummy_controller": {
                "device_class": "DummyController",
                "info": {
                    "_fun_with_specific_args": {
                        "type": "func",
                        "doc": None,
                        "signature": [
                            {
                                "name": "arg1",
                                "kind": "POSITIONAL_OR_KEYWORD",
                                "default": "_empty",
                                "annotation": "float",
                            },
                            {
                                "name": "arg2",
                                "kind": "POSITIONAL_OR_KEYWORD",
                                "default": "_empty",
                                "annotation": "int",
                            },
                        ],
                    },
                    "_func_with_args": {
                        "type": "func",
                        "doc": None,
                        "signature": [
                            {
                                "name": "args",
                                "kind": "VAR_POSITIONAL",
                                "default": "_empty",
                                "annotation": "_empty",
                            }
                        ],
                    },
                    "_func_with_args_and_kwargs": {
                        "type": "func",
                        "doc": None,
                        "signature": [
                            {
                                "name": "args",
                                "kind": "VAR_POSITIONAL",
                                "default": "_empty",
                                "annotation": "_empty",
                            },
                            {
                                "name": "kwargs",
                                "kind": "VAR_KEYWORD",
                                "default": "_empty",
                                "annotation": "_empty",
                            },
                        ],
                    },
                    "_func_with_kwargs": {
                        "type": "func",
                        "doc": None,
                        "signature": [
                            {
                                "name": "kwargs",
                                "kind": "VAR_KEYWORD",
                                "default": "_empty",
                                "annotation": "_empty",
                            }
                        ],
                    },
                    "_func_without_args_kwargs": {"type": "func", "doc": None, "signature": []},
                    "controller_show_all": {
                        "type": "func",
                        "doc": "dummy controller show all\n\n        Raises:\n            in: _description_\n            LimitError: _description_\n\n        Returns:\n            _type_: _description_\n        ",
                        "signature": [],
                    },
                    "some_var": {"type": "int"},
                    "some_var_property": {"type": "NoneType"},
                },
                "sim_state": {"type": "dict"},
            }
        }
    )
    device_info = {
        "rt_controller": messages.DeviceInfoMessage(
            device="rt_controller", info=positioner_info_with_user_access
        ),
        "samx": messages.DeviceInfoMessage(device="samx", info=positioner_info),
        "dyn_signals": messages.DeviceInfoMessage(
            device="dyn_signals",
            info={
                "device_info": {
                    "device_dotted_name": "dyn_signals",
                    "device_attr_name": "dyn_signals",
                    "device_base_class": "device",
                    "device_class": "SimDevice",
                    "signals": [],
                    "hints": {"fields": []},
                    "describe": {
                        "dyn_signals_messages_message1": {
                            "source": "SIM:dyn_signals_messages_message1",
                            "dtype": "integer",
                            "shape": [],
                            "precision": 3,
                        },
                        "dyn_signals_messages_message2": {
                            "source": "SIM:dyn_signals_messages_message2",
                            "dtype": "integer",
                            "shape": [],
                            "precision": 3,
                        },
                        "dyn_signals_messages_message3": {
                            "source": "SIM:dyn_signals_messages_message3",
                            "dtype": "integer",
                            "shape": [],
                            "precision": 3,
                        },
                        "dyn_signals_messages_message4": {
                            "source": "SIM:dyn_signals_messages_message4",
                            "dtype": "integer",
                            "shape": [],
                            "precision": 3,
                        },
                        "dyn_signals_messages_message5": {
                            "source": "SIM:dyn_signals_messages_message5",
                            "dtype": "integer",
                            "shape": [],
                            "precision": 3,
                        },
                    },
                    "describe_configuration": {},
                    "sub_devices": [
                        {
                            "device_name": "dyn_signals_messages",
                            "device_info": {
                                "device_attr_name": "messages",
                                "device_dotted_name": "messages",
                                "device_base_class": "device",
                                "device_class": "SimDevice",
                                "signals": {
                                    "message1": {
                                        "component_name": "message1",
                                        "obj_name": "dyn_signals_messages_message1",
                                        "kind_int": 1,
                                        "kind_str": "normal",
                                        "metadata": {
                                            "connected": True,
                                            "read_access": True,
                                            "write_access": True,
                                            "timestamp": 0,
                                            "status": None,
                                            "severity": None,
                                            "precision": None,
                                        },
                                    },
                                    "message2": {
                                        "component_name": "message2",
                                        "obj_name": "dyn_signals_messages_message2",
                                        "kind_int": 1,
                                        "kind_str": "normal",
                                        "metadata": {
                                            "connected": True,
                                            "read_access": True,
                                            "write_access": True,
                                            "timestamp": 0,
                                            "status": None,
                                            "severity": None,
                                            "precision": None,
                                        },
                                    },
                                    "message3": {
                                        "component_name": "message3",
                                        "obj_name": "dyn_signals_messages_message3",
                                        "kind_int": 1,
                                        "kind_str": "normal",
                                        "metadata": {
                                            "connected": True,
                                            "read_access": True,
                                            "write_access": True,
                                            "timestamp": 0,
                                            "status": None,
                                            "severity": None,
                                            "precision": None,
                                        },
                                    },
                                    "message4": {
                                        "component_name": "message4",
                                        "obj_name": "dyn_signals_messages_message4",
                                        "kind_int": 1,
                                        "kind_str": "normal",
                                        "metadata": {
                                            "connected": True,
                                            "read_access": True,
                                            "write_access": True,
                                            "timestamp": 0,
                                            "status": None,
                                            "severity": None,
                                            "precision": None,
                                        },
                                    },
                                    "message5": {
                                        "component_name": "message5",
                                        "obj_name": "dyn_signals_messages_message5",
                                        "kind_int": 1,
                                        "kind_str": "normal",
                                        "metadata": {
                                            "connected": True,
                                            "read_access": True,
                                            "write_access": True,
                                            "timestamp": 0,
                                            "status": None,
                                            "severity": None,
                                            "precision": None,
                                        },
                                    },
                                },
                                "hints": {"fields": []},
                                "describe": {
                                    "dyn_signals_messages_message1": {
                                        "source": "SIM:dyn_signals_messages_message1",
                                        "dtype": "integer",
                                        "shape": [],
                                        "precision": 3,
                                    },
                                    "dyn_signals_messages_message2": {
                                        "source": "SIM:dyn_signals_messages_message2",
                                        "dtype": "integer",
                                        "shape": [],
                                        "precision": 3,
                                    },
                                    "dyn_signals_messages_message3": {
                                        "source": "SIM:dyn_signals_messages_message3",
                                        "dtype": "integer",
                                        "shape": [],
                                        "precision": 3,
                                    },
                                    "dyn_signals_messages_message4": {
                                        "source": "SIM:dyn_signals_messages_message4",
                                        "dtype": "integer",
                                        "shape": [],
                                        "precision": 3,
                                    },
                                    "dyn_signals_messages_message5": {
                                        "source": "SIM:dyn_signals_messages_message5",
                                        "dtype": "integer",
                                        "shape": [],
                                        "precision": 3,
                                    },
                                },
                                "describe_configuration": {},
                                "sub_devices": [],
                                "custom_user_access": {},
                            },
                        }
                    ],
                    "custom_user_access": {},
                }
            },
        ),
    }
    if device_name in device_info:
        return device_info[device_name]

    device_base_class = "positioner" if device_class == "SimPositioner" else "signal"
    if device_base_class == "positioner":
        signals = positioner_info["device_info"]["signals"]
    elif device_base_class == "signal":
        signals = {
            device_name: {
                "metadata": {
                    "connected": True,
                    "read_access": True,
                    "write_access": False,
                    "timestamp": 0,
                    "status": None,
                    "severity": None,
                    "precision": None,
                }
            }
        }
    else:
        signals = {}
    dev_info = {
        "device_name": device_name,
        "device_info": {
            "device_dotted_name": device_name,
            "device_attr_name": device_name,
            "device_base_class": device_base_class,
            "device_class": device_class.__class__.__name__,
            "signals": signals,
        },
        "custom_user_acces": {},
    }

    return messages.DeviceInfoMessage(device=device_name, info=dev_info, metadata={})


class DMClientMock(DeviceManagerBase):
    def _get_device_info(self, device_name) -> messages.DeviceInfoMessage:
        return get_device_info_mock(device_name, self.get_device(device_name)["deviceClass"])

    def get_device(self, device_name):
        for dev in self._session["devices"]:
            if dev["name"] == device_name:
                return dev


class PipelineMock:  # pragma: no cover

    def __init__(self, connector) -> None:
        self._pipe_buffer = []
        self._connector = connector

    def execute(self):
        if not self._connector.store_data:
            self._pipe_buffer = []
            return []
        res = [
            getattr(self._connector, method)(*args, **kwargs)
            for method, args, kwargs in self._pipe_buffer
        ]
        self._pipe_buffer = []
        return res


class SignalMock:  # pragma: no cover
    def __init__(self) -> None:
        self.is_set = False

    def set(self):
        self.is_set = True


class ConnectorMock(RedisConnector):  # pragma: no cover
    def __init__(self, bootstrap_server: list[str] | str = "localhost:0000", store_data=True):
        if isinstance(bootstrap_server, list):
            bootstrap_server = bootstrap_server[0]
        if ":" not in bootstrap_server:
            bootstrap_server = f"{bootstrap_server}:0000"
        super().__init__(bootstrap_server)
        self.message_sent = []
        self._get_buffer = {}
        self.store_data = store_data

    def raise_alarm(
        self, severity: Alarms, alarm_type: str, source: str, msg: dict, metadata: dict
    ):
        messages.AlarmMessage(
            severity=severity, alarm_type=alarm_type, source=source, msg=msg, metadata=metadata
        )

    def log_error(self, *args, **kwargs):
        pass

    def shutdown(self):
        pass

    def register(self, *args, **kwargs):
        pass

    def unregister(self, *args, **kwargs):
        pass

    def poll_messages(self, *args, **kwargs):
        pass

    def keys(self, *args, **kwargs):
        return []

    def set(self, topic, msg, pipe=None, expire: int = None):
        if pipe:
            pipe._pipe_buffer.append(("set", (topic.endpoint, msg), {"expire": expire}))
            return
        self.message_sent.append({"queue": topic, "msg": msg, "expire": expire})

    def raw_send(self, topic, msg, pipe=None):
        if pipe:
            pipe._pipe_buffer.append(("send", (topic.endpoint, msg), {}))
            return
        self.message_sent.append({"queue": topic, "msg": msg})

    def send(self, topic, msg, pipe=None):
        if not isinstance(msg, messages.BECMessage):
            raise TypeError("Message must be a BECMessage")
        return self.raw_send(topic, msg, pipe)

    def set_and_publish(self, topic, msg, pipe=None, expire: int = None):
        if pipe:
            pipe._pipe_buffer.append(("set_and_publish", (topic.endpoint, msg), {"expire": expire}))
            return
        self.message_sent.append({"queue": topic, "msg": msg, "expire": expire})

    def lpush(self, topic, msg, pipe=None, max_size=None):
        if pipe:
            pipe._pipe_buffer.append(("lpush", (topic, msg), {}))
            return

    def rpush(self, topic, msg, pipe=None):
        if pipe:
            pipe._pipe_buffer.append(("rpush", (topic, msg), {}))
            return
        pass

    def lrange(self, topic, start, stop, pipe=None):
        if pipe:
            pipe._pipe_buffer.append(("lrange", (topic, start, stop), {}))
            return
        return []

    def get(self, topic, pipe=None):
        if isinstance(topic, EndpointInfo):
            topic = topic.endpoint
        if pipe:
            pipe._pipe_buffer.append(("get", (topic,), {}))
            return
        val = self._get_buffer.get(topic)
        if isinstance(val, list):
            return val.pop(0)
        self._get_buffer.pop(topic, None)
        return val

    def pipeline(self):
        return PipelineMock(self)

    def delete(self, topic, pipe=None):
        if pipe:
            pipe._pipe_buffer.append(("delete", (topic,), {}))
            return

    def lset(self, topic: str, index: int, msgs: str, pipe=None) -> None:
        if pipe:
            pipe._pipe_buffer.append(("lrange", (topic, index, msgs), {}))
            return

    def producer(self):
        return self

    def execute_pipeline(self, pipeline):
        pipeline.execute()

    def xadd(self, topic, msg_dict, max_size=None, pipe=None, expire: int = None):
        if pipe:
            pipe._pipe_buffer.append(("xadd", (topic, msg_dict), {"expire": expire}))
            return
        pass

    def xread(self, topic, id=None, count=None, block=None, pipe=None, from_start=False):
        if pipe:
            pipe._pipe_buffer.append(
                ("xread", (topic, id, count, block), {"from_start": from_start})
            )
            return
        return []

    def xrange(self, topic, min="-", max="+", pipe=None):
        if pipe:
            pipe._pipe_buffer.append(("xrange", (topic, min, max), {}))
            return
        return []

    def can_connect(self):
        return True

    def redis_server_is_running(self):
        return True
