from __future__ import annotations

import enum
import time
import warnings
from copy import deepcopy
from typing import Any, ClassVar, Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class BECStatus(enum.Enum):
    """BEC status enum"""

    RUNNING = 2
    BUSY = 1
    IDLE = 0
    ERROR = -1


class BECMessage(BaseModel):
    """Base Model class for BEC Messages

    Args:
        msg_type (str): ClassVar for the message type, subclasses should override this.
        metadata (dict, optional): Optional dictionary with metadata for the BECMessage

    """

    msg_type: ClassVar[str]
    metadata: dict | None = Field(default_factory=dict)

    @field_validator("metadata")
    @classmethod
    def check_metadata(cls, v):
        """Validate the metadata, return empty dict if None

        Args:
            v (dict, None): Metadata dictionary
        """
        if v is None:
            return {}
        return v

    @property
    def content(self):
        """Return the content of the message"""
        content = self.__dict__.copy()
        content.pop("metadata", None)
        return content

    def __eq__(self, other):
        if not isinstance(other, BECMessage):
            # don't attempt to compare against unrelated types
            return False

        try:
            np.testing.assert_equal(self.model_dump(), other.model_dump())
        except AssertionError:
            return False

        return self.msg_type == other.msg_type and self.metadata == other.metadata

    def loads(self):
        warnings.warn(
            "BECMessage.loads() is deprecated and should not be used anymore. When calling Connector methods, it can be omitted. When a message needs to be deserialized call the appropriate function from bec_lib.serialization",
            FutureWarning,
        )
        return self

    def dumps(self):
        warnings.warn(
            "BECMessage.dumps() is deprecated and should not be used anymore. When calling Connector methods, it can be omitted. When a message needs to be serialized call the appropriate function from bec_lib.serialization",
            FutureWarning,
        )
        return self


class BundleMessage(BECMessage):
    """Message type to send a bundle of BECMessages.

    Used to bundle together various messages, i.e. used to emit data in the scan bundler.

    Args:
        messages (list): List of BECMessage objects that are bundled together
        metadata (dict, optional): Additional metadata to describe the scan

    Examples:
        >>> BundleMessage(messages=[ScanQueueMessage(...), ScanStatusMessage(...)], metadata = {...})

    """

    msg_type: ClassVar[str] = "bundle_message"
    messages: list = Field(default_factory=list[BECMessage])

    def append(self, msg: BECMessage):
        """Append a new BECMessage to the bundle"""
        if not isinstance(msg, BECMessage):
            raise AttributeError(f"Cannot append message of type {msg.__class__.__name__}")
        # pylint: disable=no-member
        self.messages.append(msg)

    def __len__(self):
        return len(self.messages)

    def __iter__(self):
        # pylint: disable=not-an-iterable
        yield from self.messages


class ScanQueueMessage(BECMessage):
    """Message type for sending scan requests to the scan queue

    Sent by the API server / user to the scan_queue topic. It will be consumed by the scan server.
        Args:
            scan_type (str): one of the registered scan types; either rpc calls or scan types defined in the scan server
            parameter (dict): required parameters for the given scan_stype
            queue (str): either "primary" or "interception"
            metadata (dict, optional): additional metadata to describe the scan
        Examples:
            >>> ScanQueueMessage(scan_type="dscan", parameter={"motor1": "samx", "from_m1:": -5, "to_m1": 5, "steps_m1": 10, "motor2": "samy", "from_m2": -5, "to_m2": 5, "steps_m2": 10, "exp_time": 0.1})
    """

    msg_type: ClassVar[str] = "scan_queue_message"
    scan_type: str
    parameter: dict
    queue: str = Field(default="primary")


class ScanQueueHistoryMessage(BECMessage):
    """Sent after removal from the active queue. Contains information about the scan.

    Called by the ScanWorker after processing the QueueInstructionItem. It can be checked by any service.

    Args:
        status (str): Current scan status
        queue_id (str): Unique queue ID
        info (dict): Dictionary containing additional information about the scan
        queue (str): Defaults to "primary" queue. Information about the queue the scan was in.
        metadata (dict, optional): Additional metadata to describe the scan

    Examples:
        >>> ScanQueueHistoryMessage(status="open", queue_id="1234", info={"positions": {"samx": 0.5, "samy": 0.5}})
    """

    msg_type: ClassVar[str] = "queue_history"
    status: str
    queue_id: str
    info: dict
    queue: str = Field(default="primary")


class ScanStatusMessage(BECMessage):
    """Message type for sending scan status updates.

    Args:
        scan_id (str): Unique scan ID
        status (Literal["open", "paused", "aborted", "halted", "closed"]) : Current scan status
        info (dict): Dictionary containing additional information about the scan
        timestamp (float, optional): Timestamp of the scan status update. If None, the current time is used.
        metadata (dict, optional): Additional metadata to describe and identify the scan.

    Examples:
        >>> ScanStatusMessage(scan_id="1234", status="open", info={"positions": {"samx": 0.5, "samy": 0.5}})
    """

    msg_type: ClassVar[str] = "scan_status"
    scan_id: str | None
    status: Literal["open", "paused", "aborted", "halted", "closed"]
    info: dict
    timestamp: float = Field(default_factory=time.time)

    def __str__(self):
        content = deepcopy(self.__dict__)
        if content["info"].get("positions"):
            content["info"]["positions"] = "..."
        return f"{self.__class__.__name__}({content, self.metadata}))"


class ScanQueueModificationMessage(BECMessage):
    """Message type for sending scan queue modifications

    Args:
        scan_id (str): Unique scan ID
        action (str): One of the actions defined in ACTIONS: ("pause", "deferred_pause", "continue", "abort", "clear", "restart", "halt", "resume")
        parameter (dict): Additional parameters for the action
        queue (str): Defaults to "primary" queue. The name of the queue that receives the modification.
        metadata (dict, optional): Additional metadata to describe and identify the scan.

    Examples:
        >>> ScanQueueModificationMessage(scan_id=scan_id, action="abort", parameter={})
    """

    msg_type: ClassVar[str] = "scan_queue_modification"
    scan_id: str | list[str] | None | list[None]
    action: Literal[
        "pause", "deferred_pause", "continue", "abort", "clear", "restart", "halt", "resume"
    ]
    parameter: dict
    queue: str = Field(default="primary")


class ScanQueueOrderMessage(BECMessage):
    """Message type for sending scan queue order modifications

    Args:
        scan_id (str): Unique scan ID
        action (str): One of the actions defined in ACTIONS: ("move_up", "move_down", "move_top", "move_bottom", "move_to")
        queue (str): Defaults to "primary" queue. The name of the queue that receives the modification.
        metadata (dict, optional): Additional metadata to describe and identify the scan.

    Examples:
        >>> ScanQueueOrderMessage(scan_id=scan_id, action="move_up")
    """

    msg_type: ClassVar[str] = "scan_queue_order"
    scan_id: str
    action: Literal["move_up", "move_down", "move_top", "move_bottom", "move_to"]
    queue: str = Field(default="primary")
    target_position: int | None = None


class ScanQueueStatusMessage(BECMessage):
    """Message type for sending scan queue status updates

    Args:
        queue (dict): Dictionary containing the current queue status. Must contain a "primary" key.
        metadata (dict, optional): Additional metadata to describe and identify the ScanQueueStatus.

    Examples:
        >>> ScanQueueStatusMessage(queue={"primary": {}}, metadata={"RID": "1234"})
    """

    msg_type: ClassVar[str] = "scan_queue_status"
    queue: dict

    @field_validator("queue")
    @classmethod
    def check_queue(cls, v):
        """Validate the queue"""
        if not isinstance(v, dict):
            raise ValueError(f"Invalid queue {v}. Must be a dictionary")
        if "primary" not in v:
            raise ValueError(f"Invalid queue {v}. Must contain a 'primary' key")
        return v


class ClientInfoMessage(BECMessage):
    """Message type for sending information to the client
    Args:
        message (str): message to the client
        show_asap (bool, optional): True if the message should be shown immediately. Defaults to True
        # Note: The option show_asap = True/False is temporary disabled until a decision is made on how to handle it. TODO #286
        RID (str, optional): Request ID forwarded from the service, if available will be used to filter on the client site. Defaults to None.
        source (str, Literal[
            "bec_ipython_client",
            "scan_server",
            "device_server",
            "scan_bundler",
            "file_writer",
            "scihub",
            "dap",
            None]
            : Source of the message. Defaults to None.
        scope (str, optional): Scope of the message; Defaults to None. One can follow
                               a pattern to filter afterwards for specific client info; e.g. "scan", "rotation"
        severity (int, optional): severity level of the message (0: INFO, 1: WARNING, 2: ERROR); Defaults to 0
    """

    msg_type: ClassVar[str] = "client_info"
    message: str
    show_asap: bool = Field(default=True)
    RID: str | None = Field(default=None)
    source: Literal[
        "bec_ipython_client",
        "scan_server",
        "device_server",
        "scan_bundler",
        "file_writer",
        "scihub",
        "dap",
        None,
    ] = Field(default=None)
    scope: str | None = Field(default=None)
    severity: int = Field(
        default=0
    )  # TODO add enum for severity levels INFO = 0, WARNING = 1, ERROR = 2


class RequestResponseMessage(BECMessage):
    """Message type for sending back decisions on the acceptance of requests

    Args:
        accepted (bool): True if the request was accepted
        message (str, dict, optional): String or dictionary describing the decision, e.g. "Invalid request"
        metadata (dict, optional): Additional metadata, defaults to None.

    Examples:
        >>> RequestResponseMessage(accepted=True, message="Request accepted")
    """

    msg_type: ClassVar[str] = "request_response"
    accepted: bool
    message: str | dict | None = Field(default=None)


class DeviceInstructionMessage(BECMessage):
    """Message type for sending device instructions to the device server

    Args:
        device (str, list[str], None): Device name, list of device names or None
        action (Literal[ "rpc",
                        "set",
                        "read",
                        "kickoff",
                        "complete",
                        "trigger",
                        "stage",
                        "unstage",
                        "pre_scan",
                        "wait",
                        "scan_report_instruction",
                        "open_scan",
                        "baseline_reading",
                        "close_scan",
                        "open_scan_def",
                        "close_scan_def",
                        "publish_data_as_read",
                        "close_scan_group",
                        ]) : Device action, note rpc calls can run any method of the device. The function name needs to be specified in parameters['func']
        parameter (dict): Parameters required for the device action
        metadata (dict, optional): Metadata to describe the conditions of the device instruction

    Examples:
        >>> DeviceInstructionMessage(device="samx", action="stage", parameter={})
    """

    msg_type: ClassVar[str] = "device_instruction"
    device: str | list[str] | None
    action: Literal[
        "rpc",
        "set",
        "read",
        "kickoff",
        "complete",
        "trigger",
        "stage",
        "unstage",
        "pre_scan",
        "wait",
        "scan_report_instruction",
        "open_scan",
        "baseline_reading",
        "close_scan",
        "open_scan_def",
        "close_scan_def",
        "publish_data_as_read",
        "close_scan_group",
    ]
    parameter: dict


class DeviceInstructionResponse(BECMessage):
    msg_type: ClassVar[str] = "device_instruction_response"
    device: str | list[str] | None
    status: Literal["completed", "running", "error"]
    error_message: str | None = None
    instruction: DeviceInstructionMessage
    instruction_id: str
    result: Any | None = None


class DeviceMessage(BECMessage):
    """Message type for sending device readings from the device server

    Args:
        signals (dict): Dictionary containing the device signals and their values
        metadata (dict, optional): Metadata to describe the conditions of the device reading

    Examples:
        >>> BECMessage.DeviceMessage(signals={'samx': {'value': 14.999033949016491, 'timestamp': 1686385306.0265112}, 'samx_setpoint': {'value': 15.0, 'timestamp': 1686385306.016806}, 'samx_motor_is_moving': {'value': 0, 'timestamp': 1686385306.026888}}}, metadata={'stream': 'primary', 'DIID': 353, 'RID': 'd3471acc-309d-43b7-8ff8-f986c3fdecf1', 'point_id': 49, 'scan_id': '8e234698-358e-402d-a272-73e168a72f66', 'queue_id': '7a232746-6c90-44f5-81f5-74ab0ea22d4a'})
    """

    msg_type: ClassVar[str] = "device_message"
    signals: dict[str, dict[Literal["value", "timestamp"], Any]]


class DeviceRPCMessage(BECMessage):
    """Message type for sending device RPC return values from the device server

    Args:
        device (str): Device name.
        return_val (Any): Return value of the RPC call.
        out (str or dict): Output of the RPC call.
        success (bool, optional): True if the RPC call was successful. Defaults to True.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "device_rpc_message"
    device: str
    return_val: Any
    out: str | dict
    success: bool = Field(default=True)


class DeviceStatusMessage(BECMessage):
    """Message type for sending device status updates from the device server

    Args:
        device (str): Device name.
        status (int): Device status.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "device_status_message"
    device: str
    status: int


class DeviceReqStatusMessage(BECMessage):
    """Message type for sending device request status updates from the device server

    Args:
        device (str): Device name.
        success (bool): True if the request was successful.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "device_req_status_message"
    device: str
    success: bool


class DeviceInfoMessage(BECMessage):
    """Message type for sending device info updates from the device server

    Args:
        device (str): Device name.
        info (dict): Device info as a dictionary.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "device_info_message"
    device: str
    info: dict


class DeviceMonitor2DMessage(BECMessage):
    """Message type for sending device monitor updates from the device server.

    The message is send from the device_server to monitor data coming from larger detector.

    Args:
        device (str): Device name.
        data (np.ndarray): Numpy array data from the monitor
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "device_monitor2d_message"
    device: str
    data: np.ndarray
    timestamp: float = Field(default_factory=time.time)

    metadata: dict | None = Field(default_factory=dict)

    # Needed for pydantic to accept numpy arrays
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("data")
    @classmethod
    def check_data(cls, v: np.ndarray):
        """Validate the entry in data. Has to be a 2D numpy array

        Args:
            v (np.ndarray): data array
        """
        if not isinstance(v, np.ndarray):
            raise ValueError(f"Invalid array type: {type(v)}. Must be a numpy array.")
        if v.ndim == 2:
            return v
        if v.ndim == 3 and v.shape[2] == 3:
            return v
        raise ValueError(
            f"Invalid dimenson {v.ndim} for numpy array. Must be a 2D array or 3D array for rgb v.shape[2]=3."
        )


class DeviceMonitor1DMessage(BECMessage):
    """Message type for sending device monitor updates from the device server.

    The message is send from the device_server to monitor data coming from larger detector.

    Args:
        device (str): Device name.
        data (np.ndarray): Numpy array data from the monitor
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "device_monitor1d_message"
    device: str
    data: np.ndarray
    timestamp: float = Field(default_factory=time.time)

    metadata: dict | None = Field(default_factory=dict)

    # Needed for pydantic to accept numpy arrays
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("data")
    @classmethod
    def check_data(cls, v: np.ndarray):
        """Validate the entry in data. Has to be a 2D numpy array

        Args:
            v (np.ndarray): data array
        """
        if not isinstance(v, np.ndarray):
            raise ValueError(f"Invalid array type: {type(v)}. Must be a numpy array.")
        if v.ndim == 1:
            return v
        raise ValueError(f"Invalid dimenson {v.ndim} for numpy array. Must be a 1D array.")


class ScanMessage(BECMessage):
    """Message type for sending scan segment data from the scan bundler

    Args:
        point_id (int): Point ID from the scan segment.
        scan_id (str): Scan ID.
        data (dict): Scan segment data.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "scan_message"
    point_id: int
    scan_id: str
    data: dict


class ScanHistoryMessage(BECMessage):
    """Message type for sending scan history data from the file writer

    Args:
        scan_id (str): Scan ID.
        scan_number (int): Scan number.
        dataset_number (int): Dataset number.
        file_path (str): Path to the file.
        exit_status (Literal["closed", "aborted", "halted"]): Exit status of the scan.
        start_time (float): Start time of the scan.
        end_time (float): End time of the scan.
        scan_name (str): Name of the scan.
        num_points (int): Number of points in the scan.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "scan_history_message"
    scan_id: str
    scan_number: int
    dataset_number: int
    file_path: str
    exit_status: Literal["closed", "aborted", "halted"]
    start_time: float
    end_time: float
    scan_name: str
    num_points: int


class ScanBaselineMessage(BECMessage):
    """Message type for sending scan baseline data from the scan bundler

    Args:
        scan_id (str): Scan ID.
        data (dict): Scan baseline data.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "scan_baseline_message"
    scan_id: str
    data: dict


class DeviceConfigMessage(BECMessage):
    """Message type for sending device config updates

    Args:
        action (Literal['add', 'set', 'update', 'reload', or 'remove'] : Update of the device config.
        config (dict, or None): Device config (add, set, update) or None (reload).
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "device_config_message"
    action: Literal["add", "set", "update", "reload", "remove"] = Field(
        default=None, validate_default=True
    )
    config: dict | None = Field(default=None)

    @model_validator(mode="after")
    @classmethod
    def check_config(cls, values):
        """Validate the config"""
        if values.action in ["add", "set", "update"] and not values.config:
            raise ValueError(f"Invalid config {values.config}. Must be a dictionary")
        return values


class LogMessage(BECMessage):
    """Log message

    Args:
        log_type (Literal["trace", "debug", "info", "success", "warning", "error", "critical", "console_log"]) : Log type.
        log_msg (dict or str): Log message.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "log_message"
    log_type: Literal[
        "trace", "debug", "info", "success", "warning", "error", "critical", "console_log"
    ]
    log_msg: dict | str


class AlarmMessage(BECMessage):
    """Alarm message

    Args:
        severity (Alarms, Literal[0,1,2]): Severity level (0-2). ALARMS.WARNING = 0, ALARMS.MINOR = 1, ALARMS.MAJOR = 2
        alarm_type (str): Type of alarm.
        source (dict): Source of the problem.
        msg (str): Problem description.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "alarm_message"
    severity: int  # TODO change once enums moved to a separate class
    alarm_type: str
    source: dict
    msg: str


class StatusMessage(BECMessage):
    """Status message

    Args:
        name (str): Name of the status.
        status (BECStatus): Value of the BECStatus enum (RUNNING = 2,  BUSY = 1, IDLE = 0, ERROR = -1).
        info (dict): Status info.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "status_message"
    name: str
    status: BECStatus
    info: dict


class FileMessage(BECMessage):
    """File message to inform about the status of a file writing operation

    Args:
        file_path (str): Path to the file.
        done (bool): True if the file writing operation is done.
        successful (bool): True if the file writing operation was successful.
        device_name (str): Name of the device. If is_master_file is True, device_name is optional.
        is_master_file (bool, optional): True if the file is a master file. Defaults to False.
        file_type (str, optional): Type of the file. Defaults to "h5".
        hinted_h5_entries (dict[str, str], optional): Dictionary with hinted h5 entries. Defaults to None.
                    This allows the file writer to automatically create external links within the master.h5 file
                    written by BEC under the entry for the specified device. The dictionary should contain the
                    sub-entries and to where these should link in the external h5 file (file_path).
                    Example for device_name='eiger', and dict('data' : '/entry/data/data'), the location
                    '/entry/collection/devices/eiger/data' within the master file will link to '/entry/data/data'
                    of the external file.
        metadata (dict, optional): Additional metadata. Defaults to None.

    """

    msg_type: ClassVar[str] = "file_message"

    file_path: str
    done: bool
    successful: bool
    is_master_file: bool = Field(default=False)
    device_name: str | None = Field(default=None)
    file_type: str = "h5"
    hinted_h5_entries: dict[str, str] | None = None

    @field_validator("is_master_file", mode="after")
    @classmethod
    def check_is_master_file(cls, v: bool):
        """Validate is the FileMessage is for the master file"""
        if v is False:
            return v
        if v is True:
            return cls.device_name != None


class FileContentMessage(BECMessage):
    """File content message to inform about the content of a file

    Args:
        file_path (str): Path to the file.
        data (str): Content of the file.
        scan_info (dict): Scan information.
        metadata (dict, optional): Status metadata. Defaults to None.

    """

    msg_type: ClassVar[str] = "file_content_message"
    file_path: str
    data: dict
    scan_info: dict


class VariableMessage(BECMessage):
    """Message to inform about a global variable

    Args:
        value (Any): Variable value, can be of any type.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "var_message"
    value: Any


class ObserverMessage(BECMessage):
    """Message for observer updates

    Args:
        observer (list[dict]): List of observer descriptions (dictionaries).
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "observer_message"
    observer: list[dict]


class ServiceMetricMessage(BECMessage):
    """Message for service metrics

    Args:
        name (str): Name of the service.
        metrics (dict): Dictionary with service metrics.
        metadata (dict, optional): Additional metadata.

    """

    msg_type: ClassVar[str] = "service_metric_message"
    name: str
    metrics: dict


class ProcessedDataMessage(BECMessage):
    """Message for processed data

    Args:
        data (dict, list[dict]): Dictionary with processed data or list of dictionaries with processed data.
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "processed_data_message"
    data: dict | list[dict]


class DAPConfigMessage(BECMessage):
    """Message for DAP configuration

    Args:
        config (dict): DAP configuration dictionary
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "dap_config_message"
    config: dict


class DAPRequestMessage(BECMessage):
    """Message for DAP requests

    Args:
        dap_cls (str): DAP class name
        dap_type (Literal["continuous", "on_demand"]) : Different types of DAP modes
        config (dict): DAP configuration
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "dap_request_message"
    dap_cls: str
    dap_type: Literal["continuous", "on_demand"]
    config: dict


class DAPResponseMessage(BECMessage):
    """Message for DAP responses

    Args:
        success (bool): True if the request was successful
        data (tuple, optional): DAP data (tuple of data (dict) and metadata). Defaults to ({} , None).
        error (str, optional): DAP error. Defaults to None.
        dap_request (BECMessage, None): DAP request. Defaults to None.
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "dap_response_message"
    success: bool
    data: tuple | None = Field(default_factory=lambda: ({}, None))
    error: str | None = None
    dap_request: BECMessage | None = Field(default=None)


class AvailableResourceMessage(BECMessage):
    """Message for available resources such as scans, data processing plugins etc

    Args:
        resource (dict, list[dict]): Resource description
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "available_resource_message"
    resource: dict | list[dict]


class ProgressMessage(BECMessage):
    """Message for communicating the progress of a long running task

    Args:
        value (float): Current progress value
        max_value (float): Maximum progress value
        done (bool): True if the task is done
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "progress_message"
    value: float
    max_value: float
    done: bool


class GUIConfigMessage(BECMessage):
    """Message for GUI configuration

    Args:
        config (dict): GUI configuration, check widgets for more details
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "gui_config_message"
    config: dict


class GUIDataMessage(BECMessage):
    """Message for GUI data, i.e. update for DAP processes or scans

    Args:
        data (dict): GUI data
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "gui_data_message"
    data: dict


class GUIInstructionMessage(BECMessage):
    """Message for GUI instructions

    Args:
        action (str): Instruction to be executed by the GUI
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "gui_instruction_message"
    action: str
    parameter: dict


class ServiceResponseMessage(BECMessage):
    """Message for service responses

    Args:
        response (dict): Service response
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "service_response_message"
    response: dict


class CredentialsMessage(BECMessage):
    """Message for credentials

    Args:
        credentials (dict): Credentials
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "credentials_message"
    credentials: dict


class RawMessage(BECMessage):
    """Message for raw data that was not encoded as a BECMessage.
    The data dictionary is simply the raw data loaded using json.loads

    Args:
        data (dict): Raw data
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "raw_message"
    data: dict


class ServiceRequestMessage(BECMessage):
    """Message for service requests

    Args:
        request (dict): Service request
        metadata (dict, optional): Metadata. Defaults to None.
    """

    msg_type: ClassVar[str] = "service_request_message"
    action: Literal["restart"]
