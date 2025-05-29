"""
Endpoints for communication within the BEC.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from bec_lib.utils.import_utils import lazy_import

# pylint: disable=too-many-public-methods
# pylint: disable=too-many-lines


if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages
else:
    # TODO: put back normal import when Pydantic gets faster
    # from bec_lib import messages
    messages = lazy_import("bec_lib.messages")


class EndpointType(str, enum.Enum):
    """Endpoint type enum"""

    # The tuple appended to the following descriptions describes the access level of the endpoint
    # for (Unauthenticated, Authenticated, Admin) users.
    INTERNAL = "internal"  # Internal communication only within core components. (-,-,r/w)
    PUBLIC = "public"  # Public information; accessible even without authentication. (r,r,r/w)
    PERSONAL = "personal"  # Data scoped to a single user. (-,r/w,r)
    USER = "user"  # Writable data for users. (-,r/w,r/w)
    ADMIN = "admin"  # Administrative data. (-,-,r/w)
    INFO = "info"  # Similar to INTERNAL but readable by users. (-,r,r/w)


class MessageOp(list[str], enum.Enum):
    """Message operation enum"""

    SET_PUBLISH = ["register", "set_and_publish", "delete", "get", "keys"]
    SEND = ["send", "register"]
    STREAM = ["xadd", "xrange", "xread", "register_stream", "keys", "get_last", "delete"]
    LIST = ["lpush", "lrange", "rpush", "ltrim", "keys", "delete"]
    KEY_VALUE = ["set", "get", "delete", "keys"]
    SET = ["remove_from_set", "get_set_members"]


MessageType = TypeVar("MessageType", bound="type[messages.BECMessage]")


@dataclass
class EndpointInfo(Generic[MessageType]):
    """
    Dataclass for endpoint info.

    Args:
        endpoint (str): Endpoint.
        message_type (messages.BECMessage): Message type.
        message_op (MessageOp): Message operation.
    """

    endpoint: str
    message_type: MessageType
    message_op: MessageOp


class MessageEndpoints:
    """
    Class for message endpoints.
    """

    # devices feedback
    @staticmethod
    def device_status(device: str):
        """
        Endpoint for device status. This endpoint is used by the device server to publish
        the device status using a messages.DeviceStatusMessage message.

        Args:
            device (str): Device name, e.g. "samx".
        """
        endpoint = f"{EndpointType.INFO.value}/devices/status/{device}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceStatusMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def device_read(device: str):
        """
        Endpoint for device readings. This endpoint is used by the device server to publish
        the device readings using a messages.DeviceMessage message.

        Args:
            device (str): Device name, e.g. "samx".

        Returns:
            EndpointInfo: Endpoint for device readings of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/read/{device}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.DeviceMessage, message_op=MessageOp.SET_PUBLISH
        )

    @staticmethod
    def device_read_configuration(device: str):
        """
        Endpoint for device configuration readings. This endpoint is used by the device server
        to publish the device configuration readings using a messages.DeviceMessage message.

        Args:
            device (str): Device name, e.g. "samx".

        Returns:
            EndpointInfo: Endpoint for device configuration readings of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/read_configuration/{device}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.DeviceMessage, message_op=MessageOp.SET_PUBLISH
        )

    @staticmethod
    def device_readback(device: str):
        """
        Endpoint for device readbacks. This endpoint is used by the device server to publish
        the device readbacks using a messages.DeviceMessage message.

        Args:
            device (str): Device name, e.g. "samx".

        Returns:
            EndpointInfo: Endpoint for device readbacks of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/readback/{device}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.DeviceMessage, message_op=MessageOp.SET_PUBLISH
        )

    @staticmethod
    def device_raw(device: str):
        """
        Endpoint for device raw readings. This endpoint is used by the device server to publish
        the device raw readings using a messages.DeviceMessage message.

        Args:
            device (str): Device name, e.g. "samx".

        Returns:
            EndpointInfo: Endpoint for device raw readings of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/raw/{device}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.DeviceMessage, message_op=MessageOp.STREAM
        )

    @staticmethod
    def device_limits(device: str):
        """
        Endpoint for device limits. This endpoint is used by the device server to publish
        the device limits using a messages.DeviceMessage message.

        Args:
            device (str): Device name, e.g. "samx".

        Returns:
            EndpointInfo: Endpoint for device limits of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/limits/{device}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.DeviceMessage, message_op=MessageOp.SET_PUBLISH
        )

    @staticmethod
    def device_req_status(device: str):
        """
        Endpoint for device request status. This endpoint is used by the device server to publish
        the device request status using a messages.DeviceReqStatusMessage message.

        Args:
            device (str): Device name, e.g. "samx".

        Returns:
            EndpointInfo: Endpoint for device request status of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/req_status/{device}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceReqStatusMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def device_req_status_container(RID: str):
        """
        Endpoint for device request status container. This endpoint is used by the device server to publish
        the device request status using a messages.DeviceReqStatusMessage message.

        Args:
            RID (str): Request ID.

        Returns:
            EndpointInfo: Endpoint for device request status container.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/req_status_container/{RID}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceReqStatusMessage,
            message_op=MessageOp.LIST,
        )

    @staticmethod
    def device_progress(device: str):
        """
        Endpoint for device progress. This endpoint is used by the device server to publish
        the device progress using a messages.ProgressMessage message.

        Args:
            device (str): Device name, e.g. "samx".

        Returns:
            EndpointInfo: Endpoint for device progress of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/progress/{device}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ProgressMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    # device config
    @staticmethod
    def device_config_request():
        """
        Endpoint for device config request. This endpoint can be used to
        request a modification to the device config. The request is sent using
        a messages.DeviceConfigMessage message.

        Returns:
            EndpointInfo: Endpoint for device config request.
        """
        endpoint = f"{EndpointType.ADMIN.value}/devices/config_request"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.DeviceConfigMessage, message_op=MessageOp.SEND
        )

    @staticmethod
    def device_config_request_response(RID: str):
        """
        Endpoint for device config request response. This endpoint is used by the
        device server and scihub connector to inform about whether the device config
        request was accepted or rejected. The response is sent using a
        messages.RequestResponseMessage message.

        Args:
            RID (str): Request ID.

        Returns:
            EndpointInfo: Endpoint for device config request response.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/config_request_response/{RID}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.RequestResponseMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def device_server_config_request():
        """
        Endpoint for device server config request. This endpoint can be used to
        request changes to config. Typically used by the scihub connector following a
        device config request and validate a new configuration with the device server.
        The request is sent using a messages.DeviceConfigMessage message.

        Returns:
            EndpointInfo: Endpoint for device server config request.
        """
        endpoint = f"{EndpointType.INTERNAL.value}/devices/device_server_config_update"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.DeviceConfigMessage, message_op=MessageOp.SEND
        )

    @staticmethod
    def device_config_update():
        """
        Endpoint for device config update. This endpoint is used by the scihub connector
        to inform about a change to the device config. The update is sent using a
        messages.DeviceConfigMessage message.

        Returns:
            EndpointInfo: Endpoint for device config update.

        """
        endpoint = f"{EndpointType.INFO.value}/devices/config_update"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.DeviceConfigMessage, message_op=MessageOp.SEND
        )

    @staticmethod
    def device_config():
        """
        Endpoint for device config. This endpoint is used by the scihub connector
        to set the device config.

        Returns:
            EndpointInfo: Endpoint for device config.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/config"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.AvailableResourceMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def device_config_history():
        """
        Endpoint for device config history. This endpoint is used to keep track of the
        device config history using a messages.AvailableResourceMessage message. The endpoint is
        connected to a redis list.

        Returns:
            EndpointInfo: Endpoint for device config history.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/config_history"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.AvailableResourceMessage,
            message_op=MessageOp.LIST,
        )

    @staticmethod
    def device_info(device: str):
        """
        Endpoint for device info. This endpoint is used by the device server to publish
        the device info using a messages.DeviceInfoMessage message.

        Args:
            device (str): Device name, e.g. "samx".

        Returns:
            EndpointInfo: Endpoint for device info of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/info/{device}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceInfoMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def device_staged(device: str):
        """
        Endpoint for the device stage status. This endpoint is used by the device server
        to publish the device stage status using a messages.DeviceStatusMessage message.
        A device is staged when it is ready to be used in a scan. A DeviceStatus of 1 means
        that the device is staged, 0 means that the device is not staged.

        Args:
            device (str): Device name, e.g. "samx".

        Returns:
            EndpointInfo: Endpoint for the device stage status of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/staged/{device}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceStatusMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def device_async_readback(scan_id: str, device: str):
        """
        Endpoint for receiving an async device readback over Redis streams.
        This endpoint is used by the device server to publish async device
        readbacks using a messages.DeviceMessage. In addition tp scan metadata,
        the message metadata contains information on how to concatenate multiple readings.
        Further keyword arguments for GUI handling might be attached.

        Args:
            scan_id (str): unique scan identifier
            device (str): Device name, e.g. "mcs".

        Returns:
            EndpointInfo: Endpoint for device async readback of the specified device.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/async_readback/{scan_id}/{device}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.DeviceMessage, message_op=MessageOp.STREAM
        )

    @staticmethod
    def device_monitor_2d(device: str):
        """
        Endpoint for device monitoring of 2D detectors.
        This endpoint is used to publish image data from a 2D area detector.
        The purpose is to be able to monitor the detector data in real-time
        at reduced frequency/volumes. The data will most likely be made available
        from the the data backend of the detector. Details on shape and type of data
        should be specified in dtype/dshape of the dev.<device>.describe() method.

        Args:
            device (str): Device name, e.g. "eiger".

        Returns:
            EndpointInfo: Endpoint for device monitoring.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/monitor2d/{device}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceMonitor2DMessage,
            message_op=MessageOp.STREAM,
        )

    @staticmethod
    def device_monitor_1d(device: str):
        """
        Endpoint for device monitoring of 1D detectors.
        This endpoint is used to publish image data from a 1D waveform detector.
        The purpose is to be able to monitor the detector data in real-time
        at reduced frequency/volumes. The data will most likely be made available
        from the the data backend of the detector. Details on shape and type of data
        should be specified in dtype/dshape of the dev.<device>.describe() method.

        Args:
            device (str): Device name, e.g. "wave".

        Returns:
            EndpointInfo: Endpoint for device monitoring.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/monitor1d/{device}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceMonitor1DMessage,
            message_op=MessageOp.STREAM,
        )

    @staticmethod
    def device_preview(device: str, signal: str):
        """
        Endpoint for device preview. This endpoint is used to publish a preview of a device
        using a messages.DevicePreviewMessage message. The preview is typically used to broadcast
        a data stream at a reduced frequency, e.g. for a live preview of a detector stream.
        The device must implement the signal as a PreviewComponent, which also provides information
        about the number of dimensions and if possible the shape of the data.

        Args:
            device (str): Device name, e.g. "samx".
            signal (str): Signal name, e.g. "image_preview".

        Returns:
            EndpointInfo: Endpoint for device preview.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/preview/{device}/{signal}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DevicePreviewMessage,
            message_op=MessageOp.STREAM,
        )

    @staticmethod
    def device_user_roi(device: str, signal: str):
        """
        Endpoint for device user ROI. This endpoint is used to publish a user-defined
        region of interest (ROI) for a device using a messages.DeviceUserROIMessage message.
        The ROI can be defined in a GUI or programmatically. The ROI is typically used to
        send a region of interest to a device for processing.
        The device must implement the signal as a UserROIComponent.

        Args:
            device (str): Device name, e.g. "samx".
            signal (str): Signal name, e.g. "preview_roi".

        Returns:
            EndpointInfo: Endpoint for device user ROI.
        """
        endpoint = f"{EndpointType.USER.value}/devices/user_roi/{device}/{signal}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceUserROIMessage,
            message_op=MessageOp.STREAM,
        )

    # scan queue
    @staticmethod
    def scan_queue_modification():
        """
        Endpoint for scan queue modification. This endpoint is used to publish accepted
        scan queue modifications using a messages.ScanQueueModificationMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue modification.
        """
        endpoint = f"{EndpointType.INFO.value}/queue/queue_modification"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanQueueModificationMessage,
            message_op=MessageOp.SEND,
        )

    @staticmethod
    def scan_queue_modification_request():
        """
        Endpoint for scan queue modification request. This endpoint is used to request
        a scan queue modification using a messages.ScanQueueModificationMessage message.
        If accepted, the modification is published using the scan_queue_modification
        endpoint.

        Returns:
            EndpointInfo: Endpoint for scan queue modification request.
        """
        endpoint = f"{EndpointType.USER.value}/queue/queue_modification_request"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanQueueModificationMessage,
            message_op=MessageOp.SEND,
        )

    @staticmethod
    def scan_queue_order_change_request():
        """
        Endpoint for scan queue order change request. This endpoint is used to request
        a change in the scan queue order using a messages.ScanQueueOrderChangeMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue order change request.
        """
        endpoint = f"{EndpointType.USER.value}/queue/queue_order_change_request"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanQueueOrderMessage,
            message_op=MessageOp.SEND,
        )

    @staticmethod
    def scan_queue_order_change_response():
        """
        Endpoint for scan queue order change response. This endpoint is used to publish the
        information on whether the scan queue order change was accepted or rejected. The response
        is sent using a messages.RequestResponseMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue order change response.
        """
        endpoint = f"{EndpointType.INFO.value}/queue/queue_order_change_response"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.RequestResponseMessage,
            message_op=MessageOp.SEND,
        )

    @staticmethod
    def scan_queue_order_change():
        """
        Endpoint for scan queue order change. This endpoint is used to publish the
        scan queue order change using a messages.ScanQueueOrderMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue order change.
        """
        endpoint = f"{EndpointType.INFO.value}/queue/queue_order_change"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanQueueOrderMessage,
            message_op=MessageOp.SEND,
        )

    @staticmethod
    def scan_queue_insert():
        """
        Endpoint for scan queue inserts. This endpoint is used to publish accepted
        scans using a messages.ScanQueueMessage message.
        The message will be picked up by the scan queue manager and inserted into the
        scan queue.

        Returns:
            EndpointInfo: Endpoint for scan queue inserts.
        """
        endpoint = f"{EndpointType.INFO.value}/queue/queue_insert"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.ScanQueueMessage, message_op=MessageOp.SEND
        )

    @staticmethod
    def scan_queue_request():
        """
        Endpoint for scan queue request. This endpoint is used to request the new scans.
        The request is sent using a messages.ScanQueueMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue request.
        """
        endpoint = f"{EndpointType.USER.value}/queue/queue_request"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.ScanQueueMessage, message_op=MessageOp.SEND
        )

    @staticmethod
    def scan_queue_request_response():
        """
        Endpoint for scan queue request response. This endpoint is used to publish the
        information on whether the scan request was accepted or rejected. The response
        is sent using a messages.RequestResponseMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue request response.

        """
        endpoint = f"{EndpointType.INFO.value}/queue/queue_request_response"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.RequestResponseMessage,
            message_op=MessageOp.SEND,
        )

    @staticmethod
    def stop_all_devices():
        """
        Endpoint for stopping all devices. This endpoint is used to publish a message
        to stop all devices and is used by the scan server's scan queue if a scan queue
        modification was requested and accepted and requires to stop all devices.

        Returns:
            EndpointInfo: Endpoint for stopping all devices.
        """
        endpoint = f"{EndpointType.INFO.value}/queue/stop_all_devices"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.VariableMessage, message_op=MessageOp.SEND
        )

    @staticmethod
    def scan_queue_status():
        """
        Endpoint for scan queue status. This endpoint is used to publish the scan queue
        status using a messages.ScanQueueStatusMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue status.
        """
        endpoint = f"{EndpointType.INFO.value}/queue/queue_status"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanQueueStatusMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def scan_queue_history():
        """
        Endpoint for scan queue history. This endpoint is used to keep track of the
        scan queue history using a messages.ScanQueueHistoryMessage message. The endpoint is
        connected to a redis list.

        Returns:
            EndpointInfo: Endpoint for scan queue history.
        """
        endpoint = f"{EndpointType.INFO.value}/queue/queue_history"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanQueueHistoryMessage,
            message_op=MessageOp.LIST,
        )

    @staticmethod
    def scan_queue_schedule(schedule_name: str):
        """
        Endpoint for scan queue schedule. This endpoint is used to store messages.ScanQueueScheduleMessage messages
        in a redis list.

        Args:
            schedule_name (str): Name of the schedule.

        Returns:
            EndpointInfo: Endpoint for scan queue schedule.
        """
        endpoint = f"{EndpointType.USER.value}/queue/queue_schedule/{schedule_name}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.ScanQueueMessage, message_op=MessageOp.LIST
        )

    # scan info
    @staticmethod
    def scan_number():
        """
        Endpoint for scan number. This endpoint is used to publish the scan number. The
        scan number is incremented after each scan and set in redis as an integer.

        Returns:
            EndpointInfo: Endpoint for scan number.
        """
        endpoint = f"{EndpointType.USER.value}/scan_number"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.VariableMessage, message_op=MessageOp.KEY_VALUE
        )

    @staticmethod
    def dataset_number():
        """
        Endpoint for dataset number. This endpoint is used to publish the dataset number.
        The dataset number is incremented after each dataset and set in redis as an integer.

        Returns:
            EndpointInfo: Endpoint for dataset number.
        """
        endpoint = f"{EndpointType.USER.value}/dataset_number"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.VariableMessage, message_op=MessageOp.KEY_VALUE
        )

    @staticmethod
    def scan_status():
        """
        Endpoint for scan status. This endpoint is used to publish the scan status using
        a messages.ScanStatusMessage message.

        Returns:
            EndpointInfo: Endpoint for scan status.
        """
        endpoint = f"{EndpointType.INFO.value}/scans/scan_status"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanStatusMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def scan_progress():
        """
        Endpoint for scan progress. This endpoint is used to publish the scan progress using
        a messages.ProgressMessage message.

        Returns:
            EndpointInfo: Endpoint for scan progress.
        """
        endpoint = f"{EndpointType.INFO.value}/scans/scan_progress"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ProgressMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def scan_history():
        """
        Endpoint for scan history. This endpoint is used to keep track of the scan history
        using a messages.ScanHistoryMessage message. The endpoint is connected to a redis stream.

        Returns:
            EndpointInfo: Endpoint for scan history.
        """
        endpoint = f"{EndpointType.INFO.value}/scans/scan_history"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.ScanHistoryMessage, message_op=MessageOp.STREAM
        )

    @staticmethod
    def available_scans():
        """
        Endpoint for available scans. This endpoint is used to publish the available scans
        using an AvailableResourceMessage.

        Returns:
            EndpointInfo: Endpoint for available scans.
        """
        endpoint = f"{EndpointType.INFO.value}/scans/available_scans"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.AvailableResourceMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def bluesky_events() -> str:
        """
        Endpoint for bluesky events. This endpoint is used by the scan bundler to
        publish the bluesky events using a direct msgpack dump of the bluesky event.

        Returns:
            str: Endpoint for bluesky events.
        """
        return f"{EndpointType.INFO.value}/scans/bluesky-events"

    @staticmethod
    def scan_segment():
        """
        Endpoint for scan segment. This endpoint is used by the scan bundler to publish
        the scan segment using a messages.ScanMessage message.

        Returns:
            EndpointInfo: Endpoint for scan segment.
        """
        endpoint = f"{EndpointType.INFO.value}/scans/scan_segment"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.ScanMessage, message_op=MessageOp.SEND
        )

    @staticmethod
    def scan_baseline():
        """
        Endpoint for scan baseline readings. This endpoint is used by the scan bundler to
        publish the scan baseline readings using a messages.ScanBaselineMessage message.

        Returns:
            EndpointInfo: Endpoint for scan baseline readings.
        """
        endpoint = f"{EndpointType.INFO.value}/scans/scan_baseline"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanBaselineMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    # instructions
    @staticmethod
    def device_instructions():
        """
        Endpoint for device instructions. This endpoint is used by the scan server to
        publish the device instructions using a messages.DeviceInstructionMessage message.
        The device instructions are used to instruct the device server to perform
        certain actions, e.g. to move a motor.

        Returns:
            EndpointInfo: Endpoint for device instructions.
        """
        endpoint = f"{EndpointType.INTERNAL.value}/devices/instructions"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceInstructionMessage,
            message_op=MessageOp.SEND,
        )

    @staticmethod
    def device_instructions_response():
        """
        Endpoint for device instruction repsonses. This endpoint is used by the device
        server to publish responses to device instructions, typically sent by the scan
        server using a messages.DeviceInstructionResponse message. The messages are used
        to inform interested services about the status of device instructions.

        Returns:
            EndpointInfo: Endpoint for the device instruction response.
        """
        endpoint = f"{EndpointType.INTERNAL.value}/devices/instructions_response"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceInstructionResponse,
            message_op=MessageOp.SEND,
        )

    @staticmethod
    def device_rpc(rpc_id: str):
        """
        Endpoint for device rpc. This endpoint is used by the device server to publish
        the result of a device rpc using a messages.DeviceRPCMessage message.

        Args:
            rpc_id (str): RPC ID.

        Returns:
            EndpointInfo: Endpoint for device rpc.
        """
        endpoint = f"{EndpointType.INFO.value}/devices/rpc/{rpc_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DeviceRPCMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def pre_scan_macros():
        """
        Endpoint for pre scan macros. This endpoint is used to keep track of the pre scan
        macros. The endpoint is connected to a redis list.

        Returns:
            EndpointInfo: Endpoint for pre scan macros.
        """
        endpoint = f"{EndpointType.ADMIN.value}/pre_scan_macros"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.VariableMessage, message_op=MessageOp.LIST
        )

    @staticmethod
    def public_scan_info(scan_id: str):
        """
        Endpoint for scan info. This endpoint is used by the scan worker to publish the
        scan info using a messages.ScanStatusMessage message. In contrast to the scan_info endpoint,
        this endpoint is specific to a scan and has a retention time of 30 minutes.

        Args:
            scan_id (str): Scan ID.

        Returns:
            EndpointInfo: Endpoint for scan info.

        """
        endpoint = f"{EndpointType.INFO.value}/public/{scan_id}/scan_info"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanStatusMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def public_scan_segment(scan_id: str, point_id: int):
        """
        Endpoint for public scan segments. This endpoint is used by the scan bundler to
        publish the scan segment using a messages.ScanMessage message. In contrast to the
        scan_segment endpoint, this endpoint is specific to a scan and has a retention time
        of 30 minutes.

        Args:
            scan_id (str): Scan ID.
            point_id (int): Point ID to specify a single point in a scan.

        Returns:
            EndpointInfo: Endpoint for public scan segments.

        """
        endpoint = f"{EndpointType.INFO.value}/public/{scan_id}/scan_segment/{point_id}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.ScanMessage, message_op=MessageOp.KEY_VALUE
        )

    @staticmethod
    def public_scan_baseline(scan_id: str):
        """
        Endpoint for public scan baseline readings. This endpoint is used by the scan bundler
        to publish the scan baseline readings using a messages.ScanBaselineMessage message.
        In contrast to the scan_baseline endpoint, this endpoint is specific to a scan and has
        a retention time of 30 minutes.

        Args:
            scan_id (str): Scan ID.

        Returns:
            EndpointInfo: Endpoint for public scan baseline readings.
        """
        endpoint = f"{EndpointType.INFO.value}/public/{scan_id}/scan_baseline"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ScanBaselineMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def public_file(scan_id: str, name: str):
        """
        Endpoint for public file. This endpoint is used by the file writer to publish the
        status of the file writing using a messages.FileMessage message.

        Args:
            scan_id (str): Scan ID.
            name (str): File name.

        Returns:
            EndpointInfo: Endpoint for public files.
        """
        endpoint = f"{EndpointType.INFO.value}/public/{scan_id}/file/{name}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.FileMessage, message_op=MessageOp.SET_PUBLISH
        )

    @staticmethod
    def file_event(name: str):
        """
        Endpoint for public file_event. This endpoint is used by the file writer to publish the
        status of the file writing using a messages.FileMessage message.

        Args:
            name (str): File name.

        Returns:
            EndpointInfo: Endpoint for public file events.
        """
        endpoint = f"{EndpointType.INFO.value}/public/file_event/{name}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.FileMessage, message_op=MessageOp.SET_PUBLISH
        )

    @staticmethod
    def file_content():
        """
        Endpoint for file content. This endpoint is used by the file writer to publish the
        file content using a messages.FileContentMessage message.

        Returns:
            EndpointInfo: Endpoint for file content.
        """
        endpoint = f"{EndpointType.INTERNAL.value}/internal/file_content"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.FileContentMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    # log
    @staticmethod
    def log():
        """
        Endpoint for log. This endpoint is used by the redis connector to publish logs using
        a messages.LogMessage message.

        Returns:
            EndpointInfo: Endpoint for log.
        """
        endpoint = f"{EndpointType.INFO.value}/log"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.LogMessage, message_op=MessageOp.STREAM
        )

    @staticmethod
    def client_info():
        """
        Endpoint for client info. This endpoint is used by the redis connector to publish
        client info using a messages.ClientInfoMessage message.

        Returns:
            EndpointInfo: Endpoint for client info.
        """
        endpoint = f"{EndpointType.USER.value}/client_info"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.ClientInfoMessage, message_op=MessageOp.STREAM
        )

    @staticmethod
    def alarm():
        """
        Endpoint for alarms. This endpoint is used by the redis connector to publish alarms
        using a messages.AlarmMessage message.

        Returns:
            EndpointInfo: Endpoint for alarms.
        """
        endpoint = f"{EndpointType.USER.value}/alarms"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.AlarmMessage, message_op=MessageOp.SET_PUBLISH
        )

    # service
    @staticmethod
    def service_status(service_id: str):
        """
        Endpoint for service status. This endpoint is used by all BEC services to publish
        their status using a messages.StatusMessage message.
        The status message also contains the service info such as user, host, etc.

        Args:
            service_id (str): Service ID, typically a uuid4 string.

        Returns:
            EndpointInfo: Endpoint for service status.
        """
        endpoint = f"{EndpointType.USER.value}/services/status/{service_id}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.StatusMessage, message_op=MessageOp.SET_PUBLISH
        )

    @staticmethod
    def metrics(service_id: str):
        """
        Endpoint for metrics. This endpoint is used by all BEC services to publish their
        performance metrics using a messages.ServiceMetricMessage message.

        Args:
            service_id (str): Service ID, typically a uuid4 string.

        Returns:
            EndpointInfo: Endpoint for metrics.
        """
        endpoint = f"{EndpointType.USER.value}/services/metrics/{service_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ServiceMetricMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def service_response(RID: str):
        """
        Endpoint for service response. This endpoint is used by all BEC services to publish
        the result of a service request using a messages.ServiceResponseMessage message.

        Args:
            RID (str): Request ID.

        Returns:
            EndpointInfo: Endpoint for service response.
        """
        endpoint = f"{EndpointType.USER.value}/services/response/{RID}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ServiceResponseMessage,
            message_op=MessageOp.LIST,
        )

    @staticmethod
    def service_request():
        """
        Endpoint for service request. This endpoint is used to
        request e.g. resarts of the bec server.

        Returns:
            EndpointInfo: Endpoint for service request.
        """
        endpoint = f"{EndpointType.USER.value}/services/request"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ServiceRequestMessage,
            message_op=MessageOp.SEND,
        )

    # misc
    @staticmethod
    def global_vars(var_name: str):
        """
        Endpoint for global variables. This endpoint is used to publish global variables
        using a messages.VariableMessage message.

        Args:
            var_name (str): Variable name.

        Returns:
            EndpointInfo: Endpoint for global variables.
        """
        endpoint = f"{EndpointType.USER.value}/vars/{var_name}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.VariableMessage, message_op=MessageOp.KEY_VALUE
        )

    @staticmethod
    def observer():
        """
        Endpoint for observer. This endpoint is used to keep track of observer states using a.
        messages.ObserverMessage message. This endpoint is currently not used.

        Returns:
            EndpointInfo: Endpoint for observer.
        """
        endpoint = f"{EndpointType.USER.value}/observer"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.ObserverMessage, message_op=MessageOp.KEY_VALUE
        )

    @staticmethod
    def progress(var_name):
        """
        Endpoint for progress. This endpoint is used to publish the current progress
        using a messages.ProgressMessage message.

        Args:
            var_name (str): Variable name.

        Returns:
            EndpointInfo: Endpoint for progress.
        """
        endpoint = f"{EndpointType.USER.value}/progress/{var_name}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ProgressMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    # logbook
    @staticmethod
    def logbook():
        """
        Endpoint for logbook. This endpoint is used to publish logbook info such as
        url, user and token using a direct msgpack dump of a dictionary.

        Returns:
            EndpointInfo: Endpoint for logbook.
        """
        endpoint = f"{EndpointType.INFO.value}/logbook"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.CredentialsMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    # scibec
    @staticmethod
    def scibec():
        """
        Endpoint for scibec. This endpoint is used to publish scibec info such as
        url, user and token using a CredentialsMessage.

        Returns:
            EndpointInfo: Endpoint for scibec.
        """
        endpoint = f"{EndpointType.INFO.value}/scibec"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.CredentialsMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    # experiment
    @staticmethod
    def account():
        """
        Endpoint for account. This endpoint is used to publish the current account.

        Returns:
            EndpointInfo: Endpoint for account.
        """
        endpoint = f"{EndpointType.INFO.value}/account"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.VariableMessage, message_op=MessageOp.KEY_VALUE
        )

    # data processing
    @staticmethod
    def processed_data(process_id: str):
        """
        Endpoint for processed data. This endpoint is used to publish new processed data
        streams using a messages.ProcessedDataMessage message.

        Args:
            process_id (str): Process ID, typically a uuid4 string.

        Returns:
            EndpointInfo: Endpoint for processed data.
        """
        endpoint = f"{EndpointType.USER.value}/processed_data/{process_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ProcessedDataMessage,
            message_op=MessageOp.STREAM,
        )

    @staticmethod
    def dap_config():
        """
        Endpoint for DAP configuration. This endpoint is used to publish the DAP configuration
        using a messages.DAPConfigMessage message.

        Returns:
            EndpointInfo: Endpoint for DAP configuration.
        """
        endpoint = f"{EndpointType.USER.value}/dap/config"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DAPConfigMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def dap_available_plugins(plugin_id: str):
        """
        Endpoint for available DAP plugins. This endpoint is used to publish the available DAP
        plugins using a messages.AvailableResourceMessage message.

        Args:
            plugin_id (str): Plugin ID.

        Returns:
            EndpointInfo: Endpoint for available DAP plugins.
        """
        endpoint = f"{EndpointType.USER.value}/dap/available_plugins/{plugin_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.AvailableResourceMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def dap_request():
        """
        Endpoint for DAP request. This endpoint is used to request a DAP using a
        messages.DAPRequestMessage message.

        Returns:
            EndpointInfo: Endpoint for DAP request.
        """
        endpoint = f"{EndpointType.USER.value}/dap/request"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DAPRequestMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def dap_response(RID: str):
        """
        Endpoint for DAP response. This endpoint is used to publish the DAP response using a
        messages.DAPResponseMessage message.

        Args:
            RID (str): Request ID.

        Returns:
            EndpointInfo: Endpoint for DAP response.
        """
        endpoint = f"{EndpointType.USER.value}/dap/response/{RID}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.DAPResponseMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    # GUI
    @staticmethod
    def gui_config(gui_id: str):
        """
        Endpoint for GUI configuration. This endpoint is used to publish the GUI configuration
        using a messages.GUIConfigMessage message.

        Returns:
            EndpointInfo: Endpoint for GUI configuration.
        """
        endpoint = f"{EndpointType.USER.value}/gui/config/{gui_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.GUIConfigMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def gui_data(gui_id: str):
        """
        Endpoint for GUI data. This endpoint is used to publish the GUI data using a
        messages.GUIDataMessage message.

        Returns:
            EndpointInfo: Endpoint for GUI data.
        """
        endpoint = f"{EndpointType.USER.value}/gui/data/{gui_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.GUIDataMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def gui_instructions(gui_id: str):
        """
        Endpoint for GUI instructions. This endpoint is used to publish the GUI instructions
        using a messages.GUIInstructionMessage message.

        Returns:
            EndpointInfo: Endpoint for GUI instructions.
        """
        endpoint = f"{EndpointType.USER.value}/gui/instruction/{gui_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.GUIInstructionMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def gui_instruction_response(RID: str):
        """
        Endpoint for GUI instruction response. This endpoint is used to publish the GUI instruction response
        using a messages.RequestResponseMessage message.

        Returns:
            EndpointInfo: Endpoint for GUI instruction response.
        """
        endpoint = f"{EndpointType.USER.value}/gui/instruction_response/{RID}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.RequestResponseMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def gui_auto_update_config(gui_id: str):
        """
        Endpoint for Auto Update configuration.

        Returns:
            EndpointInfo: Endpoint for Auto Update configuration.
        """
        endpoint = f"{EndpointType.USER.value}/gui/config/auto_update/{gui_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.GUIAutoUpdateConfigMessage,
            message_op=MessageOp.SET_PUBLISH,
        )

    @staticmethod
    def gui_heartbeat(gui_id: str):
        """
        Endpoint for GUI heartbeat. This endpoint is used to publish the GUI heartbeat
        using a messages.StatusMessage message.

        Returns:
        EndpointInfo: Endpoint for GUI heartbeat.
        """
        endpoint = f"{EndpointType.USER.value}/gui/heartbeat/{gui_id}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.StatusMessage, message_op=MessageOp.KEY_VALUE
        )

    # Procedures

    @staticmethod
    def available_procedures() -> EndpointInfo:
        """
        Endpoint for available procedures. This endpoint is used to publish the available procedures
        using an AvailableResourceMessage.

        Returns:
            EndpointInfo: Endpoint for available procedures.
        """
        endpoint = f"{EndpointType.INFO.value}/procedures/available_procedures"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.AvailableResourceMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def procedure_request() -> EndpointInfo:
        """
        Endpoint for scan queue request. This endpoint is used to request the new scans.
        The request is sent using a messages.ScanQueueMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue request.
        """
        endpoint = f"{EndpointType.USER.value}/procedures/procedure_request"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ProcedureRequestMessage,
            message_op=MessageOp.STREAM,
        )

    @staticmethod
    def procedure_request_response() -> EndpointInfo:
        """
        Endpoint for procedure request responses. This endpoint is used to publish the
        information on whether the procedure request was accepted or rejected. The response
        is sent using a messages.RequestResponseMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue request response.

        """
        endpoint = f"{EndpointType.INFO.value}/procedures/procedure_request_response"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.RequestResponseMessage,
            message_op=MessageOp.SEND,
        )

    @staticmethod
    def procedure_execution(queue_id: str):
        """
        Endpoint for new procedure executions.
        The request is sent using a messages.ProcedureExecutionMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue request.
        """
        endpoint = f"{EndpointType.INTERNAL.value}/procedures/procedure_execution/{queue_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ProcedureExecutionMessage,
            message_op=MessageOp.LIST,
        )

    @staticmethod
    def active_procedure_executions():
        """
        Endpoint for finished procedure executions or querying currently running procedures.
        The request is sent using a messages.ProcedureExecutionMessage message.

        Returns:
            EndpointInfo: Endpoint for scan queue request.
        """
        endpoint = f"{EndpointType.INFO.value}/procedures/active_procedures"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ProcedureExecutionMessage,
            message_op=MessageOp.SET,
        )

    @staticmethod
    def gui_registry_state(gui_id: str):
        """
        Endpoint for GUI registry state. This endpoint is used to publish the GUI registry state
        using a messages.GUIRegistryStateMessage message. This message has the current state of the
        GUI registry, including all DockAreas, Docks and Widgets. It is a stream with a single message.

        Args:
            gui_id (str): GUI ID.
        Returns:
            EndpointInfo: Endpoint for GUI registry state.
        """

        endpoint = f"{EndpointType.USER.value}/gui/registry_state/{gui_id}"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.GUIRegistryStateMessage,
            message_op=MessageOp.STREAM,
        )

    @staticmethod
    def gui_acl(gui_id: str):
        """
        Endpoint for exchanging GUI ACL information. This endpoint is used by the CLI or GUI to exchange
        updates on the required ACL user. It uses a messages.CredentialsMessage message.

        Args:
            gui_id (str): GUI ID.
        Returns:
            EndpointInfo: Endpoint for GUI ACL.
        """

        endpoint = f"{EndpointType.USER.value}/gui/acl/{gui_id}"
        return EndpointInfo(
            endpoint=endpoint, message_type=messages.CredentialsMessage, message_op=MessageOp.SEND
        )

    @staticmethod
    def login_info():
        """
        Endpoint for login info. This endpoint is used to publish public info about the login
        using a messages.LoginInfoMessage message. This includes the deployment id and the host
        name.

        Returns:
            EndpointInfo: Endpoint for login info.
        """
        endpoint = f"{EndpointType.PUBLIC.value}/acl/login_info"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.LoginInfoMessage,
            message_op=MessageOp.KEY_VALUE,
        )

    @staticmethod
    def acl_accounts():
        """
        Endpoint for ACL accounts. This endpoint is used to publish the ACL accounts using a
        messages.ACLAccountsMessage message.

        Returns:
            EndpointInfo: Endpoint for ACL accounts.
        """
        endpoint = f"{EndpointType.ADMIN.value}/acl/accounts"
        return EndpointInfo(
            endpoint=endpoint,
            message_type=messages.ACLAccountsMessage,
            message_op=MessageOp.KEY_VALUE,
        )
