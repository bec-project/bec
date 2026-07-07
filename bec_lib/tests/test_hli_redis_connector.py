from typing import Any, ClassVar, Optional
from unittest import mock

import pytest

import bec_lib.messages as bec_messages
from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.messages import AlarmMessage, BECMessage, BECStatus, BundleMessage, ClientInfoMessage
from bec_lib.messaging_hooks import MessagingEvent
from bec_lib.redis_connector import IncompatibleRedisOperation, RedisConnector
from bec_lib.redis_connector.constants import WrongArguments
from bec_lib.redis_connector.validation import validate_endpoint
from bec_lib.serialization import MsgpackSerialization

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=redefined-outer-name


class TestMessage(BECMessage):
    __test__: ClassVar[bool] = False  # just for pytest to ignore this class
    msg_type: ClassVar[str] = "test_message"
    msg: Optional[str] = None


# register at BEC messages module level, to be able to
# find it when using "loads()"
bec_messages.TestMessage = TestMessage


@pytest.fixture
def hli_connector():
    _connector = RedisConnector("localhost:1", redis_cls=mock.MagicMock)
    try:
        yield _connector
    finally:
        _connector.shutdown()


@pytest.mark.parametrize(
    "severity, expected_event, expected_log_level, alarm_type, msg, compact_msg, metadata",
    [
        [
            Alarms.MAJOR,
            MessagingEvent.ALARM_MAJOR,
            "error",
            "alarm",
            "content1",
            "compact_msg",
            {"metadata": "metadata1"},
        ],
        [
            Alarms.MINOR,
            MessagingEvent.ALARM_MINOR,
            "error",
            "alarm",
            "content1",
            "compact_msg",
            {"metadata": "metadata1"},
        ],
        [
            Alarms.WARNING,
            MessagingEvent.ALARM_WARNING,
            "warning",
            "alarm",
            "content1",
            "compact_msg",
            {"metadata": "metadata1"},
        ],
    ],
)
def test_redis_connector_raise_alarm(
    hli_connector,
    severity,
    expected_event,
    expected_log_level,
    alarm_type,
    msg,
    compact_msg,
    metadata,
):
    with (
        mock.patch.object(hli_connector._managed_connection, "set_and_publish", return_value=None),
        mock.patch.object(hli_connector, "notify", return_value=None),
        mock.patch("bec_lib.redis_connector.hli.logger") as mock_logger,
    ):
        info = messages.ErrorInfo(
            error_message=msg, compact_error_message=compact_msg, exception_type=alarm_type
        )
        hli_connector.raise_alarm(severity, info, metadata)

        hli_connector._managed_connection.set_and_publish.assert_called_once_with(
            MessageEndpoints.alarm().endpoint,
            AlarmMessage(severity=severity, info=info, metadata=metadata),
        )
        hli_connector.notify.assert_called_once_with(expected_event, compact_msg)

        # the alarm is also written to the log of the service that raised it, at a level
        # matching the alarm severity (warnings -> warning, minor/major -> error)
        log_call = getattr(mock_logger, expected_log_level)
        log_call.assert_called_once()
        logged_message = log_call.call_args.args[0]
        assert msg in logged_message
        other_level = "warning" if expected_log_level == "error" else "error"
        getattr(mock_logger, other_level).assert_not_called()


def test_redis_connector_send_converts_ep(hli_connector: RedisConnector):
    topic = MessageEndpoints.scan_segment()
    msg = bec_messages.ScanMessage(point_id=1, scan_id="scan_id", data={})
    hli_connector.send(topic, msg)
    hli_connector._redis_conn.publish.assert_called_once_with(
        topic.endpoint, MsgpackSerialization.dumps(msg)
    )

    hli_connector.send(topic, msg, pipe=hli_connector.pipeline())
    hli_connector._redis_conn.pipeline().publish.assert_called_once_with(
        topic.endpoint, MsgpackSerialization.dumps(msg)
    )


@pytest.mark.parametrize("pattern", ["samx", "samy", MessageEndpoints.device_read("sam*")])
def test_redis_connector_keys(hli_connector, pattern):
    endpoint = pattern if isinstance(pattern, str) else pattern.endpoint
    ret = hli_connector.keys(pattern)
    hli_connector._managed_connection._redis_conn.keys.assert_called_once_with(endpoint)
    assert ret == hli_connector._redis_conn.keys()


def test_send_raises_on_invalid_message_type(hli_connector):
    correct_msg = bec_messages.DeviceMessage(
        signals={"samx": {"value": 1, "timestamp": 1}}, metadata={}
    )
    hli_connector.set_and_publish(MessageEndpoints.device_read("samx"), correct_msg)
    with pytest.raises(TypeError) as excinfo:
        msg = bec_messages.ScanMessage(point_id=1, scan_id="scan_id", data={}, metadata={})
        hli_connector.set_and_publish(MessageEndpoints.device_read("samx"), msg)
    assert "Message type <class 'bec_lib.messages.ScanMessage'> is not compatible " in str(
        excinfo.value
    )


def test_send_raises_on_invalid_topic(hli_connector):
    with pytest.raises(IncompatibleRedisOperation):
        hli_connector.send(MessageEndpoints.device_status("samx"), "msg")
