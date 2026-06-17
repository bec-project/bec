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
    "severity, expected_event, alarm_type, msg, compact_msg, metadata",
    [
        [
            Alarms.MAJOR,
            MessagingEvent.ALARM_MAJOR,
            "alarm",
            "content1",
            "compact_msg",
            {"metadata": "metadata1"},
        ],
        [
            Alarms.MINOR,
            MessagingEvent.ALARM_MINOR,
            "alarm",
            "content1",
            "compact_msg",
            {"metadata": "metadata1"},
        ],
        [
            Alarms.WARNING,
            MessagingEvent.ALARM_WARNING,
            "alarm",
            "content1",
            "compact_msg",
            {"metadata": "metadata1"},
        ],
    ],
)
def test_redis_connector_raise_alarm(
    hli_connector, severity, expected_event, alarm_type, msg, compact_msg, metadata
):
    with (
        mock.patch.object(hli_connector._buffered_connection, "set_and_publish", return_value=None),
        mock.patch.object(hli_connector, "notify", return_value=None),
    ):
        info = messages.ErrorInfo(
            error_message=msg, compact_error_message=compact_msg, exception_type=alarm_type
        )
        hli_connector.raise_alarm(severity, info, metadata)

        hli_connector._buffered_connection.set_and_publish.assert_called_once_with(
            MessageEndpoints.alarm(), AlarmMessage(severity=severity, info=info, metadata=metadata)
        )
        hli_connector.notify.assert_called_once_with(expected_event, compact_msg)
