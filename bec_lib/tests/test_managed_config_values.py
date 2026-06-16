from unittest.mock import Mock, patch

import pytest

from bec_lib.config_values import RedisConfigValue
from bec_lib.endpoints import EndpointInfo, MessageOp
from bec_lib.messages import BoolConfigDefaultFalse
from bec_lib.tests.utils import wait_until


@pytest.fixture
def endpoint():
    return EndpointInfo(
        endpoint="test/config", message_type=BoolConfigDefaultFalse, message_op=MessageOp.STREAM
    )


@pytest.fixture
def connector():
    connector = Mock()
    existing = BoolConfigDefaultFalse(value=True)
    connector.xread.return_value = [{"config": existing}]
    return connector


def test_fetch_existing_value(connector, endpoint):
    cfg = RedisConfigValue(connector, endpoint)
    assert cfg.value is True
    connector.register.assert_called_once_with(endpoint, cb=cfg._update_cb)


def test_invalid_endpoint_op():
    connector = Mock()

    bad_endpoint = EndpointInfo(
        endpoint="test/config", message_type=BoolConfigDefaultFalse, message_op=MessageOp.KEY_VALUE
    )

    with pytest.raises(TypeError):
        RedisConfigValue(connector, bad_endpoint)


class NotManagedMessage:
    pass


def test_invalid_endpoint_message_type():
    connector = Mock()

    bad_endpoint = EndpointInfo(
        endpoint="test/config", message_type=NotManagedMessage, message_op=MessageOp.STREAM
    )

    with pytest.raises(TypeError):
        RedisConfigValue(connector, bad_endpoint)


def test_config_value_redis_roundtrip(connected_connector, endpoint):
    recorder = Mock()

    def cb(value: bool):
        recorder(value)

    managed_var = RedisConfigValue(connected_connector, endpoint)
    managed_var.subscribe(cb)
    assert managed_var.value is False
    managed_var.value = True
    wait_until(lambda: managed_var.value is True)
    recorder.assert_called_once_with(True)


def test_wait_not_used_if_disabled(connector, endpoint):
    with patch("bec_lib.config_values.Event") as event_mock:
        cfg = RedisConfigValue(connector, endpoint, wait_for_writes=False)
        cfg.value = False
        event_mock().clear.assert_not_called()


def test_wait_used_if_not_specified(connector, endpoint):
    with patch("bec_lib.config_values.Event") as event_mock:
        cfg = RedisConfigValue(connector, endpoint)
        cfg.value = False
        event_mock().clear.assert_called()
