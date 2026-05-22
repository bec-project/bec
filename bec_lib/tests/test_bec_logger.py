import json
import os
from pathlib import Path
from unittest import mock

import pytest

from bec_lib.bec_errors import ServiceConfigError
from bec_lib.logger import BECLogger, LogLevel
from bec_lib.redis_connector import RedisConnector


@pytest.fixture
def logger():
    BECLogger._reset_singleton()
    logger = BECLogger()
    yield logger


def test_configure(logger, tmp_path):
    with mock.patch.object(logger, "_update_base_path") as mock_update_base:
        with mock.patch.object(logger, "writer_mixin") as mock_writer_mixin:
            with mock.patch.object(logger, "_update_sinks") as mock_update_sinks:
                logger._base_path = tmp_path
                logger.configure(
                    bootstrap_server=["localhost:9092"],
                    connector=mock.MagicMock(spec=RedisConnector),
                    service_name="test",
                    service_config={"log_writer": {"base_path": f"{tmp_path}"}},
                )
                assert mock_update_base.called is False
                assert mock_writer_mixin.called is False
                assert mock_update_sinks.mock_calls == mock.call
                assert logger.bootstrap_server == ["localhost:9092"]
                assert logger.service_name == "test"
                assert logger._configured is True


def test_update_base_path_correct_config(logger):
    config = {"log_writer": {"base_path": "./logs"}}
    assert logger._base_path is None
    logger._update_base_path(config)
    assert logger._base_path == os.path.join(str(Path("./").resolve()), "logs")


def test_update_base_path_wrong_config(logger):
    config = {"file_writer": {"base_path": "./"}}
    assert logger._base_path is None
    with pytest.raises(ServiceConfigError):
        logger._update_base_path(config)


@pytest.mark.parametrize(
    "log_level,sink, expected_level",
    [
        (
            LogLevel.DEBUG,
            "all",
            {
                "_redis_log_level": LogLevel.DEBUG,
                "_file_log_level": LogLevel.DEBUG,
                "_stderr_log_level": LogLevel.DEBUG,
            },
        ),
        (
            LogLevel.INFO,
            "redis",
            {
                "_redis_log_level": LogLevel.INFO,
                "_file_log_level": LogLevel.INFO,
                "_stderr_log_level": LogLevel.INFO,
            },
        ),
        (
            LogLevel.ERROR,
            "file",
            {
                "_redis_log_level": LogLevel.INFO,
                "_file_log_level": LogLevel.ERROR,
                "_stderr_log_level": LogLevel.INFO,
            },
        ),
        (
            LogLevel.WARNING,
            "stderr",
            {
                "_redis_log_level": LogLevel.INFO,
                "_file_log_level": LogLevel.INFO,
                "_stderr_log_level": LogLevel.WARNING,
            },
        ),
    ],
)
def test_set_log_level(logger, log_level, sink, expected_level):
    # set the initial log level to INFO
    logger.level = LogLevel.INFO
    logger._configured = True

    logger.set_log_level(log_level, sink)
    for key, value in expected_level.items():
        assert getattr(logger, key) == value


def test_console_redis_callback_publishes_to_log_endpoint_with_console_service_name(logger):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)

    logger._console_redis_logger_callback(
        json.dumps({"record": {"level": {"name": "CONSOLE_LOG"}}, "text": "hello"})
    )

    logger.connector.xadd.assert_called_once()
    kwargs = logger.connector.xadd.call_args.kwargs
    assert kwargs["topic"].endpoint == "info/log"
    assert kwargs["msg_dict"]["data"].log_type == "console_log"
    assert kwargs["msg_dict"]["data"].log_msg["service_name"] == "test_CONSOLE"


def test_console_redis_callback_ignores_publish_failures(logger):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger.connector.xadd.side_effect = RuntimeError("redis unavailable")

    logger._console_redis_logger_callback(
        json.dumps({"record": {"level": {"name": "CONSOLE_LOG_ERROR"}}, "text": "oops"})
    )

    logger.connector.xadd.assert_called_once()
