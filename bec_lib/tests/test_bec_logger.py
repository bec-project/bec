import datetime
import json
import os
import threading
from pathlib import Path
from queue import Empty
from unittest import mock

import pytest

from bec_lib.bec_errors import ServiceConfigError
from bec_lib.logger import BatchQueue, BECLogger, BECLoguruRotator, LogLevel
from bec_lib.redis_connector import RedisConnector


@pytest.fixture
def logger():
    BECLogger._reset_singleton()
    logger = BECLogger()
    yield logger
    logger.shutdown()


def test_batch_queue_blocks_and_atomically_drains_all_items():
    batch_queue = BatchQueue[int]()
    consumer_started = threading.Event()
    received = []

    def consume():
        consumer_started.set()
        received.extend(batch_queue.get_all(timeout=1))

    consumer = threading.Thread(target=consume)
    consumer.start()
    assert consumer_started.wait(timeout=1)
    assert consumer.is_alive()

    batch_queue.put_many([1, 2, 3])
    consumer.join(timeout=1)

    assert consumer.is_alive() is False
    assert received == [1, 2, 3]
    with pytest.raises(Empty):
        batch_queue.get_all_nowait()


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


def test_file_sink_uses_resolved_rotation_policy(logger, tmp_path):
    logger.service_name = "DeviceServer"
    logger._base_path = tmp_path
    logger._file_max_size_mb = 75
    logger._file_max_files = 3
    rotator = BECLoguruRotator(
        size=logger._file_max_size_mb * 1024 * 1024, at=datetime.time(8, 0, 0)
    )

    with mock.patch.object(logger.logger, "add") as add:
        logger.add_file_log(LogLevel.INFO)

    assert add.call_args.kwargs["rotation"].__func__ == rotator.should_rotate.__func__


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

    logger._publish_log_message(
        json.dumps({"record": {"level": {"name": "CONSOLE_LOG"}}, "text": "hello"}),
        service_name="test_CONSOLE",
    )

    logger.connector.xadd.assert_called_once()
    kwargs = logger.connector.xadd.call_args.kwargs
    assert kwargs["topic"].endpoint == "user/log"
    assert kwargs["msg_dict"]["data"].log_type == "console_log"
    assert kwargs["msg_dict"]["data"].log_msg["service_name"] == "test_CONSOLE"


def test_console_redis_callback_ignores_publish_failures(logger):
    logger._configured = True
    logger._log_throttle = 0.01
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger.connector.xadd.side_effect = RuntimeError("redis unavailable")

    logger._publish_log_message(
        json.dumps({"record": {"level": {"name": "CONSOLE_LOG_ERROR"}}, "text": "oops"}),
        service_name="test",
    )

    logger.connector.xadd.assert_called_once()


def test_redis_callback_queues_message_when_thread_is_configured(logger):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_queue = BatchQueue()
    message = json.dumps({"record": {"level": {"name": "INFO"}}, "text": "hello"})

    logger._queue_log_message(message)

    assert logger._log_queue.get_all_nowait() == [(message, None)]
    logger.connector.xadd.assert_not_called()


def test_publish_log_batch_uses_one_redis_pipeline(logger):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    pipeline = logger.connector.pipeline.return_value
    info = json.dumps({"record": {"level": {"name": "INFO"}}, "text": "hello"})
    console = json.dumps({"record": {"level": {"name": "CONSOLE_LOG"}}, "text": "console"})

    logger._publish_log_batch([(info, None), (console, "test_CONSOLE")])

    assert logger.connector.xadd.call_count == 2
    assert all(call.kwargs["pipe"] is pipeline for call in logger.connector.xadd.call_args_list)
    logger.connector.execute_pipeline.assert_called_once_with(pipeline)


def test_log_thread_publishes_queued_messages_as_one_batch(logger):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_throttle = 0.01
    batch_published = threading.Event()
    logger.connector.execute_pipeline.side_effect = lambda _: batch_published.set()
    logger._setup_log_thread()
    info = json.dumps({"record": {"level": {"name": "INFO"}}, "text": "hello"})

    logger._queue_log_message(info)
    logger._queue_log_message(info)

    assert batch_published.wait(timeout=1)
    assert logger.connector.xadd.call_count == 2
    logger.connector.execute_pipeline.assert_called_once()
