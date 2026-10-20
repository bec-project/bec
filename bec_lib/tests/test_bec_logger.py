import datetime
import gc
import json
import os
import threading
import weakref
from pathlib import Path
from queue import Empty
from unittest import mock

import pytest

from bec_lib.bec_errors import ServiceConfigError
from bec_lib.endpoints import MessageEndpoints
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


@pytest.mark.parametrize("bulk", [False, True])
def test_batch_queue_drops_new_items_when_full_and_accepts_items_after_drain(bulk):
    batch_queue = BatchQueue[int]()
    batch_queue.put_many(range(990))

    if bulk:
        batch_queue.put_many(iter(range(990, 1200)))
    else:
        for item in range(990, 1200):
            batch_queue.put(item)
    batch_queue.put(1200)
    batch_queue.put_many([1201, 1202])

    assert batch_queue.get_all_nowait() == list(range(1000))
    assert batch_queue.take_dropped_count() == 203
    assert batch_queue.take_dropped_count() == 0
    batch_queue.put(1203)
    batch_queue.put_many([1204, 1205])
    assert batch_queue.get_all_nowait() == [1203, 1204, 1205]
    assert batch_queue.take_dropped_count() == 0


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


@pytest.mark.parametrize("failure", ["file_sink", "thread_start", "started_thread"])
def test_failed_configure_cleans_up_and_can_be_retried(logger, tmp_path, failure):
    connector = mock.MagicMock(spec=RedisConnector)
    error = RuntimeError("configuration failed")
    if failure == "file_sink":
        failure_patch = mock.patch.object(logger, "add_file_log", side_effect=error)
    elif failure == "thread_start":
        failure_patch = mock.patch("bec_lib.logger.threading.Thread.start", side_effect=error)
    else:
        setup = logger._setup_log_thread

        def setup_then_fail():
            setup()
            raise error

        failure_patch = mock.patch.object(logger, "_setup_log_thread", side_effect=setup_then_fail)

    with failure_patch, pytest.raises(RuntimeError) as exc_info:
        logger.configure(
            ["localhost:1"],
            "test",
            connector=connector,
            service_config={"log_writer": {"base_path": str(tmp_path)}},
        )

    assert exc_info.value is error
    assert logger._configured is False
    assert logger._log_thread is None
    assert logger._log_event is None
    assert logger._log_queue is None
    assert logger._redis_sink_ids == []
    assert not logger.logger._core.handlers
    connector.shutdown.assert_not_called()

    logger.configure(["localhost:1"], "test", connector=connector)
    assert logger._configured
    assert logger._log_thread.is_alive()


@pytest.mark.parametrize("shutdown_fails", [False, True])
def test_failed_configure_closes_only_the_connector_it_created(logger, tmp_path, shutdown_fails):
    connector = mock.MagicMock(spec=RedisConnector)
    if shutdown_fails:
        connector.shutdown.side_effect = RuntimeError("connector cleanup failed")
    with (
        mock.patch.object(logger, "add_file_log", side_effect=PermissionError("not writable")),
        pytest.raises(PermissionError) as exc_info,
    ):
        logger.configure(
            ["localhost:1"],
            "test",
            connector_cls=mock.Mock(return_value=connector),
            service_config={"log_writer": {"base_path": str(tmp_path)}},
        )
    connector.shutdown.assert_called_once_with()
    assert logger.connector is None
    if shutdown_fails:
        assert "connector cleanup failed" in exc_info.value.__notes__[0]


def test_stop_redis_logging_preserves_local_sinks_until_shutdown(
    logger, connected_connector, tmp_path
):
    logger.configure(
        ["localhost:1"],
        "test",
        connector=connected_connector,
        service_config={"log_writer": {"base_path": str(tmp_path)}},
    )
    logger.add_console_log()
    logger.logger.error("pending before Redis closes")
    logger.stop_redis_logging()
    logger.stop_redis_logging()
    connected_connector.shutdown()
    logger.logger.error("failure during teardown")
    logger.logger.log("CONSOLE_LOG_ERROR", "console failure during teardown")

    assert "failure during teardown" in (tmp_path / "test.log").read_text()
    assert "console failure during teardown" in (tmp_path / "test_CONSOLE.log").read_text()
    assert logger._log_thread is None
    assert logger._redis_sink_ids == []
    logger.shutdown()
    assert not logger.logger._core.handlers


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


def test_deprecation_log_has_short_stderr_format(logger):
    record = {"level": mock.Mock(no=LogLevel.WARNING), "extra": {"deprecation": True}}

    assert logger.formatting(is_stderr=True)(record) == (
        "<level>>>> DEPRECATION: {message}</level>\n"
    )


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


@pytest.mark.parametrize("shutdown", [False, True])
def test_log_queue_warns_about_overflow_while_redis_publisher_is_blocked(logger, shutdown):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_throttle = 0.01
    publishing = threading.Event()
    release = threading.Event()
    drained = threading.Event()
    received = []

    def execute_pipeline(_):
        if not publishing.is_set():
            publishing.set()
            assert release.wait(timeout=5)
        else:
            drained.set()

    def collect_message(*, msg_dict, **kwargs):
        received.append(msg_dict["data"])

    logger.connector.execute_pipeline.side_effect = execute_pipeline
    logger.connector.xadd.side_effect = collect_message
    logger._setup_log_thread()

    try:
        logger._queue_log_message({"record": {"level": {"name": "INFO"}}, "text": "first"})
        assert publishing.wait(timeout=1)
        for index in range(1200):
            logger._queue_log_message({"record": {"level": {"name": "INFO"}}, "text": str(index)})
        assert len(logger._log_queue._items) == 1000
        if shutdown:
            join = logger._log_thread.join

            def release_and_join(timeout):
                assert logger._log_event.is_set()
                release.set()
                join(timeout=timeout)

            with mock.patch.object(logger._log_thread, "join", side_effect=release_and_join):
                logger.shutdown()
            assert drained.is_set()
        else:
            release.set()
            assert drained.wait(timeout=5)
    finally:
        release.set()
        logger.shutdown()

    assert [msg.log_msg["text"] for msg in received if msg.log_type == "info"] == [
        "first",
        *map(str, range(1000)),
    ]
    warnings = [msg for msg in received if msg.log_type == "warning"]
    assert len(warnings) == 1
    warning = warnings[0].log_msg
    assert warning["record"]["message"] == (
        "Dropped 200 log messages because the Redis log queue was full."
    )
    assert warning["record"]["message"] in warning["text"]
    assert warning["record"]["time"]["timestamp"] > 0
    assert warning["service_name"] == "test"
    assert logger.connector.execute_pipeline.call_count == 3


def test_overflow_warning_is_written_to_redis_once_on_shutdown(logger, connected_connector):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = connected_connector
    logger._log_queue = BatchQueue()
    logger._log_queue._MAX_SIZE = 3
    logger._log_event = threading.Event()
    for index in range(5):
        logger._queue_log_message({"record": {"level": {"name": "INFO"}}, "text": str(index)})

    logger._log_event.set()
    logger._publish_pipe_to_redis()
    logger._log_event.clear()
    logger._queue_log_message({"record": {"level": {"name": "INFO"}}, "text": "next"})
    logger._log_event.set()
    logger._publish_pipe_to_redis()

    records = connected_connector.xread(MessageEndpoints.log(), from_start=True)
    messages = [entry["data"] for entry in records]
    assert [msg.log_type for msg in messages] == ["info", "info", "info", "warning", "info"]
    warning = messages[3].log_msg
    assert warning["record"]["message"] == (
        "Dropped 2 log messages because the Redis log queue was full."
    )
    assert warning["record"]["level"] == {"name": "WARNING", "no": LogLevel.WARNING.value}
    assert warning["service_name"] == "test"
    assert messages[-1].log_msg["text"] == "next"


def test_large_log_batch_remains_readable_between_transactions(logger, connected_connector):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = connected_connector
    endpoint = MessageEndpoints.log()
    connected_connector.xread(endpoint, from_start=True)
    received = []
    execute_pipeline = connected_connector.execute_pipeline

    def execute_and_read(pipeline):
        result = execute_pipeline(pipeline)
        received.extend(
            entry["data"].log_msg["text"] for entry in connected_connector.xread(endpoint) or []
        )
        return result

    messages = [
        ({"record": {"level": {"name": "INFO"}}, "text": str(index)}, None)
        for index in range(12000)
    ]
    with mock.patch.object(connected_connector, "execute_pipeline", side_effect=execute_and_read):
        logger._publish_log_batch(messages)

    assert received == [str(index) for index in range(12000)]


def test_publish_log_batch_flushes_all_transactions_on_shutdown(logger):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_event = threading.Event()
    logger.connector.execute_pipeline.side_effect = lambda _: logger._log_event.set()
    message = {"record": {"level": {"name": "INFO"}}, "text": "hello"}

    logger._publish_log_batch([(message, None)] * (logger._MAX_REDIS_BATCH_SIZE + 1))

    assert logger.connector.execute_pipeline.call_count == 2
    assert logger.connector.xadd.call_count == logger._MAX_REDIS_BATCH_SIZE + 1


def test_shutdown_flushes_error_and_console_messages(logger, connected_connector, tmp_path):
    logger.configure(
        ["localhost:1"],
        "test",
        connector=connected_connector,
        service_config={"log_writer": {"base_path": str(tmp_path)}},
    )
    logger.add_console_log()
    logger.logger.error("failure before shutdown")
    logger.logger.log("CONSOLE_LOG_ERROR", "console failure before shutdown")

    logger.shutdown()

    records = connected_connector.xread(MessageEndpoints.log(), from_start=True) or []
    assert [entry["data"].log_msg["record"]["message"] for entry in records] == [
        "failure before shutdown",
        "console failure before shutdown",
    ]
    assert [entry["data"].log_msg["service_name"] for entry in records] == ["test", "test_CONSOLE"]
    assert logger._log_thread is None


@pytest.mark.parametrize("publish_fails", [False, True])
def test_publisher_flushes_queue_when_shutdown_precedes_worker_start(logger, publish_fails):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_queue = BatchQueue()
    logger._log_event = threading.Event()
    logger._queue_log_message({"record": {"level": {"name": "ERROR"}}, "text": "last error"})
    logger._log_event.set()
    if publish_fails:
        logger.connector.execute_pipeline.side_effect = RuntimeError("redis unavailable")

    logger._publish_pipe_to_redis()

    logger.connector.execute_pipeline.assert_called_once()
    with pytest.raises(Empty):
        logger._log_queue.get_all_nowait()


def test_queue_rejects_new_messages_during_shutdown(logger):
    logger._configured = True
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_queue = BatchQueue()
    logger._log_event = threading.Event()
    logger._log_event.set()

    logger._queue_log_message({"record": {"level": {"name": "INFO"}}, "text": "too late"})

    with pytest.raises(Empty):
        logger._log_queue.get_all_nowait()


@pytest.mark.timeout(5)
def test_shutdown_timeout_stops_additional_redis_transactions(logger):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_throttle = 0.01
    publishing = threading.Event()
    release = threading.Event()

    def execute_pipeline(_):
        publishing.set()
        release.wait(timeout=10)

    logger.connector.execute_pipeline.side_effect = execute_pipeline
    logger._setup_log_thread()
    worker = logger._log_thread
    try:
        logger._queue_log_message({"record": {"level": {"name": "INFO"}}, "text": "first"})
        assert publishing.wait(timeout=1)
        logger._queue_log_message({"record": {"level": {"name": "INFO"}}, "text": "pending"})

        logger.shutdown()

        assert worker.is_alive()
        assert not logger._configured
    finally:
        release.set()
        worker.join(timeout=1)

    assert not worker.is_alive()
    logger.connector.execute_pipeline.assert_called_once()


def test_reconfigure_rejects_a_publisher_still_stopping(logger, tmp_path):
    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_throttle = 0.01
    publishing = threading.Event()
    release = threading.Event()

    def execute_pipeline(_):
        publishing.set()
        release.wait(timeout=5)

    logger.connector.execute_pipeline.side_effect = execute_pipeline
    logger._setup_log_thread()
    worker = logger._log_thread
    original_connector = logger.connector
    replacement = mock.MagicMock(spec=RedisConnector)
    try:
        logger._queue_log_message({"record": {"level": {"name": "INFO"}}, "text": "hello"})
        assert publishing.wait(timeout=1)
        # Simulate a join timeout while the publisher is blocked in Redis.
        with mock.patch.object(worker, "join"):
            logger.shutdown()

        with pytest.raises(RuntimeError, match="previous publisher is stopping"):
            logger.configure(["localhost:1"], "replacement", connector=replacement)
        assert logger.connector is original_connector
    finally:
        release.set()
        worker.join(timeout=1)

    logger.configure(
        ["localhost:1"],
        "replacement",
        connector=replacement,
        service_config={"log_writer": {"base_path": str(tmp_path)}},
    )
    assert logger.connector is replacement
    assert logger._log_thread is not worker
    assert logger._log_thread.is_alive()


def test_queued_loguru_message_releases_bound_objects(logger):
    class Payload:
        pass

    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_queue = BatchQueue()
    logger.add_redis_log(LogLevel.INFO)
    payload = Payload()
    payload_ref = weakref.ref(payload)

    logger.logger.bind(payload=payload).info("hello")
    del payload
    gc.collect()

    assert payload_ref() is None
    queued, service_name = logger._log_queue.get_all_nowait()[0]
    assert json.loads(queued)["record"]["message"] == "hello"
    assert service_name is None


@pytest.mark.parametrize("publish_fails", [False, True])
def test_idle_log_thread_releases_previous_batch(logger, publish_fails):
    class Payload(dict):
        pass

    logger._configured = True
    logger.service_name = "test"
    logger.connector = mock.MagicMock(spec=RedisConnector)
    logger._log_throttle = 0.01
    published = threading.Event()
    idle = threading.Event()

    def execute_pipeline(_):
        published.set()
        if publish_fails:
            raise RuntimeError("redis unavailable")

    logger.connector.execute_pipeline.side_effect = execute_pipeline
    logger._setup_log_thread()
    get_all = logger._log_queue.get_all

    def get_all_and_signal_idle(timeout=None):
        if published.is_set() and timeout != 0:
            idle.set()
        return get_all(timeout=timeout)

    payload = Payload(record={"level": {"name": "INFO"}}, text="hello")
    payload_ref = weakref.ref(payload)
    with mock.patch.object(logger._log_queue, "get_all", side_effect=get_all_and_signal_idle):
        logger._queue_log_message(payload)
        del payload
        assert idle.wait(timeout=1)
        gc.collect()
        assert payload_ref() is None
