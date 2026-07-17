import json
import os
import time
from pathlib import Path
from unittest import mock

import pytest
from pydantic import ValidationError

from bec_lib.bec_errors import ServiceConfigError
from bec_lib.logger import BECLogger, LogLevel
from bec_lib.redis_connector import RedisConnector
from bec_lib.service_config import ServiceConfig


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


def test_file_log_policy_defaults(logger):
    logger.service_name = "ScanServer"

    logger._update_file_log_policy({"log_writer": {"base_path": "./logs"}})

    assert logger._file_max_size_mb == 50
    assert logger._file_max_files == 3
    assert logger._file_max_age_days == 14


def test_file_log_policy_service_override(logger):
    logger.service_name = "DeviceServer"
    config = {
        "log_writer": {
            "base_path": "./logs",
            "max_file_size_mb": 50,
            "max_files": 3,
            "service_overrides": {"DeviceServer": {"max_file_size_mb": 75}},
        }
    }

    logger._update_file_log_policy(config)

    assert logger._file_max_size_mb == 75
    assert logger._file_max_files == 3
    assert logger._file_max_age_days == 14


def test_file_log_policy_other_service_uses_global_values(logger):
    logger.service_name = "SciHub"
    config = {
        "log_writer": {
            "max_file_size_mb": 60,
            "max_files": 4,
            "max_file_age_days": 10,
            "service_overrides": {"DeviceServer": {"max_file_size_mb": 75}},
        }
    }

    logger._update_file_log_policy(config)

    assert logger._file_max_size_mb == 60
    assert logger._file_max_files == 4
    assert logger._file_max_age_days == 10


def test_file_log_policy_partial_override_inherits_global_values(logger):
    logger.service_name = "DeviceServer"
    config = {
        "log_writer": {
            "max_file_size_mb": 60,
            "max_files": 4,
            "max_file_age_days": 10,
            "service_overrides": {"DeviceServer": {"max_files": 2, "max_file_age_days": 7}},
        }
    }

    logger._update_file_log_policy(config)

    assert logger._file_max_size_mb == 60
    assert logger._file_max_files == 2
    assert logger._file_max_age_days == 7


@pytest.mark.parametrize(
    "log_writer",
    [
        {"max_file_size_mb": 0},
        {"max_file_size_mb": -1},
        {"max_files": 0},
        {"max_files": -1},
        {"max_file_age_days": 0},
        {"max_file_age_days": -1},
        {"service_overrides": {"DeviceServer": {"max_file_size_mb": 0}}},
        {"service_overrides": {"DeviceServer": {"max_files": 0}}},
        {"service_overrides": {"DeviceServer": {"max_file_age_days": 0}}},
    ],
)
def test_file_log_policy_rejects_non_positive_values(log_writer):
    with pytest.raises(ValidationError):
        ServiceConfig(config={"log_writer": log_writer})


def test_file_sink_uses_resolved_rotation_policy(logger, tmp_path):
    logger.service_name = "DeviceServer"
    logger._base_path = tmp_path
    logger._file_max_size_mb = 75
    logger._file_max_files = 3
    logger._file_max_age_days = 14

    with mock.patch.object(logger.logger, "add") as add:
        logger.add_file_log(LogLevel.INFO)

    assert add.call_args.kwargs["rotation"] == "75 MB"
    assert add.call_args.kwargs["retention"] == logger._file_retention
    assert add.call_args.kwargs["catch"] is True


def test_console_file_sink_uses_resolved_rotation_policy(logger, tmp_path):
    logger.service_name = "DeviceServer"
    logger._base_path = tmp_path
    logger._file_max_size_mb = 75
    logger._file_max_files = 3
    logger._file_max_age_days = 14

    with (
        mock.patch.object(logger.logger, "add") as add,
        mock.patch.object(logger, "add_console_redis_log"),
    ):
        logger.add_console_log()

    assert add.call_args.kwargs["rotation"] == "75 MB"
    assert add.call_args.kwargs["retention"] == logger._file_retention
    assert add.call_args.kwargs["catch"] is True


def test_file_retention_deletes_archives_older_than_max_age(logger, tmp_path):
    logger._file_max_files = 3
    logger._file_max_age_days = 14
    now = time.time()
    files = []
    for name, age_days in (("newest.log", 1), ("old.log", 15)):
        path = tmp_path / name
        path.touch()
        timestamp = now - age_days * 24 * 60 * 60
        os.utime(path, (timestamp, timestamp))
        files.append(str(path))

    logger._file_retention(files)

    assert (tmp_path / "newest.log").exists()
    assert not (tmp_path / "old.log").exists()


def test_file_sink_keeps_configured_total_file_count(logger, tmp_path):
    logger.service_name = "RotationTest"
    logger._base_path = tmp_path
    logger._file_max_size_mb = 1
    logger._file_max_files = 3
    logger._file_max_age_days = 14
    logger.add_file_log(LogLevel.INFO)

    for _ in range(4):
        logger.logger.info("x" * 600_000)

    assert len(list(tmp_path.glob("RotationTest*.log"))) == 3


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
