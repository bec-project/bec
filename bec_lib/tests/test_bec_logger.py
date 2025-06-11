import os
from pathlib import Path
from unittest import mock

import pytest

from bec_lib.bec_errors import ServiceConfigError
from bec_lib.logger import BECLogger
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
    config = {"log_writer": {"base_path": "./"}}
    assert logger._base_path is None
    logger._update_base_path(config)
    assert logger._base_path == os.path.join(str(Path("./").resolve()), "logs")


def test_update_base_path_wrong_config(logger):
    config = {"file_writer": {"base_path": "./"}}
    assert logger._base_path is None
    with pytest.raises(ServiceConfigError):
        logger._update_base_path(config)
