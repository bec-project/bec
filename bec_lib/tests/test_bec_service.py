import argparse
import contextlib
import os
import sys
from pathlib import Path
from unittest import mock

import pytest

import bec_lib
from bec_lib import messages
from bec_lib.bec_service import BECService, parse_cmdline_args
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import BECStatus
from bec_lib.redis_connector import RedisConnector
from bec_lib.service_config import ServiceConfig

# pylint: disable=no-member
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access

dir_path = os.path.dirname(bec_lib.__file__)


class MagicMockConnector(RedisConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, redis_cls=mock.MagicMock, **kwargs)


@contextlib.contextmanager
def bec_service(config, connector_cls=None, connector=None, **kwargs):
    if connector_cls is None:
        connector_cls = MagicMockConnector
    with mock.patch("bec_lib.bec_service.BECAccess") as mock_access:
        service = BECService(
            config=config, connector_cls=connector_cls, connector=connector, **kwargs
        )
        try:
            yield service
        finally:
            service.shutdown()
            bec_logger.logger.remove()
            bec_logger._reset_singleton()


def test_bec_service_init_with_service_config():
    config = ServiceConfig(redis={"host": "localhost", "port": 6379})
    with bec_service(config) as service:
        assert service._service_config == config
        assert service.bootstrap_server == "localhost:6379"
        assert service._unique_service is False


def test_bec_service_init_raises_for_invalid_config():
    with pytest.raises(TypeError):
        with bec_service(mock.MagicMock()):
            ...


def test_bec_service_init_with_service_config_path():
    with bec_service(config=f"{dir_path}/tests/test_service_config.yaml") as service:
        assert isinstance(service._service_config, ServiceConfig)
        assert service.bootstrap_server == "localhost:6379"
        assert service._unique_service is False


def test_init_runs_service_check():
    with mock.patch.object(
        BECService, "_update_existing_services", return_value=False
    ) as mock_update_existing_services:
        with bec_service(f"{dir_path}/tests/test_service_config.yaml", unique_service=True):
            mock_update_existing_services.assert_called_once()


def test_run_service_check_raises_for_existing_service():
    with mock.patch.object(
        BECService, "_update_existing_services", return_value=False
    ) as mock_update_existing_services:
        with bec_service(
            f"{dir_path}/tests/test_service_config.yaml", unique_service=True
        ) as service:
            service._services_info = {"BECService": mock.MagicMock()}
            with pytest.raises(RuntimeError):
                service._run_service_check(timeout_time=0, elapsed_time=10)


def test_run_service_check_repeats():
    with mock.patch.object(
        BECService, "_update_existing_services", return_value=False
    ) as mock_update_existing_services:
        with bec_service(
            f"{dir_path}/tests/test_service_config.yaml", unique_service=True
        ) as service:
            service._services_info = {"BECService": mock.MagicMock()}
            assert service._run_service_check(timeout_time=0.5, elapsed_time=0) is True


def test_bec_service_service_status():
    with mock.patch.object(
        BECService, "_update_existing_services", return_value=False
    ) as mock_update_existing_services:
        with bec_service(
            f"{dir_path}/tests/test_service_config.yaml", unique_service=True
        ) as service:
            mock_update_existing_services.reset_mock()
            status = service.service_status
            mock_update_existing_services.assert_called_once()


def test_bec_service_update_existing_services():
    service_keys = [
        MessageEndpoints.service_status("service1").endpoint.encode(),
        MessageEndpoints.service_status("service2").endpoint.encode(),
    ]
    service_msgs = [
        messages.StatusMessage(name="service1", status=BECStatus.RUNNING, info={}, metadata={}),
        messages.StatusMessage(name="service2", status=BECStatus.IDLE, info={}, metadata={}),
    ]
    service_metric_msgs = [
        messages.ServiceMetricMessage(name="service1", metrics={}),
        messages.ServiceMetricMessage(name="service2", metrics={}),
    ]
    connector = mock.MagicMock(spec=RedisConnector)
    connector.keys.return_value = service_keys
    msgs = service_msgs + service_metric_msgs
    connector.get.side_effect = [msg for msg in msgs]
    with bec_service(
        f"{os.path.dirname(bec_lib.__file__)}/tests/test_service_config.yaml",
        connector=connector,
        unique_service=True,
    ) as service:
        assert service._services_info == {"service1": service_msgs[0], "service2": service_msgs[1]}
        assert service._services_metric == {
            "service1": service_metric_msgs[0],
            "service2": service_metric_msgs[1],
        }


def test_bec_service_update_existing_services_ignores_wrong_msgs():
    service_keys = [
        MessageEndpoints.service_status("service1").endpoint.encode(),
        MessageEndpoints.service_status("service2").endpoint.encode(),
    ]
    service_msgs = [
        messages.StatusMessage(name="service1", status=BECStatus.RUNNING, info={}, metadata={}),
        None,
    ]
    connector = mock.MagicMock(spec=RedisConnector)
    service_metric_msgs = [None, messages.ServiceMetricMessage(name="service2", metrics={})]
    msgs = service_msgs + service_metric_msgs
    connector.keys.return_value = service_keys
    connector.get.side_effect = [msg for msg in msgs]
    with bec_service(
        f"{os.path.dirname(bec_lib.__file__)}/tests/test_service_config.yaml",
        connector=connector,
        unique_service=True,
    ) as service:
        assert service._services_info == {"service1": service_msgs[0]}


def test_bec_service_default_config():
    with bec_service(
        f"{os.path.dirname(bec_lib.__file__)}/tests/test_service_config.yaml", unique_service=True
    ) as service:
        assert service._service_config.service_config["file_writer"]["base_path"] == "./"

    config = ServiceConfig(redis={"host": "localhost", "port": 6379})
    with bec_service(config=config, unique_service=True) as service:
        bec_lib_path = str(Path(bec_lib.service_config.__file__).resolve().parent.parent.parent)
        if "nox" not in bec_lib_path:
            assert (
                os.path.abspath(service._service_config.service_config["file_writer"]["base_path"])
                == bec_lib_path
            )


def test_bec_service_show_global_vars(capsys):
    config = ServiceConfig(redis={"host": "localhost", "port": 6379})
    with bec_service(config=config, unique_service=True) as service:
        ep = MessageEndpoints.global_vars("test").endpoint.encode()
        with mock.patch.object(service.connector, "keys", return_value=[ep]):
            with mock.patch.object(service, "get_global_var", return_value="test_value"):
                service.show_global_vars()
                captured = capsys.readouterr()
                assert "test" in captured.out
                assert "test_value" in captured.out


def test_bec_service_globals(connected_connector):
    config = ServiceConfig(redis={"host": "localhost", "port": 1})
    with bec_service(config=config, unique_service=True) as service:
        service.connector = connected_connector
        service.set_global_var("test", "test_value")
        assert service.get_global_var("test") == "test_value"

        service.delete_global_var("test")
        assert service.get_global_var("test") is None


def test_bec_service_metrics(connected_connector):
    config = ServiceConfig(redis={"host": "localhost", "port": 1})
    with mock.patch("bec_lib.bec_service.BECService._start_metrics_emitter") as mock_emitter:
        with bec_service(config=config, unique_service=True) as service:
            service._metrics_emitter_event = mock.MagicMock()
            service._metrics_emitter_event.wait.side_effect = [False, True]
            service.connector = connected_connector
            assert service._services_metric == {}
            service._send_service_status()
            service._get_metrics()
            service._update_existing_services()
            assert service._services_metric != {}


def test_parse_cmdline_args_default():
    """Test parse_cmdline_args with default arguments."""
    with mock.patch.object(sys, "argv", ["script.py"]):
        args, extra_args, service_config = parse_cmdline_args()

        assert args.config == ""
        assert args.log_level is None
        assert args.file_log_level is None
        assert args.redis_log_level is None
        assert extra_args == []
        assert isinstance(service_config, ServiceConfig)


def test_parse_cmdline_args_with_config():
    """Test parse_cmdline_args with a config file argument."""
    with mock.patch.object(sys, "argv", ["script.py", "--config", "test_config.yaml"]):
        with mock.patch("bec_lib.bec_service.ServiceConfig", autospec=True) as mock_service_config:
            args, extra_args, service_config = parse_cmdline_args()

            assert args.config == "test_config.yaml"
            mock_service_config.assert_called_once_with(
                "test_config.yaml",
                cmdline_args={
                    "config": "test_config.yaml",
                    "log_level": None,
                    "file_log_level": None,
                    "redis_log_level": None,
                },
                acl={},
            )


def test_parse_cmdline_args_with_log_levels():
    """Test parse_cmdline_args with log level arguments."""
    with mock.patch.object(
        sys,
        "argv",
        [
            "script.py",
            "--log-level",
            "DEBUG",
            "--file-log-level",
            "INFO",
            "--redis-log-level",
            "WARNING",
        ],
    ):
        with mock.patch("bec_lib.service_config.ServiceConfig", autospec=True):
            # Store original log levels to restore later
            original_stderr_level = bec_logger._stderr_log_level
            original_file_level = bec_logger._file_log_level
            original_redis_level = bec_logger._redis_log_level

            try:
                args, extra_args, _ = parse_cmdline_args()

                assert args.log_level == "DEBUG"
                assert args.file_log_level == "INFO"
                assert args.redis_log_level == "WARNING"
                assert bec_logger._stderr_log_level == "DEBUG"
                assert bec_logger._file_log_level == "INFO"
                assert bec_logger._redis_log_level == "WARNING"
            finally:
                # Restore original log levels
                bec_logger._stderr_log_level = original_stderr_level
                bec_logger._file_log_level = original_file_level
                bec_logger._redis_log_level = original_redis_level


def test_parse_cmdline_args_with_defaults_for_file_and_redis_log_level():
    """Test log level defaults when only --log-level is provided."""
    with mock.patch.object(sys, "argv", ["script.py", "--log-level", "DEBUG"]):
        with mock.patch("bec_lib.service_config.ServiceConfig", autospec=True):
            # Store original log levels to restore later
            original_stderr_level = bec_logger._stderr_log_level
            original_file_level = bec_logger._file_log_level
            original_redis_level = bec_logger._redis_log_level

            try:
                args, extra_args, _ = parse_cmdline_args()

                assert args.log_level == "DEBUG"
                assert args.file_log_level is None
                assert args.redis_log_level is None
                assert bec_logger._stderr_log_level == "DEBUG"
                assert bec_logger._file_log_level == "DEBUG"  # Defaults to stderr level
                assert bec_logger._redis_log_level == "DEBUG"  # Defaults to stderr level
            finally:
                # Restore original log levels
                bec_logger._stderr_log_level = original_stderr_level
                bec_logger._file_log_level = original_file_level
                bec_logger._redis_log_level = original_redis_level


def test_parse_cmdline_args_with_custom_parser():
    """Test parse_cmdline_args with a custom parser."""
    with mock.patch.object(
        sys, "argv", ["script.py", "--custom-arg", "value", "--log-level", "INFO"]
    ):
        custom_parser = argparse.ArgumentParser()
        custom_parser.add_argument("--custom-arg", help="custom argument")

        with mock.patch("bec_lib.service_config.ServiceConfig", autospec=True):
            args, extra_args, _ = parse_cmdline_args(parser=custom_parser)

            assert args.custom_arg == "value"
            assert args.log_level == "INFO"
            assert extra_args == []


def test_parse_cmdline_args_with_extra_args():
    """Test parse_cmdline_args with extra arguments."""
    with mock.patch.object(sys, "argv", ["script.py", "--log-level", "INFO", "extra1", "extra2"]):
        with mock.patch("bec_lib.service_config.ServiceConfig", autospec=True):
            args, extra_args, _ = parse_cmdline_args()

            assert args.log_level == "INFO"
            assert extra_args == ["extra1", "extra2"]


def test_parse_cmdline_args_with_invalid_log_level():
    """Test parse_cmdline_args with an invalid log level."""
    with mock.patch.object(sys, "argv", ["script.py", "--log-level", "INVALID"]):
        with pytest.raises(SystemExit):
            parse_cmdline_args()


def test_wait_for_server_disabled():
    """Test that _wait_for_server returns immediately when wait_for_server is False"""
    config = ServiceConfig(redis={"host": "localhost", "port": 6379})
    with mock.patch("bec_lib.bec_service.BECService.wait_for_service") as mock_wait:
        with bec_service(config=config, wait_for_server=False) as service:
            service._wait_for_server()
            mock_wait.assert_not_called()


def test_wait_for_server_enabled():
    """Test that _wait_for_server calls wait_for_service for each required service"""
    config = ServiceConfig(redis={"host": "localhost", "port": 6379})
    with mock.patch("bec_lib.bec_service.BECService.wait_for_service") as mock_wait:
        with bec_service(config=config, wait_for_server=True) as service:
            assert mock_wait.call_count == 4
            mock_wait.assert_has_calls(
                [
                    mock.call("ScanServer", BECStatus.RUNNING),
                    mock.call("ScanBundler", BECStatus.RUNNING),
                    mock.call("DeviceServer", BECStatus.RUNNING),
                    mock.call("SciHub", BECStatus.RUNNING),
                ]
            )


def test_wait_for_server_keyboard_interrupt():
    """Test that _wait_for_server handles KeyboardInterrupt gracefully"""
    config = ServiceConfig(redis={"host": "localhost", "port": 6379})
    with mock.patch("bec_lib.bec_service.BECService.wait_for_service") as mock_wait:
        with bec_service(config=config, wait_for_server=True) as service:
            mock_wait.side_effect = KeyboardInterrupt
            with mock.patch.object(bec_logger.logger, "warning") as mock_warning:
                service._wait_for_server()
                mock_warning.assert_called_once_with(
                    "KeyboardInterrupt received. Stopped waiting for BEC services."
                )


def test_wait_for_service():
    """Test that wait_for_service waits for the specified service to reach the specified status"""
    config = ServiceConfig(redis={"host": "localhost", "port": 6379})
    service_status = [
        {"ScanServer": messages.StatusMessage(name="ScanServer", status=BECStatus.IDLE, info={})},
        {
            "ScanServer": messages.StatusMessage(
                name="ScanServer", status=BECStatus.RUNNING, info={}
            )
        },
    ]

    with mock.patch.object(
        BECService, "service_status", new_callable=mock.PropertyMock
    ) as mock_update:
        mock_update.side_effect = service_status
        service = BECService(config=config, connector_cls=MagicMockConnector, wait_for_server=False)
        with mock.patch("bec_lib.bec_service.time.sleep") as mock_sleep:
            service.wait_for_service("ScanServer", BECStatus.RUNNING)
            mock_sleep.assert_called_once()
        service.shutdown()
        bec_logger.logger.remove()
        bec_logger._reset_singleton()


def test_wait_for_service_busy():
    """Test that wait_for_service waits for the specified service to reach the specified status"""
    config = ServiceConfig(redis={"host": "localhost", "port": 6379})
    service_status = [
        {"ScanServer": messages.StatusMessage(name="ScanServer", status=BECStatus.IDLE, info={})},
        {"ScanServer": messages.StatusMessage(name="ScanServer", status=BECStatus.BUSY, info={})},
    ]

    with mock.patch.object(
        BECService, "service_status", new_callable=mock.PropertyMock
    ) as mock_update:
        mock_update.side_effect = service_status
        service = BECService(config=config, connector_cls=MagicMockConnector, wait_for_server=False)
        with mock.patch("bec_lib.bec_service.time.sleep") as mock_sleep:
            service.wait_for_service("ScanServer", BECStatus.BUSY)
            mock_sleep.assert_called_once()
        service.shutdown()
        bec_logger.logger.remove()
        bec_logger._reset_singleton()


def test_wait_for_service_default():
    """Test that wait_for_service waits for the specified service to reach the specified status"""
    config = ServiceConfig(redis={"host": "localhost", "port": 6379})
    service_status = [
        {"ScanServer": messages.StatusMessage(name="ScanServer", status=BECStatus.IDLE, info={})},
        {
            "ScanServer": messages.StatusMessage(
                name="ScanServer", status=BECStatus.RUNNING, info={}
            )
        },
    ]

    with mock.patch.object(
        BECService, "service_status", new_callable=mock.PropertyMock
    ) as mock_update:
        mock_update.side_effect = service_status
        service = BECService(config=config, connector_cls=MagicMockConnector, wait_for_server=False)
        with mock.patch("bec_lib.bec_service.time.sleep") as mock_sleep:
            service.wait_for_service("ScanServer")
            mock_sleep.assert_called_once()
        service.shutdown()
        bec_logger.logger.remove()
        bec_logger._reset_singleton()
