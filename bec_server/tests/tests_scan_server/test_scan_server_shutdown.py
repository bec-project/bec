"""Regression tests for scan server shutdown."""

from threading import Event
from unittest import mock

from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.scan_server import ScanServer


def test_shutdown_flushes_logs_before_closing_shared_connector(tmp_path):
    server = ScanServer.__new__(ScanServer)
    connector = mock.Mock(spec=RedisConnector)
    server.connector = connector
    server._service_info_event = Event()
    server._service_info_thread = None
    server._metrics_emitter_event = Event()
    server._metrics_emitter_thread = None
    server.builtin_actor_manager = mock.Mock()
    server.actor_manager = mock.Mock()
    server.proc_manager = mock.Mock()
    server.queue_manager = mock.Mock()
    server.device_manager = mock.Mock()
    server.device_manager.shutdown.side_effect = connector.shutdown
    server.queue_manager.shutdown.side_effect = lambda: bec_logger.logger.info("Queue stopped")
    bec_logger.configure(
        ["localhost:1"],
        "ScanServer",
        connector=connector,
        service_config={"log_writer": {"base_path": str(tmp_path)}},
    )
    publisher = bec_logger._log_thread

    server.shutdown()

    calls = [call[0] for call in connector.mock_calls]
    assert calls.index("execute_pipeline") < calls.index("shutdown")
    assert connector.xadd.call_args.kwargs["msg_dict"]["data"].log_msg["record"]["message"] == (
        "Queue stopped"
    )
    assert not publisher.is_alive()
    assert server._service_info_event.is_set()
    assert server._metrics_emitter_event.is_set()
