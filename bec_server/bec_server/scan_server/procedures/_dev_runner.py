"""Manually startable procedure runner for testing workers during development"""

if True:  # pragma: no cover # must open a clause to apply to everything

    from threading import Event
    from unittest.mock import MagicMock

    from bec_lib.logger import bec_logger
    from bec_server.scan_server.procedures.container_worker import ContainerProcedureWorker
    from bec_server.scan_server.procedures.in_process_worker import InProcessProcedureWorker
    from bec_server.scan_server.procedures.manager import ProcedureManager

    logger = bec_logger.logger

if __name__ == "__main__":  # pragma: no cover

    server = MagicMock()
    server.bootstrap_server = "localhost:6379"
    manager = ProcedureManager(server, ContainerProcedureWorker)
    try:
        logger.info(f"Running procedure manager {manager}")
        Event().wait()
    except KeyboardInterrupt:
        logger.info(f"Shutting down procedure manager {manager}")
    manager.shutdown()
