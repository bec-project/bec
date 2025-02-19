from unittest.mock import MagicMock

from bec_lib.logger import bec_logger
from bec_server.scan_server.procedures import InProcessProcedureWorker, ProcedureManager

logger = bec_logger.logger


if __name__ == "__main__":  # pragma: no cover

    server = MagicMock()
    server.bootstrap_server = "localhost:6379"
    manager = ProcedureManager(server, InProcessProcedureWorker)
    try:
        logger.info(f"Running procedure manager {manager}")
        while True:
            ...
    except KeyboardInterrupt:
        logger.info(f"Shutting down procedure manager {manager}")
    manager.shutdown()
