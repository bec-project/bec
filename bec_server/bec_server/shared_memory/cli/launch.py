# Description: Launch the shared memory manager server.
# This script is the entry point for the Shared Memory Manager Server. It is called either
# by the bec-shared-mem-manager entry point or directly from the command line.
import threading

from bec_lib.bec_service import parse_cmdline_args
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.shared_memory.manager import SharedMemoryManager

logger = bec_logger.logger
bec_logger.level = bec_logger.LOGLEVEL.INFO


def main():
    """
    Launch the shared memory manager server.
    """
    _, _, config = parse_cmdline_args()

    bec_server = SharedMemoryManager(config=config, connector_cls=RedisConnector)
    bec_server.start()

    try:
        event = threading.Event()
        logger.success(
            f"Started Shared Memory Manager server (id: {bec_server._service_id}). Press Ctrl+C to stop."
        )
        event.wait()
    except KeyboardInterrupt:
        bec_server.shutdown()
        event.set()


if __name__ == "__main__":
    main()
