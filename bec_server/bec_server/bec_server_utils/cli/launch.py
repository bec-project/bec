# Description: Launch the Supervisor service.
# This script is the entry point for the Supervisor service. It is called either
# by the bec-supervisor entry point or directly from the command line.
import threading

from bec_lib.bec_service import parse_cmdline_args
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.bec_server_utils.supervisor_service import SupervisorService

logger = bec_logger.logger
bec_logger.level = bec_logger.LOGLEVEL.INFO


def main():
    """
    Launch the Supervisor service.
    """
    _, _, config = parse_cmdline_args()

    supervisor = SupervisorService(config, RedisConnector)

    try:
        event = threading.Event()
        logger.success("Started Supervisor service")
        event.wait()
    except KeyboardInterrupt:
        supervisor.shutdown()


if __name__ == "__main__":
    main()
