# Description: Launch the SciHub connector.
# This script is the entry point for the SciHub connector. It is called either
# by the bec-scihub entry point or directly from the command line.
import threading

from bec_lib.bec_service import parse_cmdline_args
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server import scihub

logger = bec_logger.logger
bec_logger.level = bec_logger.LOGLEVEL.INFO


def main():
    """
    Launch the SciHub connector.
    """
    _, _, config = parse_cmdline_args()

    sh = scihub.SciHub(config, RedisConnector)

    try:
        event = threading.Event()
        logger.success("Started SciHub connector")
        event.wait()
    except KeyboardInterrupt:
        sh.shutdown()


if __name__ == "__main__":
    main()
