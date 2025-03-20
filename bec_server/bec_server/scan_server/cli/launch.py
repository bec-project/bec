# This file is the entry point for the Scan Server.
# It is called either by the bec-scan-server entry point or directly from the command line.

import threading

from bec_lib.bec_service import parse_cmdline_args
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server import scan_server

logger = bec_logger.logger
bec_logger.level = bec_logger.LOGLEVEL.INFO


def main():
    """
    Launch the scan server.
    """
    _, _, config = parse_cmdline_args()

    bec_server = scan_server.ScanServer(config=config, connector_cls=RedisConnector)
    try:
        event = threading.Event()
        # pylint: disable=E1102
        logger.success("Started ScanServer")
        event.wait()
    except KeyboardInterrupt:
        bec_server.shutdown()
        event.set()


if __name__ == "__main__":
    main()
