# Description: Launch the data processing server.
# This script is the entry point for the Data Processing Server. It is called either
# by the bec-dap entry point or directly from the command line.
import argparse
import threading

import bec_server.data_processing as data_processing
from bec_lib.bec_service import parse_cmdline_args
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.data_processing.image_analysis_service import ImageAnalysisService
from bec_server.data_processing.lmfit1d_service import LmfitService1D

logger = bec_logger.logger
bec_logger.level = bec_logger.LOGLEVEL.INFO


def main():
    """
    Launch the data processing server.
    """
    _, _, config = parse_cmdline_args()

    bec_server = data_processing.dap_server.DAPServer(
        config=config,
        connector_cls=RedisConnector,
        provided_services=[LmfitService1D, ImageAnalysisService],
    )
    bec_server.start()

    try:
        event = threading.Event()
        logger.success(
            f"Started DAP server (id: {bec_server._service_id}) with {', '.join(serv.__name__ for serv in bec_server._provided_services)} services active. Press Ctrl+C to stop."
        )
        event.wait()
    except KeyboardInterrupt:
        bec_server.shutdown()
        event.set()


if __name__ == "__main__":
    main()
