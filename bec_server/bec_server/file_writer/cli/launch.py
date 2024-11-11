# This file is the entry point for the file writer service.
# It is called either by the bec-file-writer entry point or directly from the command line.
import threading

from bec_lib.bec_service import parse_cmdline_args
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server import file_writer

logger = bec_logger.logger
bec_logger.level = bec_logger.LOGLEVEL.INFO


def main():
    """
    Launch the file writer.
    """
    _, _, config = parse_cmdline_args()

    file_writer_manager = file_writer.FileWriterManager(config, RedisConnector)
    try:
        event = threading.Event()
        logger.success("Started FileWriter")
        event.wait()
    except KeyboardInterrupt:
        file_writer_manager.shutdown()


if __name__ == "__main__":
    main()
