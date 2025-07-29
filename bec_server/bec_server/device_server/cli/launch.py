# This file is the entry point for the BEC device server. It is responsible for
# launching the device server and handling command line arguments.
# It is called either by the bec-device-server entry point or directly from the command line.

# pylint: disable=wrong-import-position
# pylint: disable=wrong-import-order
# we need to run the startup script before we import anything else. This is
# to ensure that the epics environment variables are set correctly.
import importlib.metadata as imd

entry_points = imd.entry_points(group="bec.deployment.device_server")
for entry_point in entry_points:
    if entry_point.name == "plugin_ds_startup":
        entry_point.load()()

import inspect
import logging
import threading

from bec_lib.bec_service import parse_cmdline_args
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.device_server.device_server import DeviceServer

logger = bec_logger.logger
bec_logger.level = bec_logger.LOGLEVEL.INFO


# ---- Forward ophyd logs to bec_logger.logger ----
# Recommended by loguru documentation how to intercept log from other libraries.
class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame:
            filename = frame.f_code.co_filename
            is_logging = filename == logging.__file__
            is_frozen = "importlib" in filename and "_bootstrap" in filename
            if depth > 0 and not (is_logging or is_frozen):
                break
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


# Set up the InterceptHandler to forward ophyd logs to the bec_logger
ophyd_logger = logging.getLogger("ophyd")
ophyd_logger.handlers.clear()
ophyd_logger.setLevel(bec_logger.level)
ophyd_logger.addHandler(InterceptHandler())
ophyd_logger.propagate = False


def main():
    """
    Launch the BEC device server.
    """
    _, _, config = parse_cmdline_args()

    s = DeviceServer(config, RedisConnector)
    try:
        event = threading.Event()
        s.start()
        logger.success("Started DeviceServer")
        event.wait()
    except KeyboardInterrupt:
        s.shutdown()


if __name__ == "__main__":
    main()
