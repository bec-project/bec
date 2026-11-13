"""Forward native logging into BEC."""

import inspect
import logging

from bec_lib.logger import bec_logger

logger = bec_logger.logger


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


def configure_logging() -> None:
    """
    Forward classic backend logs to the BEC logger.
    """
    ophyd_logger = logging.getLogger("ophyd")
    ophyd_logger.handlers.clear()
    ophyd_logger.setLevel(bec_logger.level)
    ophyd_logger.addHandler(InterceptHandler())
    ophyd_logger.propagate = False
