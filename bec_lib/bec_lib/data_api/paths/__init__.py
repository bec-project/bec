"""Transport adapters. A path imports data_path, decoding, models, scenarios and client_views; never data_api."""

from __future__ import annotations

from .file import FileDataPath
from .redis import RedisDataPath
from .zmq import ZMQDataPath

#: Paths every DataAPI registers. Routing follows their priority, not this order
#: (ZMQ 5, Redis 10, file 50).
DEFAULT_DATA_PATHS = (RedisDataPath, FileDataPath, ZMQDataPath)

__all__ = ["DEFAULT_DATA_PATHS", "FileDataPath", "RedisDataPath", "ZMQDataPath"]
