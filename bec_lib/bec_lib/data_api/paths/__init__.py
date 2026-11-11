"""Transport adapters. A path imports data_path, decoding, models, scenarios and client_views; never data_api."""

from __future__ import annotations

from .file import FileDataPath
from .redis import RedisDataPath

#: Paths every DataAPI registers, in this order (Redis priority 10, file priority 50).
DEFAULT_DATA_PATHS = (RedisDataPath, FileDataPath)

__all__ = ["DEFAULT_DATA_PATHS", "FileDataPath", "RedisDataPath"]
