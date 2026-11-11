"""Transport adapters. A path imports data_path, decoding, models, scenarios and client_views; never data_api."""

from __future__ import annotations

from .redis import RedisDataPath

#: Paths every DataAPI registers, in this order (Redis priority 10).
DEFAULT_DATA_PATHS = (RedisDataPath,)

__all__ = ["DEFAULT_DATA_PATHS", "RedisDataPath"]
