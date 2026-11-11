"""Transport adapters. A path imports data_path, decoding, models, scenarios and client_views; never data_api."""

from __future__ import annotations

#: Paths every DataAPI registers, in this order.
DEFAULT_DATA_PATHS = ()

__all__ = ["DEFAULT_DATA_PATHS"]
