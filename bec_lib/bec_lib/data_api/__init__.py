"""
BEC DataAPI: one subscription contract over live scans, scan history and scan-less device streams.

Where does new code go?
- a new transport (REST, ZMQ, websocket, replay): one DataPath subclass; register it at runtime
                                            with DataAPI.register_path or expose it under the
                                            "bec.data_api.paths" entry point; test it with
                                            bec_lib.data_api.testing.DataPathConformance
- a new BEC wire operation:                 decoding/async_updates.py holds the handler table;
                                            it also needs messages.DeviceAsyncUpdate, the file
                                            writer and the reconstruction in models.py
- a new scan-less stream classification:    decoding/signal_info.py, decoding/device_stream.py and
                                            the endpoint table in paths/redis.py
- a new correlation policy:                 data_bundler.py (partition + build_update), validated
                                            in DataAPI.subscribe
- a new scenario:                           not intended; scenarios.py is a complete partition
Layers, top-down, each importing only what is below: data_api -> subscription -> paths ->
decoding -> data_path -> data_bundler, models, scenarios, client_views, emission.
"""

from .data_api import PATH_ENTRY_POINT_GROUP, DataAPI
from .data_bundler import CorrelationGroupError, DataBundler, SourceSeries, correlation_group
from .data_path import (
    BindContext,
    DataPath,
    DataSource,
    EmissionSink,
    SourceRequest,
    SourceSpec,
    apply_size_gate,
    total_estimated_bytes,
)
from .models import (
    PAIRED_GROUP,
    SCAN_GROUP,
    AxisMode,
    CorrelationGroup,
    DeviceStream,
    GroupKind,
    SourceData,
    SourceInfo,
    SourceKey,
    SourceKind,
    SubscriptionUpdate,
    UpdateReason,
)
from .paths import DEFAULT_DATA_PATHS, FileDataPath, RedisDataPath, ZMQDataPath
from .scenarios import TERMINAL_SCAN_STATES, ScanTarget, Scenario, classify
from .subscription import DEFAULT_UPDATE_RATE, Subscription

__all__ = [
    "DataAPI",
    "PATH_ENTRY_POINT_GROUP",
    "SourceSeries",
    "correlation_group",
    "Subscription",
    "DEFAULT_UPDATE_RATE",
    "CorrelationGroupError",
    "DataBundler",
    "BindContext",
    "DataPath",
    "DataSource",
    "EmissionSink",
    "SourceRequest",
    "SourceSpec",
    "apply_size_gate",
    "total_estimated_bytes",
    "AxisMode",
    "PAIRED_GROUP",
    "CorrelationGroup",
    "DeviceStream",
    "GroupKind",
    "SCAN_GROUP",
    "SourceData",
    "SourceInfo",
    "SourceKey",
    "SourceKind",
    "SubscriptionUpdate",
    "UpdateReason",
    "DEFAULT_DATA_PATHS",
    "FileDataPath",
    "RedisDataPath",
    "ZMQDataPath",
    "ScanTarget",
    "Scenario",
    "TERMINAL_SCAN_STATES",
    "classify",
]
