"""Legacy optimizer imports must preserve the shared implementation identity."""

from bec_lib.path_optimization import PathOptimizerMixin, PathQualityStats
from bec_server.scan_server.scans.path_optimization import (
    PathOptimizerMixin as LegacyPathOptimizerMixin,
)
from bec_server.scan_server.scans.path_optimization import (
    PathQualityStats as LegacyPathQualityStats,
)


def test_legacy_optimizer_imports_preserve_identity():
    assert LegacyPathOptimizerMixin is PathOptimizerMixin
    assert LegacyPathQualityStats is PathQualityStats
