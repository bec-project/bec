"""Tests for the DataAPI v2 models and ordinal alignment engine."""

import pytest

from bec_lib.data_api.alignment import (
    Bundle,
    CorrelationGroupError,
    SourceSeries,
    validate_correlation_group,
)

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring


class TestSourceSeries:
    def test_in_order_insert(self):
        series = SourceSeries("samx", "samx", "monitored")
        for i in range(3):
            series.insert(i, float(i), 100.0 + i)
        assert series.frontier == 3
        assert series.complete
        snap = series.snapshot()
        assert snap.ordinals == (0, 1, 2)
        assert snap.values == (0.0, 1.0, 2.0)
        assert snap.complete

    def test_out_of_order_insert_fills_hole(self):
        series = SourceSeries("samx", "samx", "monitored")
        series.insert(0, 0.0, 100.0)
        series.insert(2, 2.0, 102.0)
        assert not series.complete
        assert series.frontier == 3
        series.insert(1, 1.0, 101.0)
        assert series.complete
        assert series.snapshot().values == (0.0, 1.0, 2.0)

    def test_overwrite_same_ordinal(self):
        series = SourceSeries("det", "wave", "async")
        series.insert(0, [1], 100.0)
        series.insert(0, [1, 2], 100.5)
        assert len(series) == 1
        assert series.snapshot().values == ([1, 2],)

    def test_unindexed_arrival_counter(self):
        series = SourceSeries("det", "legacy", "unindexed")
        assert series.insert(None, "a", 1.0) == 0
        assert series.insert(None, "b", 2.0) == 1
        assert series.complete

    def test_clear(self):
        series = SourceSeries("samx", "samx", "monitored")
        series.insert(0, 0.0, 100.0)
        series.clear()
        assert len(series) == 0
        assert series.frontier == 0


class TestCorrelationGroup:
    def test_scan_group_mixed_monitored_and_async(self):
        label = validate_correlation_group(
            [
                (("samx", "samx"), "monitored", None),
                (("samy", "samy"), "monitored", None),
                (("det", "wave"), "async", "monitored"),
            ]
        )
        assert label == "scan"

    def test_async_tag_group(self):
        label = validate_correlation_group(
            [(("a", "s1"), "async", "grp1"), (("b", "s2"), "async", "grp1")]
        )
        assert label == "async:grp1"

    def test_single_source_is_standalone(self):
        assert validate_correlation_group([(("a", "s1"), "async", None)]) == "standalone"
        assert validate_correlation_group([(("a", "legacy"), "unindexed", None)]) == "standalone"

    def test_mixed_groups_rejected(self):
        with pytest.raises(CorrelationGroupError):
            validate_correlation_group(
                [(("samx", "samx"), "monitored", None), (("det", "wave"), "async", "grp1")]
            )

    def test_two_ungrouped_async_rejected(self):
        with pytest.raises(CorrelationGroupError):
            validate_correlation_group([(("a", "s1"), "async", None), (("b", "s2"), "async", None)])

    def test_empty_rejected(self):
        with pytest.raises(CorrelationGroupError):
            validate_correlation_group([])


class TestBundle:
    def _bundle_xyz(self):
        bundle = Bundle("scan_1")
        x = bundle.get_series("samx", "samx", "monitored")
        y = bundle.get_series("samy", "samy", "monitored")
        z = bundle.get_series("det", "wave", "async")
        return bundle, x, y, z

    def test_aligned_intersection(self):
        bundle, x, y, z = self._bundle_xyz()
        for i in range(3):
            x.insert(i, float(i), 100.0 + i)
            y.insert(i, 100.0 + i, 100.0 + i)
        z.insert(0, 10.0, 200.0)
        z.insert(1, 20.0, 201.0)

        update = bundle.build_update("live")
        assert update.aligned_ordinals == (0, 1)
        cols = update.aligned()
        assert cols[("samx", "samx")] == (0.0, 1.0)
        assert cols[("samy", "samy")] == (100.0, 101.0)
        assert cols[("det", "wave")] == (10.0, 20.0)
        assert not update.complete  # frontiers differ

    def test_gap_does_not_shift_pairing(self):
        """The core fix over positional pairing: a missing ordinal creates a
        hole, not an off-by-one pairing."""
        bundle, x, _, z = self._bundle_xyz()
        bundle.series.pop(("samy", "samy"))
        x.insert(0, 0.0, 100.0)
        x.insert(1, 1.0, 101.0)
        x.insert(2, 2.0, 102.0)
        z.insert(0, 10.0, 200.0)
        z.insert(2, 30.0, 202.0)  # ordinal 1 lost/late

        update = bundle.build_update("live")
        assert update.aligned_ordinals == (0, 2)
        cols = update.aligned()
        # x point 2 pairs with z ordinal 2 — never with z's second arrival.
        assert cols[("samx", "samx")] == (0.0, 2.0)
        assert cols[("det", "wave")] == (10.0, 30.0)
        assert not update.complete

        # Late arrival fills the hole.
        z.insert(1, 20.0, 201.0)
        update = bundle.build_update("live")
        assert update.aligned_ordinals == (0, 1, 2)
        assert update.complete

    def test_complete_when_all_sources_at_same_frontier(self):
        bundle, x, y, z = self._bundle_xyz()
        for i in range(2):
            x.insert(i, float(i), 0)
            y.insert(i, float(i), 0)
            z.insert(i, float(i), 0)
        update = bundle.build_update("live")
        assert update.complete
        assert update.aligned_ordinals == (0, 1)

    def test_cadence_violation_logged_once(self):
        from unittest import mock

        bundle, x, _, z = self._bundle_xyz()
        bundle.series.pop(("samy", "samy"))
        x.insert(0, 0.0, 0)
        for i in range(4):
            z.insert(i, float(i), 0)
        with mock.patch("bec_lib.data_api.alignment.logger.warning") as warning:
            bundle.build_update("live")
            bundle.build_update("live")
        assert warning.call_count == 1

    def test_update_metadata_and_reason(self):
        bundle, x, y, z = self._bundle_xyz()
        x.insert(0, 0.0, 0)
        y.insert(0, 0.0, 0)
        z.insert(0, 0.0, 0)
        update = bundle.build_update("history", metadata={"file_path": "/x.h5"})
        assert update.reason == "history"
        assert update.metadata["file_path"] == "/x.h5"
        assert update.get("samx", "samx").values == (0.0,)
        assert update.get("nope", "nope") is None
