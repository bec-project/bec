"""Tests for the DataAPI v2 models and ordinal alignment engine."""

import numpy as np
import pytest

from bec_lib.data_api.data_bundler import (
    CorrelationGroupError,
    DataBundler,
    SourceSeries,
    partition_correlation_groups,
)
from bec_lib.data_api.models import CONTINUOUS_GROUP, SCAN_GROUP, CorrelationGroup, GroupKind

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
        assert list(snap.ordinals) == [0, 1, 2]
        assert list(snap.values) == [0.0, 1.0, 2.0]
        assert snap.complete

    def test_out_of_order_insert_fills_hole(self):
        series = SourceSeries("samx", "samx", "monitored")
        series.insert(0, 0.0, 100.0)
        series.insert(2, 2.0, 102.0)
        assert not series.complete
        assert series.frontier == 3
        series.insert(1, 1.0, 101.0)
        assert series.complete
        assert list(series.snapshot().values) == [0.0, 1.0, 2.0]

    def test_overwrite_same_ordinal(self):
        series = SourceSeries("det", "wave", "async")
        series.insert(0, [1], 100.0)
        series.insert(0, [1, 2], 100.5)
        assert len(series) == 1
        assert list(series.snapshot().values) == [[1, 2]]

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


class TestCorrelationGroups:
    def test_a_group_is_a_typed_value_and_not_a_text(self):
        multi = CorrelationGroup.multi("gauss", "gauss_data")
        assert multi.kind == GroupKind.MULTI and multi.device == "gauss"
        assert multi == CorrelationGroup(GroupKind.MULTI, "gauss", "gauss_data")
        assert multi != CorrelationGroup.standalone("gauss", "gauss_data")
        assert {multi: 1}[CorrelationGroup.multi("gauss", "gauss_data")] == 1  # usable as a key
        # str() is for logs and messages; a group does not compare equal to it
        assert str(multi) == "multi:gauss/gauss_data" and multi != "multi:gauss/gauss_data"
        assert str(SCAN_GROUP) == "scan" and SCAN_GROUP != "scan"
        assert str(CONTINUOUS_GROUP) == "continuous"
        with pytest.raises(AttributeError):
            multi.device = "other"

    def test_monitored_signals_form_the_scan_group(self):
        groups = partition_correlation_groups(
            [
                (("samx", "samx"), "monitored", None),
                (("samy", "samy"), "monitored", None),
                (("det", "wave"), "async", None),
            ]
        )
        assert groups == {
            SCAN_GROUP: [("samx", "samx"), ("samy", "samy")],
            CorrelationGroup.standalone("det", "wave"): [("det", "wave")],
        }

    def test_mixed_sets_partition_instead_of_error(self):
        groups = partition_correlation_groups(
            [
                (("samx", "samx"), "monitored", None),
                (("gauss", "gauss_data_x"), "async", "gauss_data"),
                (("det2", "legacy"), "unindexed", None),
            ]
        )
        assert groups == {
            SCAN_GROUP: [("samx", "samx")],
            CorrelationGroup.multi("gauss", "gauss_data"): [("gauss", "gauss_data_x")],
            CorrelationGroup.standalone("det2", "legacy"): [("det2", "legacy")],
        }

    def test_ungrouped_async_sources_are_separate_standalones(self):
        groups = partition_correlation_groups(
            [(("a", "s1"), "async", None), (("b", "s2"), "async", None)]
        )
        assert len(groups) == 2

    def test_sub_signals_of_one_multi_signal_share_a_group(self):
        groups = partition_correlation_groups(
            [
                (("gauss", "gauss_data_x"), "async", "gauss_data"),
                (("gauss", "gauss_data_y"), "async", "gauss_data"),
                (("gauss", "gauss_data_intensity"), "async", "gauss_data"),
                (("gauss", "gauss_other_a"), "async", "gauss_other"),
                (("det", "wave"), "async", None),
            ]
        )
        assert groups == {
            CorrelationGroup.multi("gauss", "gauss_data"): [
                ("gauss", "gauss_data_x"),
                ("gauss", "gauss_data_y"),
                ("gauss", "gauss_data_intensity"),
            ],
            CorrelationGroup.multi("gauss", "gauss_other"): [("gauss", "gauss_other_a")],
            CorrelationGroup.standalone("det", "wave"): [("det", "wave")],
        }

    def test_multi_signal_only_applies_to_async_sources(self):
        groups = partition_correlation_groups(
            [(("gauss", "gauss_data_x"), "unindexed", "gauss_data")]
        )
        assert groups == {
            CorrelationGroup.standalone("gauss", "gauss_data_x"): [("gauss", "gauss_data_x")]
        }

    def test_empty_rejected(self):
        with pytest.raises(CorrelationGroupError):
            partition_correlation_groups([])


class TestBundle:
    def _bundle_xyz(self):
        bundle = DataBundler("scan_1")
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
        assert list(update.aligned_ordinals) == [0, 1]
        cols = update.aligned()
        assert list(cols[("samx", "samx")]) == [0.0, 1.0]
        assert list(cols[("samy", "samy")]) == [100.0, 101.0]
        assert list(cols[("det", "wave")]) == [10.0, 20.0]
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
        assert list(update.aligned_ordinals) == [0, 2]
        cols = update.aligned()
        # x point 2 pairs with z ordinal 2 — never with z's second arrival.
        assert list(cols[("samx", "samx")]) == [0.0, 2.0]
        assert list(cols[("det", "wave")]) == [10.0, 30.0]
        assert not update.complete

        # Late arrival fills the hole.
        z.insert(1, 20.0, 201.0)
        update = bundle.build_update("live")
        assert list(update.aligned_ordinals) == [0, 1, 2]
        assert update.complete

    def test_complete_when_all_sources_at_same_frontier(self):
        bundle, x, y, z = self._bundle_xyz()
        for i in range(2):
            x.insert(i, float(i), 0)
            y.insert(i, float(i), 0)
            z.insert(i, float(i), 0)
        update = bundle.build_update("live")
        assert update.complete
        assert list(update.aligned_ordinals) == [0, 1]

    def test_lag_logged_once(self):
        from unittest import mock

        bundle, x, _, z = self._bundle_xyz()
        bundle.series.pop(("samy", "samy"))
        x.insert(0, 0.0, 0)
        for i in range(4):
            z.insert(i, float(i), 0)
        with mock.patch("bec_lib.data_api.data_bundler.logger.warning") as warning:
            bundle.build_update("live")
            first_round = warning.call_count
            bundle.build_update("live")
        # x trails the frontier: the lag is warned exactly once per scan.
        assert first_round == 1
        assert warning.call_count == 1

    def test_update_metadata_and_reason(self):
        bundle, x, y, z = self._bundle_xyz()
        x.insert(0, 0.0, 0)
        y.insert(0, 0.0, 0)
        z.insert(0, 0.0, 0)
        update = bundle.build_update("history", metadata={"file_path": "/x.h5"})
        assert update.reason == "history"
        assert update.metadata["file_path"] == "/x.h5"
        assert list(update.get("samx", "samx").values) == [0.0]
        assert update.get("nope", "nope") is None


class TestBulkIngest:
    """Bulk history fills must stay numpy end to end (GUI-freeze regression:
    per-point ingest exploded file arrays into Python objects, and the render
    paid an O(n) np.asarray on the GUI thread to rebuild them)."""

    def test_extend_zero_copy_snapshot(self):
        series = SourceSeries("det", "sig", "monitored")
        values = np.arange(1000, dtype=float)
        timestamps = np.arange(1000, dtype=float) + 100.0
        assert series.extend(values, timestamps) == 1000
        assert len(series) == 1000
        assert series.frontier == 1000
        assert series.complete
        snap = series.snapshot()
        assert np.shares_memory(snap.values, values)  # zero copy - a view of the file array
        assert np.shares_memory(snap.timestamps, timestamps)
        assert not snap.values.flags.writeable and values.flags.writeable
        assert isinstance(snap.ordinals, np.ndarray)
        assert snap.complete

    def test_extend_appends_at_the_frontier(self):
        series = SourceSeries("det", "sig", "monitored")
        series.insert(0, 1.0, 100.0)
        assert series.extend(np.arange(5.0)) == 5
        assert list(series.snapshot().ordinals) == [0, 1, 2, 3, 4, 5]

    def test_insert_after_extend_preserves_data_and_the_file_array(self):
        series = SourceSeries("det", "sig", "monitored")
        file_values = np.array([1.0, 2.0, 3.0])
        series.extend(file_values, np.array([10.0, 11.0, 12.0]))
        series.insert(1, 9.0, 11.0)  # overwrite inside the adopted array: copy on write
        assert list(file_values) == [1.0, 2.0, 3.0]
        series.insert(1, 2.0, 11.0)
        series.insert(3, 4.0, 13.0)
        snap = series.snapshot()
        assert list(snap.values) == [1.0, 2.0, 3.0, 4.0]
        assert list(snap.ordinals) == [0, 1, 2, 3]
        assert series.complete

    def test_bulk_timestamp_length_mismatch_dropped(self):
        series = SourceSeries("det", "sig", "monitored")
        series.extend(np.arange(4.0), np.arange(2.0))
        snap = series.snapshot()
        assert len(snap.timestamps) == 4
        assert snap.timestamps[0] is None

    def test_contiguous_alignment_without_set_intersection(self):
        """Complete sources of different lengths align on the common prefix
        as a zero-copy view - no per-ordinal set work."""
        bundle = DataBundler("scan_1")
        long = bundle.get_series("a", "a", "monitored")
        short = bundle.get_series("b", "b", "monitored")
        long_values = np.arange(5.0)
        long.extend(long_values)
        short.extend(np.arange(3.0) + 100.0)
        update = bundle.build_update("history")
        assert not update.complete
        assert list(update.aligned_ordinals) == [0, 1, 2]
        cols = update.aligned()
        assert list(cols[("a", "a")]) == [0.0, 1.0, 2.0]
        assert list(cols[("b", "b")]) == [100.0, 101.0, 102.0]
        assert np.shares_memory(cols[("a", "a")], long_values)  # prefix view

    def test_bulk_and_incomplete_source_fall_back_to_intersection(self):
        bundle = DataBundler("scan_1")
        bulk = bundle.get_series("a", "a", "monitored")
        gappy = bundle.get_series("b", "b", "monitored")
        bulk.extend(np.arange(5.0))
        gappy.insert(0, 10.0, 0.0)
        gappy.insert(2, 30.0, 0.0)
        update = bundle.build_update("live")
        assert list(update.aligned_ordinals) == [0, 2]
        cols = update.aligned()
        assert list(cols[("a", "a")]) == [0.0, 2.0]
        assert list(cols[("b", "b")]) == [10.0, 30.0]

    def test_single_bulk_source_aligned_is_the_file_array(self):
        bundle = DataBundler("scan_1")
        series = bundle.get_series("a", "a", "monitored")
        values = np.arange(10.0)
        series.extend(values)
        update = bundle.build_update("history")
        assert np.shares_memory(update.aligned()[("a", "a")], values)  # end-to-end zero copy


class TestContinuousCorrelation:
    def _bundle(self, max_points=None):
        bundle = DataBundler("", max_points=max_points, correlation="continuous")
        for device in ("samx", "samy"):
            bundle.get_series(device, device, "unindexed")
        return bundle

    def test_partition_puts_all_sources_in_one_group(self):
        specs = [(("samx", "samx"), "unindexed", None), (("samy", "samy"), "unindexed", None)]
        assert partition_correlation_groups(specs, correlation="continuous") == {
            CONTINUOUS_GROUP: [("samx", "samx"), ("samy", "samy")]
        }
        assert len(partition_correlation_groups(specs)) == 2  # ordinal: standalone each

    def test_rows_start_once_every_member_delivered(self):
        bundle = self._bundle()
        samx = bundle.get_series("samx", "samx", "unindexed")
        samy = bundle.get_series("samy", "samy", "unindexed")
        assert samx.insert(None, 0.0, 100.0) == -1  # held: samy has not delivered
        assert samx.insert(None, 1.0, 101.0) == -1  # latest sample wins
        assert len(samx) == 0 and len(samy) == 0
        assert samy.insert(None, 10.0, 102.0) == 0  # first full row
        update = bundle.build_update("live")
        assert {k: list(v) for k, v in update.aligned().items()} == {
            ("samx", "samx"): [1.0],
            ("samy", "samy"): [10.0],
        }
        assert update.complete and list(update.aligned_ordinals) == [0]

    def test_each_sample_appends_a_row_with_held_values(self):
        bundle = self._bundle()
        samx = bundle.get_series("samx", "samx", "unindexed")
        samy = bundle.get_series("samy", "samy", "unindexed")
        samx.insert(None, 0.0, 100.0)
        samy.insert(None, 10.0, 101.0)
        assert samx.insert(None, 1.0, 102.0) == 1
        assert samy.insert(None, 11.0, 103.0) == 2
        update = bundle.build_update("live")
        assert {k: list(v) for k, v in update.aligned().items()} == {
            ("samx", "samx"): [0.0, 1.0, 1.0],
            ("samy", "samy"): [10.0, 10.0, 11.0],
        }
        # Every member carries the timestamp of the sample that triggered the row.
        assert list(update.get("samx", "samx").timestamps) == [101.0, 102.0, 103.0]
        assert list(update.get("samy", "samy").timestamps) == [101.0, 102.0, 103.0]

    def test_max_points_bounds_rows(self):
        bundle = self._bundle(max_points=2)
        samx = bundle.get_series("samx", "samx", "unindexed")
        samy = bundle.get_series("samy", "samy", "unindexed")
        samy.insert(None, 10.0, 0.0)
        for i in range(4):
            samx.insert(None, float(i), float(i))
        update = bundle.build_update("live")
        assert {k: list(v) for k, v in update.aligned().items()} == {
            ("samx", "samx"): [2.0, 3.0],
            ("samy", "samy"): [10.0, 10.0],
        }
        assert list(update.aligned_ordinals) == [2, 3]

    def test_clear_resets_the_hold(self):
        bundle = self._bundle()
        samx = bundle.get_series("samx", "samx", "unindexed")
        samy = bundle.get_series("samy", "samy", "unindexed")
        samx.insert(None, 0.0, 0.0)
        samy.insert(None, 1.0, 1.0)
        bundle.clear()
        assert samx.insert(None, 2.0, 2.0) == -1
        assert samy.insert(None, 3.0, 3.0) == 0


class TestTrimWatermark:
    def test_trimmed_ordinals_stay_known(self):
        series = SourceSeries("samx", "samx", "monitored", max_points=2)
        for i in range(3):
            series.insert(i, float(i), None)
        assert list(series.snapshot().ordinals) == [1, 2]
        assert series.is_known(0) and series.is_known(2) and not series.is_known(3)
        series.clear()
        assert not series.is_known(0)

    def test_bulk_series_knows_its_rows(self):
        series = SourceSeries("samx", "samx", "monitored")
        assert series.extend(np.arange(3.0), np.arange(3.0)) == 3
        assert series.is_known(0) and series.is_known(2)
        assert not series.is_known(3) and not series.is_known(-1)
        assert isinstance(series.snapshot().values, np.ndarray)
