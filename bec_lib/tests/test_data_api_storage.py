"""SourceSeries columnar storage: O(1) snapshots that stay immutable, exact dtypes, alignment."""

import gc
import random
import threading
import time

import numpy as np
import pytest

from bec_lib.data_api.data_bundler import (
    DataBundler,
    SourceSeries,
    _SharedRowOrdinals,
    row_ordinals,
)
from bec_lib.data_api.models import SourceData, align_ordinals

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


class ReferenceSeries:
    """Pure-Python model of the SourceSeries semantics (sorted ordinals, overwrite, cap)."""

    def __init__(self, max_points=None):
        self.points: dict[int, tuple] = {}
        self.max_points = max_points
        self.frontier = 0
        self.watermark = 0

    def insert(self, ordinal, value, timestamp):
        ordinal = self.frontier if ordinal is None else ordinal
        self.points[ordinal] = (value, timestamp)
        self.frontier = max(self.frontier, ordinal + 1)
        if self.max_points is not None and len(self.points) > self.max_points:
            for old in sorted(self.points)[: len(self.points) - self.max_points]:
                del self.points[old]
                self.watermark = max(self.watermark, old + 1)

    def columns(self):
        ordinals = sorted(self.points)
        return (
            ordinals,
            [self.points[o][0] for o in ordinals],
            [self.points[o][1] for o in ordinals],
        )


def frozen(snapshot):
    """A deep copy of a snapshot's content, to detect later mutation."""
    return (
        list(snapshot.ordinals),
        [np.array(v, copy=True) if isinstance(v, np.ndarray) else v for v in snapshot.values],
        list(snapshot.timestamps),
    )


def same(left, right):
    return len(left) == len(right) and all(
        np.array_equal(a, b) if isinstance(a, np.ndarray) else a == b for a, b in zip(left, right)
    )


@pytest.mark.parametrize("seed", range(8))
@pytest.mark.parametrize("max_points", [None, 7])
def test_random_operations_match_the_reference_and_never_mutate_snapshots(seed, max_points):
    rng = random.Random(seed)
    series = SourceSeries("dev", "sig", "monitored", max_points=max_points)
    reference = ReferenceSeries(max_points=max_points)
    delivered = []
    for _ in range(300):
        op = rng.random()
        if op < 0.55:  # append at the frontier (the hot path)
            ordinal, value = None, rng.choice([rng.random(), rng.randint(0, 9), None, "x"])
        elif op < 0.75:  # overwrite an existing ordinal (add_slice / replace)
            if not reference.points:
                continue
            ordinal, value = rng.choice(sorted(reference.points)), rng.random()
        elif op < 0.85:  # gap ahead of the frontier
            ordinal, value = reference.frontier + rng.randint(1, 3), rng.random()
        else:  # late hole fill below the frontier
            holes = [
                o
                for o in range(reference.watermark, reference.frontier)
                if o not in reference.points
            ]
            if not holes:
                continue
            ordinal, value = rng.choice(holes), rng.random()
        timestamp = rng.choice([float(rng.randint(0, 100)), None])
        series.insert(ordinal, value, timestamp)
        reference.insert(ordinal, value, timestamp)
        if rng.random() < 0.4:
            snapshot = series.snapshot()
            delivered.append((snapshot, frozen(snapshot)))
        ordinals, values, timestamps = reference.columns()
        snapshot = series.snapshot()
        assert list(snapshot.ordinals) == ordinals
        assert same(list(snapshot.values), values)
        assert same(list(snapshot.timestamps), timestamps)
        assert series.frontier == reference.frontier
        for o in range(-1, reference.frontier + 2):
            expected = o in reference.points or 0 <= o < reference.watermark
            assert series.is_known(o) == expected, o
    for snapshot, copy in delivered:  # copy on write kept every delivered snapshot intact
        assert frozen(snapshot)[0] == copy[0]
        assert same(frozen(snapshot)[1], copy[1])
        assert same(frozen(snapshot)[2], copy[2])


def test_snapshot_is_o1_and_read_only():
    series = SourceSeries("dev", "sig", "monitored")
    series.extend(np.arange(1_000_000, dtype=float))
    series.insert(None, 1.0, 1.0)
    start = time.perf_counter()
    snapshot = series.snapshot()
    elapsed = time.perf_counter() - start
    assert elapsed < 0.001, f"snapshot of 1M points took {elapsed * 1e3:.2f} ms"
    with pytest.raises(ValueError):
        snapshot.values[0] = 42.0


def test_build_update_after_one_insert_is_independent_of_size():
    bundle = DataBundler("scan", max_points=None)
    for name in ("a", "b"):
        bundle.get_series(name, name, "monitored").extend(np.arange(1_000_000, dtype=float))
    bundle.get_series("a", "a", "monitored").insert(None, 1.0, 1.0)
    start = time.perf_counter()
    bundle.build_update("live", defer_alignment=True)
    elapsed = time.perf_counter() - start
    assert elapsed < 0.001, f"build_update took {elapsed * 1e3:.2f} ms"


def test_values_keep_their_python_types():
    series = SourceSeries("dev", "sig", "monitored")
    for value in (1.5, 2, True, None, "on", np.float64(2.5)):
        series.insert(None, value, 1.0)
    values = list(series.snapshot().values)
    assert [type(v) for v in values] == [float, int, bool, type(None), str, np.float64]
    assert values == [1.5, 2, True, None, "on", 2.5]


def test_float_column_stays_numeric_and_widens_once():
    series = SourceSeries("dev", "sig", "monitored")
    series.insert(None, 1.0, 1.0)
    series.insert(None, 2.0, 2.0)
    assert series.snapshot().values.dtype == np.float64
    first = series.snapshot()
    series.insert(None, "error", 3.0)
    assert series.snapshot().values.dtype == object
    assert list(first.values) == [1.0, 2.0]  # the earlier float view is untouched


def test_array_values_are_stored_by_reference_and_stack_in_as_array():
    series = SourceSeries("dev", "frames", "async")
    frames = [np.full((2, 3), i, dtype=float) for i in range(3)]
    for frame in frames:
        series.insert(None, frame, None)
    snapshot = series.snapshot()
    assert snapshot.values.dtype == object and snapshot.values[1] is frames[1]
    assert snapshot.as_array().shape == (3, 2, 3)


def _add_series(values):
    series = SourceSeries("dev", "sig", "async")
    series.update_metadata(async_update_type="add", max_shape=[None])
    for value in values:
        series.insert(None, value, None)
    return series.snapshot()


@pytest.mark.parametrize(
    "values, expected",
    [
        ([1, 2, 3], [1, 2, 3]),  # Python ints: an object column joined in one step
        ([1.0, 2.5], [1.0, 2.5]),  # floats: the float64 column is the dataset
        ([np.arange(2), np.arange(2, 4)], [0, 1, 2, 3]),  # equal-length fragments
        ([[0, 1, 2], [3]], [0, 1, 2, 3]),  # ragged fragments
        ([0, [1, 2], 3], [0, 1, 2, 3]),  # scalars mixed with fragments
    ],
)
def test_add_fragments_join_into_one_dataset(values, expected):
    joined = _add_series(values).as_array()
    assert joined.ndim == 1 and joined.tolist() == expected


def test_add_scalar_join_is_vectorised():
    """100k scalar fragments join without a Python loop (was ~125 ms, now ~2 ms)."""
    snapshot = _add_series(range(100_000))
    start = time.perf_counter()
    joined = snapshot.as_array()
    assert time.perf_counter() - start < 0.05
    assert joined.dtype == np.int64 and joined[-1] == 99_999
    assert snapshot.latest() is joined


def test_clear_does_not_touch_delivered_snapshots():
    series = SourceSeries("dev", "sig", "monitored")
    series.insert(None, 1.0, 1.0)
    snapshot = series.snapshot()
    series.clear()
    series.insert(None, 9.0, 9.0)
    assert list(snapshot.values) == [1.0]


def _source(ordinals):
    ordinals = np.asarray(ordinals, dtype=np.int64)
    return SourceData("d", "e", "monitored", ordinals, ordinals.astype(float), ordinals, True)


@pytest.mark.parametrize("seed", range(20))
def test_alignment_matches_set_intersection(seed):
    rng = np.random.default_rng(seed)
    sources = {}
    for index in range(rng.integers(1, 4)):
        start = int(rng.integers(0, 50))
        dense = rng.random() < 0.5
        span = np.arange(start, start + int(rng.integers(0, 400)))
        column = span if dense else np.sort(rng.choice(span, size=len(span) // 3, replace=False))
        sources[(str(index), "e")] = _source(column)
    expected = sorted(set.intersection(*(set(s.ordinals.tolist()) for s in sources.values())))
    assert align_ordinals(sources).tolist() == expected


def test_alignment_of_contiguous_sources_is_a_view():
    a = _source(np.arange(0, 10))
    b = _source(np.arange(3, 8))
    aligned = align_ordinals({("a", "e"): a, ("b", "e"): b})
    assert aligned is b.ordinals  # identity: b needs no extraction
    assert aligned.tolist() == [3, 4, 5, 6, 7]


def test_concurrent_writer_and_reader_see_consistent_snapshots():
    series = SourceSeries("dev", "sig", "monitored", max_points=500)
    lock = threading.Lock()
    stop = threading.Event()
    errors = []

    def write():
        value = 0.0
        while not stop.is_set():
            with lock:
                series.insert(None, value, value)
            value += 1.0

    def read():
        for _ in range(2000):
            with lock:
                snapshot = series.snapshot()
            ordinals, values = snapshot.ordinals, snapshot.values
            # Values were written equal to their ordinal; views must stay consistent.
            if len(ordinals) and not np.array_equal(ordinals.astype(float), values):
                errors.append("torn snapshot")

    writer = threading.Thread(target=write)
    writer.start()
    try:
        read()
    finally:
        stop.set()
        writer.join()
    assert not errors


def test_declare_stamps_reconstruction_metadata_once():
    series = SourceSeries("dev", "sig", "async")
    series.insert(None, [1, 2], None)
    before = series.snapshot()
    assert series.declare("add", (None,)) is True
    after = series.snapshot()
    assert after is not before  # a metadata change reaches the consumer
    assert after.metadata["async_update_type"] == "add" and after.metadata["max_shape"] == [None]
    assert series.declare("add", [None]) is True
    assert series.declare("add_slice", [None, None]) is False  # encoding changed mid-scan
    assert series.declare("add", [None, 4]) is False  # shape changed mid-scan
    assert series.metadata["async_update_type"] == "add"


def test_a_path_that_declares_add_gets_the_joined_dataset():
    """EX-16: fragments inserted by a path without acquisition descriptors reconstruct
    like BEC's own once the path declares the encoding."""
    series = SourceSeries("dev", "sig", "async")
    series.declare("add", [None])
    series.insert(0, [1, 2], None)
    series.insert(1, [3, 4], None)
    snapshot = series.snapshot()
    assert snapshot.as_array().tolist() == [1, 2, 3, 4]
    assert snapshot.latest().tolist() == [1, 2, 3, 4]


@pytest.mark.parametrize(
    "widening_point", [(7, 12.0), (3.0, None)], ids=["value column", "timestamp column"]
)
def test_overwrite_after_a_column_widened_leaves_delivered_snapshots_alone(widening_point):
    series = SourceSeries("det", "wave", "monitored")
    series.insert(0, 1.0, 10.0)
    series.insert(1, 2.0, 11.0)
    delivered = series.snapshot()
    series.insert(2, *widening_point)  # one column becomes an object column
    series.insert(0, 99.0, 99.0)  # overwrite of a point the delivered snapshot covers
    assert list(delivered.values) == [1.0, 2.0]
    assert list(delivered.timestamps) == [10.0, 11.0]
    assert list(series.snapshot().values)[0] == 99.0


def test_adopt_rows_shows_a_growing_part_of_one_column_without_copying():
    values = np.arange(6, dtype=np.float32).reshape(3, 2)
    timestamps = np.array([10.0, 11.0, 12.0])
    series = SourceSeries("cam", "frame", "async")

    assert series.adopt_rows(values, timestamps, 1) == 1
    first = series.snapshot()
    assert first.ordinals.tolist() == [0] and not first.complete
    assert np.shares_memory(first.values, values) and first.values.dtype == np.float32

    assert series.adopt_rows(values, timestamps, 3) == 2
    last = series.snapshot()
    assert last.ordinals.tolist() == [0, 1, 2] and last.complete
    assert last.timestamps.tolist() == [10.0, 11.0, 12.0]
    assert np.shares_memory(last.values, values) and not last.values.flags.writeable
    # the snapshot delivered before still holds exactly its row
    assert first.values.tolist() == [[0.0, 1.0]] and len(series) == 3 and series.frontier == 3


def test_adopt_rows_without_timestamps_and_with_a_retention_cap():
    values = np.arange(5.0)
    series = SourceSeries("samx", "samx", "monitored", max_points=2)

    series.adopt_rows(values, None, 3)
    assert series.snapshot().values.tolist() == [1.0, 2.0]
    assert series.snapshot().timestamps.tolist() == [None, None]
    series.adopt_rows(values, None, 5)
    assert series.snapshot().ordinals.tolist() == [3, 4]
    assert series.snapshot().values.tolist() == [3.0, 4.0]


def test_adopt_rows_rejects_other_columns_and_fewer_rows():
    values = np.arange(4.0)
    series = SourceSeries("samx", "samx", "monitored")
    series.adopt_rows(values, None, 2)

    with pytest.raises(ValueError, match="2 are held of 4"):
        series.adopt_rows(values, None, 1)
    with pytest.raises(ValueError, match="2 are held of 4"):
        series.adopt_rows(values, None, 5)
    with pytest.raises(ValueError, match="empty series"):
        series.adopt_rows(np.arange(4.0), None, 3)

    series.clear()
    assert series.complete and len(series) == 0
    assert series.adopt_rows(np.arange(2.0), None, 2) == 2 and series.complete


def test_series_of_stored_rows_share_one_ordinal_column():
    first, second = SourceSeries("cam", "a", "async"), SourceSeries("cam", "b", "async")
    first.adopt_rows(np.arange(1000.0), None, 1000)
    second.adopt_rows(np.arange(400.0), None, 400)

    ordinals_a, ordinals_b = first.snapshot().ordinals, second.snapshot().ordinals
    assert ordinals_a.tolist() == list(range(1000)) and ordinals_b.tolist() == list(range(400))
    assert np.shares_memory(ordinals_a, ordinals_b)
    assert not ordinals_a.flags.writeable and ordinals_a.dtype == np.int64
    # a larger request replaces the shared column; the views handed out before stay valid
    assert len(row_ordinals(5000)) == 5000 and ordinals_a.tolist() == list(range(1000))


def test_a_point_inserted_after_stored_rows_does_not_touch_the_shared_ordinals():
    series = SourceSeries("samx", "samx", "monitored")
    series.adopt_rows(np.arange(3.0), None, 3)
    before = series.snapshot()

    series.insert(3, 9.0, None)

    assert series.snapshot().ordinals.tolist() == [0, 1, 2, 3]
    assert series.snapshot().values.tolist() == [0.0, 1.0, 2.0, 9.0]
    assert before.ordinals.tolist() == [0, 1, 2] and row_ordinals(3).tolist() == [0, 1, 2]


def test_shared_row_ordinals_are_one_array_while_someone_holds_it():
    shared = _SharedRowOrdinals()
    first = shared.get(1000)
    assert first.tolist() == list(range(1000)) and not first.flags.writeable
    assert np.shares_memory(first, shared.get(10))  # a shorter request is a view of it
    longer = shared.get(5000)  # a longer request replaces the array
    assert len(longer) == 5000 and not np.shares_memory(first, longer)
    assert np.shares_memory(longer, shared.get(1000))
    del first, longer
    gc.collect()
    assert shared._ref() is None  # nothing holds the array any more
