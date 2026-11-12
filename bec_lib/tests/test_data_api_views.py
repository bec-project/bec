"""The reduction that view-driven loading rests on: extremes of every bin, at their rows."""

import numpy as np
import pytest

from bec_lib.data_api.views import ColumnView, ViewWindow, bin_rows_for, minmax_decimate

# pylint: disable=missing-function-docstring


def test_minmax_keeps_the_extremes_of_every_bin_in_row_order():
    values = np.array([5, 1, 9, 3, 7, 7, 7, 7, 2, 8], dtype=np.int16)

    rows, kept = minmax_decimate(values, first_row=100, bin_rows=4)

    # bins [5 1 9 3] -> 1, 9; [7 7 7 7] -> one sample; tail [2 8] -> 2, 8
    assert rows.tolist() == [101, 102, 104, 108, 109]
    assert kept.tolist() == [1, 9, 7, 2, 8] and kept.dtype == np.int16


@pytest.mark.parametrize("bin_rows", [0, 1])
def test_minmax_with_one_row_per_bin_keeps_everything(bin_rows):
    values = np.arange(5.0)
    rows, kept = minmax_decimate(values, 10, bin_rows)
    assert rows.tolist() == [10, 11, 12, 13, 14] and kept is values


def test_a_single_spike_survives_any_reduction():
    values = np.zeros(1_000_000, dtype=np.float32)
    values[777_777] = 1.0
    for bin_rows in (10, 1000, 99_999, 1_000_000):
        rows, kept = minmax_decimate(values, 0, bin_rows)
        assert kept.max() == 1.0 and 777_777 in rows
        assert len(rows) <= 2 * (1_000_000 // bin_rows + 1)


def test_bin_rows_for_a_point_budget():
    assert bin_rows_for(1000, 4000) == 1  # fits: rows as they are
    assert bin_rows_for(4000, 4000) == 1
    assert bin_rows_for(4001, 4000) == 3  # two points per bin
    assert bin_rows_for(45_000_000, 200_000) == 450
    assert bin_rows_for(0, 4000) == 1


def test_column_view_merges_the_window_into_the_overview(monkeypatch):
    from bec_lib.data_api import views

    monkeypatch.setattr(views, "OVERVIEW_POINTS", 20)
    values = np.arange(1000.0)
    view = ColumnView(total=1000)
    assert view.bin_rows == 100
    view.add_block(0, 600, values[:600])
    view.add_block(600, 1000, values[600:])
    rows, kept = view.columns()
    assert rows.tolist() == [r for b in range(0, 1000, 100) for r in (b, b + 99)]

    view.window = ViewWindow(250, 255, 1, np.arange(250, 255), values[250:255])
    rows, kept = view.columns()
    assert rows.tolist()[4:11] == [200, 250, 251, 252, 253, 254, 299]
    np.testing.assert_array_equal(kept, values[rows])
    assert view.describe(len(rows)) == {
        "total_rows": 1000,
        "loaded_rows": 1000,
        "overview_bin": 100,
        "window": {"start": 250, "stop": 255, "bin": 1},
        "points": len(rows),
    }
