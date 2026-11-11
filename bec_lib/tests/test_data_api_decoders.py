"""Message-level tests of the shared decoders (no client, no locks, no notify)."""

from types import SimpleNamespace

import numpy as np
import pytest

from bec_lib import messages
from bec_lib.data_api.data_bundler import DataBundler
from bec_lib.data_api.data_path import SourceSpec
from bec_lib.data_api.decoding import (
    ASYNC_UPDATE_HANDLERS,
    StoredRows,
    decode_async_message,
    decode_async_update,
    decode_dataset_columns,
    decode_live_data,
    decode_scan_message,
    decode_stream_message,
)


def monitored(device, entry=None):
    return SourceSpec(device=device, entry=entry or device, kind="monitored")


def async_spec(device, entry):
    return SourceSpec(device=device, entry=entry, kind="async")


def async_msg(entry, value, counter, update_type="add", **update_kwargs):
    return messages.DeviceMessage(
        signals={entry: {"value": value, "timestamp": 100.0 + counter}},
        metadata={
            "async_indices": {entry: counter},
            "async_update": messages.DeviceAsyncUpdate(
                type=update_type,
                max_shape=[None, None] if update_type == "add_slice" else [None],
                **update_kwargs,
            ).model_dump(),
        },
    )


class TestScanSegments:
    def test_live_data_inserts_only_unknown_points(self):
        bundle = DataBundler("scan_1")
        specs = [monitored("samx"), SourceSpec(device="samy", entry="samy", available=False)]
        live_data = {
            "samx": {"samx": {0: {"value": 0.0, "timestamp": 1.0}, 1: {"value": 1.0}, 2: "junk"}},
            "samy": {"samy": {0: {"value": 9.0}}},
        }
        assert decode_live_data(bundle, specs, live_data) == 2
        assert decode_live_data(bundle, specs, live_data) == 0  # nothing new
        snap = bundle.get_series("samx", "samx", "monitored").snapshot()
        assert list(snap.ordinals) == [0, 1] and list(snap.values) == [0.0, 1.0]
        assert ("samy", "samy") not in bundle.series

    def test_scan_message_keys_by_point_id(self):
        bundle = DataBundler("scan_1")
        specs = [monitored("samx"), monitored("samy")]
        msg = messages.ScanMessage(
            point_id=3,
            scan_id="scan_1",
            data={"samx": {"samx": {"value": 3.0, "timestamp": 1.0}}, "samy": {}},
            metadata={},
        )
        assert decode_scan_message(bundle, specs, msg) == 1
        assert list(bundle.get_series("samx", "samx", "monitored").snapshot().ordinals) == [3]
        assert decode_scan_message(bundle, specs, msg.model_copy(update={"point_id": None})) == 0

    def test_live_data_replay_skips_trimmed_points(self):
        bundle = DataBundler("scan_1", max_points=2)
        specs = [monitored("samx")]
        live_data = {"samx": {"samx": {i: {"value": float(i), "timestamp": 1.0} for i in range(3)}}}
        assert decode_live_data(bundle, specs, live_data) == 3
        assert decode_live_data(bundle, specs, live_data) == 0  # nothing re-inserted
        live_data["samx"]["samx"][3] = {"value": 3.0, "timestamp": 1.0}
        assert decode_live_data(bundle, specs, live_data) == 1
        assert list(bundle.get_series("samx", "samx", "monitored").snapshot().ordinals) == [2, 3]

    def test_live_data_replay_leaves_a_bulk_series_intact(self):
        bundle = DataBundler("scan_h")
        series = bundle.get_series("samx", "samx", "monitored")
        assert series.extend(np.arange(3.0), np.arange(3.0)) == 3
        live_data = {"samx": {"samx": {i: {"value": float(i), "timestamp": 1.0} for i in range(3)}}}
        assert decode_live_data(bundle, [monitored("samx")], live_data) == 0  # nothing new
        assert isinstance(series.snapshot().values, np.ndarray)


class TestAsyncStreams:
    def test_add_keys_by_message_counter(self):
        bundle, state = DataBundler("scan_1"), {}
        specs = [async_spec("det", "wave")]
        assert (
            decode_async_message(bundle, state, specs, async_msg("wave", 10.0, 0), device="det")
            == 1
        )
        assert (
            decode_async_message(bundle, state, specs, async_msg("wave", 30.0, 2), device="det")
            == 1
        )
        snap = bundle.get_series("det", "wave", "async").snapshot()
        assert list(snap.ordinals) == [0, 2] and snap.complete is False
        assert snap.metadata["async_update_type"] == "add"
        assert "acquisition_group" not in snap.metadata

    @pytest.mark.parametrize("fragment", [list, tuple, np.asarray])
    def test_add_slice_rows_accumulate_and_append(self, fragment):
        bundle, state = DataBundler("scan_1"), {}
        specs = [async_spec("det", "wave")]
        decode_async_message(
            bundle,
            state,
            specs,
            async_msg("wave", fragment([1, 2]), 0, "add_slice", index=0),
            device="det",
        )
        decode_async_message(
            bundle,
            state,
            specs,
            async_msg("wave", fragment([3]), 1, "add_slice", index=0),
            device="det",
        )
        decode_async_message(
            bundle,
            state,
            specs,
            async_msg("wave", fragment([9]), 2, "add_slice", index=1),
            device="det",
        )
        snap = bundle.get_series("det", "wave", "async").snapshot()
        assert list(snap.ordinals) == [0, 1]
        assert list(snap.values[0]) == [1, 2, 3] and list(snap.values[1]) == [9]

    def test_add_slice_delivered_rows_never_change(self):
        """Rows are read-only views of a growing buffer: later appends, a wider dtype
        and buffer growth never alter a snapshot that was already delivered."""
        bundle, state = DataBundler("scan_1"), {}
        specs = [async_spec("det", "wave")]
        series = bundle.get_series("det", "wave", "async")

        def append(i, values, index=0):
            msg = async_msg("wave", values, i, "add_slice", index=index)
            decode_async_message(bundle, state, specs, msg, device="det")

        append(0, [1, 2], index=0)
        first = series.snapshot()
        append(1, list(range(3, 40)))  # outgrows the initial buffer
        second = series.snapshot()
        append(2, [40.5])  # widens the dtype
        third = series.snapshot()
        assert list(first.values[0]) == [1, 2]
        assert list(second.values[0]) == list(range(1, 40))
        assert list(third.values[0]) == [*range(1, 40), 40.5]
        assert third.values[0].dtype == np.float64 and second.values[0].dtype.kind == "i"
        with pytest.raises(ValueError):
            third.values[0][0] = 0

    def test_add_slice_rows_stop_at_the_declared_row_size(self):
        bundle, state = DataBundler("scan_1"), {}
        specs = [async_spec("det", "wave")]
        for i, values in enumerate(([1, 2], [3, 4], [5])):
            msg = messages.DeviceMessage(
                signals={"wave": {"value": values, "timestamp": 100.0 + i}},
                metadata={
                    "async_indices": {"wave": i},
                    "async_update": {"type": "add_slice", "max_shape": [None, 3], "index": 0},
                },
            )
            decode_async_message(bundle, state, specs, msg, device="det")
        assert list(bundle.get_series("det", "wave", "async").snapshot().values[0]) == [1, 2, 3]

    def test_add_slice_two_dimensional_fragment_is_not_inserted(self):
        bundle = DataBundler("scan_1")
        msg = async_msg("wave", [[1, 2]], 0, "add_slice", index=0)
        update = messages.DeviceAsyncUpdate.model_validate(msg.metadata["async_update"])
        assert (
            decode_async_update(bundle, {}, [async_spec("det", "wave")], msg, update, device="det")
            == 0
        )
        assert list(bundle.get_series("det", "wave", "async").snapshot().values) == []

    def test_add_slice_without_index_is_rejected(self):
        bundle = DataBundler("scan_1")
        msg = messages.DeviceMessage.model_construct(
            signals={"wave": {"value": [1], "timestamp": 1.0}},
            metadata={"async_update": {"type": "add_slice", "max_shape": [None]}},
        )
        assert decode_async_message(bundle, {}, [async_spec("det", "wave")], msg, device="det") == 0
        assert not bundle.series

    def test_replace_is_a_single_point(self):
        bundle, state = DataBundler("scan_1"), {}
        specs = [async_spec("det", "wave")]
        decode_async_message(
            bundle, state, specs, async_msg("wave", [1], 0, "replace"), device="det"
        )
        decode_async_message(
            bundle, state, specs, async_msg("wave", [1, 2], 1, "replace"), device="det"
        )
        snap = bundle.get_series("det", "wave", "async").snapshot()
        assert list(snap.ordinals) == [0] and list(snap.values) == [[1, 2]]

    def test_add_without_async_index_is_dropped(self):
        bundle = DataBundler("scan_1")
        msg = async_msg("wave", 10.0, 0)
        del msg.metadata["async_indices"]
        assert decode_async_message(bundle, {}, [async_spec("det", "wave")], msg, device="det") == 0
        assert list(bundle.get_series("det", "wave", "async").snapshot().values) == []

    def test_message_without_update_metadata_is_skipped_silently(self):
        bundle = DataBundler("scan_1")
        msg = messages.DeviceMessage(
            signals={"wave": {"value": [1], "timestamp": 1.0}}, metadata={}
        )
        assert decode_async_message(bundle, {}, [async_spec("det", "wave")], msg, device="det") == 0
        assert not bundle.series

    def test_invalid_update_metadata_is_dropped(self):
        bundle = DataBundler("scan_1")
        msg = messages.DeviceMessage.model_construct(
            signals={"wave": {"value": [1], "timestamp": 1.0}},
            metadata={"async_update": {"type": "no-such-type"}},
        )
        assert decode_async_message(bundle, {}, [async_spec("det", "wave")], msg, device="det") == 0
        assert not bundle.series

    def test_unknown_handler_drops_but_custom_handler_extends(self):
        bundle = DataBundler("scan_1")
        future = SimpleNamespace(type="sparse_block", max_shape=[None], index=None)
        msg = messages.DeviceMessage.model_construct(
            signals={"wave": {"value": [1, 2], "timestamp": 1.0}},
            metadata={"async_indices": {"wave": 0}, "async_update": {"type": "sparse_block"}},
        )
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "bec_lib.data_api.decoding.async_updates.DeviceAsyncUpdate",
                SimpleNamespace(model_validate=lambda _p: future),
            )
            specs = [async_spec("det", "wave")]
            assert decode_async_message(bundle, {}, specs, msg, device="det") == 0
            seen = []

            def record_point(point):
                seen.append(point)
                return 1

            handlers = {**ASYNC_UPDATE_HANDLERS, "sparse_block": record_point}
            assert (
                decode_async_message(bundle, {}, specs, msg, device="det", handlers=handlers) == 1
            )
        assert seen[0].value == [1, 2] and seen[0].update is future

    def test_other_devices_and_missing_signals_are_ignored(self):
        bundle = DataBundler("scan_1")
        specs = [async_spec("det", "wave"), async_spec("other", "wave")]
        assert (
            decode_async_message(bundle, {}, specs, async_msg("wave", 1.0, 0), device="other") == 1
        )
        assert (
            decode_async_message(bundle, {}, specs, async_msg("other_entry", 1.0, 0), device="det")
            == 0
        )
        assert list(bundle.series) == [("other", "wave")]


class TestDeviceStreams:
    def test_readback_requires_device_message(self):
        bundle = DataBundler("")
        spec = SourceSpec(device="samx", entry="samx", kind="unindexed", storage_name="readback")
        assert decode_stream_message(bundle, spec, "readback", SimpleNamespace(data=1)) == 0
        msg = messages.DeviceMessage(
            signals={"samx": {"value": 1.5, "timestamp": 7.0}}, metadata={"scan_id": "s9"}
        )
        assert decode_stream_message(bundle, spec, "readback", msg) == 1
        assert decode_stream_message(bundle, spec, "readback", msg) == 1
        snap = bundle.get_series("samx", "samx", "unindexed").snapshot()
        assert list(snap.ordinals) == [0, 1] and list(snap.values) == [1.5, 1.5]
        assert snap.metadata == {"scan_id": "s9", "stream": "readback"}

    def test_preview_takes_the_data_field_and_message_timestamp(self):
        bundle = DataBundler("")
        spec = SourceSpec(device="eiger", entry="preview_img", kind="unindexed", stream="preview")
        msg = messages.DevicePreviewMessage(
            device="eiger", signal="preview_img", data=np.arange(3), timestamp=3.0
        )
        assert decode_stream_message(bundle, spec, "preview", msg) == 1
        assert decode_stream_message(bundle, spec, "preview", SimpleNamespace(data=None)) == 0
        snap = bundle.get_series("eiger", "preview_img", "unindexed").snapshot()
        assert list(snap.timestamps) == [3.0] and "scan_id" not in snap.metadata


class TestStoredDatasets:
    def test_bulk_fill_keeps_numpy_columns(self):
        bundle = DataBundler("scan_h")
        spec = SourceSpec(device="samx", entry="samx", kind="monitored")
        data = {"value": np.arange(4.0), "timestamp": np.arange(4.0) + 100}
        assert decode_dataset_columns(bundle, spec, data, file_path="/f.h5") == 4
        snap = bundle.get_series("samx", "samx", "monitored").snapshot()
        assert isinstance(snap.values, np.ndarray) and snap.complete
        assert snap.metadata["file_path"] == "/f.h5"

    def test_non_empty_series_falls_back_to_per_point_inserts(self):
        bundle = DataBundler("scan_h")
        spec = SourceSpec(device="samx", entry="samx", kind="monitored")
        bundle.get_series("samx", "samx", "monitored").insert(0, -1.0, None)
        assert decode_dataset_columns(bundle, spec, {"value": [5.0, 6.0], "timestamp": [1.0]}) == 2
        snap = bundle.get_series("samx", "samx", "monitored").snapshot()
        # Rows overwrite ordinals 0..n-1; a timestamp column of the wrong length is dropped.
        assert list(snap.values) == [5.0, 6.0] and list(snap.timestamps) == [None, None]

    def test_missing_values_insert_nothing(self):
        bundle = DataBundler("scan_h")
        spec = SourceSpec(device="samx", entry="samx", kind="monitored")
        assert decode_dataset_columns(bundle, spec, {"value": None}) == 0
        assert not bundle.series


class TestContinuousHold:
    def test_held_sample_reports_nothing_inserted(self):
        bundle = DataBundler("", correlation="continuous")
        for device in ("samx", "samy"):
            bundle.get_series(device, device, "unindexed")
        spec = SourceSpec(device="samx", entry="samx", kind="unindexed", stream="readback")
        msg = messages.DeviceMessage(
            signals={"samx": {"value": 1.0, "timestamp": 1.0}}, metadata={}
        )
        assert decode_stream_message(bundle, spec, "readback", msg) == 1  # first: held, reported
        assert decode_stream_message(bundle, spec, "readback", msg) == 0  # held again: silent
        spec_y = SourceSpec(device="samy", entry="samy", kind="unindexed", stream="readback")
        msg_y = messages.DeviceMessage(
            signals={"samy": {"value": 2.0, "timestamp": 2.0}}, metadata={}
        )
        assert decode_stream_message(bundle, spec_y, "readback", msg_y) == 1  # first row


def test_stored_rows_become_the_points_of_a_source():
    bundle = DataBundler("stored")
    spec = SourceSpec("det", "wave", kind="async")
    data = {"value": np.arange(6.0).reshape(3, 2), "timestamp": [1.0, 2.0, 3.0]}
    assert decode_dataset_columns(bundle, spec, data, file_path="/data/scan.h5") == 3
    snapshot = bundle.get_series("det", "wave", "async").snapshot()
    np.testing.assert_array_equal(snapshot.ordinals, [0, 1, 2])
    np.testing.assert_array_equal(snapshot.as_array(), data["value"])
    np.testing.assert_array_equal(snapshot.timestamps, [1.0, 2.0, 3.0])
    assert snapshot.metadata["file_path"] == "/data/scan.h5"


def test_stored_time_stamps_that_are_not_per_row_are_dropped():
    """An async signal stores one time stamp per update, but its samples as rows."""
    bundle = DataBundler("stored")
    spec = SourceSpec("det", "wave", kind="async")
    assert decode_dataset_columns(bundle, spec, {"value": [1, 2, 3, 4], "timestamp": [1.0]}) == 4
    snapshot = bundle.get_series("det", "wave", "async").snapshot()
    assert snapshot.as_array().tolist() == [1, 2, 3, 4]
    assert list(snapshot.timestamps) == [None] * 4


def test_stored_scalar_is_one_point_and_a_missing_value_is_none():
    bundle = DataBundler("stored")
    spec = SourceSpec("samx", "samx", kind="unindexed")
    assert decode_dataset_columns(bundle, spec, {"value": np.float64(2.5)}) == 1
    assert bundle.get_series("samx", "samx", "unindexed").snapshot().values.tolist() == [2.5]
    assert decode_dataset_columns(bundle, SourceSpec("samy", "samy"), {}) == 0


def test_stored_rows_are_written_in_blocks_and_shown_without_a_copy():
    series = DataBundler("scan").get_series("cam", "frame", "async")
    frames = np.arange(12, dtype=np.uint16).reshape(4, 3)
    rows = StoredRows(file_path="/data/scan.h5")

    assert rows.publish(series) == 0  # nothing written yet
    rows.write(0, 2, 4, {"value": frames[:2], "timestamp": np.array([1.0, 2.0])})
    assert rows.publish(series) == 2
    partial = series.snapshot()
    assert partial.values.tolist() == frames[:2].tolist() and not partial.complete
    assert partial.metadata["file_path"] == "/data/scan.h5"

    rows.write(2, 4, 4, {"value": frames[2:], "timestamp": np.array([3.0, 4.0])})
    assert rows.publish(series) == 2
    full = series.snapshot()
    assert full.values.dtype == np.uint16 and full.values.tolist() == frames.tolist()
    assert full.timestamps.tolist() == [1.0, 2.0, 3.0, 4.0] and full.complete
    assert np.shares_memory(full.values, partial.values)


def test_stored_rows_without_time_stamps_and_out_of_order():
    series = DataBundler("scan").get_series("samx", "samx", "monitored")
    rows = StoredRows()
    rows.write(0, 2, 3, {"value": [1.0, 2.0]})
    rows.publish(series)
    assert series.snapshot().timestamps.tolist() == [None, None]

    with pytest.raises(ValueError, match="do not follow the 2 rows held"):
        rows.write(1, 3, 3, {"value": [2.0, 3.0]})


def test_stored_rows_keep_rows_of_different_length_as_objects():
    series = DataBundler("scan").get_series("det", "wave", "async")
    ragged = np.empty(2, dtype=object)
    ragged[0], ragged[1] = np.array([1, 2, 3]), np.array([4])
    rows = StoredRows()
    rows.write(0, 2, 2, {"value": ragged})
    rows.publish(series)
    stored = series.snapshot().values
    assert stored.dtype == object and stored[0].tolist() == [1, 2, 3] and stored[1].tolist() == [4]
