"""Tests for the shared source classification and spec builders."""

from types import SimpleNamespace

import numpy as np

from bec_lib import messages
from bec_lib.data_api.decoding.signal_info import (
    device_signals,
    device_stream_specs,
    estimate_stored_bytes,
    is_configuration_signal,
    is_monitored_signal,
    is_preview_signal,
    multi_signal_name,
    open_scan_specs,
    recorded_points,
    scan_memory_specs,
    stored_key_candidates,
    stored_scan_specs,
)

# pylint: disable=missing-function-docstring

WAVE_ASYNC = (
    "det",
    None,
    {
        "obj_name": "wave",
        "storage_name": "wave",
        "describe": {"signal_info": {"acquisition_group": "g1"}},
    },
)


def multi_sub_signal(name: str) -> tuple:
    return (
        "gauss",
        f"data.{name}",
        {
            "component_name": "data",
            "signal_class": "AsyncMultiSignal",
            "obj_name": f"gauss_data_{name}",
            "storage_name": "gauss_data",
        },
    )


def fake_client(devices=None, async_signals=()):
    device_objs = {
        name: SimpleNamespace(_info={"signals": signals} if signals is not None else {})
        for name, signals in (devices or {}).items()
    }
    device_manager = SimpleNamespace(
        devices=device_objs, get_bec_signals=lambda _classes: list(async_signals)
    )
    return SimpleNamespace(device_manager=device_manager)


SAMX_SIGNALS = {
    "readback": {"obj_name": "samx", "component_name": "readback", "signal_class": "Signal"},
    "setpoint": {"obj_name": "samx_setpoint", "component_name": "setpoint"},
    "velocity": {"obj_name": "samx_velocity", "component_name": "velocity", "kind_str": "config"},
}
EIGER_SIGNALS = {
    "preview": {
        "obj_name": "preview_img",
        "component_name": "preview",
        "signal_class": "PreviewSignal",
    }
}


class TestDeviceLookups:
    def test_device_signals_distinguishes_unknown_from_uninformed(self):
        client = fake_client({"samx": SAMX_SIGNALS, "bare": None})
        assert device_signals(client, "samx") == SAMX_SIGNALS
        assert device_signals(client, "bare") == {}
        assert device_signals(client, "nope") is None
        assert device_signals(SimpleNamespace(device_manager=None), "samx") is None

    def test_monitored_uses_readout_priority_then_signal_names(self):
        client = fake_client({"samx": SAMX_SIGNALS, "bare": None})
        prio = {"monitored": ["samx", "bare", "ghost"]}
        assert is_monitored_signal(client, "samx", "samx", prio)
        assert is_monitored_signal(client, "samx", "samx_setpoint", prio)
        assert not is_monitored_signal(client, "samx", "velocity", prio)
        assert not is_monitored_signal(client, "samy", "samy", prio)  # not monitored
        # No signal info: the hub-signal convention decides.
        assert is_monitored_signal(client, "bare", "bare", prio)
        assert is_monitored_signal(client, "ghost", "ghost", prio)
        assert not is_monitored_signal(client, "ghost", "ghost_x", prio)
        assert not is_monitored_signal(client, "samx", "samx", None)

    def test_preview_by_obj_or_component_name(self):
        client = fake_client({"eiger": EIGER_SIGNALS, "samx": SAMX_SIGNALS})
        assert is_preview_signal(client, "eiger", "preview_img")
        assert is_preview_signal(client, "eiger", "preview")
        assert not is_preview_signal(client, "samx", "samx")
        assert not is_preview_signal(client, "nope", "preview")

    def test_configuration_by_declared_signal_kind(self):
        client = fake_client({"samx": SAMX_SIGNALS})
        assert is_configuration_signal(client, "samx", "samx_velocity")
        assert is_configuration_signal(client, "samx", "velocity")
        assert not is_configuration_signal(client, "samx", "samx")
        assert not is_configuration_signal(client, "nope", "nope")

    def test_standalone_configuration_signal_uses_describe_metadata(self):
        client = fake_client({"exposure": None})
        client.device_manager.devices["exposure"]._info = {
            "describe_configuration": {"exposure": {"dtype": "number", "shape": []}}
        }
        assert is_configuration_signal(client, "exposure", "exposure")
        assert not is_configuration_signal(client, "exposure", "other")


class TestSpecBuilders:
    def test_open_scan_specs(self):
        client = fake_client({"samx": SAMX_SIGNALS}, async_signals=[WAVE_ASYNC])
        cache = {}
        specs = open_scan_specs(
            client,
            [("samx", "samx"), ("det", "wave"), ("det", "other")],
            {"monitored": ["samx"], "baseline": []},
            cache,
        )
        assert [(s.kind, s.available) for s in specs] == [
            ("monitored", True),
            ("async", True),
            (None, False),
        ]
        # A declared acquisition group is not used for correlation.
        assert specs[1].multi_signal is None and specs[1].storage_name == "wave"
        assert ("det", "wave") in cache

    def test_open_scan_specs_mark_multi_signal_sub_signals(self):
        client = fake_client(async_signals=[WAVE_ASYNC, *map(multi_sub_signal, ("x", "y"))])
        status = messages.ScanStatusMessage(scan_id="s", status="open", info={})
        specs = open_scan_specs(
            client, [("gauss", "gauss_data_x"), ("gauss", "gauss_data_y"), ("det", "wave")], status
        )
        assert [s.multi_signal for s in specs] == ["gauss_data", "gauss_data", None]
        assert all(s.kind == "async" and s.storage_name for s in specs)

    def test_multi_signal_name(self):
        _, _, info = multi_sub_signal("x")
        assert multi_signal_name(info) == "gauss_data"
        # A single-signal group is stored under its own name: nothing to share.
        assert multi_signal_name({**info, "storage_name": "gauss_data_x"}) is None
        assert multi_signal_name({**info, "signal_class": "AsyncSignal"}) is None
        assert multi_signal_name(None) is None

    def test_scan_memory_specs(self):
        live_data = {"samx": {"samx": {0: {"value": 1.0}}}}
        specs = scan_memory_specs([("samx", "samx"), ("samy", "samy")], live_data)
        assert [(s.kind, s.available) for s in specs] == [("monitored", True), (None, False)]

    def test_device_stream_specs(self):
        client = fake_client({"eiger": EIGER_SIGNALS, "samx": SAMX_SIGNALS})
        specs = device_stream_specs(
            client,
            [
                ("wf", "monitor_1d"),
                ("eiger", "preview_img"),
                ("samx", "samx"),
                ("samx", "samx_velocity"),
                ("samx", "not_a_signal"),
                ("nope", "nope"),
            ],
        )
        assert [s.stream for s in specs] == [
            None,
            "preview",
            "readback",
            "configuration",
            None,
            None,
        ]
        assert all(s.storage_name is None for s in specs)
        # Legacy monitor entries and unknown entries of known devices are left to other paths.
        assert [s.available for s in specs] == [False, True, True, True, False, False]
        assert all(s.kind == "unindexed" for s in specs if s.available)


def history_message(stored, num_points):
    return messages.ScanHistoryMessage(
        scan_id="scan_h",
        scan_number=1,
        dataset_number=1,
        file_path="/data/scan_h_master.h5",
        exit_status="closed",
        start_time=1.0,
        end_time=2.0,
        scan_name="line_scan",
        num_points=num_points,
        num_monitored_readouts=num_points,
        stored_data_info=stored,
    )


class TestStoredScanSpecs:
    def test_datasets_with_one_row_per_readout_are_monitored(self):
        """The catalog does not say how a dataset was acquired: one row per monitored
        readout is a monitored signal (row = point); anything else stays standalone."""
        stored = {
            "samx": {"samx": {"shape": [5], "dtype": "float64"}},
            "det": {"img": {"shape": [3, 4, 4], "dtype": "uint16"}},
            "waveform": {"data": {"shape": [5, 100], "dtype": "float64"}},
        }
        specs = stored_scan_specs(
            fake_client(),
            [("samx", "samx"), ("det", "img"), ("waveform", "data"), ("det", "missing")],
            history_message(stored, 5),
        )
        assert [(s.kind, s.available) for s in specs] == [
            ("monitored", True),
            ("unindexed", True),
            ("monitored", True),
            (None, False),
        ]
        assert specs[0].estimated_bytes == 5 * 8
        assert specs[1].estimated_bytes == 3 * 4 * 4 * 2

    def test_stopped_scan_takes_its_points_from_the_datasets(self):
        """A scan that was aborted has fewer rows than planned readouts. Its monitored
        signals still share one row count, which is the number of recorded points."""
        stored = {
            "samx": {
                "samx": {"shape": [7], "dtype": "float64"},
                "samx_setpoint": {"shape": [7], "dtype": "float64"},
            },
            "bpm4i": {"bpm4i": {"shape": [7], "dtype": "float64"}},
            "samy": {"samy": {"shape": [1], "dtype": "float64"}},  # baseline
            "det": {"wave": {"shape": [40, 3], "dtype": "float64"}},  # declared async
            "old": {"trace": {"shape": [40], "dtype": "float64"}},  # no longer declared
        }
        specs = stored_scan_specs(
            fake_client(async_signals=[WAVE_ASYNC]),
            [
                ("samx", "samx"),
                ("bpm4i", "bpm4i"),
                ("samy", "samy"),
                ("det", "wave"),
                ("old", "trace"),
            ],
            history_message(stored, 10),
        )
        assert [str(spec.kind) for spec in specs] == [
            "monitored",
            "monitored",
            "unindexed",
            "async",
            "unindexed",
        ]

    def test_scan_with_one_recorded_point(self):
        stored = {
            "samx": {"samx": {"shape": [1], "dtype": "float64"}},
            "samy": {"samy": {"shape": [1], "dtype": "float64"}},
        }
        specs = stored_scan_specs(
            fake_client(), [("samx", "samx"), ("samy", "samy")], history_message(stored, 10)
        )
        assert [str(spec.kind) for spec in specs] == ["monitored", "monitored"]

    def test_points_come_from_the_datasets_without_a_readout_count(self):
        """An entry that does not say how many readouts were planned is treated like a
        stopped scan: the datasets tell."""
        stored = {
            "samx": {"samx": {"shape": [5], "dtype": "float64"}},
            "bpm4i": {"bpm4i": {"shape": [5], "dtype": "float64"}},
            "samy": {"samy": {"shape": [1], "dtype": "float64"}},
        }
        message = history_message(stored, 5)
        message.num_monitored_readouts = None
        message.num_points = None
        specs = stored_scan_specs(
            fake_client(), [("samx", "samx"), ("bpm4i", "bpm4i"), ("samy", "samy")], message
        )
        assert [str(spec.kind) for spec in specs] == ["monitored", "monitored", "unindexed"]

    def test_recorded_points(self):
        def shape(*rows):
            return {f"s{i}": {"shape": [n]} for i, n in enumerate(rows)}

        # The planned count wins when a dataset has that many rows.
        assert recorded_points({"a": shape(10, 10, 4, 4, 4)}, 10, {}) == 10
        # Otherwise the rows most datasets share; a single row counts only when alone.
        assert recorded_points({"a": shape(7, 7, 1, 1, 1)}, 10, {}) == 7
        assert recorded_points({"a": shape(1, 1)}, 10, {}) == 1
        # Declared async signals are left out, under their object or their storage name.
        declared = {"a": {"a_s0": {"obj_name": "a_s0", "storage_name": "s0"}}}
        assert recorded_points({"a": shape(40), "b": shape(7)}, None, declared) == 7
        assert recorded_points({}, 10, {}) is None
        assert recorded_points({"a": {"scalar": {"shape": []}}}, 10, {}) is None

    def test_declared_async_signal_is_async_whatever_its_row_count(self):
        """The live path classifies a declared async signal as async; a stored scan
        does the same, also when the dataset has one row per monitored readout."""
        stored = {"det": {"wave": {"shape": [5, 3], "dtype": "float64"}}}
        (spec,) = stored_scan_specs(
            fake_client(async_signals=[WAVE_ASYNC]), [("det", "wave")], history_message(stored, 5)
        )
        assert spec.kind == "async" and spec.multi_signal is None
        assert spec.storage_name == "wave"

    def test_declared_async_signal_is_read_from_its_storage_name(self):
        stored = {"det": {"saved_name": {"shape": [7], "dtype": "float64"}}}
        declared = ("det", None, {"obj_name": "det_wave", "storage_name": "saved_name"})
        (spec,) = stored_scan_specs(
            fake_client(async_signals=[declared]), [("det", "det_wave")], history_message(stored, 5)
        )
        assert spec.available and spec.kind == "async" and spec.storage_name == "saved_name"

    def test_sub_signals_of_one_multi_signal_share_their_group(self):
        stored = {
            "gauss": {
                f"gauss_data_{name}": {"shape": [4], "dtype": "float64"} for name in ("x", "y")
            }
        }
        specs = stored_scan_specs(
            fake_client(async_signals=[multi_sub_signal("x"), multi_sub_signal("y")]),
            [("gauss", "gauss_data_x"), ("gauss", "gauss_data_y")],
            history_message(stored, 4),
        )
        assert [(s.kind, s.multi_signal, s.storage_name) for s in specs] == [
            ("async", "gauss_data", "gauss_data_x"),
            ("async", "gauss_data", "gauss_data_y"),
        ]

    def test_message_without_stored_info_is_unindexed(self):
        specs = stored_scan_specs(
            fake_client(), [("samx", "samx"), ("det", "wave")], history_message({}, 5)
        )
        assert [(s.kind, s.available, s.estimated_bytes, s.provisional) for s in specs] == [
            ("unindexed", True, None, True),
            ("unindexed", True, None, True),
        ]

    def test_helpers(self):
        assert stored_key_candidates("det", "det_wave", "wave") == ["det_wave", "wave"]
        assert stored_key_candidates("det", "wave", None) == ["wave"]
        assert estimate_stored_bytes({"shape": [2, 3], "dtype": "int32"}) == 24
        assert estimate_stored_bytes({"shape": [], "dtype": "bogus"}) == 8
        assert estimate_stored_bytes(SimpleNamespace(shape=[4], dtype=np.dtype("float32"))) == 16
