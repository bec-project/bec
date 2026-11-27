"""Correlation groups: which sources can be aligned (plotted) with which."""

from types import SimpleNamespace
from unittest import mock

from bec_lib.data_api.decoding import stored_scan_specs
from bec_lib.data_api.models import PAIRED_GROUP, SCAN_GROUP, CorrelationGroup
from bec_lib.device import ReadoutPriority

from .test_data_api import (  # noqa: F401
    ASYNC_WAVE_INFO,
    async_msg,
    data_api,
    make_scan_item,
    mock_client,
    multi_signal_info,
    register_scan,
    seed_xy,
)

# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access


def _devices(mock_client, priorities):
    devices = {}
    for name, priority in priorities.items():
        dev = mock.MagicMock()
        dev.readout_priority = priority
        dev._info = {"signals": {name: {"obj_name": name, "signal_class": "Signal"}}}
        devices[name] = dev
    mock_client.device_manager.devices = mock.MagicMock()
    mock_client.device_manager.devices.get = lambda name, default=None: devices.get(name, default)


def test_open_scan_groups_monitored_and_multi_signal_sub_signals(data_api, mock_client):
    mock_client.device_manager.get_bec_signals.return_value = [
        ASYNC_WAVE_INFO,
        *multi_signal_info("gauss", "gauss_data", ("x", "y")),
    ]
    item = make_scan_item(mock_client, "scan_1", monitored=("samx", "bpm4i"))
    register_scan(mock_client, {"scan_1": item})
    sources = [
        ("samx", "samx"),
        ("bpm4i", "bpm4i"),
        ("det", "wave"),
        ("gauss", "gauss_data_x"),
        ("gauss", "gauss_data_y"),
        ("eiger", "img"),
    ]
    groups = data_api.correlation_groups(sources, scan="scan_1")
    assert groups == {
        SCAN_GROUP: [("samx", "samx"), ("bpm4i", "bpm4i")],
        CorrelationGroup.standalone("det", "wave"): [("det", "wave")],
        CorrelationGroup.multi("gauss", "gauss_data"): [
            ("gauss", "gauss_data_x"),
            ("gauss", "gauss_data_y"),
        ],
        CorrelationGroup.standalone("eiger", "img"): [("eiger", "img")],
    }


def test_live_without_a_running_scan_uses_the_device_configuration(data_api, mock_client):
    _devices(
        mock_client,
        {
            "bpm4i": ReadoutPriority.MONITORED,
            "ring": ReadoutPriority.CONTINUOUS,
            "eiger": ReadoutPriority.ASYNC,
        },
    )
    groups = data_api.correlation_groups(
        [("bpm4i", "bpm4i"), ("ring", "ring"), ("eiger", "eiger")], scan="live"
    )
    assert groups[SCAN_GROUP] == [("bpm4i", "bpm4i")]
    assert groups[CorrelationGroup.standalone("ring", "ring")] == [("ring", "ring")]
    assert groups[CorrelationGroup.standalone("eiger", "eiger")] == [("eiger", "eiger")]


def test_live_without_a_running_scan_counts_baseline_motors_as_scan_members(data_api, mock_client):
    """A scan promotes its motors to monitored: configured baseline motors must not be
    reported as unalignable with a monitored detector before the scan starts."""
    _devices(
        mock_client,
        {
            "samx": ReadoutPriority.BASELINE,
            "samy": ReadoutPriority.BASELINE,
            "bpm4i": ReadoutPriority.MONITORED,
            "flyer": ReadoutPriority.ON_REQUEST,
        },
    )
    sources = [("samx", "samx"), ("samy", "samy"), ("bpm4i", "bpm4i"), ("flyer", "flyer")]
    assert data_api.correlation_groups(sources, scan="live") == {SCAN_GROUP: sources}


def test_scanless_streams_pair_only_when_asked(data_api, mock_client):
    sources = [("samx", "samx"), ("samy", "samy")]
    assert len(data_api.correlation_groups(sources, scan=None)) == 2
    assert data_api.correlation_groups(sources, scan=None, pair_sources=True) == {
        PAIRED_GROUP: sources
    }


def test_subscription_reports_its_groups_and_routes(data_api, mock_client):
    item = make_scan_item(mock_client, "scan_1", monitored=("samx", "samy"))
    register_scan(mock_client, {"scan_1": item})
    sub = data_api.subscribe(
        sources=[("samx", "samx"), ("samy", "samy"), ("eiger", "img")],
        scan="scan_1",
        callback=lambda _: None,
    )
    assert sub.groups[SCAN_GROUP] == [("samx", "samx"), ("samy", "samy")]
    assert sub.routes == {("samx", "samx"): "redis", ("samy", "samy"): "redis"}
    sub.close()
    assert sub.groups == {}


def declaring(device, entry, group):
    """Device-manager entry of an async signal that declares a correlation group."""
    described = {"signal_info": {"correlation_group": group}}
    return (device, entry, {"obj_name": entry, "storage_name": entry, "describe": described})


def test_async_signals_are_grouped_as_they_declare(data_api, mock_client):
    mock_client.device_manager.get_bec_signals.return_value = [
        ASYNC_WAVE_INFO,
        declaring("det", "counts", "monitored"),
        declaring("panda", "enc1", "panda"),
        declaring("timer", "ticks", "panda"),
    ]
    item = make_scan_item(mock_client, "scan_1", monitored=("samx",))
    register_scan(mock_client, {"scan_1": item})
    sources = [
        ("samx", "samx"),
        ("det", "counts"),
        ("det", "wave"),
        ("panda", "enc1"),
        ("timer", "ticks"),
    ]
    assert data_api.correlation_groups(sources, scan="scan_1") == {
        # One value per scan point: paired with the monitored signals.
        SCAN_GROUP: [("samx", "samx"), ("det", "counts")],
        CorrelationGroup.standalone("det", "wave"): [("det", "wave")],
        # A tag is shared across devices.
        CorrelationGroup.tagged("panda"): [("panda", "enc1"), ("timer", "ticks")],
    }
    info = data_api.describe(sources, scan="scan_1")
    assert info[("det", "counts")].group == SCAN_GROUP
    assert str(info[("panda", "enc1")].group) == "tagged:panda"


def test_a_declared_group_wins_over_the_multi_signal_group(data_api, mock_client):
    sub_signals = multi_signal_info("gauss", "gauss_data", ("x", "y"))
    for _, _, info in sub_signals:
        info["describe"] = {"signal_info": {"correlation_group": "monitored"}}
    mock_client.device_manager.get_bec_signals.return_value = sub_signals
    item = make_scan_item(mock_client, "scan_1", monitored=("samx",))
    register_scan(mock_client, {"scan_1": item})
    sources = [("samx", "samx"), ("gauss", "gauss_data_x"), ("gauss", "gauss_data_y")]
    assert data_api.correlation_groups(sources, scan="scan_1") == {SCAN_GROUP: sources}


def test_the_deprecated_acquisition_group_is_read_too(data_api, mock_client):
    described = {"signal_info": {"acquisition_group": "monitored"}}
    mock_client.device_manager.get_bec_signals.return_value = [
        ("det", "counts", {"obj_name": "counts", "storage_name": "counts", "describe": described})
    ]
    item = make_scan_item(mock_client, "scan_1", monitored=("samx",))
    register_scan(mock_client, {"scan_1": item})
    sources = [("samx", "samx"), ("det", "counts")]
    assert data_api.correlation_groups(sources, scan="scan_1") == {SCAN_GROUP: sources}


def test_monitored_group_async_values_are_aligned_with_the_scan_points(data_api, mock_client):
    """The values of such a signal arrive on their own stream and pair with the motor by point."""
    mock_client.device_manager.get_bec_signals.return_value = [
        declaring("det", "counts", "monitored")
    ]
    item = make_scan_item(mock_client, "scan_1")
    register_scan(mock_client, {"scan_1": item})
    mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
    updates = []
    x, y = ("samx", "samx"), ("det", "counts")
    sub = data_api.subscribe(sources=[x, y], scan="live", callback=updates.append, update_rate=None)
    assert sub.groups == {SCAN_GROUP: [x, y]}
    path = data_api.paths[0]

    seed_xy(item, "scan_1", 0, 3)
    path._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
    for point, counts in enumerate([10.0, 11.0]):  # the third value has not arrived yet
        path._on_async_message({"data": async_msg("counts", counts, point)}, "scan_1", "det")

    update = updates[-1]
    assert update.metadata["group"] == SCAN_GROUP
    columns = update.aligned(keys=[x, y])
    assert list(columns[x]) == [0.0, 1.0] and list(columns[y]) == [10.0, 11.0]
    sub.close()


def test_a_declaration_changed_at_runtime_is_seen_at_the_next_resolution(data_api, mock_client):
    """A device may change what a signal declares without a config update."""
    mock_client.device_manager.get_bec_signals.return_value = [
        declaring("det", "counts", "monitored")
    ]
    item = make_scan_item(mock_client, "scan_1", monitored=("samx",))
    register_scan(mock_client, {"scan_1": item})
    x, y = ("samx", "samx"), ("det", "counts")
    sub = data_api.subscribe(sources=[x, y], scan="scan_1", callback=lambda _: None)
    assert sub.groups == {SCAN_GROUP: [x, y]}

    mock_client.device_manager.get_bec_signals.return_value = [declaring("det", "counts", "fast")]
    data_api.refresh("scan_1")
    assert sub.groups == {SCAN_GROUP: [x], CorrelationGroup.tagged("fast"): [y]}
    sub.close()


def _stored_scan(detector_rows):
    """A stored scan of three points with a detector dataset of ``detector_rows`` rows."""
    stored = {
        "samx": {"samx": {"shape": (3,), "dtype": "float64"}},
        "det": {"counts": {"shape": (detector_rows,), "dtype": "float64"}},
    }
    return SimpleNamespace(stored_data_info=stored, num_points=3, num_monitored_readouts=3)


def test_stored_scan_pairs_a_monitored_declaration_only_with_one_row_per_point(mock_client):
    """
    The declaration is today's, the file is what was recorded: a detector that missed a
    point, or was declared "monitored" later, must not be paired with the points by row.
    """
    mock_client.device_manager.get_bec_signals.return_value = [
        declaring("det", "counts", "monitored")
    ]
    sources = [("samx", "samx"), ("det", "counts")]
    complete = {spec.key: spec for spec in stored_scan_specs(mock_client, sources, _stored_scan(3))}
    assert complete[("det", "counts")].correlation_group == "monitored"
    short = {spec.key: spec for spec in stored_scan_specs(mock_client, sources, _stored_scan(2))}
    assert short[("det", "counts")].correlation_group is None
