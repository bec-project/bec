"""Correlation groups: which sources can be aligned (plotted) with which."""

from unittest import mock

from bec_lib.data_api.models import CONTINUOUS_GROUP, SCAN_GROUP, CorrelationGroup
from bec_lib.device import ReadoutPriority

from .test_data_api import (  # noqa: F401
    ASYNC_WAVE_INFO,
    data_api,
    make_scan_item,
    mock_client,
    multi_signal_info,
    register_scan,
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


def test_scanless_streams_pair_only_continuously(data_api, mock_client):
    sources = [("samx", "samx"), ("samy", "samy")]
    assert len(data_api.correlation_groups(sources, scan=None)) == 2
    assert data_api.correlation_groups(sources, scan=None, correlation="continuous") == {
        CONTINUOUS_GROUP: sources
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
