"""Ingest paths: co-delivered continuous members and per-segment scan ingest."""

from unittest import mock

from bec_lib import messages

from .test_data_api import data_api, make_scan_item, mock_client, register_scan  # noqa: F401

# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access


def _motor(*entries):
    motor = mock.MagicMock()
    motor._info = {
        "signals": {entry: {"obj_name": entry, "signal_class": "Signal"} for entry in entries}
    }
    return motor


def test_entries_of_one_readback_message_form_one_continuous_row(data_api, mock_client):
    """A device's readback and setpoint arrive in one message: one row per message,
    not one row per entry with a stale partner."""
    motor = _motor("samx", "samx_setpoint")
    mock_client.device_manager.devices = mock.MagicMock()
    mock_client.device_manager.devices.get = lambda name, default=None: (
        motor if name == "samx" else default
    )
    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx"), ("samx", "samx_setpoint")],
        scan=None,
        callback=updates.append,
        min_emit_interval=0,
        correlation="continuous",
    )
    plugin = data_api.paths[0]
    assert len([key for key in plugin._feeds if key[0] == "stream"]) == 1  # one feed per device
    for value in (0.0, 1.0, 2.0):
        msg = messages.DeviceMessage(
            signals={
                "samx": {"value": value, "timestamp": value},
                "samx_setpoint": {"value": value, "timestamp": value},
            },
            metadata={},
        )
        plugin._on_stream_message({"data": msg}, ("stream", "readback", "samx", ""))
    columns = updates[-1].aligned()
    assert list(columns[("samx", "samx")]) == [0.0, 1.0, 2.0]
    assert list(columns[("samx", "samx_setpoint")]) == [0.0, 1.0, 2.0]
    sub.close()


def test_a_scan_segment_is_ingested_without_rewalking_the_scan(data_api, mock_client):
    item = make_scan_item(mock_client, "scan_1", monitored=("samx",))
    register_scan(mock_client, {"scan_1": item})
    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx")], scan="scan_1", callback=updates.append, min_emit_interval=0
    )
    plugin = data_api.paths[0]
    # A segment carries its own point: scan storage (live data) is not consulted.
    mock_client.queue.scan_storage.find_scan_by_ID.side_effect = AssertionError("re-walk")
    for point_id in range(3):
        plugin._on_scan_segment(
            {
                "scan_id": "scan_1",
                "point_id": point_id,
                "data": {"samx": {"samx": {"value": float(point_id), "timestamp": 1.0}}},
            },
            {"scan_id": "scan_1"},
        )
    assert list(updates[-1].get("samx", "samx").values) == [0.0, 1.0, 2.0]
    # A segment repeated (e.g. also seen by the backfill) is not applied twice.
    plugin._on_scan_segment(
        {"scan_id": "scan_1", "point_id": 1, "data": {"samx": {"samx": {"value": 9.0}}}},
        {"scan_id": "scan_1"},
    )
    assert list(updates[-1].get("samx", "samx").values) == [0.0, 1.0, 2.0]
    mock_client.queue.scan_storage.find_scan_by_ID.side_effect = None
    sub.close()
