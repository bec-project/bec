"""Registered procedure delegates sequences to existing BEC authorities."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bec_lib.scan_sequencer import Schedule
from bec_server.procedures.builtin_procedures import run_sequence
from bec_server.procedures.procedure_registry import callable_from_name
from bec_server.scan_server.scan_manager import scan_signature_with_modifiers
from bec_server.scan_server.scans.time_scan import TimeScan


@pytest.mark.parametrize("json_text", [False, True])
def test_registered_sequence_procedure_uses_scan_and_device_apis(json_text):
    info = {
        "class": "TimeScan",
        "base_class": "ScanBaseV4",
        "arg_input": {},
        "arg_bundle_size": TimeScan.arg_bundle_size,
        "required_kwargs": [],
        "signature": scan_signature_with_modifiers(TimeScan),
        "doc": "TimeScan",
    }
    trace = []
    status = SimpleNamespace(done=True, wait=lambda **kwargs: None)
    device = SimpleNamespace(
        read_only=False, set=lambda value: trace.append(("set", value)) or status
    )
    report = SimpleNamespace(
        request=SimpleNamespace(queue=SimpleNamespace(status="COMPLETED")), wait=Mock()
    )
    scan = SimpleNamespace(
        scan_info=info, run=lambda *args, **kwargs: trace.append(("scan", args, kwargs)) or report
    )
    client = SimpleNamespace(
        device_manager=SimpleNamespace(devices={"samx": device}),
        scans=SimpleNamespace(
            _available_scans={"time_scan": scan},
            umv=lambda dev, value, **kwargs: trace.append(("umv", value)) or report,
            time_scan=scan.run,
        ),
        callbacks=SimpleNamespace(poll=lambda: None),
    )
    document = Schedule(
        id="test",
        revision=1,
        root={
            "id": "root",
            "type": "sequence",
            "children": [
                {"id": "set", "type": "set", "device": "samx", "value": 1},
                {"id": "scan", "type": "scan_call", "name": "time_scan", "positional": [1, 0.1]},
            ],
        },
    )
    assert callable_from_name("run_sequence") is run_sequence
    run_sequence(document.model_dump_json() if json_text else document.model_dump(), bec=client)
    assert trace == [("umv", 1), ("scan", (1, 0.1), {})]
    report.wait.assert_not_called()


def test_metadata_fixture_matches_real_time_scan_signature():
    import json
    from pathlib import Path

    fixture = Path(__file__).resolve().parents[3] / "bec_lib/tests/scan_sequencer/scan_info.json"
    recorded = json.loads(fixture.read_text())["time_scan"]
    assert recorded["signature"] == scan_signature_with_modifiers(TimeScan)
    assert recorded["arg_bundle_size"] == TimeScan.arg_bundle_size
