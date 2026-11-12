"""Sequences compose real queue requests and proxy sets against demo services."""

from unittest.mock import Mock

import pytest

from bec_lib.procedures.helper import ProcedureState
from bec_lib.scan_sequencer import Definition, DefinitionLibrary, Schedule, Sequencer


@pytest.mark.timeout(180)
def test_sequence_uses_existing_scan_queue_and_device_statuses(
    bec_ipython_client_fixture, monkeypatch
):
    # Use the standard demo configuration loaded by the shared client fixture.
    grid_arguments = ["samx", -1.0, 1.0, 2, "samy", -1.0, 1.0, 2]
    grid_keywords = {"relative": False, "exp_time": 0.01}
    definition = Definition(
        id="measure_grid",
        version=1,
        parameters={
            "type": "object",
            "properties": {"position": {"type": "number"}},
            "required": ["position"],
            "additionalProperties": False,
        },
        body={
            "id": "measurement",
            "type": "sequence",
            "children": [
                {
                    "id": "move_z",
                    "type": "set",
                    "device": "samz",
                    "value": {"param": "position"},
                    "wait": True,
                },
                {
                    "id": "grid",
                    "type": "scan_call",
                    "name": "grid_scan",
                    "positional": grid_arguments,
                    "arguments": grid_keywords,
                },
            ],
        },
    )
    document = Schedule(
        id="grid_at_z_positions",
        revision=1,
        root={
            "id": "repeat",
            "type": "repeat",
            "count": 2,
            "body": {
                "id": "positions",
                "type": "for_each",
                "variable": "position",
                "values": [1, -1],
                "body": {
                    "id": "measure",
                    "type": "component_call",
                    "definition": {"id": "measure_grid", "version": 1},
                    "arguments": {"position": {"ref": "position"}},
                },
            },
        },
    )
    scheduler = Sequencer(bec_ipython_client_fixture, DefinitionLibrary([definition]))
    trace = []
    native_move = bec_ipython_client_fixture.scans.umv
    native_scan = bec_ipython_client_fixture.scans.grid_scan

    def record_move(target, value, **kwargs):
        trace.append(("set", target.name, value))
        return native_move(target, value, **kwargs)

    def record_scan(*args, **kwargs):
        trace.append(("scan", "grid_scan"))
        return native_scan(*args, **kwargs)

    monkeypatch.setattr(bec_ipython_client_fixture.scans, "umv", record_move)
    monkeypatch.setattr(bec_ipython_client_fixture.scans, "grid_scan", record_scan)
    scheduler.execute(document)
    assert trace == [
        ("set", "samz", 1),
        ("scan", "grid_scan"),
        ("set", "samz", -1),
        ("scan", "grid_scan"),
        ("set", "samz", 1),
        ("scan", "grid_scan"),
        ("set", "samz", -1),
        ("scan", "grid_scan"),
    ]

    # Execute the readable export through the same real public IPython calls.
    move = Mock(wraps=bec_ipython_client_fixture.scans.umv)
    acquire = Mock(wraps=bec_ipython_client_fixture.scans.grid_scan)
    native_calls = Mock()
    native_calls.attach_mock(move, "umv")
    native_calls.attach_mock(acquire, "grid_scan")
    monkeypatch.setattr(bec_ipython_client_fixture.scans, "umv", move)
    monkeypatch.setattr(bec_ipython_client_fixture.scans, "grid_scan", acquire)
    namespace = {
        "bec": bec_ipython_client_fixture,
        "dev": bec_ipython_client_fixture.device_manager.devices,
        "scans": bec_ipython_client_fixture.scans,
    }
    source = scheduler.export(document, format="python")
    with bec_ipython_client_fixture.scans.hide_report:
        exec(compile(source, "exported_sequence.py", "exec"), namespace)
    assert move.call_count == acquire.call_count == 4
    assert [call.args[1] for call in move.call_args_list] == [1, -1, 1, -1]
    assert all(
        call.args[0] is bec_ipython_client_fixture.device_manager.devices.samz
        for call in move.call_args_list
    )
    assert [call[0] for call in native_calls.mock_calls] == ["umv", "grid_scan"] * 4
    assert all(
        list(call.args) == grid_arguments and call.kwargs == grid_keywords
        for call in acquire.call_args_list
    )

    # Submit to the scan server's procedure manager using the public client API.
    status = bec_ipython_client_fixture.proc.request_new(
        "run_sequence",
        ((document.model_dump_json(), [definition.model_dump()]), {}),
        queue="sequence-e2e",
    )
    status.wait(timeout_s=90, raise_on_failure=True)
    assert status.state == ProcedureState.SUCCESS
