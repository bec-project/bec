"""Sequences compose real queue requests and proxy sets against demo services."""

from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from bec_lib.scan_sequencer import (
    BECAdapter,
    Definition,
    DefinitionLibrary,
    ScanCatalog,
    Schedule,
    Sequencer,
)


@pytest.mark.timeout(120)
def test_sequence_uses_existing_scan_queue_and_device_statuses(
    bec_ipython_client_fixture, tmp_path, monkeypatch
):
    # This test exercises queue composition with one axis and one camera, keeping acquisition ownership
    # and pre-scan lifecycle behavior on the existing scan server.
    config = tmp_path / "sequence_devices.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "samx": {
                    "deviceClass": "ophyd_devices.SimPositioner",
                    "readoutPriority": "baseline",
                    "deviceConfig": {"delay": 0.01, "limits": [-50, 50]},
                    "enabled": True,
                    "readOnly": False,
                    "softwareTrigger": False,
                },
                "eiger": {
                    "deviceClass": "ophyd_devices.SimCamera",
                    "readoutPriority": "monitored",
                    "deviceConfig": {"device_access": True},
                    "enabled": True,
                    "readOnly": False,
                    "softwareTrigger": True,
                },
            }
        )
    )
    bec_ipython_client_fixture.config.update_session_with_file(
        str(config), save_recovery=False, force=True
    )
    examples = Path(__file__).resolve().parents[3] / "docs/scan_sequencer/examples"
    definition = Definition.model_validate_json((examples / "measure_position.json").read_text())
    document = Schedule.model_validate_json((examples / "alternating_positions.json").read_text())
    scheduler = Sequencer(
        ScanCatalog.from_client(bec_ipython_client_fixture), DefinitionLibrary([definition])
    )
    trace = []

    class RecordingAdapter(BECAdapter):
        def set(self, name, value, *, wait=True):
            trace.append(("set", value))
            return super().set(name, value, wait=wait)

        def scan(self, name, positional, arguments):
            trace.append(("scan", name))
            return super().scan(name, positional, arguments)

    scheduler.execute(document, RecordingAdapter(bec_ipython_client_fixture))
    assert trace == [
        ("set", 1),
        ("scan", "time_scan"),
        ("set", -1),
        ("scan", "time_scan"),
        ("set", 1),
        ("scan", "time_scan"),
        ("set", -1),
        ("scan", "time_scan"),
    ]

    # Execute the readable export through the same real public IPython calls.
    move = Mock(wraps=bec_ipython_client_fixture.scans.umv)
    acquire = Mock(wraps=bec_ipython_client_fixture.scans.time_scan)
    monkeypatch.setattr(bec_ipython_client_fixture.scans, "umv", move)
    monkeypatch.setattr(bec_ipython_client_fixture.scans, "time_scan", acquire)
    namespace = {}
    source = scheduler.export(document, format="python")
    exec(compile(source, "exported_sequence.py", "exec"), namespace)
    namespace["run"](bec_ipython_client_fixture)
    assert move.call_count == acquire.call_count == 4
    assert [call.args[1] for call in move.call_args_list] == [1, -1, 1, -1]

    status = bec_ipython_client_fixture.proc.request_new(
        "run_sequence",
        ((document.model_dump_json(), [definition.model_dump()]), {}),
        queue="sequence-e2e",
    )
    status.wait(timeout_s=30, raise_on_failure=True)
    assert status.done
