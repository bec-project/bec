from collections import Counter

import h5py
import pytest

from bec_lib import endpoints
from bec_lib.bec_errors import ScanAbortion
from bec_lib.tests.utils import wait_until
from bec_server.scan_server.scans import acquire, line_scan, scan_modifier


class _CallbackRecorder:
    def __init__(self):
        self.data = []

    def record(self, data, metadata):
        self.data.append((data["scan_id"], data["point_id"], metadata["entry_role"]))


class FailingAcquire(acquire.Acquire):
    """Test acquisition that fails after opening."""

    scan_name = "failing_subscan_acquire"

    @scan_modifier.scan_hook
    def at_each_point(self):
        raise RuntimeError("Intentional failure in an acquisition")


class FailingCloseAcquire(acquire.Acquire):
    """Test acquisition that fails after its close action."""

    scan_name = "failing_close_subscan_acquire"

    @scan_modifier.scan_hook
    def close_scan(self):
        super().close_scan()
        raise RuntimeError("Intentional failure after the acquisition close action")


class BracketedLineScan(line_scan.LineScan):
    """Test plugin that captures the motor position before and after a line scan."""

    scan_name = "bracketed_line_scan"

    @scan_modifier.scan_hook
    def configure_subscans(self):
        on_request = self.scan_info.readout_priority_modification["on_request"]
        fail_role = self.scan_info.user_metadata.get("fail_role")
        before = FailingAcquire if fail_role == "before" else acquire.Acquire
        self.add_subscan(
            before,
            exp_time=0.01,
            monitored=["samx"],
            on_request=on_request,
        )
        after = {"after": FailingAcquire, "close": FailingCloseAcquire}.get(
            fail_role, acquire.Acquire
        )
        self.add_subscan(
            after,
            exp_time=0.02,
            monitored=["samx"],
            on_request=on_request,
            when="after",
        )


@pytest.mark.timeout(100)
@pytest.mark.parametrize(
    "client_fixture, fail_role, event_devices",
    [
        pytest.param("bec_client_lib", None, False, id="complete"),
        pytest.param("bec_client_lib", "after", False, id="failed_after"),
        pytest.param("bec_client_lib", "before", False, id="failed_before"),
        pytest.param("bec_client_lib", "close", False, id="failed_close"),
        pytest.param("bec_ipython_client_fixture", None, False, id="ipython"),
        pytest.param("bec_client_lib", None, True, id="event_devices"),
    ],
)
def test_subscan_sequence_one_file_and_one_history_record(
    request, capsys, client_fixture, fail_role, event_devices
):
    if not request.config.getoption("--start-servers"):
        pytest.skip("This test loads a scan plugin into the local test scan server.")
    bec = request.getfixturevalue(client_fixture)
    manager = bec.device_manager
    motor = manager.devices.samx
    on_request = {device.name for device in manager.devices.async_devices()}
    on_request.update(
        device
        for device, _, _ in manager.get_bec_signals(
            ["AsyncSignal", "DynamicSignal", "AsyncMultiSignal", "FileEventSignal"]
        )
    )
    if event_devices:
        on_request.difference_update({"eiger", "monitor_async"})
        camera = manager.devices.eiger
        previous_write_to_disk = camera.write_to_disk.get()
        camera.write_to_disk.set(True).wait()
    bec.scans.umv(motor, -1, relative=False)

    events = []
    sync_recorder = _CallbackRecorder()
    async_recorder = _CallbackRecorder()

    def capture(msg):
        events.append((msg.topic, msg.value))

    entry_status = endpoints.MessageEndpoints.scan_status()
    bec.connector.register(entry_status, cb=capture)
    try:
        report = bec.scans.bracketed_line_scan(
            motor,
            0,
            1,
            steps=3,
            relative=False,
            exp_time=0.03,
            on_request=sorted(on_request),
            metadata={"fail_role": fail_role},
            callback=sync_recorder.record,
            async_callback=async_recorder.record,
        )
        if fail_role:
            with pytest.raises(ScanAbortion):
                report.wait(timeout=30)
        else:
            report.wait(timeout=30, num_points=True, file_written=True)
        scan_id = report.scan.scan_id
        wait_until(lambda: bec.history.get_by_scan_id(scan_id) is not None, timeout_s=10)
    finally:
        bec.connector.unregister(entry_status, cb=capture)
        if event_devices:
            camera.write_to_disk.set(previous_write_to_disk).wait()

    if client_fixture == "bec_ipython_client_fixture":
        expected_callbacks = [
            (f"{scan_id}-0", 0, "before"),
            *((f"{scan_id}-1", point, "main") for point in range(3)),
            (f"{scan_id}-2", 0, "after"),
        ]
        wait_until(lambda: len(async_recorder.data) >= len(expected_callbacks), timeout_s=5)
        assert Counter(sync_recorder.data) == Counter(expected_callbacks)
        assert Counter(async_recorder.data) == Counter(expected_callbacks)
        assert f"finished. Scan ID {scan_id}" in capsys.readouterr().out
    assert len(report.scan.live_data) == (0 if fail_role == "before" else 3)
    acquisitions = [
        event
        for _, event in events
        if event.scan_id == f'{scan_id}-{event.info.get("entry_index")}'
    ]
    expected_statuses = (
        [("open", 0), ("aborted", 0)]
        if fail_role == "before"
        else [
            ("open", 0),
            ("closed", 0),
            ("open", 1),
            ("closed", 1),
            ("open", 2),
            ("aborted" if fail_role else "closed", 2),
        ]
    )
    assert [
        (event.status, event.info["sequence_index"]) for event in acquisitions
    ] == expected_statuses
    assert all(event.info["total_sequences"] == 3 for event in acquisitions)
    assert all(event.scan_id != scan_id for _, event in events)
    descriptor = acquisitions[0].info["sequence_start"]
    assert "status" not in descriptor
    assert "scan_id" not in descriptor
    assert all("sequence_start" not in event.info for event in acquisitions[1:])
    parent_updates = [update for event in acquisitions for update in event.sequence_updates()]
    if fail_role:
        [parent_terminal] = parent_updates
        assert parent_terminal.status == "aborted"
    else:
        assert parent_updates == []
        assert [event.closes_sequence() for event in acquisitions] == [False] * 5 + [True]
    assert [item["role"] for item in descriptor["info"]["entry_sequence"]] == [
        "before",
        "main",
        "after",
    ]

    data = bec.history.get_by_scan_id(scan_id)
    assert data._msg.exit_status == ("aborted" if fail_role else "closed")
    assert sum(item._msg.scan_id == scan_id for item in bec.history[:]) == 1
    with h5py.File(data._msg.file_path) as master:
        expected = ("entry_0",) if fail_role == "before" else ("entry_0", "entry_1", "entry_2")
        assert tuple(sorted(master)) == expected
        assert master.attrs["default"] == ("entry_0" if fail_role == "before" else "entry_1")
    for status in acquisitions:
        if status.status != "closed":
            continue
        entry_status = bec.connector.get(
            endpoints.MessageEndpoints.public_scan_info(status.scan_id)
        )
        assert entry_status.scan_id == status.scan_id
        point = bec.connector.get(endpoints.MessageEndpoints.public_scan_segment(status.scan_id, 0))
        assert point.scan_id == status.scan_id

    if event_devices:
        with h5py.File(data._msg.file_path) as master:
            camera_paths = set()
            for index, count in enumerate((1, 3, 1)):
                collection = master[f"entry_{index}/collection"]
                camera_group = collection["file_references/eiger"]
                link = camera_group.get("data", getlink=True)
                assert isinstance(link, h5py.ExternalLink)
                assert f"_entry_{index}_" in link.filename
                assert link.path == "/entry/data/data"
                camera_paths.add(link.filename)
                assert len(collection["devices/monitor_async/monitor_async_data/value"]) == count
            assert len(camera_paths) == 3
