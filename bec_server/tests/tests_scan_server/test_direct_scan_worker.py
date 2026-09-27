import threading
from contextlib import nullcontext
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_server.scan_server.direct_scan_worker import DirectScanWorker
from bec_server.scan_server.errors import DeviceInstructionError, ScanAbortion, UserScanInterruption
from bec_server.scan_server.queue_channels import (
    ExecutionControl,
    ExecutionToken,
    InstructionQueueStatus,
    ScanAssignment,
)
from bec_server.scan_server.scans.scan_base import ScanBase
from bec_server.scan_server.scans.scan_modifier import ScanModifier, scan_hook, scan_hook_impl
from bec_server.scan_server.tests.utils import ScanServerMock


class _TestDirectScan(ScanBase):
    scan_name = "_v4_test_direct_scan"
    scan_type = None

    def __init__(self, *args, called_steps=None, fail_step=None, **kwargs):
        self.called_steps = called_steps if called_steps is not None else []
        self.fail_step = fail_step
        super().__init__(*args, **kwargs)
        self.scan_info.scan_number = 7

    def _record_step(self, step_name: str):
        self.called_steps.append(step_name)
        if self.fail_step == step_name:
            raise RuntimeError(f"{step_name} failed")

    @scan_hook
    def prepare_scan(self):
        self._record_step("prepare_scan")

    @scan_hook
    def open_scan(self):
        self._record_step("open_scan")

    @scan_hook
    def stage(self):
        self._record_step("stage")

    @scan_hook
    def pre_scan(self):
        self._record_step("pre_scan")

    @scan_hook
    def scan_core(self):
        self._record_step("scan_core")
        self.at_each_point("scan_core-point")

    @scan_hook
    def at_each_point(self, point):
        self.called_steps.append(("at_each_point", point))

    @scan_hook
    def post_scan(self):
        self._record_step("post_scan")

    @scan_hook
    def unstage(self):
        self._record_step("unstage")

    @scan_hook
    def close_scan(self):
        self._record_step("close_scan")

    @scan_hook
    def on_exception(self, exc):
        self.called_steps.append(("on_exception", exc))


class _HookRecordingModifier(ScanModifier):
    @scan_hook_impl("stage", "before")
    def before_stage(self):
        self.scan.called_steps.append("modifier:before_stage")

    @scan_hook_impl("scan_core", "before")
    def before_scan_core(self):
        self.scan.called_steps.append("modifier:before_scan_core")

    @scan_hook_impl("pre_scan", "replace")
    def replace_pre_scan(self):
        self.scan.called_steps.append("modifier:replace_pre_scan")

    @scan_hook_impl("scan_core", "replace")
    def replace_scan_core(self):
        self.scan.called_steps.append("modifier:replace_scan_core")

    @scan_hook_impl("scan_core", "after")
    def after_scan_core(self):
        self.scan.called_steps.append("modifier:after_scan_core")

    @scan_hook_impl("at_each_point", "before")
    def before_at_each_point(self, point):
        self.scan.called_steps.append(("modifier:before_at_each_point", point))

    @scan_hook_impl("at_each_point", "replace")
    def replace_at_each_point(self, point):
        self.scan.called_steps.append(("modifier:replace_at_each_point", point))

    @scan_hook_impl("at_each_point", "after")
    def after_at_each_point(self, point):
        self.scan.called_steps.append(("modifier:after_at_each_point", point))

    @scan_hook_impl("close_scan", "after")
    def after_close_scan(self):
        self.scan.called_steps.append("modifier:after_close_scan")

    @scan_hook_impl("on_exception", "before")
    def before_on_exception(self, exc):
        self.scan.called_steps.append(("modifier:before_on_exception", exc))

    @scan_hook_impl("on_exception", "replace")
    def replace_on_exception(self, exc):
        self.scan.called_steps.append(("modifier:replace_on_exception", exc))

    @scan_hook_impl("on_exception", "after")
    def after_on_exception(self, exc):
        self.scan.called_steps.append(("modifier:after_on_exception", exc))


@pytest.fixture
def direct_worker_context(dm_with_devices):
    server = ScanServerMock(dm_with_devices)
    server.connector.raise_alarm = mock.Mock()
    server.device_manager._rpc_method = mock.Mock(side_effect=lambda _: nullcontext())
    worker = SimpleNamespace(
        parent=server,
        connector=server.connector,
        device_manager=server.device_manager,
        queue_name="secondary",
        report=mock.Mock(),
    )
    yield SimpleNamespace(
        scan_server=server, worker=worker, executor=DirectScanWorker(worker=worker)
    )
    server.shutdown()


@pytest.fixture
def make_assignment(direct_worker_context):
    def build(fail_step=None):
        server = direct_worker_context.scan_server
        scan = _TestDirectScan(
            scan_id="scan-id",
            redis_connector=server.connector,
            device_manager=server.device_manager,
            instruction_handler=server.queue_manager.instruction_handler,
            scan_modifier=None,
            request_inputs={},
            system_config={},
            fail_step=fail_step,
        )
        scan.scan_info.metadata["RID"] = "rid-1"
        scan.actions._initialize_scan = mock.Mock()
        scan.actions._send_scan_status = mock.Mock()
        scan.actions.send_client_info = mock.Mock()
        msg = messages.ScanQueueMessage(
            scan_type="test",
            parameter={"args": {}, "kwargs": {}},
            queue="secondary",
            metadata={"RID": "rid-1"},
        )
        return ScanAssignment(
            ExecutionToken("generation", "queue-id", 1),
            "secondary",
            scan,
            msg,
            ExecutionControl(),
            7,
            8,
        )

    return build


def test_run_full_lifecycle_and_copied_report(direct_worker_context, make_assignment):
    assignment = make_assignment()
    scan = assignment.scan
    report = direct_worker_context.executor.run(assignment)
    assert scan.called_steps == [
        "prepare_scan",
        "open_scan",
        "stage",
        "pre_scan",
        "scan_core",
        ("at_each_point", "scan_core-point"),
        "post_scan",
        "unstage",
        "close_scan",
    ]
    scan.actions._initialize_scan.assert_called_once_with()
    assert report.terminal and report.status == InstructionQueueStatus.COMPLETED
    assert report.token == assignment.token
    assert scan.scan_info.dataset_number == 8
    assert scan.scan_info.scan_queue == "secondary"
    assert scan.scan_info.metadata["queue_id"] == "queue-id"
    assert direct_worker_context.executor.scan is None
    scan.scan_info.scan_report_instructions.append({"changed": True})
    assert report.request.report_instructions == []


def test_initialization_precedes_hooks(direct_worker_context, make_assignment):
    assignment = make_assignment()
    scan = assignment.scan
    scan.actions._initialize_scan.side_effect = lambda: scan.called_steps.append("init")
    direct_worker_context.executor.run(assignment)
    assert scan.called_steps[:2] == ["init", "prepare_scan"]


def test_modifier_hooks_keep_their_order(direct_worker_context, make_assignment):
    assignment = make_assignment()
    scan = assignment.scan
    scan._scan_modifier = _HookRecordingModifier(scan)
    scan._scan_modifier_hooks = {
        "stage": {"before": "before_stage"},
        "pre_scan": {"replace": "replace_pre_scan"},
        "scan_core": {
            "before": "before_scan_core",
            "replace": "replace_scan_core",
            "after": "after_scan_core",
        },
        "close_scan": {"after": "after_close_scan"},
    }
    direct_worker_context.executor.run(assignment)
    assert scan.called_steps == [
        "prepare_scan",
        "open_scan",
        "modifier:before_stage",
        "stage",
        "modifier:replace_pre_scan",
        "modifier:before_scan_core",
        "modifier:replace_scan_core",
        "modifier:after_scan_core",
        "post_scan",
        "unstage",
        "close_scan",
        "modifier:after_close_scan",
    ]


@pytest.mark.parametrize("fail_step", [None, "prepare_scan", "scan_core", "close_scan"])
def test_release_locks_on_every_exit(direct_worker_context, make_assignment, fail_step):
    assignment = make_assignment(fail_step=fail_step)
    registry = direct_worker_context.scan_server.device_lock_registry
    registry.acquire_many("rid-1", ["samx"])
    report = direct_worker_context.executor.run(assignment)
    assert registry.get_owned_devices("rid-1") == []
    assert report.status == (
        InstructionQueueStatus.STOPPED if fail_step else InstructionQueueStatus.COMPLETED
    )


def test_error_cleanup_precedes_alarm_and_preserves_root_cause(
    direct_worker_context, make_assignment
):
    assignment = make_assignment()
    cause = ValueError("root")

    def fail():
        raise ScanAbortion("wrapper") from cause

    assignment.scan.scan_core = fail
    report = direct_worker_context.executor.run(assignment)
    assert assignment.scan.called_steps[-1] == ("on_exception", cause)
    assert report.status == InstructionQueueStatus.STOPPED
    direct_worker_context.worker.connector.raise_alarm.assert_not_called()


def test_error_alarm_contains_request_and_queue_identity(direct_worker_context, make_assignment):
    assignment = make_assignment(fail_step="scan_core")
    direct_worker_context.executor.run(assignment)
    call = direct_worker_context.worker.connector.raise_alarm.call_args
    assert call.kwargs["severity"] == Alarms.MAJOR
    assert call.kwargs["metadata"]["RID"] == "rid-1"
    assert call.kwargs["metadata"]["queue"] == "secondary"
    assert call.kwargs["metadata"]["queue_id"] == "queue-id"
    assert call.kwargs["info"].exception_type == "RuntimeError"


def test_device_error_preserves_error_info(direct_worker_context, make_assignment):
    assignment = make_assignment()
    info = messages.ErrorInfo(
        error_message="full",
        compact_error_message="failure",
        exception_type="DeviceError",
        device="samx",
    )
    assignment.scan.scan_core = mock.Mock(side_effect=DeviceInstructionError(info))
    direct_worker_context.executor.run(assignment)
    assert direct_worker_context.worker.connector.raise_alarm.call_args.kwargs["info"] == info


@pytest.mark.parametrize(
    "action, expected, cleanup",
    [("abort", "aborted", True), ("halt", "halted", False), ("complete", "user_completed", True)],
)
def test_interruptions_preserve_status_and_cleanup(
    direct_worker_context, make_assignment, action, expected, cleanup
):
    assignment = make_assignment()

    def interrupt():
        receipt = assignment.control.stop((expected, "user"), cleanup=cleanup)
        receipt.set()
        assignment.scan.actions._interruption_callback()

    assignment.scan.scan_core = interrupt
    report = direct_worker_context.executor.run(assignment)
    assert (
        any(
            isinstance(step, tuple) and step[0] == "on_exception"
            for step in assignment.scan.called_steps
        )
        == cleanup
    )
    assert report.exit_info == (expected, "user")
    assignment.scan.actions._send_scan_status.assert_called_with(expected, reason="user")
    direct_worker_context.worker.connector.raise_alarm.assert_not_called()


def test_shutdown_before_dispatch_runs_no_hooks(direct_worker_context, make_assignment):
    assignment = make_assignment()
    assignment.control.shutdown()
    report = direct_worker_context.executor.run(assignment)
    assert assignment.scan.called_steps == []
    assert report.status == InstructionQueueStatus.STOPPED
    assignment.scan.actions._send_scan_status.assert_not_called()


def test_disabled_cleanup_halts_on_error(direct_worker_context, make_assignment):
    assignment = make_assignment(fail_step="scan_core")
    assignment.scan.scan_info.run_on_exception_hook = False
    direct_worker_context.executor.run(assignment)
    assert not any(
        isinstance(step, tuple) and step[0] == "on_exception"
        for step in assignment.scan.called_steps
    )
    assignment.scan.actions._send_scan_status.assert_called_with("halted", reason="alarm")


def test_cleanup_uses_distinct_event_and_repeat_stop_is_not_lost(
    direct_worker_context, make_assignment
):
    assignment = make_assignment()
    observed = []

    def interrupt():
        assignment.control.stop(("aborted", "user")).set()
        assignment.control.checkpoint()

    def cleanup(exc):
        observed.append(assignment.scan._shutdown_event)
        assignment.control.stop(("halted", "user"), cleanup=False).set()
        assignment.control.checkpoint()

    assignment.scan.scan_core = interrupt
    assignment.scan.on_exception = cleanup
    report = direct_worker_context.executor.run(assignment)
    assert observed == [assignment.control.cleanup_event]
    assert assignment.control.execution_event.is_set()
    assert assignment.control.cleanup_event.is_set()
    assert report.exit_info == ("aborted", "user")


def test_pause_is_woken_by_continue(direct_worker_context, make_assignment):
    assignment = make_assignment()
    executor = direct_worker_context.executor
    executor.assignment, executor.scan = assignment, assignment.scan
    paused = threading.Event()
    assignment.scan.actions._send_scan_status.side_effect = lambda status: (
        paused.set() if status == "paused" else None
    )
    assignment.control.set_status(InstructionQueueStatus.PAUSED)
    thread = threading.Thread(target=executor.check_for_interruption)
    thread.start()
    assert paused.wait(1)
    assignment.control.set_status(InstructionQueueStatus.RUNNING)
    thread.join(1)
    assert not thread.is_alive()
    assert assignment.scan.actions._send_scan_status.call_args_list == [mock.call("paused")]
