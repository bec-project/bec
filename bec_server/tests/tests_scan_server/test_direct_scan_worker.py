from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_server.scan_server.direct_scan_worker import DirectScanWorker
from bec_server.scan_server.errors import DeviceInstructionError, ScanAbortion, UserScanInterruption
from bec_server.scan_server.scan_queue import (
    DirectInstructionQueueItem,
    InstructionQueueStatus,
    ScanQueue,
)
from bec_server.scan_server.scans.scan_base import ScanBase
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

    def prepare_scan(self):
        self._record_step("prepare_scan")

    def open_scan(self):
        self._record_step("open_scan")

    def stage(self):
        self._record_step("stage")

    def pre_scan(self):
        self._record_step("pre_scan")

    def scan_core(self):
        self._record_step("scan_core")

    def post_scan(self):
        self._record_step("post_scan")

    def unstage(self):
        self._record_step("unstage")

    def close_scan(self):
        self._record_step("close_scan")


@pytest.fixture
def direct_worker_context(dm_with_devices):
    scan_server = ScanServerMock(dm_with_devices)
    queue_manager = scan_server.queue_manager
    queue_manager.shutdown()
    queue_manager.send_queue_status = mock.MagicMock()
    scan_queue = ScanQueue(queue_manager, queue_name="primary")
    queue_manager.queues["primary"] = scan_queue
    scan_server.connector.raise_alarm = mock.MagicMock()
    scan_server.connector.send_client_info = mock.MagicMock()
    scan_queue.abort = mock.MagicMock()

    queue = DirectInstructionQueueItem(scan_queue, mock.MagicMock(), scan_queue.scan_worker)
    queue.append_to_queue_history = mock.MagicMock()
    scan_queue.queue.append(queue)
    scan_queue.active_instruction_queue = queue

    yield SimpleNamespace(
        connector=scan_server.connector,
        device_manager=scan_server.device_manager,
        direct_worker=DirectScanWorker(worker=scan_queue.scan_worker),
        instruction_handler=queue_manager.instruction_handler,
        queue=queue,
        queue_manager=queue_manager,
        queue_state=scan_queue,
        scan_worker=scan_queue.scan_worker,
        scan_server=scan_server,
    )

    scan_server.shutdown()


@pytest.fixture
def make_scan(direct_worker_context):
    def _build(*, called_steps=None, fail_step=None):
        scan = _TestDirectScan(
            scan_id="scan-id",
            redis_connector=direct_worker_context.connector,
            device_manager=direct_worker_context.device_manager,
            instruction_handler=direct_worker_context.instruction_handler,
            request_inputs={},
            system_config={},
            called_steps=called_steps,
            fail_step=fail_step,
        )
        scan.actions._send_scan_status = mock.MagicMock()
        scan.actions.send_client_info = mock.MagicMock()
        scan._shutdown_event = mock.MagicMock()
        return scan

    return _build


def _append_scan(queue: DirectInstructionQueueItem, scan: _TestDirectScan):
    queue.scans.append(scan)
    queue.scan_msgs.append(
        messages.ScanQueueMessage(
            scan_type=scan.scan_info.scan_name,
            parameter={"args": {}, "kwargs": {}},
            queue="primary",
            metadata={"RID": "rid-1"},
        )
    )


def test_check_for_interruption_sends_paused_status_via_scan_actions(
    direct_worker_context, make_scan
):
    scan = make_scan()
    direct_worker_context.direct_worker.scan = scan
    direct_worker_context.scan_worker.status = InstructionQueueStatus.PAUSED

    def _resume(_seconds):
        direct_worker_context.scan_worker.status = InstructionQueueStatus.RUNNING

    with mock.patch("bec_server.scan_server.direct_scan_worker.time.sleep", side_effect=_resume):
        direct_worker_context.direct_worker.check_for_interruption()

    scan.actions._send_scan_status.assert_called_once_with("paused")


def test_check_for_interruption_raises_user_interruption_on_stop(direct_worker_context):
    direct_worker_context.scan_worker.status = InstructionQueueStatus.STOPPED
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.queue.exit_info = ("user_completed", "user")

    with pytest.raises(UserScanInterruption) as exc:
        direct_worker_context.direct_worker.check_for_interruption()

    assert exc.value.exit_info == ("user_completed", "user")


def test_check_for_interruption_raises_scan_abortion_without_exit_info(direct_worker_context):
    direct_worker_context.scan_worker.status = InstructionQueueStatus.STOPPED
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.queue.exit_info = None

    with pytest.raises(ScanAbortion):
        direct_worker_context.direct_worker.check_for_interruption()


def test_check_for_interruption_does_not_send_paused_without_scan(direct_worker_context):
    direct_worker_context.scan_worker.status = InstructionQueueStatus.PAUSED

    def _resume(_seconds):
        direct_worker_context.scan_worker.status = InstructionQueueStatus.RUNNING

    with mock.patch("bec_server.scan_server.direct_scan_worker.time.sleep", side_effect=_resume):
        direct_worker_context.direct_worker.check_for_interruption()


def test_process_instructions_runs_scan_and_resets_state(direct_worker_context, make_scan):
    scan = make_scan()
    _append_scan(direct_worker_context.queue, scan)

    with mock.patch.object(direct_worker_context.direct_worker, "run") as run_mock:
        with mock.patch.object(direct_worker_context.direct_worker, "reset") as reset_mock:
            direct_worker_context.direct_worker.process_instructions(direct_worker_context.queue)

    run_mock.assert_called_once_with(scan)
    assert direct_worker_context.queue.status == InstructionQueueStatus.COMPLETED
    assert direct_worker_context.scan_worker.current_instruction_queue_item is None
    reset_mock.assert_called_once_with()


def test_process_instructions_returns_when_queue_has_no_scan(direct_worker_context):
    direct_worker_context.queue.move_to_next_scan = mock.MagicMock(return_value=None)

    with mock.patch("bec_server.scan_server.direct_scan_worker.logger.error") as log_error:
        direct_worker_context.direct_worker.process_instructions(direct_worker_context.queue)

    log_error.assert_called_once_with("No scan found in the queue item to process.")
    assert (
        direct_worker_context.scan_worker.current_instruction_queue_item
        is direct_worker_context.queue
    )


def test_run_executes_full_scan_sequence_in_order(direct_worker_context, make_scan):
    called_steps = []
    scan = make_scan(called_steps=called_steps)
    direct_worker_context.queue.active_scan = scan
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    rpc_cm = mock.MagicMock()
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=rpc_cm)

    direct_worker_context.direct_worker.run(scan)

    assert (
        scan.actions._interruption_callback
        == direct_worker_context.direct_worker.check_for_interruption
    )
    assert (
        scan.actions._update_queue_info_callback
        == direct_worker_context.direct_worker.update_queue_info
    )
    direct_worker_context.device_manager._rpc_method.assert_called_once_with(scan.actions.rpc_call)
    assert called_steps == [
        "prepare_scan",
        "open_scan",
        "stage",
        "pre_scan",
        "scan_core",
        "post_scan",
        "unstage",
        "close_scan",
    ]
    assert direct_worker_context.queue.status == InstructionQueueStatus.COMPLETED
    assert direct_worker_context.scan_worker.current_instruction_queue_item is None
    assert direct_worker_context.direct_worker.scan is None


def test_run_returns_early_when_signal_event_is_set(direct_worker_context, make_scan):
    scan = make_scan(fail_step="scan_core")
    direct_worker_context.queue.active_scan = scan
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.scan_worker.signal_event.set()
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=mock.MagicMock())
    direct_worker_context.direct_worker._handle_exception = mock.MagicMock()

    direct_worker_context.direct_worker.run(scan)

    direct_worker_context.direct_worker._handle_exception.assert_not_called()
    assert direct_worker_context.queue.status == InstructionQueueStatus.PENDING
    direct_worker_context.scan_worker.signal_event.clear()


def test_run_returns_early_when_current_queue_is_none(direct_worker_context, make_scan):
    scan = make_scan(fail_step="scan_core")
    direct_worker_context.scan_worker.current_instruction_queue_item = None
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=mock.MagicMock())
    direct_worker_context.direct_worker._handle_exception = mock.MagicMock()

    direct_worker_context.direct_worker.run(scan)

    direct_worker_context.direct_worker._handle_exception.assert_not_called()


def test_run_reraises_when_queue_is_already_stopped(direct_worker_context, make_scan):
    scan = make_scan(fail_step="scan_core")
    direct_worker_context.queue.active_scan = scan
    direct_worker_context.queue.stopped = True
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=mock.MagicMock())

    with pytest.raises(RuntimeError, match="scan_core failed"):
        direct_worker_context.direct_worker.run(scan)

    direct_worker_context.queue.stopped = False


def test_run_reraises_when_queue_has_no_active_request_block(direct_worker_context, make_scan):
    scan = make_scan(fail_step="scan_core")
    direct_worker_context.queue.active_scan = None
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=mock.MagicMock())

    with pytest.raises(RuntimeError, match="scan_core failed"):
        direct_worker_context.direct_worker.run(scan)


def test_run_uses_on_exception_cleanup_before_handling_error(direct_worker_context, make_scan):
    scan = make_scan(fail_step="scan_core")
    direct_worker_context.queue.active_scan = scan
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=mock.MagicMock())
    direct_worker_context.direct_worker._run_on_exception_hook = mock.MagicMock()
    direct_worker_context.direct_worker._handle_exception = mock.MagicMock(
        side_effect=ScanAbortion()
    )

    with pytest.raises(ScanAbortion):
        direct_worker_context.direct_worker.run(scan)

    assert direct_worker_context.queue.stopped is True
    assert direct_worker_context.scan_worker.status == InstructionQueueStatus.RUNNING
    assert scan.actions._metadata_suffix == "__on-exception"
    direct_worker_context.direct_worker._run_on_exception_hook.assert_called_once()
    assert isinstance(
        direct_worker_context.direct_worker._run_on_exception_hook.call_args.args[0], RuntimeError
    )
    direct_worker_context.direct_worker._handle_exception.assert_called_once()


def test_run_handles_cleanup_exception_before_original_error(direct_worker_context, make_scan):
    scan = make_scan(fail_step="scan_core")
    cleanup_exc = UserScanInterruption(exit_info=("halted", "user"))
    direct_worker_context.queue.active_scan = scan
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=mock.MagicMock())
    direct_worker_context.direct_worker._run_on_exception_hook = mock.MagicMock(
        side_effect=cleanup_exc
    )
    direct_worker_context.direct_worker._handle_exception = mock.MagicMock(
        side_effect=ScanAbortion()
    )

    with pytest.raises(ScanAbortion):
        direct_worker_context.direct_worker.run(scan)

    direct_worker_context.connector.send_client_info.assert_called_once_with("")
    assert direct_worker_context.direct_worker._handle_exception.call_args.args[0] is cleanup_exc
    direct_worker_context.queue.stopped = False


def test_handle_exception_raises_alarm_for_device_instruction_error(
    direct_worker_context, make_scan
):
    scan = make_scan()
    direct_worker_context.direct_worker.scan = scan
    error_info = messages.ErrorInfo(
        error_message="device failed",
        compact_error_message="DeviceInstructionError",
        exception_type="DeviceInstructionError",
        device="samx",
    )
    exc = DeviceInstructionError(error_info)

    with pytest.raises(ScanAbortion):
        direct_worker_context.direct_worker._handle_exception(exc)

    direct_worker_context.connector.raise_alarm.assert_called_once_with(
        severity=Alarms.MAJOR, info=error_info, metadata={"scan_id": "scan-id", "scan_number": 7}
    )


def test_handle_exception_raises_alarm_for_generic_exception(direct_worker_context, make_scan):
    scan = make_scan()
    direct_worker_context.direct_worker.scan = scan

    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        with pytest.raises(ScanAbortion):
            direct_worker_context.direct_worker._handle_exception(exc)

    direct_worker_context.connector.raise_alarm.assert_called_once()
    assert (
        direct_worker_context.connector.raise_alarm.call_args.kwargs["info"].exception_type
        == "RuntimeError"
    )


def test_propagate_error_raises_major_alarm_with_scan_metadata(direct_worker_context, make_scan):
    scan = make_scan()
    direct_worker_context.direct_worker.scan = scan

    direct_worker_context.direct_worker._propagate_error("traceback", RuntimeError("boom"))

    direct_worker_context.connector.raise_alarm.assert_called_once()
    assert direct_worker_context.connector.raise_alarm.call_args.kwargs["severity"] == Alarms.MAJOR
    assert direct_worker_context.connector.raise_alarm.call_args.kwargs["metadata"] == {
        "scan_id": "scan-id",
        "scan_number": 7,
    }
    assert (
        direct_worker_context.connector.raise_alarm.call_args.kwargs["info"].exception_type
        == "RuntimeError"
    )


@pytest.mark.parametrize(
    ("scan_id", "scan_number", "expected"),
    [
        (None, None, {}),
        ("scan-id", None, {"scan_id": "scan-id"}),
        (None, 7, {"scan_number": 7}),
        ("scan-id", 7, {"scan_id": "scan-id", "scan_number": 7}),
    ],
)
def test_get_metadata_for_alarm(direct_worker_context, make_scan, scan_id, scan_number, expected):
    direct_worker_context.direct_worker.scan = SimpleNamespace(
        scan_info=SimpleNamespace(scan_id=scan_id, scan_number=scan_number)
    )

    assert direct_worker_context.direct_worker.get_metadata_for_alarm() == expected


def test_run_on_exception_hook_invokes_scan_hook_when_enabled(direct_worker_context, make_scan):
    scan = make_scan()
    scan.on_exception = mock.MagicMock()
    direct_worker_context.direct_worker.scan = scan
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.queue.run_on_exception_hook = True
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=mock.MagicMock())
    exc = ScanAbortion()

    direct_worker_context.direct_worker._run_on_exception_hook(exc)

    scan._shutdown_event.clear.assert_called_once_with()
    scan.on_exception.assert_called_once_with(exc)


def test_run_on_exception_hook_uses_root_cause(direct_worker_context, make_scan):
    scan = make_scan()
    scan.on_exception = mock.MagicMock()
    direct_worker_context.direct_worker.scan = scan
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.queue.run_on_exception_hook = True
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=mock.MagicMock())
    root_cause = RuntimeError("root cause")

    try:
        raise root_cause
    except RuntimeError as cause:
        exc = ScanAbortion()
        exc.__cause__ = cause
        direct_worker_context.direct_worker._run_on_exception_hook(exc)

    scan.on_exception.assert_called_once_with(root_cause)


def test_run_on_exception_hook_returns_when_scan_is_none(direct_worker_context):
    direct_worker_context.direct_worker.scan = None
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue

    direct_worker_context.direct_worker._run_on_exception_hook(ScanAbortion())


def test_run_on_exception_hook_returns_when_on_exception_is_missing(
    direct_worker_context, make_scan
):
    scan = make_scan()
    direct_worker_context.direct_worker.scan = scan
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.queue.run_on_exception_hook = True

    direct_worker_context.direct_worker._run_on_exception_hook(ScanAbortion())


def test_run_on_exception_hook_sends_client_info_when_hook_fails(direct_worker_context, make_scan):
    scan = make_scan()

    def _fail(_exc):
        raise RuntimeError("cleanup failed")

    scan.on_exception = _fail
    direct_worker_context.direct_worker.scan = scan
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.queue.run_on_exception_hook = True
    direct_worker_context.device_manager._rpc_method = mock.MagicMock(return_value=mock.MagicMock())

    direct_worker_context.direct_worker._run_on_exception_hook(ScanAbortion())

    scan.actions.send_client_info.assert_called_once_with("")


def test_run_on_exception_hook_skips_when_disabled(direct_worker_context, make_scan):
    scan = make_scan()
    scan.on_exception = mock.MagicMock()
    direct_worker_context.direct_worker.scan = scan
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue
    direct_worker_context.queue.run_on_exception_hook = False

    direct_worker_context.direct_worker._run_on_exception_hook(ScanAbortion())

    scan.on_exception.assert_not_called()


def test_handle_scan_abortion_sends_abort_status_via_scan_actions(direct_worker_context, make_scan):
    scan = make_scan()
    direct_worker_context.queue.exit_info = None
    direct_worker_context.queue.run_on_exception_hook = True
    direct_worker_context.direct_worker.scan = scan
    direct_worker_context.direct_worker.reset = mock.MagicMock()

    direct_worker_context.direct_worker._handle_scan_abortion(
        direct_worker_context.queue, ScanAbortion()
    )

    scan.actions._send_scan_status.assert_called_once_with("aborted", reason="alarm")
    assert direct_worker_context.queue.status == InstructionQueueStatus.STOPPED
    direct_worker_context.queue.append_to_queue_history.assert_called_once_with()
    direct_worker_context.queue_state.abort.assert_called_once_with()
    direct_worker_context.direct_worker.reset.assert_called_once_with()
    assert direct_worker_context.scan_worker.status == InstructionQueueStatus.RUNNING


def test_handle_scan_abortion_returns_when_scan_is_none(direct_worker_context):
    direct_worker_context.direct_worker.scan = None

    direct_worker_context.direct_worker._handle_scan_abortion(
        direct_worker_context.queue, ScanAbortion()
    )

    direct_worker_context.queue.append_to_queue_history.assert_not_called()


def test_handle_scan_abortion_sends_user_status_via_scan_actions(direct_worker_context, make_scan):
    scan = make_scan()
    direct_worker_context.queue.exit_info = None
    direct_worker_context.direct_worker.scan = scan

    direct_worker_context.direct_worker._handle_scan_abortion(
        direct_worker_context.queue, UserScanInterruption(exit_info=("user_completed", "user"))
    )

    scan.actions._send_scan_status.assert_called_once_with("user_completed", reason="user")


def test_handle_scan_abortion_halts_when_exception_hook_is_disabled(
    direct_worker_context, make_scan
):
    scan = make_scan()
    direct_worker_context.queue.exit_info = None
    direct_worker_context.queue.run_on_exception_hook = False
    direct_worker_context.direct_worker.scan = scan
    direct_worker_context.direct_worker.reset = mock.MagicMock()

    direct_worker_context.direct_worker._handle_scan_abortion(
        direct_worker_context.queue, ScanAbortion()
    )

    scan.actions._send_scan_status.assert_called_once_with("halted", reason="alarm")


def test_update_queue_info_forwards_to_queue_manager(direct_worker_context):
    direct_worker_context.scan_worker.current_instruction_queue_item = direct_worker_context.queue

    direct_worker_context.direct_worker.update_queue_info()

    direct_worker_context.queue_manager.send_queue_status.assert_called_once_with()
