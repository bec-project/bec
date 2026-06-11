from __future__ import annotations

import threading
from unittest import mock

import pytest

from bec_ipython_client.signals import SigintHandler
from bec_lib.bec_errors import ScanInterruption
from bec_lib.request_context import ActiveRequestContext, active_request_context


@pytest.fixture
def bec_with_pending_live_request():
    bec = mock.MagicMock()
    bec._service_config = mock.MagicMock(abort_on_ctrl_c=True)
    return bec


def test_sigint_handler_raises_keyboard_interrupt_for_pending_live_request(
    bec_with_pending_live_request,
):
    handler = SigintHandler(bec_with_pending_live_request)
    token = active_request_context.set(
        ActiveRequestContext(request_id="pending-request", queue_status="PENDING")
    )

    try:
        with pytest.raises(KeyboardInterrupt):
            handler._normal_mode()
    finally:
        active_request_context.reset(token)

    bec_with_pending_live_request.queue.request_scan_interruption.assert_not_called()
    bec_with_pending_live_request.queue.request_scan_abortion.assert_not_called()


def test_sigint_handler_pending_live_request_does_not_abort_running_scan_from_other_client():
    bec = mock.MagicMock()
    bec._service_config = mock.MagicMock(abort_on_ctrl_c=True)
    bec.queue.scan_storage.current_scan_info = mock.MagicMock(
        status="RUNNING",
        is_scan=[True],
        request_blocks=[mock.MagicMock(RID="other-client-request")],
    )
    handler = SigintHandler(bec)
    token = active_request_context.set(
        ActiveRequestContext(request_id="local-pending-request", queue_status="PENDING")
    )

    try:
        with pytest.raises(KeyboardInterrupt):
            handler._normal_mode()
    finally:
        active_request_context.reset(token)

    bec.queue.request_scan_interruption.assert_not_called()
    bec.queue.request_scan_abortion.assert_not_called()


def test_sigint_handler_requests_deferred_pause_for_running_scan():
    bec = mock.MagicMock()
    bec._service_config = mock.MagicMock(abort_on_ctrl_c=True)
    bec._live_updates = None
    bec.queue.scan_storage.current_scan_info = mock.MagicMock(status="RUNNING", is_scan=[True])

    handler = SigintHandler(bec)
    token = active_request_context.set(
        ActiveRequestContext(request_id="running-request", queue_status="RUNNING")
    )

    try:
        with mock.patch.object(threading, "Thread") as thread_cls:
            handler._normal_mode()
    finally:
        active_request_context.reset(token)

    thread_cls.assert_called_once_with(
        target=bec.queue.request_scan_interruption,
        kwargs={"deferred_pause": True, "request_id": "running-request"},
        daemon=True,
    )
    thread_cls.return_value.start.assert_called_once_with()


def test_sigint_handler_requests_abort_for_running_scan_after_second_sigint():
    bec = mock.MagicMock()
    bec._service_config = mock.MagicMock(abort_on_ctrl_c=True)
    bec._live_updates = None
    bec.queue.scan_storage.current_scan_info = mock.MagicMock(
        status="DEFERRED_PAUSE", is_scan=[True]
    )

    handler = SigintHandler(bec)
    handler.last_sigint_time = 0
    token = active_request_context.set(
        ActiveRequestContext(request_id="running-request", queue_status="DEFERRED_PAUSE")
    )

    try:
        with (
            mock.patch("bec_ipython_client.signals.time.time", return_value=5),
            mock.patch.object(threading, "Thread") as thread_cls,
            pytest.raises(ScanInterruption, match="User abort."),
        ):
            handler._normal_mode()
    finally:
        active_request_context.reset(token)

    thread_cls.assert_called_once_with(
        target=bec.queue.request_scan_abortion,
        kwargs={"request_id": "running-request"},
        daemon=True,
    )
    thread_cls.return_value.start.assert_called_once_with()


def test_sigint_handler_without_active_or_pending_scan_reraises_keyboard_interrupt():
    bec = mock.MagicMock()
    bec._service_config = mock.MagicMock(abort_on_ctrl_c=True)
    bec.queue.scan_storage.current_scan_info = None
    handler = SigintHandler(bec)

    with pytest.raises(KeyboardInterrupt):
        handler._normal_mode()

    bec.queue.request_scan_interruption.assert_not_called()
    bec.queue.request_scan_abortion.assert_not_called()


def test_sigint_handler_without_request_context_passes_none_to_thread_target():
    bec = mock.MagicMock()
    bec._service_config = mock.MagicMock(abort_on_ctrl_c=True)
    bec._live_updates = None
    bec.queue.scan_storage.current_scan_info = mock.MagicMock(status="RUNNING", is_scan=[True])
    handler = SigintHandler(bec)

    with mock.patch.object(threading, "Thread") as thread_cls:
        handler._normal_mode()

    thread_cls.assert_called_once_with(
        target=bec.queue.request_scan_interruption,
        kwargs={"deferred_pause": True, "request_id": None},
        daemon=True,
    )
