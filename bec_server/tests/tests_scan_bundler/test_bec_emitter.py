from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_server.scan_bundler.bec_emitter import BECEmitter


@pytest.fixture
def bec_emitter_mock(scan_bundler_mock):
    with mock.patch.object(BECEmitter, "_start_buffered_connector") as start:
        emitter = BECEmitter(scan_bundler_mock)
        start.assert_called_once()
        with mock.patch.object(emitter, "_get_messages_from_buffer"):
            yield emitter
    emitter.shutdown()


def test_on_scan_point_emit_BEC(bec_emitter_mock):
    sb = bec_emitter_mock.scan_bundler
    with mock.patch.object(bec_emitter_mock, "_send_bec_scan_point") as send:
        bec_emitter_mock.on_scan_point_emit("scan_id", 2)
        send.assert_called_once_with("scan_id", 2)


def test_on_baseline_emit_BEC(bec_emitter_mock):
    sb = bec_emitter_mock.scan_bundler
    with mock.patch.object(bec_emitter_mock, "_send_baseline") as send:
        bec_emitter_mock.on_baseline_emit("scan_id")
        send.assert_called_once_with("scan_id")


def test_send_bec_scan_point(bec_emitter_mock):
    sb = bec_emitter_mock.scan_bundler
    scan_id = "lkajsdlkj"
    point_id = 2
    sb.sync_storage[scan_id] = {"info": {}, "status": "open", "sent": set()}
    sb.sync_storage[scan_id][point_id] = {}
    msg = messages.ScanMessage(
        point_id=point_id,
        scan_id=scan_id,
        data=sb.sync_storage[scan_id][point_id],
        metadata={"scan_id": "lkajsdlkj", "scan_type": None, "scan_report_devices": None},
    )
    with mock.patch.object(bec_emitter_mock, "add_message") as send:
        bec_emitter_mock._send_bec_scan_point(scan_id, point_id)
        send.assert_called_once_with(
            msg,
            MessageEndpoints.scan_segment(),
            MessageEndpoints.public_scan_segment(scan_id, point_id),
        )


def test_send_bec_scan_point_skips_point_progress_with_device_progress_sub(bec_emitter_mock):
    sb = bec_emitter_mock.scan_bundler
    scan_id = "lkajsdlkj"
    point_id = 2
    sb.sync_storage[scan_id] = {
        "info": {},
        "status": "open",
        "sent": set(),
        "device_progress_sub": {"topics": MessageEndpoints.device_progress("samx")},
    }
    sb.sync_storage[scan_id][point_id] = {}

    with (
        mock.patch.object(bec_emitter_mock, "add_message") as send,
        mock.patch.object(bec_emitter_mock, "_update_scan_progress") as update_progress,
    ):
        bec_emitter_mock._send_bec_scan_point(scan_id, point_id)
        send.assert_called_once()
        update_progress.assert_not_called()


def test_send_baseline_BEC(bec_emitter_mock):
    sb = bec_emitter_mock.scan_bundler
    scan_id = "lkajsdlkj"
    sb.sync_storage[scan_id] = {"info": {}, "status": "open", "sent": set()}
    sb.sync_storage[scan_id]["baseline"] = {}
    msg = messages.ScanBaselineMessage(scan_id=scan_id, data=sb.sync_storage[scan_id]["baseline"])
    with mock.patch.object(sb, "connector") as connector:
        bec_emitter_mock._send_baseline(scan_id)
        pipe = connector.pipeline()
        connector.set.assert_called_once_with(
            MessageEndpoints.public_scan_baseline(scan_id), msg, expire=1800, pipe=pipe
        )
        connector.set_and_publish.assert_called_once_with(
            MessageEndpoints.scan_baseline(), msg, pipe=pipe
        )


@pytest.mark.parametrize(
    "msgs",
    [
        ([]),
        (
            [
                (
                    messages.ScanMessage(point_id=1, scan_id="scan_id", data={}, metadata={}),
                    "endpoint",
                    None,
                )
            ]
        ),
        (
            [
                (
                    messages.ScanMessage(point_id=1, scan_id="scan_id", data={}, metadata={}),
                    "endpoint",
                    None,
                ),
                (
                    messages.ScanMessage(point_id=2, scan_id="scan_id", data={}, metadata={}),
                    "endpoint",
                    None,
                ),
            ]
        ),
        (
            [
                (
                    messages.ScanMessage(point_id=1, scan_id="scan_id", data={}, metadata={}),
                    "endpoint",
                    "public_endpoint",
                ),
                (
                    messages.ScanMessage(point_id=2, scan_id="scan_id", data={}, metadata={}),
                    "endpoint",
                    "public_endpoint",
                ),
            ]
        ),
    ],
)
def test_publish_data(msgs, bec_emitter_mock):
    connector = bec_emitter_mock.connector = mock.MagicMock()
    bec_emitter_mock._get_messages_from_buffer.return_value = msgs
    bec_emitter_mock._publish_data()
    bec_emitter_mock._get_messages_from_buffer.assert_called_once()

    if not msgs:
        connector.send.assert_not_called()
        return

    pipe = connector.pipeline()
    msgs_bundle = messages.BundleMessage()
    _, endpoint, _ = msgs[0]
    for msg, endpoint, public in msgs:
        msg_dump = msg
        msgs_bundle.append(msg_dump)
        if public:
            connector.set.assert_has_calls(connector.set(public, msg_dump, pipe=pipe, expire=1800))

    connector.send.assert_called_with(endpoint, msgs_bundle, pipe=pipe)


@pytest.mark.parametrize(
    "msg,endpoint,public",
    [
        (
            messages.ScanMessage(point_id=1, scan_id="scan_id", data={}, metadata={}),
            "endpoint",
            None,
        ),
        (
            messages.ScanMessage(point_id=1, scan_id="scan_id", data={}, metadata={}),
            "endpoint",
            "public",
        ),
    ],
)
def test_add_message(msg, endpoint, public):
    connector = mock.MagicMock()
    emitter = BECEmitter(connector)
    emitter.add_message(msg, endpoint, public)
    msgs = emitter._get_messages_from_buffer()
    out_msg, out_endpoint, out_public = msgs[0]
    assert out_msg == msg
    assert out_endpoint == endpoint
    assert out_public == public
    emitter.shutdown()


def test_bec_emitter_scan_status_update_open_updates_subscription(bec_emitter_mock):
    bec_emitter_mock.scan_bundler.sync_storage["lkajsdlkj"] = {
        "info": {},
        "status": "open",
        "sent": set(),
        "baseline": {},
    }
    msg = messages.ScanStatusMessage(scan_id="lkajsdlkj", status="open", info={"num_points": 10})
    with mock.patch.object(bec_emitter_mock, "_update_device_progress_subscription") as update_sub:
        bec_emitter_mock.on_scan_status_update(msg)
        update_sub.assert_called_once_with("lkajsdlkj")


@pytest.mark.parametrize(
    "msg, sent, progress, ref_scan_id",
    [
        (
            messages.ScanStatusMessage(scan_id="lkajsdlkj", status="open", info={"num_points": 10}),
            {0, 1},
            1,
            "lkajsdlkj",
        ),
        (
            messages.ScanStatusMessage(
                scan_id="lkajsdlkj", status="closed", info={"num_points": 10}
            ),
            {0, 1},
            9,
            "lkajsdlkj",
        ),
        (
            messages.ScanStatusMessage(
                scan_id="lkajsdlkj", status="aborted", info={"num_points": 10}
            ),
            {0, 1},
            1,
            "lkajsdlkj",
        ),
        (
            messages.ScanStatusMessage(
                scan_id="wrong_scan_id", status="aborted", info={"num_points": 10}
            ),
            {0, 1},
            1,
            "lkajsdlkj",
        ),
        (
            messages.ScanStatusMessage(
                scan_id="lkajsdlkj", status="aborted", info={"num_points": 10}
            ),
            {},
            0,
            "lkajsdlkj",
        ),
    ],
)
def test_bec_emitter_scan_status_update_point_progress_path(
    bec_emitter_mock, msg, sent, progress, ref_scan_id
):
    sb = bec_emitter_mock.scan_bundler
    sb.sync_storage[ref_scan_id] = {"info": {}, "status": msg.status, "sent": sent, "baseline": {}}

    with (
        mock.patch.object(bec_emitter_mock, "_update_scan_progress") as update,
        mock.patch.object(bec_emitter_mock, "_update_device_progress_subscription") as update_sub,
    ):
        bec_emitter_mock.on_scan_status_update(msg)
        if msg.status == "open":
            update.assert_not_called()
            update_sub.assert_called_once_with(msg.scan_id)
        elif msg.scan_id != ref_scan_id:
            update.assert_not_called()
            update_sub.assert_not_called()
        else:
            update.assert_called_once_with(msg.scan_id, progress, done=True)
            update_sub.assert_not_called()


def test_bec_emitter_scan_status_update_missing_scan_id_does_not_update(bec_emitter_mock):
    msg = messages.ScanStatusMessage(
        scan_id="wrong_scan_id", status="aborted", info={"num_points": 10}
    )
    with mock.patch.object(bec_emitter_mock, "_update_scan_progress") as update:
        bec_emitter_mock.on_scan_status_update(msg)
        update.assert_not_called()


@pytest.mark.parametrize("status", ["closed", "aborted"])
def test_bec_emitter_scan_status_update_wrong_scan_id_does_not_emit_progress(
    bec_emitter_mock, status
):
    msg = messages.ScanStatusMessage(
        scan_id="wrong_scan_id", status=status, info={"num_points": 10}
    )
    with (
        mock.patch.object(bec_emitter_mock, "_update_scan_progress") as update,
        mock.patch.object(bec_emitter_mock, "send_scan_progress") as send_scan_progress,
    ):
        bec_emitter_mock.on_scan_status_update(msg)
        update.assert_not_called()
        send_scan_progress.assert_not_called()


def test_update_device_progress_subscription_registers_device_progress(bec_emitter_mock):
    sb = bec_emitter_mock.scan_bundler
    scan_id = "scan_id"
    sb.sync_storage[scan_id] = {"info": {}, "status": "open", "sent": set()}
    sb.scan_report_instructions[scan_id] = [{"device_progress": ["samx"]}]

    with mock.patch.object(bec_emitter_mock.connector, "register") as register:
        bec_emitter_mock._update_device_progress_subscription(scan_id)

    expected_sub = {
        "topics": MessageEndpoints.device_progress(device="samx"),
        "cb": bec_emitter_mock._on_device_progress,
        "scan_id": scan_id,
    }
    register.assert_called_once_with(**expected_sub)
    assert sb.sync_storage[scan_id]["device_progress_sub"] == expected_sub


def test_on_device_progress_done_unregisters_and_emits_progress(bec_emitter_mock):
    sb = bec_emitter_mock.scan_bundler
    scan_id = "scan_id"
    sub = {
        "topics": MessageEndpoints.device_progress(device="samx"),
        "cb": bec_emitter_mock._on_device_progress,
        "scan_id": scan_id,
    }
    sb.sync_storage[scan_id] = {
        "info": {},
        "status": "open",
        "sent": set(),
        "device_progress_sub": sub,
    }
    progress_msg = messages.ProgressMessage(value=3, max_value=7, done=True)
    msg_obj = MessageObject(MessageEndpoints.device_progress("samx").endpoint, progress_msg)

    with (
        mock.patch.object(bec_emitter_mock.connector, "unregister") as unregister,
        mock.patch.object(bec_emitter_mock, "send_scan_progress") as send_scan_progress,
    ):
        bec_emitter_mock._on_device_progress(msg_obj, scan_id)

    unregister.assert_called_once_with(**sub)
    send_scan_progress.assert_called_once_with(scan_id, value=3, max_value=7, done=True)


def test_scan_status_update_closed_with_device_progress_unsubscribes_and_emits_last_progress(
    bec_emitter_mock,
):
    sb = bec_emitter_mock.scan_bundler
    scan_id = "scan_id"
    sub = {
        "topics": MessageEndpoints.device_progress(device="samx"),
        "cb": bec_emitter_mock._on_device_progress,
        "scan_id": scan_id,
    }
    sb.sync_storage[scan_id] = {
        "info": {},
        "status": "closed",
        "sent": {0, 1},
        "baseline": {},
        "device_progress_sub": sub,
        "last_progress_sent": messages.ProgressMessage(value=4, max_value=9, done=False),
    }
    msg = messages.ScanStatusMessage(scan_id=scan_id, status="closed", info={"num_points": 10})

    with (
        mock.patch.object(bec_emitter_mock.connector, "unregister") as unregister,
        mock.patch.object(bec_emitter_mock, "send_scan_progress") as send_scan_progress,
    ):
        bec_emitter_mock.on_scan_status_update(msg)

    unregister.assert_called_once_with(**sub)
    send_scan_progress.assert_called_once_with(scan_id, value=4, max_value=9, done=True)


@pytest.mark.parametrize("status", ["closed", "aborted"])
def test_scan_status_update_device_progress_without_last_progress_emits_done_message(
    bec_emitter_mock, status
):
    sb = bec_emitter_mock.scan_bundler
    scan_id = "scan_id"
    sub = {
        "topics": MessageEndpoints.device_progress(device="samx"),
        "cb": bec_emitter_mock._on_device_progress,
        "scan_id": scan_id,
    }
    sb.sync_storage[scan_id] = {
        "info": {},
        "status": status,
        "sent": {0, 1},
        "baseline": {},
        "device_progress_sub": sub,
    }
    msg = messages.ScanStatusMessage(scan_id=scan_id, status=status, info={"num_points": 10})

    with (
        mock.patch.object(bec_emitter_mock.connector, "unregister") as unregister,
        mock.patch.object(bec_emitter_mock, "send_scan_progress") as send_scan_progress,
    ):
        bec_emitter_mock.on_scan_status_update(msg)

    unregister.assert_called_once_with(**sub)
    send_scan_progress.assert_called_once_with(scan_id, value=0, max_value=0, done=True)


def test_on_cleanup_unregisters_device_progress_subscription(bec_emitter_mock):
    sb = bec_emitter_mock.scan_bundler
    scan_id = "scan_id"
    sub = {
        "topics": MessageEndpoints.device_progress(device="samx"),
        "cb": bec_emitter_mock._on_device_progress,
        "scan_id": scan_id,
    }
    sb.sync_storage[scan_id] = {"device_progress_sub": sub}

    with mock.patch.object(bec_emitter_mock.connector, "unregister") as unregister:
        bec_emitter_mock.on_cleanup(scan_id)

    unregister.assert_called_once_with(**sub)
