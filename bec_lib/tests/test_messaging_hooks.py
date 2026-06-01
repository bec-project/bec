from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints, MessageOp
from bec_lib.messaging_hooks import MessagingEvent, MessagingManager
from bec_lib.messaging_services import NotificationMessageObject


@pytest.fixture
def messaging_manager(connected_connector):
    manager = MessagingManager(connected_connector)
    yield manager
    manager.shutdown()


@pytest.fixture
def messaging_manager_with_initial_config(connected_connector):
    config_msg = messages.NotificationConfigMessage(
        routes={
            "new_scan": [messages.NotificationServiceTarget(service_name="scilog", scope="logbook")]
        }
    )
    connected_connector.set_and_publish(MessageEndpoints.notification_config(), config_msg)

    manager = MessagingManager(connected_connector)
    yield manager
    manager.shutdown()


def _available_services_message():
    return messages.AvailableMessagingServicesMessage(
        config=messages.MessagingConfig(
            signal=messages.MessagingServiceScopeConfig(enabled=True, default=None),
            teams=messages.MessagingServiceScopeConfig(enabled=False, default=None),
            scilog=messages.MessagingServiceScopeConfig(enabled=True, default=None),
        ),
        deployment_services=[
            messages.SciLogServiceInfo(
                id="scilog-default", scope="logbook", enabled=True, logbook_id="lb-1"
            ),
            messages.SignalServiceInfo(
                id="signal-default", scope="ops", enabled=True, group_id="g-1"
            ),
        ],
        session_services=[],
    )


def test_notification_endpoints():
    event_endpoint = MessageEndpoints.notification("new_scan")
    config_endpoint = MessageEndpoints.notification_config()

    assert event_endpoint.endpoint == "internal/messaging_services/notification/new_scan"
    assert event_endpoint.message_type is messages.NotificationMessage
    assert event_endpoint.message_op == MessageOp.SEND

    assert config_endpoint.endpoint == "user/messaging_services/notification_config"
    assert config_endpoint.message_type is messages.NotificationConfigMessage
    assert config_endpoint.message_op == MessageOp.SET_PUBLISH


def test_messaging_manager_loads_initial_config(messaging_manager_with_initial_config):
    assert messaging_manager_with_initial_config.config == {
        MessagingEvent.SCAN: [
            messages.NotificationServiceTarget(service_name="scilog", scope="logbook")
        ]
    }


def test_messaging_manager_routes_notifications_to_message_service_queue(
    connected_connector, messaging_manager
):
    available_services = _available_services_message()
    messaging_manager._service_by_name["scilog"]._on_new_scope_change_msg(
        {"data": available_services}
    )
    messaging_manager._service_by_name["signal"]._on_new_scope_change_msg(
        {"data": available_services}
    )
    messaging_manager._service_by_name["teams"]._on_new_scope_change_msg(
        {"data": available_services}
    )

    messaging_manager.on_notification_config(
        messages.NotificationConfigMessage(
            routes={
                "new_scan": [
                    messages.NotificationServiceTarget(service_name="scilog", scope="logbook")
                ]
            }
        )
    )
    msg_obj = NotificationMessageObject().add_text("Scan started").add_tags(["bec", "new_scan"])
    messaging_manager.on_notification(
        MessagingEvent.SCAN,
        messages.NotificationMessage(event="new_scan", message=msg_obj._content),
    )

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    assert len(out) == 1
    sent_message = out[0]["data"]
    assert sent_message.service_name == "scilog"
    assert sent_message.scope == "logbook"
    assert isinstance(sent_message.message[0], messages.MessagingServiceTextContent)
    assert sent_message.message[0].content == "Scan started"
    assert isinstance(sent_message.message[1], messages.MessagingServiceTagsContent)
    assert sent_message.message[1].tags == ["bec", "new_scan"]


def test_messaging_manager_routes_generic_notification_to_signal_as_plain_text(
    connected_connector, messaging_manager
):
    available_services = _available_services_message()
    messaging_manager._service_by_name["signal"]._on_new_scope_change_msg(
        {"data": available_services}
    )

    messaging_manager.on_notification_config(
        messages.NotificationConfigMessage(
            routes={
                "alarm_major": [
                    messages.NotificationServiceTarget(service_name="signal", scope="ops")
                ]
            }
        )
    )

    messaging_manager.on_notification(
        MessagingEvent.ALARM_MAJOR,
        messages.NotificationMessage(
            event="alarm_major",
            message=NotificationMessageObject()
            .add_text("Beamline checks failed", bold=True, color="red")
            .add_tags(["alarm"])
            ._content,
        ),
    )

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    assert len(out) == 1
    sent_message = out[0]["data"]
    assert sent_message.service_name == "signal"
    assert sent_message.scope == "ops"
    assert len(sent_message.message) == 1
    assert isinstance(sent_message.message[0], messages.MessagingServiceTextContent)
    assert sent_message.message[0].content == "Beamline checks failed"


def test_handle_notification_routes_message_from_topic_suffix(messaging_manager):
    notification_message = messages.NotificationMessage(
        event="scan_completed",
        message=NotificationMessageObject().add_text("Scan complete")._content,
    )
    msg_obj = MessageObject(
        MessageEndpoints.notification("scan_completed").endpoint, notification_message
    )

    with mock.patch.object(messaging_manager, "on_notification") as on_notification:
        messaging_manager._handle_notification(msg_obj)

    on_notification.assert_called_once_with("scan_completed", notification_message)


def test_handle_notification_config_forwards_message_value(messaging_manager):
    config_message = messages.NotificationConfigMessage(
        routes={
            "alarm_minor": [messages.NotificationServiceTarget(service_name="signal", scope="ops")]
        }
    )
    msg_obj = MessageObject(MessageEndpoints.notification_config().endpoint, config_message)

    with mock.patch.object(messaging_manager, "on_notification_config") as on_notification_config:
        messaging_manager._handle_notification_config(msg_obj)

    on_notification_config.assert_called_once_with(config_message)


def test_to_service_message_for_teams_strips_html_and_ignores_non_text(messaging_manager):
    available_services = messages.AvailableMessagingServicesMessage(
        config=messages.MessagingConfig(
            signal=messages.MessagingServiceScopeConfig(enabled=True, default=None),
            teams=messages.MessagingServiceScopeConfig(enabled=True, default=None),
            scilog=messages.MessagingServiceScopeConfig(enabled=True, default=None),
        ),
        deployment_services=[
            messages.TeamsServiceInfo(
                id="teams-default",
                scope="ops-team",
                enabled=True,
                workflow_webhook_url="https://example.invalid/webhook",
            )
        ],
        session_services=[],
    )
    messaging_manager._service_by_name["teams"]._on_new_scope_change_msg(
        {"data": available_services}
    )
    message = messages.NotificationMessage(
        event="alarm_major",
        message=NotificationMessageObject()
        .add_text("Beamline checks failed", bold=True, color="red")
        .add_tags(["alarm"])
        ._content,
    )

    routed_message = messaging_manager.to_service_message(
        messaging_manager._service_by_name["teams"], message
    )

    assert len(routed_message._content) == 1
    assert isinstance(routed_message._content[0], messages.MessagingServiceTextContent)
    assert routed_message._content[0].content == "Beamline checks failed"


def test_to_service_message_for_signal_preserves_files_and_plain_text(messaging_manager):
    available_services = _available_services_message()
    messaging_manager._service_by_name["signal"]._on_new_scope_change_msg(
        {"data": available_services}
    )
    file_content = messages.MessagingServiceFileContent(
        filename="alarm.png", mime_type="image/png", data=b"png-bytes", width=128, height=64
    )
    message = messages.NotificationMessage(
        event="alarm_warning",
        message=NotificationMessageObject()
        .add_text("Beamline checks failed", italic=True)
        .add_tags(["alarm"])
        ._content
        + [file_content],
    )

    routed_message = messaging_manager.to_service_message(
        messaging_manager._service_by_name["signal"], message
    )

    assert len(routed_message._content) == 2
    assert isinstance(routed_message._content[0], messages.MessagingServiceTextContent)
    assert routed_message._content[0].content == "Beamline checks failed"
    assert routed_message._content[1] == file_content


def test_to_service_message_raises_for_unsupported_service(messaging_manager):
    message = messages.NotificationMessage(
        event="alarm_minor",
        message=NotificationMessageObject().add_text("Beamline checks failed")._content,
    )

    with mock.patch("bec_lib.messaging_hooks.logger.warning") as warning:
        with pytest.raises(ValueError, match="Unsupported messaging service: unsupported"):
            messaging_manager.to_service_message(
                SimpleNamespace(_SERVICE_NAME="unsupported"), message
            )

    warning.assert_not_called()
