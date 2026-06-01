from __future__ import annotations

import enum
from typing import TYPE_CHECKING, cast

from bs4 import BeautifulSoup

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messaging_services import (
    MessagingService,
    SciLogMessagingService,
    SignalMessagingService,
    TeamsMessagingService,
)

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger


class MessagingEvent(str, enum.Enum):
    """
    Enumeration of messaging events that can trigger configured hooks.
    """

    SCAN = "new_scan"
    SCAN_COMPLETED = "scan_completed"
    ALARM_WARNING = "alarm_warning"
    ALARM_MINOR = "alarm_minor"
    ALARM_MAJOR = "alarm_major"
    SCAN_INTERLOCK = "scan_interlock"


class MessagingManager:
    """
    Manage notification routing from internal events to concrete messaging
    services.
    """

    def __init__(self, connector: RedisConnector):
        self.connector = connector
        self.config: dict[str, list[messages.NotificationServiceTarget]] = {}
        signal = SignalMessagingService(self.connector)
        scilog = SciLogMessagingService(self.connector)
        teams = TeamsMessagingService(self.connector)
        self._service_by_name = {"signal": signal, "scilog": scilog, "teams": teams}

        self.connector.register(
            patterns=MessageEndpoints.notification("*"), cb=self._handle_notification
        )
        self.connector.register(
            topics=MessageEndpoints.notification_config(), cb=self._handle_notification_config
        )

        config_msg = self.connector.get(MessageEndpoints.notification_config())
        if config_msg is not None:
            self.on_notification_config(config_msg)

    def _handle_notification(self, msg_obj: MessageObject[messages.NotificationMessage], **_kwargs):
        prefix = MessageEndpoints.notification("").endpoint
        event_type_str = msg_obj.topic.removeprefix(prefix)
        self.on_notification(event_type_str, cast(messages.NotificationMessage, msg_obj.value))

    def _handle_notification_config(
        self, msg_obj: MessageObject[messages.NotificationConfigMessage], **_kwargs
    ):
        self.on_notification_config(cast(messages.NotificationConfigMessage, msg_obj.value))

    def on_notification(self, event_type: str, message: messages.NotificationMessage) -> None:
        """
        Handle a notification event by routing it to the configured messaging services.

        Args:
            event_type(str): The type of the event that triggered the notification.
            message(messages.NotificationMessage): The notification message containing the content to be sent.
        """
        routes = self.config.get(event_type, [])
        for route in routes:
            service = self._service_by_name.get(route.service_name)
            if service is None:
                logger.warning(f"Unknown messaging service: {route.service_name}")
                continue
            try:
                routed_message = self.to_service_message(service, message)
                logger.info(f"Routing notification for {event_type} to {route.service_name}")
                routed_message.send(scope=route.scope)
            except RuntimeError as exc:
                logger.warning(
                    f"Failed to send notification for {event_type} via {route.service_name}: {exc}"
                )

    def on_notification_config(self, message: messages.NotificationConfigMessage) -> None:
        """
        Update the notification routing configuration based on the received message.

        Args:
            message(NotificationConfigMessage): The message containing the new routing configuration.
        """
        config: dict[str, list[messages.NotificationServiceTarget]] = {}
        for event_name, targets in message.routes.items():
            config[event_name] = targets
        self.config = config

    def shutdown(self) -> None:
        """
        Shutdown the messaging manager by unregistering all notification handlers.
        """

        self.connector.unregister(
            patterns=MessageEndpoints.notification("*"), cb=self._handle_notification
        )
        self.connector.unregister(
            topics=MessageEndpoints.notification_config(), cb=self._handle_notification_config
        )

    def to_service_message(
        self, service: MessagingService, message: messages.NotificationMessage
    ) -> messages.MessagingServiceMessage:
        """
        Convert a generic NotificationMessage into a MessagingServiceMessage specific to the given service.

        Args:
            service(MessagingService): The messaging service for which to convert the message.
            message(NotificationMessage): The generic notification message to convert.

        Returns:
            MessagingServiceMessage: The converted message ready to be sent via the specified service.
        """
        # pylint: disable=protected-access
        match service._SERVICE_NAME:
            case SciLogMessagingService._SERVICE_NAME:
                scilog_message = service.new()
                scilog_message._content = message.message
                return scilog_message
            case TeamsMessagingService._SERVICE_NAME:
                teams_message = service.new()
                for content in message.message:
                    if isinstance(content, messages.MessagingServiceTextContent):
                        teams_message.add_text(
                            BeautifulSoup(content.content, "html.parser").get_text()
                        )
                return teams_message

            case SignalMessagingService._SERVICE_NAME:
                signal_message = service.new()
                for content in message.message:
                    if isinstance(content, messages.MessagingServiceTextContent):
                        signal_message.add_text(
                            BeautifulSoup(content.content, "html.parser").get_text()
                        )
                    elif isinstance(content, messages.MessagingServiceFileContent):
                        signal_message._content.append(content)

                return signal_message
            case _:
                raise ValueError(f"Unsupported messaging service: {service._SERVICE_NAME}")
