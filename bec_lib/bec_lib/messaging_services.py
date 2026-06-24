from __future__ import annotations

import enum
import html
import inspect
import mimetypes
import os
import textwrap
from abc import ABC
from typing import TYPE_CHECKING, Generic, Literal, Self, TypeVar

from rich.console import Console
from rich.table import Table

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib.client import BECClient
    from bec_lib.connector import MessageObject
    from bec_lib.redis_connector import RedisConnector

# Type variable for the message object class
MessageObjectT = TypeVar("MessageObjectT", bound="MessageServiceObject")


def _normalize_tags(tags: str | list[str]) -> list[str]:
    """
    Normalize tag input to a stable duplicate-free list.
    """
    if isinstance(tags, str):
        tags = [tags]
    return list(dict.fromkeys(tags))


def _build_attachment_content(
    file_path: str, width: int | str | None = None, height: int | str | None = None
):
    """
    Load a file attachment and return a messaging content block.
    """

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Attachment file not found: {file_path}")

    file_size = os.path.getsize(file_path)
    max_size = 5 * 1024 * 1024
    if file_size > max_size:
        raise ValueError(
            f"Attachment file size exceeds the maximum limit of 5 MB: {file_size} bytes"
        )

    with open(file_path, "rb") as f:
        file_data = f.read()

    filename = os.path.basename(file_path)
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type is None:
        mime_type = "application/octet-stream"

    return messages.MessagingServiceFileContent(
        filename=filename, mime_type=mime_type, data=file_data, width=width, height=height
    )


def _format_rich_text(
    text: str,
    bold: bool = False,
    italic: bool = False,
    color: Literal["red", "green", "black", "yellow", "pink", "blue"] | None = None,
) -> str:
    """
    Build the small subset of HTML used by rich messaging services.
    """
    if bold or italic or color:
        if bold:
            text = f"<strong>{text}</strong>"
        if italic:
            text = f"<em>{text}</em>"
        if color:
            if color == "black":
                pass
            elif color in ["red", "green"]:
                text = f'<mark class="pen-{color}">{text}</mark>'
            elif color in ["yellow", "pink", "blue"]:
                text = f'<mark class="marker-{color}">{text}</mark>'
        text = f"<p>{text}</p>"
    return text


class MessagingContainer:
    """
    A container for providing easy access to multiple messaging services.
    """

    def __init__(self, connector: RedisConnector, client: BECClient | None = None) -> None:
        self.scilog = SciLogMessagingService(connector, client=client)
        self.teams = TeamsMessagingService(connector)
        self.signal = SignalMessagingService(connector)


class MessageServiceObject:
    """
    A class representing a message object for a messaging service.
    """

    def __init__(self, service: MessagingService, scope: str | list[str] | None = None) -> None:
        self._service = service
        self._scope = scope
        self._content = []

    def add_text(self, text: str, **kwargs) -> Self:
        """
        Add text to the message object.

        Args:
            text (str): The text to add.
            **kwargs: Additional keyword arguments for specific messaging services.

        Returns:
            MessageObject: The updated message object.
        """
        # Implementation to add text to the message
        self._content.append(messages.MessagingServiceTextContent(content=text))
        return self

    def add_attachment(
        self, file_path: str, width: int | str | None = None, height: int | str | None = None
    ) -> Self:
        """
        Add an attachment to the message object. The file is read from
        the given file path and included in the message, including its
        metadata such as filename and MIME type.

        Please note that the maximum allowed file size for attachments is 5 MB.

        Args:
            file_path (str): The file path of the attachment to add.
            width (int | str | None): The display width of the attachment. Optional.
            height (int | str | None): The display height of the attachment. Optional.

        Raises:
            FileNotFoundError: If the attachment file does not exist.
            ValueError: If the attachment file size exceeds the maximum limit of 5 MB.

        Returns:
            MessageObject: The updated message object.
        """

        self._content.append(_build_attachment_content(file_path, width=width, height=height))

        return self

    def send(self, scope: str | list[str] | None = None) -> None:
        """
        Send the message using the associated messaging service.
        Please note that if the service does not provide a default scope and more than one
        scope is available, you must specify the scope when sending the message.

        Args:
            scope (str | list[str] | None): The scope or recipient for the message. If None, uses the scope set during initialization.
        """
        self._service.send(self, scope=scope)


class MessagingService(ABC, Generic[MessageObjectT]):
    """
    Abstract base class for messaging services.
    Inherit from this class to implement specific messaging services.
    At minimum, override the _SERVICE_NAME attribute.
    """

    _SERVICE_NAME = "generic"
    _MESSAGE_OBJECT_CLASS: type[MessageObjectT] = MessageServiceObject  # type: ignore
    _SUPPORTS_EMPTY_SCOPES = False

    def __init__(self, redis_connector: RedisConnector) -> None:
        self._redis_connector = redis_connector
        self._scopes: set[str] = set()
        self._auto_notifications: dict[str, list[str]] = {}
        self._enabled = False
        self._default_scope: str | list[str] | None = None
        self._service_config: messages.AvailableMessagingServicesMessage | None = None
        self._redis_connector.register(
            MessageEndpoints.available_messaging_services(),
            cb=self._on_new_scope_change_msg,
            from_start=True,
        )
        self._redis_connector.register(
            MessageEndpoints.notification_config(), cb=self._on_notification_config_change_msg
        )
        config_msg = self._redis_connector.get(MessageEndpoints.notification_config())
        if config_msg is not None:
            self._update_auto_notifications(config_msg)

    def set_default_scope(self, scope: str | list[str] | None) -> None:
        """
        Set the default scope for messages sent by this service.

        Args:
            scope (str | list[str] | None): The default scope to set. If None, clears the default scope.
        """
        if scope is not None and scope not in self._scopes:
            raise ValueError(f"Scope '{scope}' is not available for this messaging service.")
        self._default_scope = scope

    def set_auto_notifications(
        self,
        event_type: (
            Literal[
                "new_scan",
                "scan_completed",
                "alarm_warning",
                "alarm_minor",
                "alarm_major",
                "scan_interlock",
            ]
            | str
        ),
        enabled: bool,
        scopes: list[str] | str | None = None,
    ) -> None:
        """
        Set automatic notifications for a specific event type.

        Args:
            event_type (Literal["new_scan", "scan_completed", "alarm_warning", "alarm_minor", "alarm_major", "scan_interlock"] | str): The type of event to set notifications for.
            enabled (bool): Whether to enable or disable notifications for the event.
            scopes (list[str] | str | None): The scopes to apply the notifications to.
        """
        event_name = event_type.value if isinstance(event_type, enum.Enum) else event_type
        scopes_list: list[str] = []
        if scopes is not None:
            if isinstance(scopes, str):
                scopes_list = [scopes]
            else:
                scopes_list = scopes

            for scope in scopes_list:
                if scope not in self._scopes:
                    raise ValueError(
                        f"Scope '{scope}' is not available for this messaging service."
                    )
        else:
            if self._default_scope is not None:
                scopes_list = (
                    [self._default_scope]
                    if isinstance(self._default_scope, str)
                    else self._default_scope
                )
            elif not self._SUPPORTS_EMPTY_SCOPES:
                raise ValueError(
                    "Scopes must be provided when there is no default scope and empty scopes are not supported."
                )

        if enabled:
            # merge with existing scopes if already enabled for this event type
            existing_scopes = set(self._auto_notifications.get(event_name, []))
            existing_scopes.update(scopes_list)
            self._auto_notifications[event_name] = list(existing_scopes)
        else:
            # if disabling, remove the scopes for this event type, or the entire event type if no scopes are provided
            if scopes_list:
                existing_scopes = set(self._auto_notifications.get(event_name, []))
                existing_scopes.difference_update(scopes_list)
                if existing_scopes:
                    self._auto_notifications[event_name] = list(existing_scopes)
                else:
                    self._auto_notifications.pop(event_name, None)
            else:
                self._auto_notifications.pop(event_name, None)

        self._sync_auto_notifications_config(event_name, enabled=enabled, scopes=scopes_list)

    def _sync_auto_notifications_config(
        self, event_name: str, enabled: bool, scopes: list[str]
    ) -> None:
        config_msg = self._redis_connector.get(MessageEndpoints.notification_config())
        if config_msg is None:
            config_msg = messages.NotificationConfigMessage()

        routes = {name: list(targets) for name, targets in config_msg.routes.items()}
        event_routes = list(routes.get(event_name, []))

        if enabled:
            scopes_to_add = scopes or [None]
            for scope in scopes_to_add:
                target = messages.NotificationServiceTarget(
                    service_name=self._SERVICE_NAME, scope=scope
                )
                if not any(existing == target for existing in event_routes):
                    event_routes.append(target)
        else:
            if scopes:
                event_routes = [
                    target
                    for target in event_routes
                    if not (target.service_name == self._SERVICE_NAME and target.scope in scopes)
                ]
            else:
                event_routes = [
                    target for target in event_routes if target.service_name != self._SERVICE_NAME
                ]

        if event_routes:
            routes[event_name] = event_routes
        else:
            routes.pop(event_name, None)

        self._redis_connector.set_and_publish(
            MessageEndpoints.notification_config(),
            messages.NotificationConfigMessage(routes=routes, metadata=config_msg.metadata),
        )

    def _on_notification_config_change_msg(
        self,
        message: (
            MessageObject[messages.NotificationConfigMessage]
            | dict[str, messages.NotificationConfigMessage]
        ),
    ) -> None:
        config_msg = message.value if hasattr(message, "value") else message["data"]
        self._update_auto_notifications(config_msg)

    def _update_auto_notifications(self, config_msg: messages.NotificationConfigMessage) -> None:
        auto_notifications: dict[str, list[str]] = {}
        for event_name, targets in config_msg.routes.items():
            scopes: list[str] = []
            has_matching_service = False
            for target in targets:
                if target.service_name != self._SERVICE_NAME:
                    continue
                has_matching_service = True
                if isinstance(target.scope, str):
                    scopes.append(target.scope)
                elif isinstance(target.scope, list):
                    scopes.extend(target.scope)

            if has_matching_service:
                auto_notifications[event_name] = list(dict.fromkeys(scopes))

        self._auto_notifications = auto_notifications

    def _on_new_scope_change_msg(
        self, message: dict[str, messages.AvailableMessagingServicesMessage]
    ) -> None:
        """
        Callback for scope changes. Currently a placeholder for future functionality.

        Args:
            message (dict[str, messages.AvailableMessagingServicesMessage]): The scope change message.
        """
        msg = message["data"]
        # pylint: disable=protected-access
        self._service_config = msg
        self._update_messaging_services(msg)

    def _update_messaging_services(
        self, service_info: messages.AvailableMessagingServicesMessage
    ) -> None:
        """
        Update the messaging service scopes and enabled status based on the provided scope information.

        Args:
            service_info (messages.AvailableMessagingServicesMessage): The new messaging service information.
        """
        # merge the scopes from deployment services and session services if they are enabled
        # if a service is found in both deployment and session, the session service will take precedence
        self._scopes = set()

        config_for_service: messages.MessagingServiceScopeConfig | None = getattr(
            service_info.config, self._SERVICE_NAME, None
        )
        if config_for_service is None or not config_for_service.enabled:
            # If the service is disabled in the config, mark it as disabled and return early
            self._enabled = False
            return

        self._default_scope = config_for_service.default

        self._enabled = True

        for service in service_info.deployment_services:
            # Only consider services that match the service type and are enabled
            if service.service_type != self._SERVICE_NAME:
                continue
            if not service.enabled:
                continue
            self._scopes.update(
                service.scope if isinstance(service.scope, list) else [service.scope]
            )
        for service in service_info.session_services:
            # Only consider services that match the service type and are enabled
            if service.service_type != self._SERVICE_NAME:
                continue
            if not service.enabled:
                continue
            self._scopes.update(
                service.scope if isinstance(service.scope, list) else [service.scope]
            )

        # Some services require configured scopes, while others can address
        # recipients directly without any pre-registered scope.
        if not self._scopes and not self._SUPPORTS_EMPTY_SCOPES:
            self._enabled = False

    def new(self, text: str | None = None) -> MessageObjectT:
        """
        Create a new message object associated with this messaging service.

        Args:
            text (str | None): Optional initial text content for the message.

        Returns:
            MessageServiceObject: A new message object.

        Examples:
            >>> # Create a new message object with initial text and send it in one line
            >>> messaging_service.new("Hello, World!").send()

            >>> # Create a new message object, add text and an attachment, then send it
            >>> msg = messaging_service.new()
            >>> msg.add_text("Hello, World!")
            >>> msg.add_attachment("./file.txt")
            >>> msg.send()

        Raises:
            RuntimeError: If the messaging service is not enabled.
        """
        if not self._enabled:
            raise RuntimeError(f"Messaging service '{self._SERVICE_NAME}' is not enabled.")
        obj = self._MESSAGE_OBJECT_CLASS(self, scope=self._default_scope)  # type: ignore
        if text is not None:
            obj.add_text(text)
        return obj

    def send(self, message: MessageServiceObject, scope: str | list[str] | None = None) -> None:
        """
        Send a message using the messaging service.

        Args:
            message (MessageServiceObject): The message to send.
            scope (str | list[str] | None): The scope or recipient for the message.

        Raises:
            RuntimeError: If the messaging service is not enabled.
        """
        if not self._enabled:
            raise RuntimeError(f"Messaging service '{self._SERVICE_NAME}' is not enabled.")
        bec_message = messages.MessagingServiceMessage(
            service_name=self._SERVICE_NAME,  # type: ignore
            message=message._content,  # pylint: disable=protected-access
            scope=(
                scope if scope is not None else message._scope  # pylint: disable=protected-access
            ),
        )
        self._redis_connector.xadd(
            MessageEndpoints.message_service_queue(),
            {"data": bec_message},
            max_size=50,
            expire=1000,
        )


class SciLogMessageServiceObject(MessageServiceObject):
    """
    A class representing a message object for the SciLog messaging service.
    """

    def add_text(
        self,
        text: str,
        bold: bool = False,
        italic: bool = False,
        color: Literal["red", "green", "black", "yellow", "pink", "blue"] | None = None,
        **kwargs,
    ) -> Self:
        """
        Add text to the SciLog message with optional inline HTML formatting.

        When any formatting option is supplied the text is wrapped in a ``<p>``
        paragraph so SciLog renders it correctly.

        Args:
            text: The text content.
            bold: Wrap the text in ``<strong>``.
            italic: Wrap the text in ``<em>``.
            color: Highlight colour using SciLog's pens or markers.
                Supported values for pens are: "red", "green" and "black".
                Supported values for markers are: "yellow", "pink", "blue".

        Returns:
            SciLogMessageServiceObject: The updated message object.

        Examples:
            >>> msg.add_text("Checks failed", bold=True, color="red")
        """
        super().add_text(_format_rich_text(text, bold=bold, italic=italic, color=color))
        return self

    def add_tags(self, tags: str | list[str]) -> Self:
        """
        Add tags to the SciLog message object.

        Args:
            tags (str | list[str]): The tag or list of tags to add.

        Returns:
            SciLogMessageServiceObject: The updated message object.
        """
        # Ensure that there are no duplicates...
        default_tags = (
            self._service.get_default_tags()  # type: ignore
            if self._service and hasattr(self._service, "get_default_tags")
            else []
        )
        tags = _normalize_tags(default_tags + _normalize_tags(tags))
        self._content.append(messages.MessagingServiceTagsContent(tags=tags))
        return self

    def send(self, scope: str | list[str] | None = None) -> None:
        """
        Send the message using the associated messaging service. If no scope is provided, uses the default scope set for the service.
        If there are no tags in the content, adds the default tags before sending.

        Args:
            scope (str | list[str] | None): The scope or recipient for the message. If None, uses the default scope set for the service.
        """
        # If there are no tags in the content, add the default tags before sending
        if not any(
            isinstance(content, messages.MessagingServiceTagsContent) for content in self._content
        ):
            if self._service and hasattr(self._service, "get_default_tags"):
                self.add_tags(self._service.get_default_tags())  # type: ignore
        super().send(scope=scope)


class SciLogMessagingService(MessagingService[SciLogMessageServiceObject]):
    """Messaging service for SciLog platform."""

    _SERVICE_NAME = "scilog"
    _MESSAGE_OBJECT_CLASS = SciLogMessageServiceObject

    def __init__(self, redis_connector: RedisConnector, client: BECClient | None = None) -> None:
        super().__init__(redis_connector)
        self._default_tags: list[str] = ["bec"]
        self._client = client

    def set_default_tags(self, tags: str | list[str]):
        """
        Set default tags for the SciLog message object. These tags will be included in every message sent using this object.

        Args:
            tags (str | list[str]): The default tag or list of tags to set.
        """
        if isinstance(tags, str):
            tags = [tags]
        self._default_tags = tags

    def get_default_tags(self) -> list[str]:
        """
        Get the current default tags for the SciLog message object.

        Returns:
            list[str]: The current default tags.
        """
        return self._default_tags

    @staticmethod
    def _table_cell(value: str, bold: bool = False) -> str:
        content = html.escape(value)
        if bold:
            content = f"<strong>{content}</strong>"
        return f"<td>{content}</td>"

    @classmethod
    def _position_table_html(cls, rows: list[dict[str, str]]) -> str:
        table_rows = [
            "<tr>"
            f"{cls._table_cell('device', bold=True)}"
            f"{cls._table_cell('readback', bold=True)}"
            f"{cls._table_cell('setpoint', bold=True)}"
            f"{cls._table_cell('limits', bold=True)}"
            "</tr>"
        ]
        table_rows.extend(
            "<tr>"
            f"{cls._table_cell(row['name'])}"
            f"{cls._table_cell(row['readback'])}"
            f"{cls._table_cell(row['setpoint'])}"
            f"{cls._table_cell(row['limits'])}"
            "</tr>"
            for row in rows
        )
        return (
            f"<figure class=\"table\"><table><tbody>{''.join(table_rows)}</tbody></table></figure>"
        )

    @staticmethod
    def _code_block_html(source: str, language: str = "python") -> str:
        escaped_language = html.escape(language, quote=True)
        escaped_source = html.escape(textwrap.dedent(source).strip("\n"))
        return f'<pre><code class="language-{escaped_language}">{escaped_source}</code></pre>'

    def log_positions(
        self,
        devices: list[str] | str | None = None,
        scope: str | list[str] | None = None,
        title: str | None = None,
        tags: str | list[str] | None = None,
    ) -> None:
        """
        Send the current device positions to SciLog as an HTML table.

        Args:
            devices: Device names, glob patterns or device objects accepted by ``dev.wm``.
            scope: Optional override for the SciLog scope.
            title: Optional text shown above the table.
            tags: Optional tags added to the SciLog message.
        """
        if self._client is None or getattr(self._client, "device_manager", None) is None:
            raise RuntimeError(
                "SciLog position logging requires a client-backed messaging service."
            )
        if self._client.device_manager is None or self._client.device_manager.devices is None:
            raise RuntimeError(
                "SciLog position logging requires a client-backed messaging service with a device manager."
            )
        dev = self._client.device_manager.devices
        rows = dev._position_rows(devices)
        message = self.new()
        content = self._position_table_html(rows)
        if title:
            content = f"<p>{html.escape(title)}</p>{content}"
        message.add_text(content)
        if tags is not None:
            message.add_tags(tags)
        message.send(scope=scope)

        print("The following position table was sent to SciLog:")

        console = Console()
        table = Table()
        table.add_column("", justify="center")
        table.add_column("readback", justify="center")
        table.add_column("setpoint", justify="center")
        table.add_column("limits", justify="center")
        for row in rows:
            table.add_row(row["name"], row["readback"], row["setpoint"], row["limits"])
        console.print(table)

    def log_code(
        self,
        source: object,
        scope: str | list[str] | None = None,
        title: str | None = None,
        tags: str | list[str] | None = None,
        language: str = "python",
    ) -> None:
        """
        Send source code to SciLog as a syntax-highlighted code block.

        Args:
            source: A callable or raw source string to send.
            scope: Optional override for the SciLog scope.
            title: Optional text shown above the code block.
            tags: Optional tags added to the SciLog message.
            language: Syntax highlighting language class for the code block.

        Examples:
            >>> # Log a function's source code
            >>> def my_function():
            >>>     print("Hello, World!")
            >>> bec.messaging.scilog.log_code(my_function, title="My Function", tags="code")
        """
        if isinstance(source, str):
            source_code = source
        else:
            try:
                source_code = inspect.getsource(source)
            except (OSError, TypeError) as exc:
                raise ValueError(
                    "Could not extract source code. Pass a function with available source or a source string."
                ) from exc

        content = self._code_block_html(source_code, language=language)
        if title:
            content = f"<p>{html.escape(title)}</p>{content}"
        message = self.new()
        message.add_text(content)
        if tags is not None:
            message.add_tags(tags)
        message.send(scope=scope)


class TeamsMessagingService(MessagingService[MessageServiceObject]):
    """Messaging service for Microsoft Teams platform."""

    _SERVICE_NAME = "teams"


class SignalMessageServiceObject(MessageServiceObject):
    """
    A class representing a message object for the Signal messaging service.
    """

    def add_sticker(self, sticker: str) -> Self:
        """
        Add sticker to the Signal message object.

        Args:
            sticker (str): The sticker to add.

        Returns:
            SignalMessageServiceObject: The updated message object.
        """
        self._content.append(messages.MessagingServiceStickerContent(sticker_id=sticker))
        return self

    def send(self, scope: str | list[str] | None = None) -> None:
        """
        Send the message using the associated messaging service.

        For Signal, the scope can be either a registered scope for a group or one
        or more phone numbers for direct messages. Valid phone numbers are
        normalized before sending; unparsable values are left untouched and treated
        as regular scopes.

        Args:
            scope (str | list[str] | None): The scope or recipient for the
                message. If None, uses the default scope set for the service.
        """
        if scope is None:
            return super().send(scope=scope)
        normalized_scope = self._service._normalize_scope(scope)  # type: ignore[attr-defined]
        return super().send(scope=normalized_scope)


class SignalMessagingService(MessagingService[SignalMessageServiceObject]):
    """Messaging service for Signal platform."""

    _SERVICE_NAME = "signal"
    _MESSAGE_OBJECT_CLASS = SignalMessageServiceObject
    _SUPPORTS_EMPTY_SCOPES = True

    @staticmethod
    def _normalize_phone_number(value: str) -> str:
        """
        Normalize a valid phone number to E.164 format.

        If parsing or validation fails, the original value is returned so it can
        still be interpreted as a named Signal scope.
        """
        import phonenumbers

        candidate = value.strip()
        region = None if candidate.startswith("+") else "CH"
        try:
            parsed_number = phonenumbers.parse(candidate, region)
        except phonenumbers.NumberParseException:
            return value

        if not phonenumbers.is_valid_number(parsed_number):
            return value

        return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)

    def _normalize_scope(self, scope: str | list[str]) -> str | list[str]:
        """Normalize Signal phone numbers while preserving plain scopes."""
        if isinstance(scope, str):
            return self._normalize_phone_number(scope)
        return [self._normalize_phone_number(entry) for entry in scope]


class NotificationMessageObject(SciLogMessageServiceObject):
    """
    Generic notification payload that can be adapted to concrete messaging
    services during routing.
    """

    def __init__(self):
        super().__init__(service=None)  # type: ignore
