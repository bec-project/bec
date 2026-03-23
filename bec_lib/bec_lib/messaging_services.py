from __future__ import annotations

import mimetypes
import os
from abc import ABC
from typing import TYPE_CHECKING, Generic, Self, TypeVar

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector

# Type variable for the message object class
MessageObjectT = TypeVar("MessageObjectT", bound="MessageServiceObject")


class MessagingContainer:
    """
    A container for providing easy access to multiple messaging services.
    """

    def __init__(self, connector: RedisConnector) -> None:
        self.scilog = SciLogMessagingService(connector)
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

        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"Attachment file not found: {file_path}")

        file_size = os.path.getsize(file_path)
        max_size = 5 * 1024 * 1024  # 5 MB
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

        self._content.append(
            messages.MessagingServiceFileContent(
                filename=filename, mime_type=mime_type, data=file_data, width=width, height=height
            )
        )

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

    def __init__(self, redis_connector: RedisConnector) -> None:
        self._redis_connector = redis_connector
        self._scopes: set[str] = set()
        self._enabled = False
        self._default_scope: str | list[str] | None = None
        self._service_config: messages.AvailableMessagingServicesMessage | None = None
        self._redis_connector.register(
            MessageEndpoints.available_messaging_services(),
            cb=self._on_new_scope_change_msg,
            from_start=True,
        )

    def set_default_scope(self, scope: str | list[str] | None) -> None:
        """
        Set the default scope for messages sent by this service.

        Args:
            scope (str | list[str] | None): The default scope to set. If None, clears the default scope.
        """
        if scope is not None and scope not in self._scopes:
            raise ValueError(f"Scope '{scope}' is not available for this messaging service.")
        self._default_scope = scope

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

        # If there are no scopes available for this service, or all are disabled, mark the service as disabled
        if not self._scopes:
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
        color: str | None = None,
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
            color: Highlight colour using SciLog's pen marks (e.g. ``"red"``,
                ``"yellow"``, ``"green"``, ``"blue"``, ``"pink"``).

        Returns:
            SciLogMessageServiceObject: The updated message object.

        Examples:
            >>> msg.add_text("Checks failed", bold=True, color="red")
        """
        if bold or italic or color:
            if bold:
                text = f"<strong>{text}</strong>"
            if italic:
                text = f"<em>{text}</em>"
            if color:
                text = f'<mark class="pen-{color}">{text}</mark>'
            text = f"<p>{text}</p>"
        return super().add_text(text)

    def add_tags(self, tags: str | list[str]) -> Self:
        """
        Add tags to the SciLog message object.

        Args:
            tags (str | list[str]): The tag or list of tags to add.

        Returns:
            SciLogMessageServiceObject: The updated message object.
        """
        if isinstance(tags, str):
            tags = [tags]

        # Ensure that there are no duplicates...
        tags = list(set(self._service.get_default_tags() + tags))  # type: ignore
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
            self.add_tags(self._service.get_default_tags())  # type: ignore
        super().send(scope=scope)


class SciLogMessagingService(MessagingService[SciLogMessageServiceObject]):
    """Messaging service for SciLog platform."""

    _SERVICE_NAME = "scilog"
    _MESSAGE_OBJECT_CLASS = SciLogMessageServiceObject

    def __init__(self, redis_connector: RedisConnector) -> None:
        super().__init__(redis_connector)
        self._default_tags: list[str] = ["bec"]

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


class SignalMessagingService(MessagingService[SignalMessageServiceObject]):
    """Messaging service for Signal platform."""

    _SERVICE_NAME = "signal"
    _MESSAGE_OBJECT_CLASS = SignalMessageServiceObject
