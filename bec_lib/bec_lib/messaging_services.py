from __future__ import annotations

import os
from abc import ABC
from typing import TYPE_CHECKING, Generic, Self, TypeVar

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector

# Type variable for the message object class
MessageObjectT = TypeVar("MessageObjectT", bound="MessageServiceObject")


class MessageServiceObject:
    """
    A class representing a message object for a messaging service.
    """

    def __init__(self, service: MessagingService) -> None:
        self._service = service
        self._content = []

    def add_text(self, text: str) -> Self:
        """
        Add text to the message object.

        Args:
            text (str): The text to add.

        Returns:
            MessageObject: The updated message object.
        """
        # Implementation to add text to the message
        self._content.append(messages.MessagingServiceTextContent(content=text))
        return self

    def add_attachment(self, file_path: str) -> Self:
        """
        Add an attachment to the message object. The file is read from
        the given file path and included in the message, including its
        metadata such as filename and MIME type.

        Please note that the maximum allowed file size for attachments is 5 MB.

        Args:
            file_path (str): The file path of the attachment to add.

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
        file_extension = os.path.splitext(filename)[1].lower()
        if file_extension == ".txt":
            mime_type = "text/plain"
        elif file_extension in [".jpg", ".jpeg"]:
            mime_type = "image/jpeg"
        elif file_extension == ".png":
            mime_type = "image/png"
        elif file_extension == ".pdf":
            mime_type = "application/pdf"
        else:
            mime_type = "application/octet-stream"

        self._content.append(
            messages.MessagingServiceFileContent(
                filename=filename, mime_type=mime_type, data=file_data
            )
        )

        return self

    def send(self) -> None:
        """
        Send the message using the associated messaging service.
        """
        self._service.send(self)


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
        self._scopes = set()
        self._enabled = False
        self._redis_connector.register(
            MessageEndpoints.available_messaging_services(),
            cb=self._on_new_scope_change_msg,
            parent=self,
            from_start=True,
        )

    @staticmethod
    def _on_new_scope_change_msg(
        message: dict[str, messages.AvailableResourceMessage], parent: MessagingService
    ) -> None:
        """
        Callback for scope changes. Currently a placeholder for future functionality.

        Args:
            message (dict[str, messages.AvailableResourceMessage]): The scope change message.
            parent (MessagingService): The parent messaging service instance.
        """
        msg = message["data"]
        # pylint: disable=protected-access
        parent._update_messaging_services(msg)

    def _update_messaging_services(self, service_info: messages.AvailableResourceMessage) -> None:
        """
        Update the messaging service scopes and enabled status based on the provided scope information.

        Args:
            service_info (messages.AvailableResourceMessage): The new messaging service information.
        """
        for resource in service_info.resource:
            if not isinstance(resource, messages.MessagingServiceConfig):
                continue
            if resource.service_name == self._SERVICE_NAME:
                self._scopes = set(resource.scopes)
                self._enabled = resource.enabled
                break

    def new(self) -> MessageObjectT:
        """
        Create a new message object associated with this messaging service.

        Returns:
            MessageServiceObject: A new message object.
        """
        if not self._enabled:
            raise RuntimeError(f"Messaging service '{self._SERVICE_NAME}' is not enabled.")
        return self._MESSAGE_OBJECT_CLASS(self)  # type: ignore

    def send(self, message: MessageServiceObject) -> None:
        """
        Send a message using the messaging service.

        Args:
            message (MessageServiceObject): The message to send.
        """
        if not self._enabled:
            raise RuntimeError(f"Messaging service '{self._SERVICE_NAME}' is not enabled.")
        bec_message = messages.MessagingServiceMessage(
            service_name=self._SERVICE_NAME,  # type: ignore
            message=message._content,  # pylint: disable=protected-access
        )
        self._redis_connector.xadd(
            MessageEndpoints.message_service_queue(), {"data": bec_message}, max_size=50, expire=100
        )


class SciLogMessageServiceObject(MessageServiceObject):
    """
    A class representing a message object for the SciLog messaging service.
    """

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
        self._content.append(messages.MessagingServiceTagsContent(tags=tags))
        return self


class SciLogMessagingService(MessagingService[SciLogMessageServiceObject]):
    """Messaging service for SciLog platform."""

    _SERVICE_NAME = "scilog"
    _MESSAGE_OBJECT_CLASS = SciLogMessageServiceObject


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

    def add_giphy(self, giphy: str) -> Self:
        """
        Add giphy to the Signal message object.

        Args:
            giphy (str): The giphy to add.

        Returns:
            SignalMessageServiceObject: The updated message object.
        """
        self._content.append(messages.MessagingServiceGiphyContent(giphy_url=giphy))
        return self


class SignalMessagingService(MessagingService[SignalMessageServiceObject]):
    """Messaging service for Signal platform."""

    _SERVICE_NAME = "signal"
    _MESSAGE_OBJECT_CLASS = SignalMessageServiceObject
