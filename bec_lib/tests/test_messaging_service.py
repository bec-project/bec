import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messaging_services import (
    MessageServiceObject,
    SciLogMessagingService,
    SignalMessageServiceObject,
    SignalMessagingService,
)


@pytest.fixture
def scilog_service(connected_connector):
    service = SciLogMessagingService(connected_connector)
    available_services = messages.AvailableMessagingServicesMessage(
        config=messages.MessagingConfig(
            signal=messages.MessagingServiceScopeConfig(enabled=False),
            teams=messages.MessagingServiceScopeConfig(enabled=False),
            scilog=messages.MessagingServiceScopeConfig(enabled=True),
        ),
        deployment_services=[
            messages.SciLogServiceInfo(
                id="test_scilog", scope="default", enabled=True, logbook_id="test_logbook"
            )
        ],
        session_services=[],
    )
    SciLogMessagingService._on_new_scope_change_msg(
        message={"data": available_services}, parent=service
    )
    yield service


@pytest.fixture
def signal_service(connected_connector):
    service = SignalMessagingService(connected_connector)
    available_services = messages.AvailableMessagingServicesMessage(
        config=messages.MessagingConfig(
            signal=messages.MessagingServiceScopeConfig(enabled=True),
            teams=messages.MessagingServiceScopeConfig(enabled=False),
            scilog=messages.MessagingServiceScopeConfig(enabled=False),
        ),
        deployment_services=[
            messages.SignalServiceInfo(
                id="test_signal", scope="default", enabled=True, group_id="test_group"
            )
        ],
        session_services=[],
    )
    SignalMessagingService._on_new_scope_change_msg(
        message={"data": available_services}, parent=service
    )
    yield service


@pytest.fixture
def scilog_message(scilog_service):
    message = scilog_service.new()
    yield message


def test_scilog_messaging_service_new(scilog_service):
    service = scilog_service
    message = service.new()
    assert isinstance(message, MessageServiceObject)
    assert message._service == service  # pylint: disable=protected-access
    assert message._content == []  # pylint: disable=protected-access


def test_scilog_messaging_service_send(scilog_message, connected_connector):
    message = scilog_message
    message.add_text("Test message")

    message.send()
    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "scilog"
    assert len(out.message) == 1
    assert isinstance(out.message[0], messages.MessagingServiceTextContent)
    assert out.message[0].content == "Test message"


def test_scilog_messaging_service_send_with_attachment(
    scilog_message, tmp_path, connected_connector
):
    # Create a temporary file to use as an attachment
    file_path = tmp_path / "test.txt"
    file_content = "This is a test file."
    with open(file_path, "w") as f:
        f.write(file_content)

    message = scilog_message
    message.add_text("Test message with attachment")
    message.add_attachment(str(file_path))

    message.send()
    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "scilog"
    assert len(out.message) == 2

    # Check text part
    text = out.message[0]
    assert isinstance(text, messages.MessagingServiceTextContent)
    assert text.content == "Test message with attachment"

    # Check attachment part
    attachment = out.message[1]
    assert isinstance(attachment, messages.MessagingServiceFileContent)

    assert attachment.filename == "test.txt"
    assert attachment.mime_type == "text/plain"
    assert attachment.data == file_content.encode()


def test_scilog_messaging_service_send_image_attachment(
    scilog_message, tmp_path, connected_connector
):
    # Create a temporary image file to use as an attachment
    file_path = tmp_path / "image.png"
    with open(file_path, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")  # Write minimal PNG header

    message = scilog_message
    message.add_text("Test message with image attachment")
    message.add_attachment(str(file_path))

    message.send()
    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "scilog"
    assert len(out.message) == 2

    # Check text part
    assert isinstance(out.message[0], messages.MessagingServiceTextContent)
    assert out.message[0].content == "Test message with image attachment"

    # Check attachment part
    attachment = out.message[1]
    assert isinstance(attachment, messages.MessagingServiceFileContent)

    assert attachment.filename == "image.png"
    assert attachment.mime_type == "image/png"
    assert attachment.data == b"\x89PNG\r\n\x1a\n"


def test_messaging_service_attachement_raises_if_too_large(scilog_message, tmp_path):
    # Create a temporary file larger than 5MB
    file_path = tmp_path / "large_file.bin"
    with open(file_path, "wb") as f:
        f.write(b"\0" * (5 * 1024 * 1024 + 1))  # 5MB + 1 byte

    message = scilog_message
    message.add_text("Test message with large attachment")

    with pytest.raises(ValueError, match="Attachment file size exceeds the maximum limit of 5 MB:"):
        message.add_attachment(str(file_path))


def test_scilog_messaging_service_add_tags(scilog_message, connected_connector):
    message = scilog_message
    message.add_text("Test message with tags")
    message.add_tags(["tag1", "tag2"])

    message.send()
    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "scilog"
    assert len(out.message) == 2
    text_part = out.message[0]
    tags_part = out.message[1]
    assert isinstance(text_part, messages.MessagingServiceTextContent)
    assert isinstance(tags_part, messages.MessagingServiceTagsContent)

    assert text_part.content == "Test message with tags"
    assert sorted(tags_part.tags) == sorted(
        ["bec", "tag1", "tag2"]
    )  # default "bec" tag should be included


def test_signal_messaging_service_new(signal_service):
    service = signal_service
    message = service.new()
    assert isinstance(message, SignalMessageServiceObject)
    assert message._service == service  # pylint: disable=protected-access
    assert message._content == []  # pylint: disable=protected-access


def test_attachment_file_not_found(scilog_message):
    message = scilog_message
    message.add_text("Test message with missing attachment")

    with pytest.raises(FileNotFoundError, match="Attachment file not found:"):
        message.add_attachment("/path/to/nonexistent/file.txt")


@pytest.mark.parametrize(
    "file_extension,expected_mime_type",
    [
        (".txt", "text/plain"),
        (".jpg", "image/jpeg"),
        (".jpeg", "image/jpeg"),
        (".png", "image/png"),
        (".pdf", "application/pdf"),
        (".bin", "application/octet-stream"),
    ],
)
def test_attachment_file_extensions(
    scilog_message, tmp_path, connected_connector, file_extension, expected_mime_type
):
    # Create a temporary file with the specified extension
    file_path = tmp_path / f"test{file_extension}"
    with open(file_path, "wb") as f:
        f.write(b"test content")

    message = scilog_message
    message.add_text("Test message with attachment")
    message.add_attachment(str(file_path))

    message.send()
    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]

    # Check attachment part
    attachment = out.message[1]
    assert isinstance(attachment, messages.MessagingServiceFileContent)
    assert attachment.filename == f"test{file_extension}"
    assert attachment.mime_type == expected_mime_type
    assert attachment.data == b"test content"


def test_disabled_service_cannot_create_message(connected_connector):
    service = SciLogMessagingService(connected_connector)
    # Create a disabled service
    available_services = messages.AvailableMessagingServicesMessage(
        config=messages.MessagingConfig(
            signal=messages.MessagingServiceScopeConfig(enabled=False),
            teams=messages.MessagingServiceScopeConfig(enabled=False),
            scilog=messages.MessagingServiceScopeConfig(enabled=False),
        ),
        deployment_services=[
            messages.SciLogServiceInfo(
                id="test_scilog", scope="default", enabled=False, logbook_id="test_logbook"
            )
        ],
        session_services=[],
    )
    SciLogMessagingService._on_new_scope_change_msg(
        message={"data": available_services}, parent=service
    )

    with pytest.raises(RuntimeError, match="Messaging service 'scilog' is not enabled."):
        service.new()


def test_disabled_service_cannot_send_message(connected_connector):
    # First create an enabled service and a message
    service = SciLogMessagingService(connected_connector)
    available_services = messages.AvailableMessagingServicesMessage(
        config=messages.MessagingConfig(
            signal=messages.MessagingServiceScopeConfig(enabled=False),
            teams=messages.MessagingServiceScopeConfig(enabled=False),
            scilog=messages.MessagingServiceScopeConfig(enabled=True),
        ),
        deployment_services=[
            messages.SciLogServiceInfo(
                id="test_scilog", scope="default", enabled=True, logbook_id="test_logbook"
            )
        ],
        session_services=[],
    )
    SciLogMessagingService._on_new_scope_change_msg(
        message={"data": available_services}, parent=service
    )
    message = service.new()
    message.add_text("Test message")

    # Now disable the service
    disabled_services = messages.AvailableMessagingServicesMessage(
        config=messages.MessagingConfig(
            signal=messages.MessagingServiceScopeConfig(enabled=False),
            teams=messages.MessagingServiceScopeConfig(enabled=False),
            scilog=messages.MessagingServiceScopeConfig(enabled=False),
        ),
        deployment_services=[
            messages.SciLogServiceInfo(
                id="test_scilog", scope="default", enabled=False, logbook_id="test_logbook"
            )
        ],
        session_services=[],
    )
    SciLogMessagingService._on_new_scope_change_msg(
        message={"data": disabled_services}, parent=service
    )

    with pytest.raises(RuntimeError, match="Messaging service 'scilog' is not enabled."):
        message.send()


def test_signal_messaging_service_send_with_sticker(signal_service, connected_connector):
    message = signal_service.new()
    message.add_text("Test message with sticker")
    message.add_sticker("sticker_123")

    message.send()
    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "signal"
    assert len(out.message) == 2

    # Check text part
    text_part = out.message[0]
    assert isinstance(text_part, messages.MessagingServiceTextContent)
    assert text_part.content == "Test message with sticker"

    # Check sticker part
    sticker_part = out.message[1]
    assert isinstance(sticker_part, messages.MessagingServiceStickerContent)
    assert sticker_part.sticker_id == "sticker_123"


def test_scilog_add_tags_with_string(scilog_message, connected_connector):
    """Test that add_tags works with a string input."""
    message = scilog_message
    message.add_text("Test message with single tag")
    message.add_tags("single_tag")

    message.send()
    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    assert len(out) == 1
    out = out[0]["data"]

    tags_part = out.message[1]
    assert isinstance(tags_part, messages.MessagingServiceTagsContent)
    assert sorted(tags_part.tags) == sorted(
        ["bec", "single_tag"]
    )  # default "bec" tag should be included


def test_signal_message_service_uses_default_scope(connected_connector):
    """Test that SignalMessagingService message uses default scope."""
    service = SignalMessagingService(connected_connector)
    # Configure signal service with multiple scopes
    available_services = messages.AvailableMessagingServicesMessage(
        config=messages.MessagingConfig(
            signal=messages.MessagingServiceScopeConfig(enabled=True),
            teams=messages.MessagingServiceScopeConfig(enabled=False),
            scilog=messages.MessagingServiceScopeConfig(enabled=False),
        ),
        deployment_services=[
            messages.SignalServiceInfo(
                id="test_signal_1", scope="user", enabled=True, group_id="test_group_1"
            ),
            messages.SignalServiceInfo(
                id="test_signal_2", scope="admin", enabled=True, group_id="test_group_2"
            ),
        ],
        session_services=[],
    )
    SignalMessagingService._on_new_scope_change_msg(
        message={"data": available_services}, parent=service
    )

    service.set_default_scope("user")
    message = service.new()
    message.send()
    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.scope == "user"

    with pytest.raises(
        ValueError, match="Scope 'invalid_scope' is not available for this messaging service."
    ):
        service.set_default_scope("invalid_scope")


def test_scilog_message_add_tags_with_default_tags(scilog_message, connected_connector):
    """Test that add_tags correctly combines default tags with provided tags."""

    scilog_message._service.set_default_tags(["default_tag1", "default_tag2"])  # type: ignore

    message = scilog_message
    message.add_text("Test message with default and additional tags")
    message.add_tags(["additional_tag1", "additional_tag2"])

    message.send()
    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    assert len(out) == 1
    out = out[0]["data"]

    tags_part = out.message[1]
    assert isinstance(tags_part, messages.MessagingServiceTagsContent)
    assert sorted(tags_part.tags) == sorted(
        ["default_tag1", "default_tag2", "additional_tag1", "additional_tag2"]
    )


def test_scilog_message_add_duplicate_tags(scilog_message, connected_connector):
    """Test that add_tags does not create duplicate tags when default tags overlap with provided tags."""

    scilog_message._service.set_default_tags(["bec", "default_tag"])  # type: ignore

    message = scilog_message
    message.add_text("Test message with duplicate tags")
    message.add_tags(["bec", "default_tag", "additional_tag"])

    message.send()
    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    assert len(out) == 1
    out = out[0]["data"]

    tags_part = out.message[1]
    assert isinstance(tags_part, messages.MessagingServiceTagsContent)
    # The final tags should include all unique tags without duplicates
    assert sorted(tags_part.tags) == sorted(["bec", "default_tag", "additional_tag"])
