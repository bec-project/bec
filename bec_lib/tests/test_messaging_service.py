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
    available_services = messages.AvailableResourceMessage(
        resource=[
            messages.MessagingServiceConfig(
                service_name="scilog", scopes=["default", "alerts"], enabled=True
            )
        ]
    )
    SciLogMessagingService._on_new_scope_change_msg(
        message={"data": available_services}, parent=service
    )
    yield service


@pytest.fixture
def signal_service(connected_connector):
    service = SignalMessagingService(connected_connector)
    available_services = messages.AvailableResourceMessage(
        resource=[
            messages.MessagingServiceConfig(
                service_name="signal", scopes=["default", "signals"], enabled=True
            )
        ]
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
    assert tags_part.tags == ["tag1", "tag2"]


def test_signal_messaging_service_new(signal_service):
    service = signal_service
    message = service.new()
    assert isinstance(message, SignalMessageServiceObject)
    assert message._service == service  # pylint: disable=protected-access
    assert message._content == []  # pylint: disable=protected-access
