from bec_lib.endpoints import MessageEndpoints
from bec_lib.messaging_services import MessageServiceObject, SciLogMessagingService


def test_scilog_messaging_service_new(connected_connector):
    service = SciLogMessagingService(connected_connector)
    message = service.new()
    assert isinstance(message, MessageServiceObject)
    assert message._service == service  # pylint: disable=protected-access
    assert message._content == []  # pylint: disable=protected-access


def test_scilog_messaging_service_send(connected_connector):
    service = SciLogMessagingService(connected_connector)
    message = service.new()
    message.add_text("Test message")

    message.send()
    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "scilog"
    assert len(out.message) == 1
    assert out.message[0].content_type == "text"
    assert out.message[0].content == "Test message"


def test_scilog_messaging_service_send_with_attachment(tmp_path, connected_connector):
    # Create a temporary file to use as an attachment
    file_path = tmp_path / "test.txt"
    file_content = "This is a test file."
    with open(file_path, "w") as f:
        f.write(file_content)

    service = SciLogMessagingService(connected_connector)
    message = service.new()
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
    assert out.message[0].content_type == "text"
    assert out.message[0].content == "Test message with attachment"

    # Check attachment part
    assert out.message[1].content_type == "file"
    attachment = out.message[1].content
    assert attachment["filename"] == "test.txt"
    assert attachment["mime_type"] == "text/plain"
    assert attachment["data"] == file_content.encode()


def test_scilog_messaging_service_send_image_attachment(tmp_path, connected_connector):
    # Create a temporary image file to use as an attachment
    file_path = tmp_path / "image.png"
    with open(file_path, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")  # Write minimal PNG header

    service = SciLogMessagingService(connected_connector)
    message = service.new()
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
    assert out.message[0].content_type == "text"
    assert out.message[0].content == "Test message with image attachment"

    # Check attachment part
    assert out.message[1].content_type == "file"
    attachment = out.message[1].content
    assert attachment["filename"] == "image.png"
    assert attachment["mime_type"] == "image/png"
    assert attachment["data"] == b"\x89PNG\r\n\x1a\n"


def test_scilog_messaging_service_add_tags(connected_connector):
    service = SciLogMessagingService(connected_connector)
    message = service.new()
    message.add_text("Test message with tags")
    message.add_tags(["tag1", "tag2"])

    message.send()
    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "scilog"
    assert len(out.message) == 2
    assert out.message[0].content_type == "text"
    assert out.message[0].content == "Test message with tags"
    assert out.message[1].content_type == "tags"
    assert out.message[1].content == ["tag1", "tag2"]
