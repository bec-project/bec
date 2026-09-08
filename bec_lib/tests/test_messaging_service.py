import time
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messaging_hooks import MessagingEvent, MessagingManager
from bec_lib.messaging_services import (
    MessageServiceObject,
    NotificationMessageObject,
    SciLogMessagingService,
    SciLogTable,
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
    service._on_new_scope_change_msg(message={"data": available_services})
    yield service


@pytest.fixture
def scilog_service_with_owner(connected_connector):
    owner = SimpleNamespace(
        device_manager=SimpleNamespace(
            devices=SimpleNamespace(
                _position_rows=mock.Mock(
                    return_value=[
                        {
                            "name": "samx",
                            "readback": "1.0000",
                            "setpoint": "1.5000",
                            "limits": "[]",
                        },
                        {
                            "name": "samy",
                            "readback": "2.0000",
                            "setpoint": "2.5000",
                            "limits": "[-1, 1]",
                        },
                    ]
                )
            )
        )
    )
    service = SciLogMessagingService(connected_connector, client=owner)
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
    service._on_new_scope_change_msg(message={"data": available_services})
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
    service._on_new_scope_change_msg(message={"data": available_services})
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
    assert len(out.message) == 2
    assert isinstance(out.message[0], messages.MessagingServiceTextContent)
    assert out.message[0].content == "Test message"

    tags = out.message[1]
    assert isinstance(tags, messages.MessagingServiceTagsContent)
    assert tags.tags == ["bec"]  # default tag should be included


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
    assert len(out.message) == 3

    # Check text part
    text = out.message[0]
    assert isinstance(text, messages.MessagingServiceTextContent)
    assert text.content == "Test message with attachment"

    # Check attachment part
    attachment = out.message[1]
    assert isinstance(attachment, messages.MessagingServiceFileContent)

    tags = out.message[2]
    assert isinstance(tags, messages.MessagingServiceTagsContent)
    assert tags.tags == ["bec"]  # default tag should be included

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
    assert len(out.message) == 3

    # Check text part
    assert isinstance(out.message[0], messages.MessagingServiceTextContent)
    assert out.message[0].content == "Test message with image attachment"

    # Check attachment part
    attachment = out.message[1]
    assert isinstance(attachment, messages.MessagingServiceFileContent)

    tags = out.message[2]
    assert isinstance(tags, messages.MessagingServiceTagsContent)
    assert tags.tags == ["bec"]  # default tag should be included

    assert attachment.filename == "image.png"
    assert attachment.mime_type == "image/png"
    assert attachment.data == b"\x89PNG\r\n\x1a\n"


def test_messaging_service_attachment_raises_if_too_large(scilog_message, tmp_path):
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


def test_scilog_custom_table(scilog_message, connected_connector):
    scilog_message.add_text("Before table")
    table = scilog_message.add_table(columns=["Quantity"], title="Custom snapshot")
    table.add_column("Value")
    scilog_message.add_text("After table")

    # Readbacks and derived values are captured as the table is populated.
    x, y = 3.0, 4.0
    assert table.add_row("samx", f"{x:.4f}") is table
    table.add_row("samy", f"{y:.4f}")
    table.add_row("Calculated radius", f"{(x**2 + y**2) ** 0.5:.4f}")
    scilog_message.add_tags("snapshot")
    scilog_message.send(scope="default")

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    message = out[0]["data"]
    assert message.service_name == "scilog"
    assert message.scope == "default"
    assert len(message.message) == 4
    assert message.message[0].content == "Before table"
    assert message.message[2].content == "After table"
    assert sorted(message.message[3].tags) == ["bec", "snapshot"]
    content = message.message[1]
    assert isinstance(content, messages.MessagingServiceTextContent)
    assert content.content == (
        '<p>Custom snapshot</p><figure class="table"><table><tbody>'
        "<tr><td><strong>Quantity</strong></td><td><strong>Value</strong></td></tr>"
        "<tr><td>samx</td><td>3.0000</td></tr>"
        "<tr><td>samy</td><td>4.0000</td></tr>"
        "<tr><td>Calculated radius</td><td>5.0000</td></tr>"
        "</tbody></table></figure>"
    )


def test_scilog_table_escapes_text():
    table = SciLogTable(columns=["Name <&>", "Value"], title='Sample "A" < B')
    table.add_row("<script>alert('x')</script>", "first\nsecond & third")

    content = table.to_html()
    assert "<p>Sample &quot;A&quot; &lt; B</p>" in content
    assert "<strong>Name &lt;&amp;&gt;</strong>" in content
    assert "<td>&lt;script&gt;alert(&#x27;x&#x27;)&lt;/script&gt;</td>" in content
    assert "<td>first<br>second &amp; third</td>" in content


def test_scilog_table_preserves_cell_text():
    table = SciLogTable()
    assert table.add_column("Text") is table
    table.add_column("Number")
    table.add_row("empty", "")
    table.add_row("zero", "0.00")
    table.add_row("rounded", f"{1.234:.2f}")

    content = table.to_html()
    assert "<td>empty</td><td></td>" in content
    assert "<td>zero</td><td>0.00</td>" in content
    assert "<td>rounded</td><td>1.23</td>" in content


@pytest.mark.parametrize("values", [(), ("1",), ("1", "2", "3")])
def test_scilog_table_rejects_incorrect_row_width(values):
    table = SciLogTable(columns=["A", "B"])
    before = table.to_html()

    with pytest.raises(ValueError, match=f"Expected 2 values, got {len(values)}"):
        table.add_row(*values)

    assert table.to_html() == before


def test_scilog_table_requires_columns_before_rows():
    table = SciLogTable()
    with pytest.raises(ValueError, match="Add columns before adding rows"):
        table.add_row("1")

    table.add_column("A").add_row("1")
    before = table.to_html()
    with pytest.raises(ValueError, match="Add all columns before adding rows"):
        table.add_column("B")
    assert table.to_html() == before


@pytest.mark.parametrize("header", [123, None, ["Readback"]])
def test_scilog_table_rejects_invalid_headers_without_mutating(header):
    table = SciLogTable(columns=["Device"])
    before = table.to_html()
    with pytest.raises(TypeError, match="Column header must be a string"):
        table.add_column(header)
    assert table.to_html() == before
    table.add_column("Readback")
    assert "<td><strong>Readback</strong></td>" in table.to_html()
    table.add_row("samx", "1.25")
    assert "<td>samx</td><td>1.25</td>" in table.to_html()


@pytest.mark.parametrize("columns", ["Device", ("Device",), ["Device", 1], {"Device": "Readback"}])
def test_scilog_table_rejects_invalid_columns(scilog_message, columns):
    with pytest.raises(TypeError, match="columns must be a list of strings or None"):
        scilog_message.add_table(columns=columns)
    assert scilog_message._content == []


@pytest.mark.parametrize("title", [123, 0, False, [], ["Snapshot"]])
def test_scilog_table_rejects_invalid_titles_without_blocking_message(
    scilog_message, connected_connector, title
):
    scilog_message.add_text("Existing note")
    table = scilog_message.add_table(columns=["Device"])
    table.add_row("samx")

    with pytest.raises(TypeError, match="title must be a string or None"):
        scilog_message.add_table(columns=["Invalid table"], title=title)

    table.add_row("samy")
    scilog_message.add_text("Summary")
    scilog_message.send()

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    content = out[0]["data"].message
    assert len(content) == 4
    assert content[0].content == "Existing note"
    assert "<tr><td>samx</td></tr><tr><td>samy</td></tr>" in content[1].content
    assert content[2].content == "Summary"
    assert content[3].tags == ["bec"]


@pytest.mark.parametrize("value", [1, 1.25, None, False, ["text"]])
def test_scilog_table_rejects_non_string_cells(value):
    table = SciLogTable(columns=["Device", "Readback"])
    table.add_row("samx", "1.25")
    before = table.to_html()

    with pytest.raises(TypeError, match="Row values must be strings"):
        table.add_row("samy", value)

    assert table.to_html() == before


def test_scilog_table_copies_column_headings(scilog_message, connected_connector):
    columns = ["Device", "Readback"]
    table = scilog_message.add_table(columns=columns)
    columns[0] = "Changed"
    columns.append("Extra")
    table.add_row("samx", "1.25")
    scilog_message.send()

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    content = out[0]["data"].message[0].content
    assert "<td><strong>Device</strong></td><td><strong>Readback</strong></td>" in content
    assert "<td>samx</td><td>1.25</td>" in content
    assert "Changed" not in content
    assert "Extra" not in content


def test_scilog_tables_are_independent_and_keep_default_tags(scilog_message, connected_connector):
    first = scilog_message.add_table(columns=["First"])
    second = scilog_message.add_table(columns=["Second"])
    second.add_row("two")
    first.add_row("one")
    scilog_message.send()
    first.add_row("three")
    scilog_message.send()

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    first_send, second_send = [entry["data"] for entry in out]
    assert "<td>one</td>" in first_send.message[0].content
    assert "<td>three</td>" not in first_send.message[0].content
    assert "<td>three</td>" in second_send.message[0].content
    assert first_send.message[1] == second_send.message[1]
    assert "<td>two</td>" in first_send.message[1].content
    for message in (first_send, second_send):
        assert len(message.message) == 3
        assert message.message[2].tags == ["bec"]


@pytest.mark.parametrize("direct_service_send", [False, True])
def test_scilog_table_renders_once_when_sent(
    scilog_service, scilog_message, connected_connector, direct_service_send
):
    table = scilog_message.add_table(columns=["Device", "Readback"])
    with mock.patch.object(table, "_update_content", wraps=table._update_content) as render:
        for index in range(1000):
            table.add_row(f"motor_{index}", "1.25")
        render.assert_not_called()

        if direct_service_send:
            scilog_service.send(scilog_message)
        else:
            scilog_message.send()
        render.assert_called_once()
        table.to_html()
        render.assert_called_once()

        table.add_row("extra", "2.50")
        render.assert_called_once()
        assert "<td>extra</td><td>2.50</td>" in table.to_html()
        assert render.call_count == 2

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    content = out[0]["data"].message[0].content
    assert "<td>motor_999</td><td>1.25</td>" in content
    assert "<td>extra</td>" not in content


def test_scilog_notification_table_is_rendered_before_routing(scilog_service, connected_connector):
    notification = NotificationMessageObject()
    table = notification.add_table(columns=["Device", "Readback"])
    table.add_row("samx", "1.25")
    manager = MessagingManager(connected_connector)
    try:
        routed = manager.to_service_message(
            scilog_service,
            messages.NotificationMessage(event="new_scan", message=notification._content),
        )
        routed.send()
    finally:
        manager.shutdown()

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    assert "<td>samx</td><td>1.25</td>" in out[0]["data"].message[0].content


def test_scilog_log_positions(scilog_service_with_owner, connected_connector):
    scilog_service_with_owner.log_positions(
        devices="sam*", title="Current positions", tags="snapshot"
    )

    scilog_service_with_owner._client.device_manager.devices._position_rows.assert_called_once_with(  # type: ignore[attr-defined]  # pylint: disable=protected-access
        "sam*"
    )

    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "scilog"
    assert len(out.message) == 2
    assert isinstance(out.message[0], messages.MessagingServiceTextContent)
    assert "<p>Current positions</p>" in out.message[0].content
    assert '<figure class="table"><table><tbody>' in out.message[0].content
    assert "<strong>device</strong>" in out.message[0].content
    assert "<td>samx</td><td>1.0000</td><td>1.5000</td><td>[]</td>" in out.message[0].content
    assert "<td>samy</td><td>2.0000</td><td>2.5000</td><td>[-1, 1]</td>" in out.message[0].content
    assert isinstance(out.message[1], messages.MessagingServiceTagsContent)
    assert sorted(out.message[1].tags) == ["bec", "snapshot"]


def test_scilog_log_positions_requires_owner(scilog_service):
    with pytest.raises(
        RuntimeError, match="SciLog position logging requires a client-backed messaging service."
    ):
        scilog_service.log_positions()


def test_scilog_log_code(connected_connector):
    def my_func():
        print()

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
    service._on_new_scope_change_msg(message={"data": available_services})

    service.log_code(my_func, title="Function source", tags="code")

    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "scilog"
    assert len(out.message) == 2
    assert isinstance(out.message[0], messages.MessagingServiceTextContent)
    assert "<p>Function source</p>" in out.message[0].content
    assert '<pre><code class="language-python">' in out.message[0].content
    assert "def my_func():" in out.message[0].content
    assert "    print()" in out.message[0].content
    assert isinstance(out.message[1], messages.MessagingServiceTagsContent)
    assert sorted(out.message[1].tags) == ["bec", "code"]


def test_scilog_log_code_raises_for_missing_source(scilog_service):
    with pytest.raises(
        ValueError,
        match="Could not extract source code. Pass a function with available source or a source string.",
    ):
        scilog_service.log_code(len)


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
    service._on_new_scope_change_msg(message={"data": available_services})

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
    service._on_new_scope_change_msg(message={"data": available_services})
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
    service._on_new_scope_change_msg(message={"data": disabled_services})

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


def test_notification_message_object_to_scilog_message(scilog_service):
    scilog_service.set_default_scope("default")
    notification = (
        NotificationMessageObject()
        .add_text("Beamline checks failed", bold=True, color="red")
        .add_tags(["alarm"])
    )
    manager = MessagingManager(scilog_service._redis_connector)  # pylint: disable=protected-access

    try:
        scilog_message = manager.to_service_message(
            scilog_service,
            messages.NotificationMessage(event="alarm_major", message=notification._content),
        )
    finally:
        manager.shutdown()

    assert scilog_message._service == scilog_service  # pylint: disable=protected-access
    assert scilog_message._scope == "default"  # pylint: disable=protected-access
    assert len(scilog_message._content) == 2  # pylint: disable=protected-access
    assert isinstance(
        scilog_message._content[0], messages.MessagingServiceTextContent
    )  # pylint: disable=protected-access
    assert (
        scilog_message._content[0].content  # pylint: disable=protected-access
        == '<p><mark class="pen-red"><strong>Beamline checks failed</strong></mark></p>'
    )
    assert isinstance(
        scilog_message._content[1], messages.MessagingServiceTagsContent
    )  # pylint: disable=protected-access


def test_notification_message_object_to_signal_message(signal_service):
    signal_service.set_default_scope("default")
    notification = (
        NotificationMessageObject()
        .add_text("Beamline checks failed", bold=True, color="red")
        .add_tags(["alarm"])
    )
    manager = MessagingManager(signal_service._redis_connector)  # pylint: disable=protected-access

    try:
        signal_message = manager.to_service_message(
            signal_service,
            messages.NotificationMessage(event="alarm_major", message=notification._content),
        )
    finally:
        manager.shutdown()

    assert signal_message._service == signal_service  # pylint: disable=protected-access
    assert signal_message._scope == "default"  # pylint: disable=protected-access
    assert len(signal_message._content) == 1  # pylint: disable=protected-access
    assert isinstance(
        signal_message._content[0], messages.MessagingServiceTextContent
    )  # pylint: disable=protected-access
    assert (
        signal_message._content[0].content == "Beamline checks failed"
    )  # pylint: disable=protected-access


def test_signal_service_can_create_message_without_configured_scopes(connected_connector):
    """Test that Signal remains enabled even without predefined scopes."""
    service = SignalMessagingService(connected_connector)
    available_services = messages.AvailableMessagingServicesMessage(
        config=messages.MessagingConfig(
            signal=messages.MessagingServiceScopeConfig(enabled=True),
            teams=messages.MessagingServiceScopeConfig(enabled=False),
            scilog=messages.MessagingServiceScopeConfig(enabled=False),
        ),
        deployment_services=[],
        session_services=[],
    )
    service._on_new_scope_change_msg(message={"data": available_services})

    message = service.new("Direct Signal message")
    message.send(scope="+41791234567")
    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.service_name == "signal"
    assert out.scope == "+41791234567"


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
    service._on_new_scope_change_msg(message={"data": available_services})

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


@pytest.mark.parametrize(
    ("scope", "expected_scope"),
    [
        ("079 123 45 67", "+41791234567"),
        ("0041 79 123 45 67", "+41791234567"),
        ("beamline-ops", "beamline-ops"),
        (["079 123 45 67", "beamline-ops"], ["+41791234567", "beamline-ops"]),
        ("+41791234567", "+41791234567"),
    ],
)
def test_signal_message_service_normalizes_scope_inputs(
    signal_service, connected_connector, scope, expected_scope
):
    """Test that Signal normalizes phone numbers while preserving named scopes."""
    message = signal_service.new("Signal recipient test")

    message.send(scope=scope)
    out = connected_connector.xread(
        MessageEndpoints.message_service_queue(), from_start=True, count=1
    )
    assert len(out) == 1
    out = out[0]["data"]
    assert out.scope == expected_scope


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


def test_scilog_default_tags_added_on_send_without_explicit_tags(
    scilog_message, connected_connector
):
    """Test that default tags are automatically added before sending when no tags were explicitly set."""
    scilog_message._service.set_default_tags(["bec", "auto_tag"])  # type: ignore

    message = scilog_message
    message.add_text("Test message without explicit tags")
    # Deliberately do NOT call add_tags - default tags should be injected by send()
    message.send()

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    assert len(out) == 1
    out = out[0]["data"]

    # The message should contain text + tags even though add_tags was never called
    assert len(out.message) == 2
    text_part = out.message[0]
    tags_part = out.message[1]
    assert isinstance(text_part, messages.MessagingServiceTextContent)
    assert isinstance(tags_part, messages.MessagingServiceTagsContent)
    assert sorted(tags_part.tags) == sorted(["bec", "auto_tag"])


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


def test_scilog_add_text_no_formatting(scilog_message):
    scilog_message.add_text("plain")
    assert scilog_message._content[0].content == "plain"


def test_scilog_add_text_bold(scilog_message):
    scilog_message.add_text("hello", bold=True)
    assert scilog_message._content[0].content == "<p><strong>hello</strong></p>"


def test_scilog_add_text_italic(scilog_message):
    scilog_message.add_text("hello", italic=True)
    assert scilog_message._content[0].content == "<p><em>hello</em></p>"


@pytest.mark.parametrize(
    ("color", "expected_content"),
    [
        ("red", '<p><mark class="pen-red">warn</mark></p>'),
        ("green", '<p><mark class="pen-green">warn</mark></p>'),
        ("yellow", '<p><mark class="marker-yellow">warn</mark></p>'),
        ("pink", '<p><mark class="marker-pink">warn</mark></p>'),
        ("blue", '<p><mark class="marker-blue">warn</mark></p>'),
        ("black", "<p>warn</p>"),
    ],
)
def test_scilog_add_text_color(scilog_message, color, expected_content):
    scilog_message.add_text("warn", color=color)
    assert scilog_message._content[0].content == expected_content


def test_scilog_add_text_bold_and_color(scilog_message, connected_connector):
    """bold + color produces the expected nested HTML and round-trips through redis."""
    scilog_message.add_text("Beamline checks failed", bold=True, color="red")
    scilog_message.send()

    out = connected_connector.xread(MessageEndpoints.message_service_queue(), from_start=True)
    text_part = out[0]["data"].message[0]
    assert isinstance(text_part, messages.MessagingServiceTextContent)
    assert (
        text_part.content
        == '<p><mark class="pen-red"><strong>Beamline checks failed</strong></mark></p>'
    )


def test_set_auto_notifications_persists_notification_config(scilog_service, connected_connector):
    scilog_service.set_auto_notifications(MessagingEvent.SCAN, enabled=True, scopes="default")

    config_msg = connected_connector.get(MessageEndpoints.notification_config())
    assert config_msg == messages.NotificationConfigMessage(
        routes={
            "new_scan": [messages.NotificationServiceTarget(service_name="scilog", scope="default")]
        }
    )
    assert scilog_service._auto_notifications == {
        "new_scan": ["default"]
    }  # pylint: disable=protected-access


def test_set_auto_notifications_merges_with_existing_routes(scilog_service, connected_connector):
    connected_connector.set_and_publish(
        MessageEndpoints.notification_config(),
        messages.NotificationConfigMessage(
            routes={
                "new_scan": [
                    messages.NotificationServiceTarget(service_name="signal", scope="beamline-ops")
                ]
            }
        ),
    )

    scilog_service.set_auto_notifications(MessagingEvent.SCAN, enabled=True, scopes="default")

    config_msg = connected_connector.get(MessageEndpoints.notification_config())
    assert config_msg == messages.NotificationConfigMessage(
        routes={
            "new_scan": [
                messages.NotificationServiceTarget(service_name="signal", scope="beamline-ops"),
                messages.NotificationServiceTarget(service_name="scilog", scope="default"),
            ]
        }
    )
    assert scilog_service._auto_notifications == {
        "new_scan": ["default"]
    }  # pylint: disable=protected-access


def test_set_auto_notifications_disable_removes_only_matching_service_scope(
    scilog_service, connected_connector
):
    connected_connector.set_and_publish(
        MessageEndpoints.notification_config(),
        messages.NotificationConfigMessage(
            routes={
                "new_scan": [
                    messages.NotificationServiceTarget(service_name="signal", scope="beamline-ops"),
                    messages.NotificationServiceTarget(service_name="scilog", scope="default"),
                ]
            }
        ),
    )

    scilog_service.set_auto_notifications(MessagingEvent.SCAN, enabled=False, scopes="default")

    config_msg = connected_connector.get(MessageEndpoints.notification_config())
    assert config_msg == messages.NotificationConfigMessage(
        routes={
            "new_scan": [
                messages.NotificationServiceTarget(service_name="signal", scope="beamline-ops")
            ]
        }
    )
    assert scilog_service._auto_notifications == {}  # pylint: disable=protected-access


def test_set_auto_notifications_uses_default_scope_when_scopes_omitted(
    scilog_service, connected_connector
):
    scilog_service.set_default_scope("default")

    scilog_service.set_auto_notifications(MessagingEvent.SCAN, enabled=True)

    config_msg = connected_connector.get(MessageEndpoints.notification_config())
    assert config_msg == messages.NotificationConfigMessage(
        routes={
            "new_scan": [messages.NotificationServiceTarget(service_name="scilog", scope="default")]
        }
    )
    assert scilog_service._auto_notifications == {
        "new_scan": ["default"]
    }  # pylint: disable=protected-access


def test_messaging_service_tracks_external_notification_config_updates(
    scilog_service, connected_connector
):
    connected_connector.set_and_publish(
        MessageEndpoints.notification_config(),
        messages.NotificationConfigMessage(
            routes={
                "new_scan": [
                    messages.NotificationServiceTarget(service_name="signal", scope="beamline-ops"),
                    messages.NotificationServiceTarget(service_name="scilog", scope="default"),
                ],
                "alarm_major": [
                    messages.NotificationServiceTarget(
                        service_name="scilog", scope=["default", "secondary"]
                    )
                ],
            }
        ),
    )

    deadline = time.time() + 1
    while (
        time.time() < deadline
        and scilog_service._auto_notifications  # pylint: disable=protected-access
        != {"new_scan": ["default"], "alarm_major": ["default", "secondary"]}
    ):
        time.sleep(0.01)

    assert scilog_service._auto_notifications == {  # pylint: disable=protected-access
        "new_scan": ["default"],
        "alarm_major": ["default", "secondary"],
    }


def test_messaging_service_loads_notification_config_on_init(connected_connector):
    connected_connector.set_and_publish(
        MessageEndpoints.notification_config(),
        messages.NotificationConfigMessage(
            routes={
                "new_scan": [
                    messages.NotificationServiceTarget(service_name="scilog", scope="default")
                ]
            }
        ),
    )

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
    service._on_new_scope_change_msg(message={"data": available_services})

    assert service._auto_notifications == {
        "new_scan": ["default"]
    }  # pylint: disable=protected-access
