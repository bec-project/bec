import time

import pytest

from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.script_executor import upload_script


def test_upload_script(connected_connector):
    script_content = "print('Hello, World!')"
    script_id = upload_script(connected_connector, script_content)

    # Verify that the script content was uploaded
    uploaded_content = connected_connector.get(MessageEndpoints.script_content(script_id))
    assert uploaded_content.value == script_content


def test_script_executor(connected_connector, capsys):
    script_content = "a = 2; print(a)"
    script_id = upload_script(connected_connector, script_content)

    a = 1

    client = BECClient()
    client.connector = connected_connector
    # Capture stdout
    client._run_script(script_id)
    output = capsys.readouterr().out
    assert "2" in output

    assert a == 1  # The script should not modify the local variable


@pytest.mark.timeout(5)
def test_script_executor_failure(connected_connector):
    script_content = "print(unknown_variable)"
    script_id = upload_script(connected_connector, script_content)

    received_data = []

    def update_received_data(msg):
        msg = msg.value
        if msg.status == "failed":
            received_data.append(msg)

    client = BECClient()
    client.connector = connected_connector
    client.connector.register(
        MessageEndpoints.script_execution_info(script_id), cb=update_received_data
    )

    try:
        client._run_script(script_id)
    except Exception as e:
        pass

    while not received_data:
        time.sleep(0.1)

    assert received_data[0].status == "failed"
    assert "NameError" in received_data[0].traceback
    assert "exec" not in received_data[0].traceback
