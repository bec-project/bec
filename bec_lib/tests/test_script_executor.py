import sys
from unittest import mock

import pytest

from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.script_executor import ScriptExecutor, upload_script


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
