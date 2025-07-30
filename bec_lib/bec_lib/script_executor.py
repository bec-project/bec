from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Literal

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib.client import BECClient


class ScriptExecutor:

    def __init__(self, client: BECClient):
        self.client = client

    def _send_status(
        self, script_id: str, status: Literal["running", "completed", "failed"], current_lines=None
    ):
        msg = messages.ScriptExecutionInfoMessage(
            script_id=script_id, status=status, current_lines=current_lines
        )
        self.client.connector.send(MessageEndpoints.script_execution_info(script_id), msg)

    def __call__(self, script_id: str, script_text: str):
        def tracer(frame, event, arg):
            if event != "line":
                return tracer
            filename = frame.f_code.co_filename
            # Filter on typical dynamic code filenames:
            if filename == f"<script {script_id}>":
                self._send_status(script_id, "running", current_lines=[frame.f_lineno])
            return tracer

        sys.settrace(tracer)
        try:
            self._send_status(script_id, "running")
            # pylint: disable=exec-used
            compiled_code = compile(script_text, f"<script {script_id}>", "exec")
            exec(compiled_code)
        except Exception as e:
            self._send_status(script_id, "failed")
            raise e
        else:
            self._send_status(script_id, "completed")
        finally:
            sys.settrace(None)
