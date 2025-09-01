from __future__ import annotations

import sys
import traceback
import uuid
from typing import TYPE_CHECKING, Literal

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector


def upload_script(connector: RedisConnector, script_content: str) -> str:
    """
    Upload a script to the Redis database.

    Args:
        connector (RedisConnector): The Redis connector.
        script_content (str): The content of the script to upload.

    Returns:
        str: The ID of the uploaded script.
    """

    script_id = str(uuid.uuid4())
    connector.set(
        MessageEndpoints.script_content(script_id),
        messages.VariableMessage(value=script_content),
        expire=86400,
    )
    return script_id


class ScriptExecutor:

    def __init__(self, connector: RedisConnector):
        self.connector = connector

    def _get_content(self, script_id: str):
        """
        Get the content of a script from the Redis database.

        Args:
            script_id (str): The ID of the script to retrieve.

        Returns:
            str: The content of the script, or None if not found.
        """
        msg = self.connector.get(MessageEndpoints.script_content(script_id))
        return msg.value if msg else None

    def _send_status(
        self,
        script_id: str,
        status: Literal["running", "completed", "failed", "aborted"],
        current_lines=None,
        tb=None,
    ):
        msg = messages.ScriptExecutionInfoMessage(
            script_id=script_id, status=status, current_lines=current_lines, traceback=tb
        )
        self.connector.send(MessageEndpoints.script_execution_info(script_id), msg)

    def __call__(self, script_id: str):
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
            script_text = self._get_content(script_id)
            if not script_text:
                self._send_status(script_id, "failed")
                return
            self._send_status(script_id, "running")
            # pylint: disable=exec-used
            compiled_code = compile(script_text, f"<script {script_id}>", "exec")
            exec(compiled_code)
        except Exception as e:
            exc_type, exc_value, exc_tb = sys.exc_info()
            tb_frames = traceback.extract_tb(exc_tb)

            # Find frame with <script> and remove everything before it
            script_frame = next(
                (f for f in tb_frames if f.filename == f"<script {script_id}>"), None
            )
            if script_frame:
                tb_frames = tb_frames[tb_frames.index(script_frame) :]

            # Format the remaining traceback
            formatted_tb = "".join(traceback.format_list(tb_frames))
            # Add exception type and message
            formatted_exc = (
                formatted_tb + f"{exc_type.__name__ if exc_type else 'Unknown'}: {exc_value}\n"
            )
            self._send_status(script_id, "failed", tb=formatted_exc)
            raise e
        except KeyboardInterrupt:
            self._send_status(script_id, "aborted")
        else:
            self._send_status(script_id, "completed")
        finally:
            sys.settrace(None)
            self.connector.delete(MessageEndpoints.script_content(script_id))
