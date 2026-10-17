"""Procedure requests use the authenticated user's personal endpoint."""

from unittest.mock import MagicMock

from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import ProcedureRequestMessage
from bec_lib.procedures.helper import FrontendProcedureHelper


def test_procedure_submission_tracks_current_acl_user():
    conn = MagicMock()
    helper = FrontendProcedureHelper(conn)
    for username in ["alice", "bob", "default"]:
        conn.username = username
        status = helper.request.procedure("sleep", args_kwargs=((), {"time_s": 1}))
        endpoint, message = conn.send.call_args.args
        assert endpoint == MessageEndpoints.procedure_request(username)
        assert endpoint.endpoint == f"personal/{username}/procedures/procedure_request"
        assert message.metadata["client_info"]["acl_user"] == username
        assert message.execution_id == status._id
        assert message.args_kwargs == ((), {"time_s": 1})


def test_submission_replaces_untrusted_acl_identity():
    conn = MagicMock()
    conn.username = "alice"
    helper = FrontendProcedureHelper(conn)
    message = ProcedureRequestMessage(
        identifier="sleep", metadata={"client_info": {"acl_user": "bob"}, "custom": "preserved"}
    )
    helper.request._procedure(message)
    endpoint, submitted = conn.send.call_args.args
    assert endpoint == MessageEndpoints.procedure_request("alice")
    assert submitted.metadata == {"client_info": {"acl_user": "alice"}, "custom": "preserved"}
