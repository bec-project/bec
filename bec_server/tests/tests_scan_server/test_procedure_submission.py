"""Validate procedure submission identities at the personal endpoint boundary."""

from unittest.mock import MagicMock, patch

import pytest

from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import ProcedureRequestMessage
from bec_lib.redis_connector import MessageObject
from bec_server.procedures.manager import ProcedureManager, ProcedureManagerBase
from bec_server.procedures.subprocess_worker import SubProcessWorker


@pytest.fixture
def manager():
    instance = ProcedureManager.__new__(ProcedureManager)
    instance._ack = MagicMock()
    instance._process_queue_request = MagicMock()
    return instance


@pytest.mark.parametrize("username", ["alice", "bob", "default"])
def test_valid_user_reaches_procedure_manager(manager, username):
    message = ProcedureRequestMessage(
        identifier="sleep", metadata={"client_info": {"acl_user": username}}
    )
    request = MessageObject(
        topic=MessageEndpoints.procedure_request(username).endpoint, value=message
    )
    manager._procedure_request_callback(request)
    manager._process_queue_request.assert_called_once_with(request, token=None)
    manager._ack.assert_not_called()


@pytest.mark.parametrize(
    "topic, metadata, error",
    [
        (
            MessageEndpoints.procedure_request("alice").endpoint,
            {"client_info": {"acl_user": "bob"}},
            "Username in topic does not match client info.",
        ),
        (
            MessageEndpoints.procedure_request("alice").endpoint,
            {},
            "Missing client info in request metadata.",
        ),
        (
            MessageEndpoints.procedure_request("alice").endpoint,
            {"client_info": "alice"},
            "Missing client info in request metadata.",
        ),
        (
            MessageEndpoints.procedure_request("alice").endpoint,
            {"client_info": {"acl_user": None}},
            "Username in topic does not match client info.",
        ),
        (
            "user/procedures/procedure_request",
            {"client_info": {"acl_user": "alice"}},
            "Could not extract username from topic.",
        ),
        (
            MessageEndpoints.procedure_request("alice/extra").endpoint,
            {"client_info": {"acl_user": "alice"}},
            "Could not extract username from topic.",
        ),
        (
            MessageEndpoints.procedure_request("alice").endpoint + "/extra",
            {"client_info": {"acl_user": "alice"}},
            "Could not extract username from topic.",
        ),
    ],
)
def test_invalid_identity_is_rejected(manager, topic, metadata, error):
    message = ProcedureRequestMessage(identifier="sleep", metadata=metadata)
    manager._procedure_request_callback(MessageObject(topic=topic, value=message))
    manager._ack.assert_called_once_with(False, error, message.execution_id)
    manager._process_queue_request.assert_not_called()


def test_default_user_without_acl_username(manager):
    message = ProcedureRequestMessage(
        identifier="sleep", metadata={"client_info": {"acl_user": None}}
    )
    request = MessageObject(
        topic=MessageEndpoints.procedure_request("default").endpoint, value=message
    )
    manager._procedure_request_callback(request)
    manager._process_queue_request.assert_called_once_with(request, token=None)


def test_execution_does_not_forward_private_submission_metadata(manager):
    manager._helper = MagicMock()
    manager._active_workers = {}
    message = ProcedureRequestMessage(
        identifier="sleep",
        metadata={"client_info": {"acl_user": "alice"}, "private": "request-only"},
    )
    execution = manager._respond_to_valid_request(message)
    assert execution.metadata == {}
    assert message.metadata["client_info"]["acl_user"] == "alice"
    assert message.metadata["private"] == "request-only"
    manager._helper.push.exec.assert_not_called()


def test_private_submission_metadata_is_not_logged(manager):
    message = ProcedureRequestMessage(identifier="sleep", metadata={"private": "request-only"})
    manager._validate_request = MagicMock(return_value=None)
    request = MessageObject(
        topic=MessageEndpoints.procedure_request("alice").endpoint, value=message
    )
    with patch("bec_server.procedures.manager.logger") as logger:
        ProcedureManagerBase._process_queue_request(manager, request)
    assert logger.debug.called
    assert "request-only" not in repr(logger.mock_calls)


def test_manager_subscribes_only_to_personal_requests():
    with (
        patch("bec_server.procedures.manager.RedisConnector") as connector,
        patch("bec_server.procedures.queue_manager.BackendProcedureHelper"),
    ):
        connector().host = "localhost"
        connector().port = 6379
        instance = ProcedureManager("localhost:6379", SubProcessWorker)
        try:
            connector().register.assert_any_call(
                patterns=MessageEndpoints.procedure_request("*"),
                cb=instance._procedure_request_callback,
            )
        finally:
            instance.shutdown()
        connector().unregister.assert_any_call(
            patterns=MessageEndpoints.procedure_request("*"),
            cb=instance._procedure_request_callback,
        )
