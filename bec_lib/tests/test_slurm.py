"""Credential bootstrap, rotation, and lifecycle of BEC's optional Slurm manager."""

from __future__ import annotations

import sys
import threading
from types import ModuleType
from unittest import mock

import pytest
from pydantic import ValidationError

from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints, MessageOp
from bec_lib.messages import SlurmCredentialsMessage
from bec_lib.redis_connector import MessageObject
from bec_lib.serialization import MsgpackSerialization, json_ext
from bec_lib.slurm import SlurmClient


def credentials(generation=1, token="first-token", **kwargs):
    return SlurmCredentialsMessage(
        cluster="ra", username="beamline", token=token, generation=generation, **kwargs
    )


@pytest.fixture
def manager_cls(monkeypatch):
    """Provide an optional dependency without requiring Slurm or starting threads."""
    package = ModuleType("slurm_manager")
    module = ModuleType("slurm_manager.slurm_manager")
    module.SlurmManager = mock.Mock()
    monkeypatch.setitem(sys.modules, "slurm_manager", package)
    monkeypatch.setitem(sys.modules, "slurm_manager.slurm_manager", module)
    return module.SlurmManager


@pytest.fixture
def connector():
    connection = mock.Mock()
    connection.get.return_value = None
    return connection


@pytest.mark.parametrize("serializer", [MsgpackSerialization, json_ext])
def test_credentials_contract(serializer):
    message = credentials()
    restored = serializer.loads(serializer.dumps(message))
    assert restored == message
    assert restored.token == "first-token"
    assert "first-token" not in repr(message)
    assert "first-token" not in str(message)
    endpoint = MessageEndpoints.slurm_credentials()
    assert endpoint.endpoint == "info/slurm/credentials"
    assert endpoint.message_type is SlurmCredentialsMessage
    assert endpoint.message_op == MessageOp.SET_PUBLISH


@pytest.mark.parametrize(
    "invalid", [{"token": ""}, {"token": " "}, {"token": "a\nb"}, {"generation": -1}]
)
def test_invalid_credentials(invalid):
    values = credentials().model_dump()
    values.update(invalid)
    with pytest.raises(ValidationError):
        SlurmCredentialsMessage(**values)


def test_bootstrap_is_lazy_and_rotation_preserves_manager(connector, manager_cls):
    connector.get.return_value = credentials()
    client = SlurmClient(connector)
    try:
        manager_cls.for_cluster.assert_not_called()
        manager = client.manager
        manager_cls.for_cluster.assert_called_once_with(
            "ra", username="beamline", token="first-token"
        )
        client._credentials_callback(MessageObject(topic="", value=credentials(2, "rotated")))
        manager.set_credentials.assert_called_once_with(username="beamline", token="rotated")
        assert client.manager is manager
        # A delayed initial read or notification must not revert a rotation.
        client._credentials_callback(MessageObject(topic="", value=credentials()))
        client._credentials_callback(MessageObject(topic="", value=credentials(2, "duplicate")))
        manager.set_credentials.assert_called_once()
    finally:
        client.shutdown()
    manager.shutdown.assert_called_once_with(timeout=None)


def test_subscribe_before_initial_read_does_not_overwrite_newer_callback(connector, manager_cls):
    def register(*, topics, cb):
        cb(MessageObject(topic=topics.endpoint, value=credentials(2, "newer")))

    connector.register.side_effect = register
    connector.get.return_value = credentials(1, "older")
    client = SlurmClient(connector)
    try:
        client.manager
        manager_cls.for_cluster.assert_called_once_with("ra", username="beamline", token="newer")
    finally:
        client.shutdown()


def test_waits_for_credentials_and_uses_latest_before_first_access(connector, manager_cls):
    client = SlurmClient(connector)
    try:
        with pytest.raises(RuntimeError, match="not provided"):
            client.manager
        client._credentials_callback(MessageObject(topic="", value=credentials()))
        client._credentials_callback(MessageObject(topic="", value=credentials(3, "latest")))
        client.manager
        manager_cls.for_cluster.assert_called_once_with("ra", username="beamline", token="latest")
    finally:
        client.shutdown()


def test_optional_dependency_missing_only_fails_when_accessed(connector, monkeypatch):
    monkeypatch.setitem(sys.modules, "slurm_manager.slurm_manager", None)
    connector.get.return_value = credentials()
    client = SlurmClient(connector)
    try:
        with pytest.raises(ImportError, match=r"bec_lib\[slurm\]"):
            client.manager
    finally:
        client.shutdown()


def test_shutdown_unsubscribes_and_ignores_inflight_updates(connector, manager_cls):
    connector.get.return_value = credentials()
    client = SlurmClient(connector)
    manager = client.manager
    client.shutdown()
    client.shutdown()
    client._credentials_callback(MessageObject(topic="", value=credentials(2)))
    connector.unregister.assert_called_once_with(
        topics=MessageEndpoints.slurm_credentials(), cb=client._credentials_callback
    )
    manager.set_credentials.assert_not_called()
    manager.shutdown.assert_called_once_with(timeout=None)
    with pytest.raises(RuntimeError, match="shut down"):
        client.manager


def test_initial_read_failure_unregisters(connector):
    connector.get.side_effect = RuntimeError("read failed")
    with pytest.raises(RuntimeError, match="read failed"):
        SlurmClient(connector)
    connector.unregister.assert_called_once()


def test_username_and_token_rotate_together(connector, manager_cls):
    connector.get.return_value = credentials()
    client = SlurmClient(connector)
    try:
        manager = client.manager
        client._credentials_callback(
            MessageObject(
                topic="",
                value=SlurmCredentialsMessage(
                    cluster="ra", username="next-account", token="next-token", generation=2
                ),
            )
        )
        manager.set_credentials.assert_called_once_with(username="next-account", token="next-token")
    finally:
        client.shutdown()


def test_cluster_change_requires_restart(connector, manager_cls):
    connector.get.return_value = credentials()
    client = SlurmClient(connector)
    try:
        manager = client.manager
        client._credentials_callback(
            MessageObject(
                topic="",
                value=SlurmCredentialsMessage(
                    cluster="sfonline", token="other-token", generation=2
                ),
            )
        )
        manager.set_credentials.assert_not_called()
        assert client.manager is manager
    finally:
        client.shutdown()


def test_bec_client_exposes_manager_and_shuts_it_down_before_connector():
    client = BECClient(forced=True)
    with pytest.raises(RuntimeError, match="Start the BEC client"):
        client.slurm
    helper = mock.Mock()
    client._slurm_client = helper
    assert client.slurm is helper.manager
    order = mock.Mock()
    order.attach_mock(helper.shutdown, "slurm")
    with mock.patch("bec_lib.client.BECService.shutdown") as shutdown:
        order.attach_mock(shutdown, "bec")
        client.shutdown()
    assert order.mock_calls[:2] == [mock.call.slurm(), mock.call.bec(None)]
    assert client._slurm_client is None


def test_redis_publication_rotates_existing_manager(connected_connector, manager_cls):
    endpoint = MessageEndpoints.slurm_credentials()
    connected_connector.set_and_publish(endpoint, credentials())
    client = SlurmClient(connected_connector)
    try:
        manager = client.manager
        updated = threading.Event()
        manager.set_credentials.side_effect = lambda **kwargs: updated.set()
        connected_connector.set_and_publish(endpoint, credentials(2, "redis-rotation"))
        assert updated.wait(timeout=5), "Credential publication was not delivered"
        manager.set_credentials.assert_called_once_with(username="beamline", token="redis-rotation")
        assert client.manager is manager
    finally:
        client.shutdown()


def test_real_manager_uses_rotated_bec_credentials(connected_connector):
    """Exercise the BEC subscription and actual Slurm REST authentication together."""
    pytest.importorskip("slurm_manager.slurm_manager")
    endpoint = MessageEndpoints.slurm_credentials()
    connected_connector.set_and_publish(endpoint, credentials())
    with mock.patch("slurm_manager.slurm_rest_client.build_opener") as build_opener:
        opener = build_opener.return_value
        opener.open.return_value.__enter__.return_value.read.return_value = b'{"jobs": []}'
        client = SlurmClient(connected_connector)
        try:
            manager = client.manager
            manager.rest_client.get_job("123")
            first_request = opener.open.call_args.args[0]
            assert first_request.get_header("X-slurm-user-token") == "first-token"
            updated = threading.Event()
            set_credentials = manager.set_credentials

            def update(**kwargs):
                set_credentials(**kwargs)
                updated.set()

            with mock.patch.object(manager, "set_credentials", side_effect=update):
                connected_connector.set_and_publish(endpoint, credentials(2, "rotated-token"))
                assert updated.wait(timeout=5), "Credential rotation was not delivered"
            manager.rest_client.get_job("123")
            second_request = opener.open.call_args.args[0]
            assert second_request.get_header("X-slurm-user-token") == "rotated-token"
            assert second_request.get_header("X-slurm-user-name") == "beamline"
            assert client.manager is manager
        finally:
            client.shutdown()
