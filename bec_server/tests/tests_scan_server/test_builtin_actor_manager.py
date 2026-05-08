from threading import Event
from unittest.mock import MagicMock, patch

import pytest

from bec_server.actors.builtin_actor_manager import BuiltinActorManager


class DummyActor:
    def __init__(self, client, name, exec_id):
        self.client = client
        self.name = name
        self.exec_id = exec_id
        self.stop_event = Event()

    def run(self):
        pass


@pytest.fixture
def mocked_manager():
    with (
        patch("bec_server.actors.builtin_actor_manager.BECClient") as mock_client_cls,
        patch.object(BuiltinActorManager, "_start_all"),
    ):
        mock_client = MagicMock()
        mock_client.connector = MagicMock()
        mock_client.builtin_actors = MagicMock()
        mock_client_cls.return_value = mock_client

        manager = BuiltinActorManager("localhost:6379")
        yield manager, mock_client


def test_init_registers_callback(mocked_manager):
    manager, mock_client = mocked_manager

    mock_client.start.assert_called_once()

    mock_client.connector.register.assert_called_once()
    args, kwargs = mock_client.connector.register.call_args

    assert "cb" in kwargs
    assert kwargs["cb"] == manager._on_state_changed


def test_start_actor_starts_thread(mocked_manager):
    manager, _ = mocked_manager

    with patch("bec_server.actors.builtin_actor_manager.Thread") as mock_thread_cls:
        mock_thread = MagicMock()
        mock_thread_cls.return_value = mock_thread

        manager._start_actor(DummyActor)

        assert "DummyActor" in manager._actors_threads_and_stops

        actor, thread, stop_event = manager._actors_threads_and_stops["DummyActor"]

        assert isinstance(actor, DummyActor)
        assert thread == mock_thread
        assert stop_event == actor.stop_event

        mock_thread.start.assert_called_once()


def test_start_actor_does_not_duplicate(mocked_manager):
    manager, _ = mocked_manager

    with patch("bec_server.actors.builtin_actor_manager.Thread"):
        manager._start_actor(DummyActor)
        manager._start_actor(DummyActor)

    assert len(manager._actors_threads_and_stops) == 1


def test_stop_actor_sets_event_and_joins(mocked_manager):
    manager, _ = mocked_manager

    actor = DummyActor(None, "DummyActor", "DummyActor")
    mock_thread = MagicMock()

    manager._actors_threads_and_stops["DummyActor"] = (actor, mock_thread, actor.stop_event)

    manager._stop_actor("DummyActor")

    assert actor.stop_event.is_set()
    mock_thread.join.assert_called_once()


def test_stop_actor_missing_is_noop(mocked_manager):
    manager, _ = mocked_manager

    # Should not raise
    manager._stop_actor("MissingActor")


def test_on_state_changed_starts_enabled_actor(mocked_manager):
    manager, mock_client = mocked_manager

    manager._builtin_actors = {"DummyActor": DummyActor}

    msg = MagicMock()
    msg.value.actor_name = "DummyActor"

    mock_client.builtin_actors.check_enabled.return_value = True

    with patch.object(manager, "_start_actor") as mock_start:
        manager._on_state_changed(msg)

    mock_start.assert_called_once_with(DummyActor)


def test_on_state_changed_unknown_actor(mocked_manager):
    manager, _ = mocked_manager

    msg = MagicMock()
    msg.value.actor_name = "UnknownActor"

    with (
        patch.object(manager, "_start_actor") as mock_start,
        patch.object(manager, "_stop_actor") as mock_stop,
    ):
        manager._on_state_changed(msg)

    mock_start.assert_not_called()
    mock_stop.assert_not_called()


def test_shutdown_stops_all_and_shuts_down_client(mocked_manager):
    manager, mock_client = mocked_manager

    manager._actors_threads_and_stops = {
        "Actor1": (MagicMock(), MagicMock(), Event()),
        "Actor2": (MagicMock(), MagicMock(), Event()),
    }

    with patch.object(manager, "_stop_actor") as mock_stop:
        manager.shutdown()

    mock_stop.assert_any_call("Actor1")
    mock_stop.assert_any_call("Actor2")
    assert mock_stop.call_count == 2

    mock_client.shutdown.assert_called_once()
