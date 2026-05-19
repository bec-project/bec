import threading
from unittest.mock import MagicMock, patch

import pytest

from bec_server.actors.scan_interlock import ScanInterlockActor


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.connector = MagicMock()
    client.connector.get.return_value = None
    client.queue = MagicMock()
    return client


@pytest.fixture
def actor(mock_client):

    actor = ScanInterlockActor(
        client=mock_client, name="ScanInterlockActor", exec_id="ScanInterlockActor"
    )

    actor.client = mock_client
    actor.state_table = {}
    actor.state_cache = {}
    actor.state_table_lock = threading.RLock()

    return actor


class TestScanInterlockActor:
    def test_update_watched_states_in_redis(self, actor, mock_client):
        mock_client.connector.set.reset_mock()
        with patch(
            "bec_server.actors.scan_interlock.ScanInterlockStateTableContent"
        ) as mock_content:
            actor._update_watched_states_in_redis()

        mock_client.connector.set.assert_called_once()
        mock_content.assert_called_once_with(states_watched=actor.state_table)

    def test_on_state_modification_remove_all(self, actor, mock_client):
        actor.state_table = {"beam_ok": "valid", "vacuum_ok": "valid"}
        actor.state_cache = {"beam_ok": "valid", "vacuum_ok": "valid"}
        actor._update_cache = MagicMock()

        msg = MagicMock()
        msg.action = "remove_all"
        msg.state_name = None
        msg.status = None

        with patch("bec_server.actors.actor.BlStateActor.evaluate"):
            actor._on_state_modification({"data": msg})

        assert actor.state_table == {}
        assert actor.state_cache == {}

        assert mock_client.connector.unregister.call_count == 2

    def test_on_state_modification_add(self, actor, mock_client):
        actor._update_cache = MagicMock()
        mock_client.connector.register.reset_mock()
        msg = MagicMock(action="add", state_name="beam_ok", status="valid")
        actor._on_state_modification({"data": msg})
        assert actor.state_table["beam_ok"] == "valid"
        mock_client.connector.register.assert_called_once()
        actor._update_cache.assert_called_once()

    def test_on_state_modification_remove(self, actor, mock_client):
        actor.state_table["beam_ok"] = "valid"
        actor.state_cache["beam_ok"] = "valid"
        actor._update_cache = MagicMock()

        msg = MagicMock(action="remove", state_name="beam_ok", status=None)

        with patch("bec_server.actors.actor.BlStateActor.evaluate"):
            actor._on_state_modification({"data": msg})

        assert "beam_ok" not in actor.state_table
        assert "beam_ok" not in actor.state_cache

        mock_client.connector.unregister.assert_called_once()

    def test_on_state_modification_remove_missing(self, actor, mock_client):
        actor._update_cache = MagicMock()
        msg = MagicMock(action="remove", state_name="missing", status=None)

        with patch("bec_server.actors.actor.BlStateActor.evaluate"):
            actor._on_state_modification({"data": msg})

        mock_client.connector.unregister.assert_not_called()

    def test_some_mismatch_action_adds_lock(self, actor, mock_client):
        actor.lock_id = None

        with patch("bec_server.actors.scan_interlock.uuid4", return_value="uuid-1234"):
            actor.some_mismatch_action(mock_client)
        assert actor.lock_id == "uuid-1234"
        mock_client.queue.add_queue_lock.assert_called_once_with(
            queue="primary", reason="ScanInterlockActor", lock_id="uuid-1234"
        )

    def test_some_mismatch_action_skips_if_lock_exists(self, actor, mock_client):
        actor.lock_id = "existing-lock"
        actor.some_mismatch_action(mock_client)
        mock_client.queue.add_queue_lock.assert_not_called()

    def test_some_mismatch_action_skips_if_no_queue(self, actor, mock_client):
        actor.client.queue = None
        actor.some_mismatch_action(mock_client)
        assert actor.lock_id is None

    def test_all_match_action_unlocks(self, actor):
        with patch.object(actor, "_unlock") as mock_unlock:
            actor.all_match_action(actor.client)

        mock_unlock.assert_called_once()

    def test_unlock_removes_lock(self, actor, mock_client):
        actor.lock_id = "lock-123"
        actor._unlock()
        mock_client.queue.remove_queue_lock.assert_called_once_with(
            queue="primary", lock_id="lock-123"
        )
        assert actor.lock_id is None

    def test_unlock_skips_without_lock(self, actor, mock_client):
        actor.lock_id = None
        actor._unlock()
        mock_client.queue.remove_queue_lock.assert_not_called()

    def test_unlock_keeps_lock_if_remove_fails(self, actor, mock_client):
        actor.lock_id = "lock-123"
        mock_client.queue.remove_queue_lock.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError):
            actor._unlock()

        assert actor.lock_id is None
