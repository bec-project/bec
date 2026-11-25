from __future__ import annotations

from bec_lib.scan_queue_cache import ScanQueueCache


def test_departed_items_age_out_in_departure_order():
    """Pending entries stay live while finished entries age out through bounded history."""
    cache = ScanQueueCache[str](maxlen=2, key=str)
    cache.retain(["first submitted", "second submitted"])
    cache.append("first submitted")
    cache.append("second submitted")
    cache.retain(["first submitted"])
    cache.retain([])
    assert list(cache) == ["second submitted", "first submitted"]

    cache.retain(["pending", "newest completion"])
    cache.append("pending")
    cache.append("newest completion")
    assert cache.get("first submitted") == "first submitted"
    assert cache.get("second submitted") == "second submitted"
    cache.retain(["pending"])
    assert cache.get("second submitted") is None
    assert list(cache) == ["first submitted", "newest completion", "pending"]
    assert cache[-1] == "pending"
    assert len(cache) == 3


def test_history_entry_becomes_live_again_without_duplication():
    """A queued response keeps its object when a later snapshot supplies its ID."""
    cache = ScanQueueCache[str](maxlen=1, key=str)
    cache.append("request")
    cache.retain(["request"])
    cache.append("other")
    assert list(cache) == ["other", "request"]
    cache.retain([])
    assert list(cache) == ["other", "request"]


def test_clearing_backlog_preserves_head_result():
    """A simultaneous departure cannot evict the head behind its cancelled backlog."""
    cache = ScanQueueCache[str](maxlen=2, key=str)
    cache.retain(["active", "pending-1", "pending-2", "pending-3"], result_ids=["active"])
    for item in ("active", "pending-1", "pending-2", "pending-3"):
        cache.append(item)
    cache.retain([])
    assert cache.get("active") == "active"
    assert len(cache) == 3


def test_cancelled_backlogs_preserve_results_from_every_queue():
    """Pending departures cannot evict another queue's finished result."""
    cache = ScanQueueCache[str](maxlen=2, key=str)
    cache.retain(
        ["primary-head", "primary-pending", "secondary-head", "secondary-pending"],
        result_ids=["primary-head", "secondary-head"],
    )
    for item in ("primary-head", "primary-pending", "secondary-head", "secondary-pending"):
        cache.append(item)
    cache.retain([])
    assert cache.get("primary-head") == "primary-head"
    assert cache.get("secondary-head") == "secondary-head"
    assert len(cache) <= 4
    cache.retain(["later-cancellation"], result_ids=[])
    cache.append("later-cancellation")
    cache.retain([])
    assert cache.get("primary-head") == "primary-head"
    assert cache.get("secondary-head") == "secondary-head"
    assert len(cache) <= 4
