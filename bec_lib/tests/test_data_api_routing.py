"""Source routing with independent adapters and shared correlation groups."""

from types import SimpleNamespace

import pytest

from bec_lib.data_api import DataPath, SourceSpec
from bec_lib.data_api.scenarios import Scenario

from .test_data_api import data_api, mock_client  # noqa: F401


class MemoryPath(DataPath):
    """A cached external catalog and feed, with no BEC scan item or Redis data."""

    scenarios = frozenset({Scenario.OPEN_SCAN, Scenario.CLOSED_SCAN, Scenario.NO_SCAN})
    priority = 0

    def __init__(self, client, name, keys):
        super().__init__(client)
        self.data_source = name
        self.keys = set(keys)
        self.status = Scenario.OPEN_SCAN
        self.requests = []
        self.resolutions = []

    def describe_scan(self, scan_id):
        return self.status if scan_id == "remote" else None

    def current_open_scan(self):
        return "remote" if self.status == Scenario.OPEN_SCAN else None

    def resolve(self, sources, context):
        self.resolutions.append(list(sources))
        return [
            SourceSpec(*key, kind="monitored", available=key in self.keys, estimated_bytes=60)
            for key in sources
        ]

    def _open(self, request):
        self.requests.append(request)
        request.notify("history" if self.status == Scenario.CLOSED_SCAN else "backfill")

    def close(self, request):
        self.requests = [current for current in self.requests if current is not request]

    def push(self, key, ordinal, value):
        for request in list(self.requests):
            if key not in {spec.key for spec in request.specs}:
                continue
            with self._lock:
                request.bundle.get_series(*key, "monitored").insert(ordinal, value, ordinal)
            request.notify("live")


@pytest.fixture
def paths(data_api, mock_client):
    mock_client.history = SimpleNamespace(get_message=lambda scan_id: None)
    mock_client.queue.scan_storage.find_scan_by_ID.return_value = None
    first = MemoryPath(mock_client, "first", [("samx", "samx")])
    second = MemoryPath(mock_client, "second", [("det", "wave")])
    data_api.register_path(first)
    data_api.register_path(second)
    return first, second


def test_remote_scan_combines_adapters_by_acquisition_key(data_api, paths):
    first, second = paths
    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx"), ("det", "wave")],
        scan="remote",
        callback=updates.append,
        min_emit_interval=0,
    )
    assert second.resolutions[-1] == [("det", "wave")]
    assert first.requests[0].bundle is second.requests[0].bundle
    first.push(("samx", "samx"), 2, 12)
    second.push(("det", "wave"), 0, 20)
    assert list(updates[-1].aligned_ordinals) == []
    second.push(("det", "wave"), 2, 22)
    assert {k: list(v) for k, v in updates[-1].aligned().items()} == {
        ("samx", "samx"): [12],
        ("det", "wave"): [22],
    }
    assert not sub.unbound_sources
    sub.close()
    assert not first.requests and not second.requests


def test_size_gate_covers_the_sum_of_adapters(data_api, paths):
    first, second = paths
    sub = data_api.subscribe(
        sources=[("samx", "samx"), ("det", "wave")], scan="remote", size_limit_bytes=100
    )
    assert sub.estimated_bytes == 120 and sub.size_gated
    assert not first.requests and not second.requests
    assert data_api.estimate_bytes(sub.sources, "remote") == 120
    sub.confirm_size()
    assert len(first.requests) == len(second.requests) == 1
    assert not sub.size_gated
    sub.close()


def test_a_path_that_resolves_no_source_does_not_claim_the_scan(data_api):
    """An empty answer declines like ``None``: the scan must not look served."""

    class Empty(MemoryPath):
        def resolve(self, sources, context):
            return []

    data_api.register_path(Empty(data_api.client, "empty", []))
    assert data_api.knows_scan("remote")
    with pytest.raises(ValueError, match="'empty' cannot serve scan 'remote'"):
        data_api.subscribe(
            sources=[("samx", "samx")],
            scan="remote",
            force_source="empty",
            callback=lambda update: None,
        )


def test_forced_source_never_falls_through(data_api, paths):
    first, second = paths
    sub = data_api.subscribe(
        sources=[("samx", "samx"), ("det", "wave")], scan="remote", force_source="first"
    )
    assert sub.unbound_sources == [("det", "wave")]
    assert first.requests and not second.requests
    sub.close()


def test_refresh_retries_unavailable_source_and_follows_external_scan(data_api, paths):
    first, second = paths
    second.keys.clear()
    # The paths stream "remote" live (current_open_scan): a live follower binds at once.
    sub = data_api.subscribe(sources=[("samx", "samx"), ("det", "wave")])
    assert sub.scan_id == "remote" and sub.unbound_sources == [("det", "wave")]
    second.keys.add(("det", "wave"))
    data_api.refresh("remote")
    assert not sub.unbound_sources
    assert len(first.requests) == len(second.requests) == 1
    sub.close()


def test_replacement_rebinds_every_adapter_in_the_group(data_api, paths, mock_client):
    first, second = paths
    sub = data_api.subscribe(sources=[("samx", "samx"), ("det", "wave")], scan="remote")
    replacement = MemoryPath(mock_client, "second", [("det", "wave")])
    data_api.register_path(replacement, replace=True)
    assert not second.requests
    assert len(first.requests) == len(replacement.requests) == 1
    assert first.requests[0].bundle is replacement.requests[0].bundle
    sub.close()


def test_failed_adapter_open_cleans_up_all_requests(data_api, paths, monkeypatch):
    first, second = paths

    def fail(request):
        raise RuntimeError("feed unavailable")

    monkeypatch.setattr(second, "_open", fail)
    with pytest.raises(RuntimeError, match="feed unavailable"):
        data_api.subscribe(sources=[("samx", "samx"), ("det", "wave")], scan="remote")
    assert not first.requests and not second.requests
