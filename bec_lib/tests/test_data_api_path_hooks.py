"""Public hooks for data path authors: attach, refresh, live follow, request helpers, discovery."""

from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib.data_api import (
    AxisMode,
    CorrelationMode,
    DataAPI,
    DataBundler,
    DataPath,
    DataSource,
    DeviceStream,
    ScanTarget,
    SourceKind,
    SourceSeries,
    SourceSpec,
    UpdateReason,
    apply_size_gate,
)
from bec_lib.data_api import data_api as data_api_module
from bec_lib.data_api.scenarios import Scenario
from bec_lib.data_api.testing import REMOTE_SCAN, ReferencePath, make_mock_client, wait_for

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

A = ("samx", "samx")
B = ("det", "wave")


@pytest.fixture
def api():
    data_api = DataAPI(make_mock_client())
    yield data_api
    data_api.close()


def subscribe(api, sources=(A,), scan=REMOTE_SCAN, **kwargs):
    updates = []
    sub = api.subscribe(
        sources=list(sources), scan=scan, callback=updates.append, min_emit_interval=0, **kwargs
    )
    return sub, updates


def test_a_path_that_skips_the_base_constructor_is_rejected(api):
    class Forgetful(ReferencePath):
        data_source = "forgetful"

        def __init__(self, client):  # pylint: disable=super-init-not-called
            self.client = client

    with pytest.raises(TypeError, match=r"Forgetful.__init__\(\) has to call super"):
        api.register_path(Forgetful(api.client))
    assert "Forgetful" not in [type(path).__name__ for path in api.paths]


def test_register_path_attaches_the_facade_before_connect(api):
    seen = {}

    class Probe(ReferencePath):
        def connect(self):
            seen["api"], seen["lock"] = self.api, self.lock
            super().connect()

    api.register_path(Probe(api.client, keys=[A]))
    assert seen["api"] is api and seen["lock"] is api._lock


def test_refresh_without_changes_keeps_the_binding_and_its_data(api):
    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, updates = subscribe(api)
    path.send(REMOTE_SCAN, A, 0, 1.0)
    assert wait_for(lambda: list(updates[-1].get(*A).values) == [1.0])
    generation = sub.generation
    api.refresh(REMOTE_SCAN)
    assert sub.generation == generation
    sub.close()


def test_refresh_rebinds_when_a_source_becomes_available(api):
    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, updates = subscribe(api, sources=(A, B))
    assert sub.unbound_sources == [B]
    with path.lock:
        path.keys.add(B)
    api.refresh(REMOTE_SCAN)
    assert sub.unbound_sources == []
    assert updates[-1].reason == "rebind"
    sub.close()


def test_refresh_of_one_scan_leaves_other_subscriptions_alone(api):
    path = ReferencePath(api.client, keys=[A], scans=[REMOTE_SCAN, "other"], priority=0)
    api.register_path(path)
    sub, _ = subscribe(api)
    generation = sub.generation
    api.refresh("other")
    assert sub.generation == generation
    sub.close()


def test_a_vanished_scan_is_reported_as_an_error(api):
    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, updates = subscribe(api)
    with path.lock:
        path.scans.clear()
    api.refresh(REMOTE_SCAN)
    assert "no longer served" in updates[-1].get(*A).metadata["error"]
    sub.close()


def test_scanless_refresh_needs_no_private_sentinel(api):
    class StreamPath(ReferencePath):
        scenarios = frozenset({Scenario.NO_SCAN})

        def resolve(self, sources, context):
            return [
                SourceSpec(*key, kind="unindexed", available=key in self.keys) for key in sources
            ]

    path = StreamPath(api.client, data_source="streams", priority=0)
    api.register_path(path)
    sub, _ = subscribe(api, sources=(("cam", "frames"),), scan=None)
    assert sub.unbound_sources == [("cam", "frames")]
    with path.lock:
        path.keys.add(("cam", "frames"))
    api.refresh()  # scan-less subscriptions
    assert sub.unbound_sources == []
    sub.close()


def test_live_subscription_follows_a_scan_streamed_by_a_path(api):
    path = ReferencePath(api.client, keys=[A], scans=["ext-1"], priority=0)
    api.register_path(path)
    sub, updates = subscribe(api, scan="live")
    assert sub.scan_id == "ext-1"
    path.send("ext-1", A, 0, 5.0)
    assert wait_for(lambda: updates and list(updates[-1].get(*A).values) == [5.0])
    sub.close()


def test_reasons_and_the_live_target_are_enums_that_accept_their_strings(api):
    path = ReferencePath(api.client, keys=[A], scans=["ext-1"], priority=0)
    api.register_path(path)
    sub, updates = subscribe(api, scan=ScanTarget.LIVE)  # the same as scan="live"
    assert sub.scan_id == "ext-1"
    request = path.active_requests[0]
    with path.lock:
        request.series(A).insert(0, 5.0, None)
    request.notify("live")  # a data path may still pass the plain string
    assert wait_for(lambda: updates and list(updates[-1].get(*A).values) == [5.0])
    assert all(isinstance(update.reason, UpdateReason) for update in updates)
    assert updates[-1].reason is UpdateReason.LIVE
    assert updates[-1].reason == "live" and f"{updates[-1].reason}" == "live"
    with pytest.raises(ValueError, match="not a valid UpdateReason"):
        request.notify("refresh")
    sub.close()


def test_string_options_are_enums_that_accept_their_strings(api):
    spec = SourceSpec("det", "wave", kind="unindexed", stream="preview")  # as a path passes them
    assert spec.kind is SourceKind.UNINDEXED and spec.stream is DeviceStream.PREVIEW
    assert spec.kind == "unindexed" and f"{spec.stream}" == "preview"
    with pytest.raises(ValueError):
        SourceSpec("det", "wave", kind="scalar")
    assert SourceSeries("det", "wave", "async").kind is SourceKind.ASYNC

    assert DataBundler("scan-1", correlation="continuous").correlation is CorrelationMode.CONTINUOUS
    with pytest.raises(ValueError, match="Unknown correlation"):
        api.subscribe(sources=[A], scan=None, correlation="pairwise", callback=print)

    assert DataSource.REDIS == "redis" and DataSource.FILE == "file"

    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, updates = subscribe(api)
    path.send(REMOTE_SCAN, A, 0, 1.0)
    assert wait_for(lambda: updates and list(updates[-1].get(*A).values) == [1.0])
    update = updates[-1]
    assert list(update.axis("index")) == list(update.axis(AxisMode.INDEX)) == [0]
    with pytest.raises(ValueError):
        update.axis("angle")
    sub.close()


def test_live_follower_moves_only_to_scans_that_serve_one_of_its_sources(api):
    first = ReferencePath(api.client, "first", keys=[A], scans=["ext-1"], priority=0)
    second = ReferencePath(api.client, "second", keys=[B], scans=["ext-2"], priority=1)
    api.register_path(first)
    api.register_path(second)
    sub, _ = subscribe(api, scan="live")
    assert sub.scan_id == "ext-1"
    api.refresh("ext-2")  # describes an open scan, but has none of the sources
    assert sub.scan_id == "ext-1"
    sub.close()


def test_registering_a_path_retries_unbound_subscriptions(api):
    first = ReferencePath(api.client, "first", keys=[A], priority=0)
    api.register_path(first)
    sub, _ = subscribe(api, sources=(A, B))
    assert sub.unbound_sources == [B]
    api.register_path(ReferencePath(api.client, "second", keys=[B], priority=1))
    assert sub.unbound_sources == []
    sub.close()


def test_unregister_path_reroutes_its_subscriptions(api):
    first = ReferencePath(api.client, "first", keys=[A], priority=0)
    second = ReferencePath(api.client, "second", keys=[A], priority=1)
    api.register_path(first)
    api.register_path(second)
    sub, _ = subscribe(api)
    api.unregister_path("first")
    assert first not in api.paths
    assert second.active_requests and not first.active_requests
    with pytest.raises(ValueError):
        api.unregister_path("first")
    sub.close()


def test_set_status_reaches_the_consumer_without_new_points(api):
    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, updates = subscribe(api)
    request = path.active_requests[0]
    with path.lock:
        request.set_status(A, error="connection lost")
    request.notify("live")
    assert updates[-1].get(*A).metadata["error"] == "connection lost"
    with path.lock:
        request.set_status(A, error=None)
    request.notify("live")
    assert "error" not in updates[-1].get(*A).metadata
    sub.close()


def test_adopt_keeps_continuous_members_and_partner_series():
    bundle = DataBundler("scan", correlation="continuous")
    held = bundle.get_series("samx", "samx", "unindexed")
    partner = bundle.get_series("samy", "samy", "unindexed")
    loaded = SourceSeries("samx", "samx", "unindexed")
    loaded.insert(0, 1.0, None)
    request = SimpleNamespace(specs=[SourceSpec("samx", "samx")], bundle=bundle)
    from bec_lib.data_api.data_path import SourceRequest

    SourceRequest.adopt(request, {("samx", "samx"): loaded, ("samy", "samy"): loaded})
    assert bundle.series[("samx", "samx")] is held
    assert bundle.series[("samy", "samy")] is partner


def test_installed_paths_are_registered_from_entry_points(monkeypatch):
    class Discovered(ReferencePath):
        def __init__(self, client):
            super().__init__(client, data_source="discovered", keys=[A])

    entry_point = mock.MagicMock()
    entry_point.load.return_value = Discovered
    broken = mock.MagicMock()
    broken.load.side_effect = ImportError("missing optional dependency")

    def entry_points(group):
        assert group == data_api_module.PATH_ENTRY_POINT_GROUP
        return [entry_point, broken]

    monkeypatch.setattr(data_api_module.importlib.metadata, "entry_points", entry_points)
    api = DataAPI(make_mock_client())
    try:
        assert any(isinstance(path, Discovered) for path in api.paths)
    finally:
        api.close()


def test_follower_created_before_registration_binds_to_a_running_external_scan(api):
    sub, updates = subscribe(api, scan="live")
    assert sub.scan_id is None  # BEC runs no scan, no path describes one yet
    path = ReferencePath(api.client, keys=[A], scans=["ext-1"], priority=0)
    api.register_path(path)
    assert sub.scan_id == "ext-1"
    sub.close()


def test_flush_delivers_the_final_state_uncoalesced(api):
    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    updates = []
    sub = api.subscribe(
        sources=[A], scan=REMOTE_SCAN, callback=updates.append, min_emit_interval=60
    )
    request = path.active_requests[0]
    for ordinal in range(3):
        with path.lock:
            request.series(A).insert(ordinal, float(ordinal), None)
        request.notify("live")  # coalesced: the backfill opened a 60 s window
    assert list(updates[-1].get(*A).values) == []
    request.flush()
    assert updates[-1].reason == "live" and list(updates[-1].get(*A).values) == [0.0, 1.0, 2.0]
    sub.close()


def test_a_revision_bump_rebinds_and_an_unchanged_one_does_not(api):
    class ArchivePath(ReferencePath):
        revision = 1

        def resolve(self, sources, context):
            specs = super().resolve(sources, context)
            for spec in specs or ():
                spec.revision = self.revision
            return specs

    path = ArchivePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, _ = subscribe(api)
    generation = sub.generation
    api.refresh(REMOTE_SCAN)
    assert sub.generation == generation
    with path.lock:
        path.revision = 2  # the archive ingested more data for the scan
    api.refresh(REMOTE_SCAN)
    assert sub.generation > generation
    sub.close()


def test_rerouting_the_same_scan_keeps_the_passed_size_budget(api):
    class GatingPath(ReferencePath):
        def resolve(self, sources, context):
            specs = super().resolve(sources, context)
            for spec in specs or ():
                spec.estimated_bytes = 1000
            apply_size_gate(specs or [], context.size_limit_bytes)
            return specs

    path = GatingPath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, _ = subscribe(api, sources=(A, B), size_limit_bytes=100)
    assert sub.size_gated
    sub.confirm_size()
    assert not sub.size_gated
    with path.lock:
        path.keys.add(B)  # the catalog changed: refresh re-binds the scan ...
    api.refresh(REMOTE_SCAN)
    assert sub.unbound_sources == []
    assert not sub.size_gated  # ... without asking again for the already accepted scan
    sub.close()


def test_refresh_after_close_is_a_no_op():
    data_api = DataAPI(make_mock_client())
    data_api.close()
    data_api.refresh("anything")  # path threads may still report after close


def test_refresh_never_pulls_a_follower_off_the_running_bec_scan(api):
    """An external scan announced while BEC runs one must not steal live followers."""
    from bec_lib.data_api import client_views

    path = ReferencePath(api.client, keys=[A], scans=["ext-1"], priority=0)
    api.register_path(path)
    bec_scan = SimpleNamespace(
        status="open", status_message=object(), start_time=None, end_time=None
    )
    api.client.queue.scan_storage.current_scan_id = ["scan_1"]
    api.client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: (
        bec_scan if sid == "scan_1" else None
    )
    assert client_views.current_live_scan(api.client) == ("scan_1", bec_scan)
    with mock.patch.object(
        type(path),
        "resolve",
        lambda self, sources, context: [
            SourceSpec(*key, kind="monitored", available=True) for key in sources
        ],
    ):
        sub, _ = subscribe(api, scan="live")
        assert sub.scan_id == "scan_1"
        api.refresh("ext-1")
        assert sub.scan_id == "scan_1"
        sub.close()


def test_retry_unbound_picks_up_newly_servable_sources(api):
    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, _ = subscribe(api, sources=(A, B))
    assert sub.unbound_sources == [B]
    with path.lock:
        path.keys.add(B)
    api.retry_unbound()
    assert sub.unbound_sources == []
    sub.close()


def test_stream_values_from_any_transport_form_one_continuous_row():
    from bec_lib.data_api.decoding import decode_stream_values

    bundle = DataBundler("", correlation="continuous")
    for name in ("x", "y"):
        bundle.get_series(name, name, "unindexed")
    specs = [SourceSpec("x", "x"), SourceSpec("y", "y")]
    for value in (1.0, 2.0):
        decode_stream_values(bundle, [(spec, value, value) for spec in specs], "zmq")
    update = bundle.build_update("live")
    assert {key: list(col) for key, col in update.aligned().items()} == {
        ("x", "x"): [1.0, 2.0],
        ("y", "y"): [1.0, 2.0],
    }


def _recording_subscription(api, path):
    """Subscribe to A and record (update, thread, lock held) per delivery."""
    import threading

    from bec_lib.data_api.subscription import lock_held

    seen = []
    sub = api.subscribe(
        sources=[A],
        scan=REMOTE_SCAN,
        callback=lambda u: seen.append((u, threading.current_thread(), lock_held(api._lock))),
        min_emit_interval=0,
    )
    assert wait_for(lambda: path.active_requests)
    return sub, seen


def test_notify_with_the_lock_held_is_delivered_off_the_lock(api):
    import threading

    from bec_lib.data_api import subscription as subscription_module

    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, seen = _recording_subscription(api, path)
    before = len(seen)
    with mock.patch.object(subscription_module.logger, "warning") as warning:
        with path.lock:
            request = path.active_requests[0]
            for ordinal in range(2):
                request.series(A).insert(ordinal, float(ordinal), None)
                request.notify("live")
            assert len(seen) == before  # nothing runs consumer code under the lock
        assert wait_for(lambda: len(seen) > before and list(seen[-1][0].get(*A).values) == [0, 1])
    _, thread, held = seen[-1]
    assert thread is not threading.current_thread() and not held
    assert warning.call_count == 1  # logged once per subscription
    sub.close()


def test_request_batch_inserts_under_the_lock_and_notifies_once(api):
    from bec_lib.data_api.subscription import lock_held

    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, seen = _recording_subscription(api, path)
    request = path.active_requests[0]
    before = len(seen)
    with request.batch("live") as batch:
        assert batch is request and lock_held(api._lock)
        request.series(A).insert(0, 1.0, None)
        request.series(A).insert(1, 2.0, None)
        assert len(seen) == before
    assert wait_for(lambda: len(seen) == before + 1)
    update, _, held = seen[-1]
    assert list(update.get(*A).values) == [1.0, 2.0] and not held
    with pytest.raises(ValueError):
        with request.batch("live"):
            request.series(A).insert(2, 3.0, None)
            raise ValueError("transport failed")
    assert len(seen) == before + 1  # a failed batch notifies nothing
    sub.close()


def test_error_of_a_vanished_scan_clears_when_the_scan_is_served_again(api):
    path = ReferencePath(api.client, keys=[A], priority=0)
    api.register_path(path)
    sub, updates = subscribe(api)
    path.send(REMOTE_SCAN, A, 0, 1.0)
    assert wait_for(lambda: list(updates[-1].get(*A).values) == [1.0])
    path.scans.discard(REMOTE_SCAN)
    api.refresh(REMOTE_SCAN)
    assert "no longer served" in updates[-1].get(*A).metadata["error"]
    path.scans.add(REMOTE_SCAN)  # e.g. the path's catalog reconnected
    api.refresh(REMOTE_SCAN)
    path.send(REMOTE_SCAN, A, 1, 2.0)
    assert wait_for(lambda: list(updates[-1].get(*A).values) == [1.0, 2.0])
    assert "error" not in updates[-1].get(*A).metadata
    assert updates[-1].complete
    sub.close()


def test_a_path_breaking_the_resolve_contract_is_skipped(api):
    class TooFew(ReferencePath):
        def resolve(self, sources, context):
            return [SourceSpec(*sources[0], kind="monitored"), SourceSpec("not", "asked")]

    api.register_path(TooFew(api.client, "broken", keys=[A], priority=0))
    api.register_path(ReferencePath(api.client, "good", keys=[A, B], priority=1))
    sub, _ = subscribe(api, sources=[A, B])
    assert sub.routes == {A: "good", B: "good"}
    sub.close()


def test_a_path_describing_an_invalid_scenario_is_skipped(api):
    class Confused(ReferencePath):
        def describe_scan(self, scan_id):
            return Scenario.NO_SCAN

    api.register_path(Confused(api.client, "confused", keys=[A], scans=("other",), priority=0))
    api.register_path(ReferencePath(api.client, "good", keys=[A], scans=("ext",), priority=1))
    sub, _ = subscribe(api, scan="ext")
    assert sub.routes == {A: "good"}
    sub.close()


@pytest.mark.parametrize(
    "call",
    [
        lambda api: api.refresh(REMOTE_SCAN),
        lambda api: api.retry_unbound(),
        lambda api: api.correlation_groups([A], scan=REMOTE_SCAN),
        lambda api: api.estimate_bytes([A], REMOTE_SCAN),
        lambda api: api.register_path(ReferencePath(api.client, "late", keys=[A])),
        lambda api: api.unregister_path("redis"),
    ],
    ids=[
        "refresh",
        "retry_unbound",
        "correlation_groups",
        "estimate_bytes",
        "register_path",
        "unregister_path",
    ],
)
def test_facade_calls_with_the_lock_held_are_refused(api, call):
    with api._lock:
        with pytest.raises(RuntimeError, match="lock held"):
            call(api)
    assert api.client.queue.scan_storage.find_scan_by_ID.call_count == 0
