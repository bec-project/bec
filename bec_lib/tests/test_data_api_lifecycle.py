"""Subscription lifecycle: rebinds, path faults, joins, re-entrant calls and progress."""

import threading

import pytest

from bec_lib.data_api import DataAPI, DataPath, SourceSpec
from bec_lib.data_api.scenarios import Scenario
from bec_lib.data_api.testing import REMOTE_SCAN, make_mock_client, wait_for

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

A = ("samx", "samx")
B = ("det", "wave")


class RecordingPath(DataPath):
    """Serves its keys for the remote scan and records the facade's calls."""

    scenarios = frozenset({Scenario.OPEN_SCAN})

    def __init__(self, client, data_source="recording", keys=(A,), priority=0, log=None):
        super().__init__(client)
        self.data_source = data_source
        self.priority = priority
        self.keys = set(keys)
        self.log = log if log is not None else []
        self.notify_on_close = False

    def describe_scan(self, scan_id):
        return Scenario.OPEN_SCAN if scan_id == REMOTE_SCAN else None

    def resolve(self, sources, context):
        return [SourceSpec(*key, kind="monitored", available=key in self.keys) for key in sources]

    def _open(self, request):
        self.log.append(("open", self.data_source, id(request)))
        request.notify("backfill")

    def close(self, request):
        self.log.append(("close", self.data_source, id(request)))
        if self.notify_on_close:
            for spec in request.specs:
                request.bundle.get_series(*spec.key, "monitored").insert(99, -1.0, None)
            request.notify("live")

    def join(self, request, timeout):
        self.log.append(("join", self.data_source, id(request)))


@pytest.fixture
def api():
    data_api = DataAPI(make_mock_client())
    yield data_api
    data_api.close()


def subscribe(api, sources=(A,), **kwargs):
    updates = []
    sub = api.subscribe(
        sources=list(sources),
        scan=REMOTE_SCAN,
        callback=updates.append,
        min_emit_interval=0,
        **kwargs,
    )
    return sub, updates


def test_rebind_opens_the_new_request_before_releasing_the_old_one(api):
    path = RecordingPath(api.client)
    api.register_path(path)
    sub, _ = subscribe(api)
    first = path.log[-1][2]
    sub.set_sources([A])
    events = [(kind, rid) for kind, _, rid in path.log if kind in ("open", "close")]
    second = [rid for kind, rid in events if kind == "open"][-1]
    assert events.index(("open", second)) < events.index(("close", first))
    sub.close()


def test_rebind_joins_the_released_requests_off_the_caller_thread(api):
    path = RecordingPath(api.client)
    api.register_path(path)
    sub, _ = subscribe(api)
    first = path.log[-1][2]
    sub.set_sources([A])
    assert wait_for(lambda: ("join", "recording", first) in path.log)
    sub.close()


def test_notify_from_close_cannot_hide_the_rebind_update(api):
    path = RecordingPath(api.client)
    path.notify_on_close = True
    api.register_path(path)
    sub, updates = subscribe(api)
    sub.set_sources([A])
    rebinds = [update for update in updates if update.reason == "rebind"]
    assert rebinds and rebinds[-1].generation == sub.generation
    # The old request's flush never reaches the consumer under the new generation.
    assert all(99 not in list(update.get(*A).ordinals) for update in rebinds)
    sub.close()


def test_a_raising_resolve_declines_and_the_next_path_serves(api):
    broken = RecordingPath(api.client, "broken", keys=(A,), priority=0)
    broken.resolve = lambda sources, context: (_ for _ in ()).throw(RuntimeError("down"))
    healthy = RecordingPath(api.client, "healthy", keys=(A,), priority=1)
    api.register_path(broken)
    api.register_path(healthy)
    sub, updates = subscribe(api)
    assert sub.unbound_sources == []
    assert any(entry[:2] == ("open", "healthy") for entry in healthy.log)
    assert updates
    sub.close()


def test_a_raising_close_does_not_stop_teardown(api):
    path = RecordingPath(api.client)

    def broken_close(request):
        raise RuntimeError("socket already gone")

    path.close = broken_close
    api.register_path(path)
    sub, _ = subscribe(api)
    sub.close()
    assert sub._closed and not path.active_requests
    api.client.callbacks.remove.assert_called()


def test_subscribe_from_inside_a_path_fails_fast(api):
    errors = []

    class NestingPath(RecordingPath):
        def _open(self, request):
            try:
                api.subscribe(sources=[A], scan=None, callback=print)
            except RuntimeError as exc:
                errors.append(exc)
            super()._open(request)

    api.register_path(NestingPath(api.client))
    sub, _ = subscribe(api)
    assert errors and "from inside a data path" in str(errors[0])
    sub.close()


def test_registration_racing_close_disconnects_the_path():
    data_api = DataAPI(make_mock_client())
    disconnected = threading.Event()

    class ClosingPath(RecordingPath):
        def connect(self):
            data_api.close()

        def disconnect(self):
            disconnected.set()

    with pytest.raises(RuntimeError, match="raced"):
        data_api.register_path(ClosingPath(data_api.client))
    assert disconnected.is_set()


def test_progress_is_one_monotonic_fraction_over_all_reporting_requests(api):
    class ProgressPath(RecordingPath):
        reports_progress = True

        def _open(self, request):
            self.log.append(("open", self.data_source, id(request)))
            self.log.append(("request", request))

    first = ProgressPath(api.client, "first", keys=(A,), priority=0)
    second = ProgressPath(api.client, "second", keys=(B,), priority=1)
    api.register_path(first)
    api.register_path(second)
    fractions = []
    sub, _ = subscribe(api, sources=(A, B), progress_callback=fractions.append)
    request_a = next(entry[1] for entry in first.log if entry[0] == "request")
    request_b = next(entry[1] for entry in second.log if entry[0] == "request")
    request_a.progress_callback(1.0)
    request_a.progress_callback(0.2)  # never goes backwards
    request_b.progress_callback(0.5)
    request_b.progress_callback(1.0)
    assert fractions == [0.5, 0.75, 1.0]
    sub.close()


def test_close_waits_for_a_callback_delivered_by_another_thread(api):
    api.register_path(RecordingPath(api.client))
    running, release, finished = threading.Event(), threading.Event(), threading.Event()

    def callback(_update):
        running.set()
        release.wait(2)
        finished.set()

    sub, _ = subscribe(api)
    # A snapshot built on this thread (as bind() or set_sources() do) ...
    with api._lock:
        sub._callback = callback
        sub._emit_now(sub._all_requests()[0], "live")
    # ... and delivered by another one (the scheduler's hand-off, a dispatcher handler).
    other = threading.Thread(target=sub._flush_deferred)
    other.start()
    assert running.wait(2)
    threading.Timer(0.1, release.set).start()
    sub.close()
    assert finished.is_set()
    other.join()
