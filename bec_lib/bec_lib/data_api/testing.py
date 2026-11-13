"""
Test support for DataAPI data paths.

A data path author tests an adapter against the DataAPI contract by subclassing
:class:`DataPathConformance` and implementing its hooks; every test uses only the
public DataAPI / Subscription surface, so it runs unchanged against any transport::

    from bec_lib.data_api.testing import DataPathConformance

    class TestMyPath(DataPathConformance):
        def make_path(self, client, data_source, keys):
            return MyPath(client, data_source=data_source, keys=keys)

        def publish(self, path, key, ordinal, value):
            my_fake_server.send(key, ordinal, value)

:class:`ReferencePath` is a complete push adapter (one receiver thread fanning out
to the open requests) and the template to copy for a new transport.
"""

from __future__ import annotations

import queue
import threading
import time
from types import SimpleNamespace
from typing import Any, Callable, Iterable
from unittest import mock

import pytest

from .data_api import DataAPI
from .data_path import BindContext, DataPath, SourceRequest, SourceSpec
from .models import SourceKey, SourceKind, UpdateReason
from .scenarios import Scenario
from .subscription import lock_held

__all__ = [
    "REMOTE_SCAN",
    "DataPathConformance",
    "HistoryStub",
    "ReferencePath",
    "lock_held",
    "make_mock_client",
    "wait_for",
]

#: Scan id the conformance tests subscribe to; unknown to BEC, described by the paths.
REMOTE_SCAN = "remote"


class HistoryStub:
    """Minimal ``client.history``: registry entries and optional file containers."""

    def __init__(self, messages: dict | None = None, containers: dict | None = None):
        self.messages = dict(messages or {})
        self.containers = dict(containers or {})

    def get_message(self, scan_id: str) -> Any:
        """Return the registry entry of a scan, or None."""
        return self.messages.get(scan_id)

    def latest_message(self) -> Any:
        """Return the registry entry of the most recent scan, or None."""
        return max(self.messages.values(), key=lambda message: message.scan_number, default=None)

    def get_by_scan_id(self, scan_id: str) -> Any:
        """Return the file container of a scan, or None."""
        return self.containers.get(scan_id)


def make_mock_client(connector: Any = None) -> mock.MagicMock:
    """
    Build a client double that is enough for a DataAPI with custom data paths.

    BEC itself knows no scan and no device: scan storage and the history are empty,
    so only the paths under test describe and serve data.

    Args:
        connector: Redis connector to use (e.g. a fakeredis one); a mock by default.

    Returns:
        mock.MagicMock: The client.
    """
    from bec_lib.client import BECClient  # pylint: disable=import-outside-toplevel

    client = mock.MagicMock(spec=BECClient)
    client.started = True
    client.connector = connector if connector is not None else mock.MagicMock()
    client.callbacks = mock.MagicMock()
    client.callbacks.register = mock.MagicMock(return_value="callback_id")
    client.queue = mock.MagicMock()
    client.queue.scan_storage.current_scan_id = []
    client.queue.scan_storage.find_scan_by_ID.return_value = None
    client.device_manager = mock.MagicMock()
    client.device_manager.get_bec_signals.return_value = []
    client.device_manager.devices = {}
    client.history = HistoryStub()
    return client


def wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> bool:
    """Poll ``predicate`` until it is true or ``timeout`` seconds passed."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


class ReferencePath(DataPath):
    """
    Template of a push transport: one connection, many subscriptions.

    A receiver thread (started in :meth:`connect`) reads ``(scan_id, key, ordinal,
    value, timestamp)`` records from ``wire`` — the stand-in for a socket — keeps a
    per-key history so that a newly opened request can be given the full state,
    and fans each record out to the open requests that asked for the key.

    Args:
        client: BEC client.
        data_source (str): Name of the data source (used by ``force_source``).
        keys (Iterable[SourceKey]): Sources this transport can serve.
        scans (Iterable[str]): Scans this transport describes as open.
        priority (int): Routing priority (lower first).
    """

    scenarios = frozenset({Scenario.OPEN_SCAN, Scenario.CLOSED_SCAN})

    def __init__(
        self,
        client,
        data_source: str = "reference",
        keys: Iterable[SourceKey] = (),
        scans: Iterable[str] = (REMOTE_SCAN,),
        priority: int = 100,
    ):
        super().__init__(client)
        self.data_source = data_source
        self.priority = priority
        self.keys = set(keys)
        self.scans = set(scans)
        self.wire: queue.Queue = queue.Queue()
        self._points: dict[tuple[str, SourceKey], dict[int, tuple[Any, Any]]] = {}
        self._receiver: threading.Thread | None = None

    # --- transport ---------------------------------------------------------

    def connect(self) -> None:
        self._receiver = threading.Thread(
            target=self._receive, name=f"{self.data_source}-receiver", daemon=True
        )
        self._receiver.start()

    def disconnect(self) -> None:
        self.wire.put(None)
        if self._receiver is not None and self._receiver is not threading.current_thread():
            self._receiver.join(timeout=2)
        self._receiver = None

    def send(self, scan_id: str, key: SourceKey, ordinal: int, value: Any, timestamp=None):
        """Simulate the remote end publishing one point."""
        self.wire.put((scan_id, key, ordinal, value, timestamp))

    def _receive(self) -> None:
        while True:
            record = self.wire.get()
            if record is None:
                return
            scan_id, key, ordinal, value, timestamp = record
            touched = []
            with self.lock:
                self._points.setdefault((scan_id, key), {})[ordinal] = (value, timestamp)
                for request in self.active_requests:
                    if request.scan_id == scan_id and key in {s.key for s in request.specs}:
                        request.series(key).insert(ordinal, value, timestamp)
                        touched.append(request)
            for request in touched:  # notify only after releasing the lock
                request.notify(UpdateReason.LIVE)

    # --- DataAPI contract ----------------------------------------------------

    def describe_scan(self, scan_id: str) -> Scenario | None:
        return Scenario.OPEN_SCAN if scan_id in self.scans else None

    def current_open_scan(self) -> str | None:
        return next(iter(sorted(self.scans)), None)

    def resolve(self, sources: list[SourceKey], context: BindContext) -> list[SourceSpec] | None:
        if context.scan_id not in self.scans:
            return None
        return [
            SourceSpec(*key, kind=SourceKind.MONITORED, available=key in self.keys)
            for key in sources
        ]

    def _open(self, request: SourceRequest) -> None:
        # The full state: a new request (or a rebind) starts from everything received.
        for spec in request.specs:
            points = self._points.get((request.scan_id, spec.key), {})
            series = request.series(spec)
            for ordinal, (value, timestamp) in sorted(points.items()):
                series.insert(ordinal, value, timestamp)
        request.notify(UpdateReason.BACKFILL)

    def close(self, request: SourceRequest) -> None:
        """Nothing per request to release: the connection is path-wide."""


class DataPathConformance:
    """
    Contract tests for a data path; subclass and implement the hooks.

    The adapter under test serves source ``A`` for scan :data:`REMOTE_SCAN`; a
    :class:`ReferencePath` partner serves ``B`` and acts as fallback, correlation
    partner and replacement peer.
    """

    A: SourceKey = ("samx", "samx")
    B: SourceKey = ("det", "wave")

    #: False for paths that read a scan once per binding (history, archives): data is
    #: then put in place with :meth:`seed` *before* subscribing or rebinding.
    delivers_after_open: bool = True

    # --- hooks -------------------------------------------------------------

    def make_path(self, client, data_source: str, keys: list[SourceKey]) -> DataPath:
        """Return a new adapter serving ``keys`` for scan :data:`REMOTE_SCAN`."""
        raise NotImplementedError

    def publish(self, path: DataPath, key: SourceKey, ordinal: int, value: Any) -> None:
        """Make the adapter's remote end deliver one point of ``key``."""
        raise NotImplementedError

    def wait_ready(self, path: DataPath, keys: list[SourceKey]) -> None:
        """
        Wait until ``path`` can receive published points for ``keys`` (default: no-op).

        Pub/sub transports implement it to avoid losing points sent before a new
        subscriber is connected (the ZMQ "slow joiner").
        """

    def seed(self, path: DataPath, key: SourceKey, ordinal: int, value: Any) -> None:
        """Make ``key``'s point available before a binding opens (default: :meth:`publish`)."""
        self.publish(path, key, ordinal, value)

    def _put(self, path, key, ordinal, value, *, before: bool) -> None:
        """Seed before a binding (read-once paths) or publish after it (streaming paths)."""
        if before == self.delivers_after_open:
            return
        if before:
            self.seed(path, key, ordinal, value)
        else:
            self.wait_ready(path, [key])
            self.publish(path, key, ordinal, value)

    def active_resources(self, path: DataPath) -> int:
        """Number of per-request resources the adapter still holds."""
        return len(path.active_requests)

    # --- fixtures ----------------------------------------------------------

    @pytest.fixture
    def env(self):
        """A DataAPI with the adapter under test and a reference partner."""
        client = make_mock_client()
        api = DataAPI(client)
        path = self.make_path(client, "under_test", [self.A])
        ref = ReferencePath(client, "reference", [self.B], priority=path.priority + 1)
        api.register_path(path)
        api.register_path(ref)
        try:
            yield SimpleNamespace(api=api, client=client, path=path, ref=ref)
        finally:
            api.close()

    def _subscribe(self, env, sources=None, **kwargs):
        updates: list = []
        sub = env.api.subscribe(
            sources=list(sources or (self.A, self.B)),
            scan=REMOTE_SCAN,
            callback=updates.append,
            min_emit_interval=0,
            **kwargs,
        )
        return sub, updates

    @staticmethod
    def _values(update, key) -> list:
        source = update.get(*key) if update is not None else None
        return [] if source is None else list(source.values)

    # --- contract tests ----------------------------------------------------

    def test_resolve_returns_one_spec_per_key(self, env):
        context = BindContext(scan_id=REMOTE_SCAN, scenario=Scenario.OPEN_SCAN, have_item=True)
        specs = env.path.resolve([self.A, self.B], context)
        assert specs is None or [spec.key for spec in specs] == [self.A, self.B]

    def test_unavailable_key_falls_through(self, env):
        self._put(env.path, self.A, 0, 1.0, before=True)
        sub, updates = self._subscribe(env)
        assert sub.unbound_sources == []
        self._put(env.path, self.A, 0, 1.0, before=False)
        env.ref.send(REMOTE_SCAN, self.B, 0, 2.0)
        assert wait_for(lambda: updates and list(updates[-1].aligned().get(self.A, ())) == [1.0])
        assert list(updates[-1].aligned()[self.B]) == [2.0]
        sub.close()

    def test_force_source_never_falls_through(self, env):
        sub, _ = self._subscribe(env, force_source="under_test")
        assert sub.unbound_sources == [self.B]
        assert not env.ref.active_requests
        sub.close()

    def test_shared_bundle_leaves_partner_series_alone(self, env):
        for ordinal, value in ((0, 1.0), (1, 1.5)):
            self._put(env.path, self.A, ordinal, value, before=True)
        sub, updates = self._subscribe(env)
        env.ref.send(REMOTE_SCAN, self.B, 0, 2.0)
        for ordinal, value in ((0, 1.0), (1, 1.5)):
            self._put(env.path, self.A, ordinal, value, before=False)
        assert wait_for(lambda: updates and len(self._values(updates[-1], self.A)) == 2)
        assert wait_for(lambda: self._values(updates[-1], self.B) == [2.0])
        sub.close()

    def test_close_releases_and_stops_delivery(self, env):
        sub, updates = self._subscribe(env)
        assert self.active_resources(env.path) >= 1
        sub.close()
        assert self.active_resources(env.path) == 0
        before = len(updates)
        self.publish(env.path, self.A, 5, 9.0)  # nobody may receive it any more
        time.sleep(0.05)
        assert len(updates) == before

    def test_partner_failure_during_open_cleans_up(self, env, monkeypatch):
        def fail(request):
            raise RuntimeError("partner feed unavailable")

        monkeypatch.setattr(env.ref, "_open", fail)
        with pytest.raises(RuntimeError, match="partner feed unavailable"):
            self._subscribe(env)
        assert self.active_resources(env.path) == 0

    def test_replace_rebinds_to_new_instance(self, env):
        sub, updates = self._subscribe(env)
        replacement = self.make_path(env.client, "under_test", [self.A])
        self._put(replacement, self.A, 0, 3.0, before=True)
        env.api.register_path(replacement, replace=True)
        assert self.active_resources(env.path) == 0
        assert self.active_resources(replacement) >= 1
        self._put(replacement, self.A, 0, 3.0, before=False)
        assert wait_for(lambda: updates and self._values(updates[-1], self.A) == [3.0])
        sub.close()

    def test_rebind_generation_drops_old_request(self, env):
        sub, updates = self._subscribe(env)
        first_generation = sub.generation
        self._put(env.path, self.A, 0, 4.0, before=True)
        sub.set_sources([self.A])
        assert sub.generation > first_generation
        self._put(env.path, self.A, 0, 4.0, before=False)
        assert wait_for(
            lambda: updates
            and updates[-1].generation == sub.generation
            and self._values(updates[-1], self.A) == [4.0]
        )
        assert updates[-1].generation == sub.generation
        assert self.active_resources(env.path) == 1
        sub.close()
