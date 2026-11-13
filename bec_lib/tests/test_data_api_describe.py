"""Asking the DataAPI what it can serve, and the helpers a data path gets for it."""

import threading
import time
from types import SimpleNamespace

import numpy as np
import pytest

from bec_lib.data_api import DataAPI, FileDataPath, SourceInfo, SourceSpec
from bec_lib.data_api.data_bundler import DataBundler
from bec_lib.data_api.decoding import open_scan_specs
from bec_lib.data_api.models import CONTINUOUS_GROUP, SCAN_GROUP
from bec_lib.data_api.scenarios import Scenario
from bec_lib.data_api.testing import REMOTE_SCAN, ReferencePath, make_mock_client, wait_for

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

A = ("samx", "samx")
B = ("det", "wave")
EXT = ("extdaq", "counts")


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


# --- describe / knows_scan -------------------------------------------------------------


def test_describe_reports_availability_group_and_serving_path(api):
    api.register_path(ReferencePath(api.client, "reference", keys=[A]))
    info = api.describe([A, B], scan=REMOTE_SCAN)
    assert list(info) == [A, B]
    assert info[A] == SourceInfo("samx", "samx", True, "monitored", SCAN_GROUP, "reference")
    assert not info[B].available and info[B].data_source is None
    assert info[B].group == SCAN_GROUP  # the missing member keeps its identity
    assert api.correlation_groups([A, B], scan=REMOTE_SCAN) == {SCAN_GROUP: [A, B]}


def test_describe_of_a_scan_nobody_knows_reports_nothing_available(api):
    info = api.describe([A], scan="no-such-scan")
    assert not info[A].available


def test_idle_live_description_asks_the_paths_what_they_add_to_the_next_scan(api):
    class PerPoint(ReferencePath):
        def describe_sources(self, sources):
            return [SourceSpec(*key, kind="monitored") for key in sources if key in self.keys]

    api.register_path(PerPoint(api.client, "daq", keys=[EXT], scans=()))
    info = api.describe([A, EXT], scan="live")
    assert info[EXT].available and info[EXT].group == SCAN_GROUP and info[EXT].data_source == "daq"
    assert not info[A].available  # the mock client has no such device
    assert api.correlation_groups([EXT], scan="live") == {SCAN_GROUP: [EXT]}


def test_idle_live_description_without_the_hook_stays_unavailable(api):
    api.register_path(ReferencePath(api.client, "plain", keys=[EXT], scans=()))
    assert not api.describe([EXT], scan="live")[EXT].available


def test_continuous_description_is_one_group(api):
    info = api.describe([A, B], scan=None, correlation="continuous")
    assert {item.group for item in info.values()} == {CONTINUOUS_GROUP}


def test_knows_scan_covers_scans_only_a_path_describes(api):
    assert not api.knows_scan(REMOTE_SCAN)
    api.register_path(ReferencePath(api.client, "reference", keys=[A]))
    assert api.knows_scan(REMOTE_SCAN)
    assert not api.knows_scan("other") and not api.knows_scan("")


# --- resolve may return a subset ---------------------------------------------------------


def test_resolve_may_return_only_the_sources_a_path_knows(api):
    class OwnKeysOnly(ReferencePath):
        def resolve(self, sources, context):
            if context.scan_id not in self.scans:
                return None
            return [SourceSpec(*key, kind="monitored") for key in sources if key in self.keys]

    api.register_path(OwnKeysOnly(api.client, "first", keys=[A], priority=0))
    api.register_path(ReferencePath(api.client, "second", keys=[B], priority=1))
    sub, _ = subscribe(api, sources=[A, B])
    assert sub.routes == {A: "first", B: "second"}
    assert sub.groups == {SCAN_GROUP: [A, B]}
    sub.close()


def test_a_path_that_is_silent_about_a_source_does_not_change_its_identity(api):
    class OwnKeysOnly(ReferencePath):
        def resolve(self, sources, context):
            return [SourceSpec(*key, kind="unindexed") for key in sources if key in self.keys]

    api.register_path(OwnKeysOnly(api.client, "stream", keys=[B], priority=0))
    api.register_path(ReferencePath(api.client, "scan", keys=[], priority=1))
    info = api.describe([A], scan=REMOTE_SCAN)
    assert not info[A].available and info[A].group == SCAN_GROUP


# --- stored scans of another catalog -----------------------------------------------------


def test_file_path_subclass_serves_its_own_catalog_through_public_hooks(api):
    class Reference:
        def read(self):
            return {"value": np.arange(3.0), "timestamp": np.arange(3.0)}

    class Archive(FileDataPath):
        data_source = "archive"
        priority = 70

        def describe_scan(self, scan_id):
            return Scenario.CLOSED_SCAN if scan_id == "ext-1" else None

        def catalog_entry(self, scan_id):
            if scan_id != "ext-1":
                return None
            return SimpleNamespace(
                stored_data_info={"stage": {"x": {"shape": (3,), "dtype": "float64"}}},
                num_monitored_readouts=3,
                file_path="https://archive/ext-1",
            )

        def _load_container(self, request, msg):
            return SimpleNamespace(devices={"stage": {"x": Reference()}})

    api.register_path(Archive(api.client))
    assert api.knows_scan("ext-1")
    sub, updates = subscribe(api, sources=[("stage", "x")], scan="ext-1")
    assert wait_for(lambda: updates and list(updates[-1].get("stage", "x").values) == [0, 1, 2])
    assert sub.routes == {("stage", "x"): "archive"}
    sub.close()


# --- monitored classification ------------------------------------------------------------


def test_configuration_and_omitted_signals_are_not_scan_members():
    client = make_mock_client()
    client.device_manager.devices = {
        "samx": SimpleNamespace(
            _info={
                "signals": {
                    "readback": {"obj_name": "samx", "kind_str": "hinted"},
                    "setpoint": {"obj_name": "samx_setpoint", "kind_str": "normal"},
                    "velocity": {"obj_name": "samx_velocity", "kind_str": "config"},
                    "debug": {"obj_name": "samx_debug", "kind_str": "omitted"},
                    "legacy": {"obj_name": "samx_legacy"},
                }
            }
        )
    }
    keys = [("samx", name) for name in ("samx", "samx_setpoint", "samx_velocity", "samx_debug")]
    specs = open_scan_specs(client, keys + [("samx", "samx_legacy")], {"monitored": ["samx"]})
    assert [spec.available for spec in specs] == [True, True, False, False, True]


# --- per-curve alignment -----------------------------------------------------------------


def _update(points):
    bundle = DataBundler("scan")
    for key, ordinals in points.items():
        series = bundle.get_series(*key, "monitored")
        for ordinal in ordinals:
            series.insert(ordinal, float(ordinal), 100.0 + ordinal)
    return bundle.build_update("live")


def test_an_update_says_whether_it_belongs_to_a_scan():
    assert _update({A: [0]}).has_scan
    assert not DataBundler("").build_update("live").has_scan  # a scan-less device stream


def test_axis_of_a_selection_needs_its_source_in_the_selection():
    """An x column is only parallel to columns it was aligned with."""
    bundle = DataBundler("scan-1")
    det, samx, samy = ("det", "det"), ("samx", "samx"), ("samy", "samy")
    for ordinal in range(4):
        bundle.get_series(*det, "monitored").insert(ordinal, 10.0 * ordinal, 100.0 + ordinal)
    for ordinal, value in ((0, 0.0), (2, 2.0), (5, 5.0)):
        bundle.get_series(*samx, "monitored").insert(ordinal, value, 200.0 + ordinal)
    bundle.get_series(*samy, "monitored").insert(7, 7.0, 300.0)
    update = bundle.build_update("live")

    # samx was not aligned with det: its values at det's ordinals do not exist.
    with pytest.raises(ValueError, match="not part of the selection"):
        update.axis("device", samx, keys=[det])
    with pytest.raises(ValueError, match="not part of the selection"):
        update.axis("timestamp", samx, keys=[det])

    # Aligned together, x and y are the points both have.
    assert list(update.axis("device", samx, keys=[det, samx])) == [0.0, 2.0]
    assert list(update.aligned([det, samx])[det]) == [0.0, 20.0]
    # Without a source the first source of the selection supplies the timestamps,
    # not the first source of the update.
    assert list(update.axis("timestamp", keys=[samx])) == [200.0, 202.0, 205.0]
    assert list(update.axis("index", keys=[det])) == [0, 1, 2, 3]


def test_aligned_keys_pair_only_the_named_sources():
    silent = ("samx", "samx_velocity")
    update = _update({A: range(5), B: [0, 1, 2, 4], silent: []})
    assert len(update.aligned()[A]) == 0  # the silent member blanks the whole group
    pair = update.aligned(keys=[A, B])
    assert list(pair) == [A, B]
    assert list(pair[A]) == list(pair[B]) == [0.0, 1.0, 2.0, 4.0]
    assert list(update.ordinals(keys=[A, B])) == [0, 1, 2, 4]
    assert list(update.aligned(keys=[A])[A]) == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert list(update.axis("timestamp", source=B, keys=[A, B])) == [100.0, 101.0, 102.0, 104.0]
    assert list(update.axis("device", source=A, keys=[A, B])) == [0.0, 1.0, 2.0, 4.0]
    assert update.aligned(keys=[A, ("not", "subscribed")]).keys() == {A}
    assert (
        update.aligned(keys=[A, B])[A] is not None and update.ordinals() is update.aligned_ordinals
    )


# --- request worker and path-wide status -------------------------------------------------


def test_request_worker_is_stopped_and_joined_by_the_base_class(api):
    stopped = threading.Event()

    class Polling(ReferencePath):
        def _open(self, request):
            request.start_worker(self._poll, name="poll")

        def _poll(self, request):
            count = 0
            while not request.stop_event.wait(0.01):
                with request.batch("live"):
                    if request.cancelled:
                        break
                    request.series(A).insert(count, float(count), None)
                count += 1
            stopped.set()

    api.register_path(Polling(api.client, "polling", keys=[A]))
    sub, updates = subscribe(api)
    assert wait_for(lambda: updates and len(updates[-1].get(*A).values) >= 3)
    (worker,) = sub._all_requests()[0].workers
    sub.close()
    assert stopped.is_set() and not worker.is_alive()


def test_close_joins_every_worker_a_request_started(api):
    """A request may run several workers, e.g. a poll loop and a later history fetch."""
    workers = []

    class TwoWorkers(ReferencePath):
        def _open(self, request):
            workers.append(request.start_worker(self._slow_to_stop, name="first"))
            workers.append(request.start_worker(self._wait_for_stop, name="second"))

        def _slow_to_stop(self, request):
            request.stop_event.wait(5)
            time.sleep(0.1)  # still busy when the second worker has ended

        def _wait_for_stop(self, request):
            request.stop_event.wait(5)

    api.register_path(TwoWorkers(api.client, "two", keys=[A]))
    sub, _updates = subscribe(api)
    sub.close()
    assert [worker.is_alive() for worker in workers] == [False, False]


def test_join_shares_one_timeout_between_the_workers_of_a_request(api):
    release = threading.Event()

    class Stuck(ReferencePath):
        def _open(self, request):
            for index in range(3):
                request.start_worker(lambda _request: release.wait(5), name=f"stuck-{index}")

    path = Stuck(api.client, "stuck", keys=[A])
    api.register_path(path)
    sub, _updates = subscribe(api)
    started = time.monotonic()
    path.join(sub._all_requests()[0], timeout=0.2)
    elapsed = time.monotonic() - started
    release.set()
    sub.close()
    assert 0.15 < elapsed < 0.45


def test_report_shows_and_clears_a_path_wide_error(api):
    path = ReferencePath(api.client, "reference", keys=[A, B])
    api.register_path(path)
    sub, updates = subscribe(api, sources=[A, B])
    path.report("connection lost")
    assert wait_for(
        lambda: all(source.metadata.get("error") for source in updates[-1].sources.values())
    )
    assert not updates[-1].complete
    path.report(None)
    assert wait_for(
        lambda: not any(source.metadata.get("error") for source in updates[-1].sources.values())
    )
    sub.close()
