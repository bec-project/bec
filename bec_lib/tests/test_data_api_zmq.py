"""The ZMQ data path: with a stand-in receiver, and once with a real ZMQ publisher."""

import json
import queue
import time
from types import SimpleNamespace

import numpy as np
import pytest

from bec_lib.data_api import DataAPI
from bec_lib.data_api.paths import zmq as zmq_path
from bec_lib.data_api.testing import make_mock_client, wait_for

# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access

PREVIEW = ("cam", "preview")
ADDRESS = "tcp://127.0.0.1:5555"
ENCODER = "adzmq-array-1.0"
#: Seconds from the Unix epoch to the EPICS epoch (1990-01-01), as ADZMQ stamps its frames.
EPICS_EPOCH = 631_152_000.0


def advertise(client, device="cam", signal="preview", **transport):
    """Give the client a device whose preview signal advertises a ZMQ stream."""
    described = {"transport": "zmq", "address": ADDRESS, "encoder": ENCODER, **transport}
    info = {
        "signals": {
            signal: {
                "obj_name": f"{device}_{signal}",
                "component_name": signal,
                "signal_class": "PreviewSignal",
                "describe": {"signal_info": {"transports": [described]}},
            }
        }
    }
    client.device_manager.devices[device] = SimpleNamespace(_info=info)


class FakeStream:
    """What the path uses of a receiver: start, take_frame, decode, stop."""

    created: list["FakeStream"] = []

    def __init__(self):
        self.frames: queue.Queue = queue.Queue()
        self.started_with = None
        self.stopped = False
        FakeStream.created.append(self)

    def start(self, transport):
        self.started_with = transport

    def take_frame(self):
        frame = self.frames.get()
        if isinstance(frame, Exception):
            raise frame
        return frame

    def decode(self, frame):
        if frame.data is None:
            raise ValueError("malformed frame")
        return SimpleNamespace(data=frame.data)

    def stop(self):
        self.stopped = True
        self.frames.put(None)

    def send(self, data, stamp=1000.0):
        metadata = {} if stamp is None else {"timeStamp": stamp}
        timestamp = None if stamp is None else stamp + EPICS_EPOCH
        self.frames.put(SimpleNamespace(data=data, metadata=metadata, timestamp=timestamp))


@pytest.fixture
def api(monkeypatch):
    FakeStream.created = []
    monkeypatch.setitem(zmq_path.STREAMS, ENCODER, FakeStream)
    api = DataAPI(make_mock_client())
    advertise(api.client)
    path = next(path for path in api.paths if path.data_source == "zmq")
    path.max_rate = 1000.0
    yield api
    api.close()


def zmq_path_of(api):
    return next(path for path in api.paths if path.data_source == "zmq")


def subscribe(api, sources=(PREVIEW,)):
    updates = []
    sub = api.subscribe(
        sources=list(sources), scan=None, callback=updates.append, min_emit_interval=0, max_points=2
    )
    return sub, updates


def test_preview_frames_arrive_with_unix_time(api):
    sub, updates = subscribe(api)
    assert sub.routes == {PREVIEW: "zmq"}
    stream = FakeStream.created[0]
    assert stream.started_with.address == ADDRESS
    stream.send(np.ones((4, 3)), stamp=10.0)
    assert wait_for(lambda: len(updates[-1].get(*PREVIEW).values) == 1)
    source = updates[-1].get(*PREVIEW)
    assert source.values[-1].shape == (4, 3)
    assert source.timestamps[-1] == 10.0 + EPICS_EPOCH
    sub.close()


@pytest.mark.parametrize("stamp", [None, 0.0])
def test_a_frame_without_a_time_stamp_is_stamped_on_arrival(api, stamp):
    sub, updates = subscribe(api)
    before = time.time()
    FakeStream.created[0].send(np.ones((2, 2)), stamp=stamp)
    assert wait_for(lambda: len(updates[-1].get(*PREVIEW).values) == 1)
    assert before <= updates[-1].get(*PREVIEW).timestamps[-1] <= time.time()
    sub.close()


def test_two_subscribers_share_one_receiver_and_the_last_one_stops_it(api):
    first, _ = subscribe(api)
    second, updates = subscribe(api)
    assert len(FakeStream.created) == 1
    stream = FakeStream.created[0]
    thread = zmq_path_of(api).feeds[ADDRESS].thread
    stream.send(np.zeros((2, 2)))
    assert wait_for(lambda: len(updates[-1].get(*PREVIEW).values) == 1)
    first.close()
    assert not stream.stopped and thread.is_alive()
    second.close()
    assert wait_for(lambda: stream.stopped and not thread.is_alive())
    assert not zmq_path_of(api).feeds


def test_a_malformed_frame_does_not_stop_the_stream(api):
    sub, updates = subscribe(api)
    stream = FakeStream.created[0]
    stream.send(None)
    stream.send(np.zeros((2, 2)))
    assert wait_for(lambda: len(updates[-1].get(*PREVIEW).values) == 1)
    sub.close()


def test_a_receiver_that_broke_is_reported_and_replaced_for_the_next_subscriber(api):
    first, updates = subscribe(api)
    broken = FakeStream.created[0]
    broken.frames.put(RuntimeError("socket closed"))
    assert wait_for(lambda: "socket closed" in updates[-1].get(*PREVIEW).metadata.get("error", ""))
    assert wait_for(lambda: broken.stopped and not zmq_path_of(api).feeds)

    second, fresh = subscribe(api)
    assert len(FakeStream.created) == 2
    FakeStream.created[1].send(np.zeros((2, 2)))
    assert wait_for(lambda: len(fresh[-1].get(*PREVIEW).values) == 1)
    first.close()  # the request of the broken receiver leaves the new one alone
    assert not FakeStream.created[1].stopped
    second.close()
    assert wait_for(lambda: FakeStream.created[1].stopped and not zmq_path_of(api).feeds)


def test_transport_is_found_by_component_name_and_by_signal_name(api):
    path = zmq_path_of(api)
    for entry in ("preview", "cam_preview"):
        assert path.transport_of(("cam", entry)).address == ADDRESS
    assert path.transport_of(("cam", "other")) is None


@pytest.mark.parametrize(
    "transport",
    [{"socket_type": "pull"}, {"encoder": "something-else"}, {"transport": "redis"}],
    ids=["pull socket", "unknown encoder", "redis transport"],
)
def test_other_transports_are_left_to_the_redis_path(api, transport):
    advertise(api.client, device="other", **transport)
    info = api.describe([PREVIEW, ("other", "preview")], scan=None)
    assert info[PREVIEW].data_source == "zmq"
    assert info[("other", "preview")].data_source == "redis"


def test_previews_come_from_redis_without_pyzmq(api, monkeypatch):
    monkeypatch.setattr(zmq_path, "pyzmq_installed", lambda: False)
    assert api.describe([PREVIEW], scan=None)[PREVIEW].data_source == "redis"


def test_frames_of_a_real_publisher_arrive():
    """The whole chain once: ZMQ PUB socket, ADZMQ receiver, data path, subscription."""
    zmq = pytest.importorskip("zmq")
    publisher = zmq.Context.instance().socket(zmq.PUB)
    port = publisher.bind_to_random_port("tcp://127.0.0.1")
    api = DataAPI(make_mock_client())
    advertise(api.client, address=f"tcp://127.0.0.1:{port}")
    image = np.arange(12, dtype=np.uint16).reshape(3, 4)
    header = {"htype": "array-1.0", "shape": [3, 4], "type": "uint16", "timeStamp": 100.0}
    updates = []
    sub = api.subscribe(sources=[PREVIEW], scan=None, callback=updates.append, max_points=1)

    def frame_arrived() -> bool:
        # A subscriber that is still connecting misses frames: publish until one arrives.
        publisher.send_multipart([json.dumps(header).encode(), image.tobytes()])
        return bool(updates) and len(updates[-1].get(*PREVIEW).values) == 1

    try:
        assert wait_for(frame_arrived, timeout=5)
        source = updates[-1].get(*PREVIEW)
        assert np.array_equal(source.values[-1], image)
        assert source.timestamps[-1] == 100.0 + EPICS_EPOCH
    finally:
        sub.close()
        api.close()
        publisher.close(linger=0)
