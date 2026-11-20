"""
ZMQ data path: detector previews straight from the detector's ADZMQ publisher.

A preview signal keeps its usual source key, for example ``("cam", "preview")``. The path
sits ahead of Redis (priority 5): a preview signal whose ``signal_info.transports``
advertises a ZMQ SUB stream is read from the detector directly, every other source falls
through to the Redis path. Consumers and widgets do not change.

The receiver is ``bec_lib.zmq_transports.ADZMQStream``. It needs PyZMQ (the ``adzmq`` extra
of bec_lib); without it this path declines every source and previews are served from Redis
as before. Only SUB streams are used: a PULL socket would split the frames between clients.
"""

from __future__ import annotations

import importlib.util
import threading
import time
from dataclasses import dataclass, field

from bec_lib.logger import bec_logger
from bec_lib.messages import ZMQSignalTransport
from bec_lib.zmq_transports import ADZMQStream, SignalStream

from ..data_path import BindContext, DataPath, DataSource, SourceRequest, SourceSpec
from ..decoding import decode_stream_values, device_signals
from ..models import DeviceStream, SourceKey, SourceKind, UpdateReason
from ..scenarios import Scenario

logger = bec_logger.logger

#: The receivers by the encoder a transport advertises.
STREAMS: dict[str, type[SignalStream]] = {"adzmq-array-1.0": ADZMQStream}
#: Seconds a receiver gets to stop when the path disconnects.
_STOP_TIMEOUT = 2.0


def pyzmq_installed() -> bool:
    """Whether PyZMQ is installed; the receiver imports it only when a stream starts."""
    return importlib.util.find_spec("zmq") is not None


@dataclass(eq=False)
class _Feed:
    """One receiver for one address, shared by every request that reads from it."""

    address: str
    stream: SignalStream
    requests: dict[SourceRequest, list[SourceSpec]] = field(default_factory=dict)
    stopping: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None


class ZMQDataPath(DataPath):
    """Serves detector previews from ZMQ publishers, ahead of the Redis preview."""

    data_source = DataSource.ZMQ
    scenarios = frozenset({Scenario.NO_SCAN})
    priority = 5

    #: Frames decoded per second and receiver; a display cannot show more.
    max_rate = 30.0

    def __init__(self, client, lock: threading.RLock | None = None):
        super().__init__(client, lock)
        #: The running receivers by address.
        self.feeds: dict[str, _Feed] = {}
        # Receivers whose last request closed: stopped and joined outside the lock (see join).
        self._retired: dict[SourceRequest, list[_Feed]] = {}

    # --- routing ----------------------------------------------------------------------

    def transport_of(self, key: SourceKey) -> ZMQSignalTransport | None:
        """
        The ZMQ SUB stream a signal advertises in its signal info.

        Args:
            key (SourceKey): (device, entry); the entry is the component or the signal name.

        Returns:
            ZMQSignalTransport | None: The transport, or None if the signal has none that
                this path can receive.
        """
        for component, info in (device_signals(self.client, key[0]) or {}).items():
            if not isinstance(info, dict) or key[1] not in (component, info.get("obj_name")):
                continue
            described = (info.get("describe") or {}).get("signal_info") or {}
            for item in described.get("transports") or []:
                usable = item.get("transport") == "zmq" and item.get("encoder") in STREAMS
                if usable and item.get("socket_type", "sub") == "sub":
                    return ZMQSignalTransport.model_validate(item)
        return None

    def resolve(self, sources: list[SourceKey], context: BindContext) -> list[SourceSpec] | None:
        if not pyzmq_installed():
            return None
        return [
            SourceSpec(*key, kind=SourceKind.UNINDEXED, stream=DeviceStream.PREVIEW)
            for key in sources
            if self.transport_of(tuple(key)) is not None
        ]

    # --- requests ---------------------------------------------------------------------

    def _open(self, request: SourceRequest) -> None:
        # A stream has nothing from before the subscription: the first update of a source
        # is its first frame. Only a source that cannot be opened is reported at once.
        failed = False
        for spec in request.specs:
            transport = self.transport_of(spec.key)
            if transport is None:  # withdrawn since the request was resolved (config reload)
                request.set_status(spec.key, error="The signal advertises no ZMQ stream any more.")
                failed = True
                continue
            feed = self.feeds.get(transport.address) or self._start_feed(transport)
            feed.requests.setdefault(request, []).append(spec)
        if failed:
            request.notify(UpdateReason.LIVE)

    def _start_feed(self, transport: ZMQSignalTransport) -> _Feed:
        feed = _Feed(transport.address, STREAMS[transport.encoder]())
        feed.stream.start(transport)
        feed.thread = threading.Thread(
            target=self._consume, args=(feed,), name="data-api-zmq", daemon=True
        )
        self.feeds[transport.address] = feed
        feed.thread.start()
        return feed

    def close(self, request: SourceRequest) -> None:
        # Under the lock: leave the receivers; one without requests is retired for join().
        retired = []
        for feed in [feed for feed in self.feeds.values() if request in feed.requests]:
            del feed.requests[request]
            if not feed.requests:
                del self.feeds[feed.address]
                feed.stopping.set()
                retired.append(feed)
        if retired:
            self._retired[request] = retired

    def join(self, request: SourceRequest, timeout: float) -> None:
        with self.lock:
            retired = self._retired.pop(request, [])
        for feed in retired:
            self._stop_feed(feed, timeout)

    def disconnect(self) -> None:
        with self.lock:
            feeds = list(self.feeds.values())
            for retired in self._retired.values():
                feeds.extend(retired)
            self.feeds.clear()
            self._retired.clear()
        for feed in feeds:
            feed.stopping.set()
            self._stop_feed(feed, _STOP_TIMEOUT)

    @staticmethod
    def _stop_feed(feed: _Feed, timeout: float) -> None:
        feed.stream.stop()  # joins the receiver and wakes the consumer
        if feed.thread is not None and feed.thread is not threading.current_thread():
            feed.thread.join(timeout)

    # --- frames -----------------------------------------------------------------------

    def _consume(self, feed: _Feed) -> None:
        """Decode the frames of one receiver and hand them to its requests."""
        while not feed.stopping.is_set():
            try:
                frame = feed.stream.take_frame()
            except ValueError as exc:  # one message with a malformed header
                logger.warning(f"Dropped a ZMQ frame from {feed.address}: {exc}")
                continue
            except Exception as exc:  # pylint: disable=broad-except
                logger.opt(exception=exc).error(f"The ZMQ receiver of {feed.address} broke.")
                self._give_up(feed, f"ZMQ receiver stopped: {exc}")
                return
            if frame is None:  # the receiver was stopped
                return
            try:
                image = feed.stream.decode(frame)
            except Exception as exc:  # pylint: disable=broad-except
                # One frame that cannot be decoded (a truncated JPEG, a wrong size); the
                # next may be fine.
                logger.warning(f"Dropped a ZMQ frame from {feed.address}: {exc}")
                continue
            # A publisher that leaves its frames unstamped must not date them to 1990.
            stamped = frame.timestamp is not None and frame.metadata.get("timeStamp")
            timestamp = frame.timestamp if stamped else time.time()
            touched = []
            with self.lock:
                for request, specs in feed.requests.items():
                    points = [(spec, image.data, timestamp) for spec in specs]
                    if decode_stream_values(request.bundle, points, DeviceStream.PREVIEW):
                        touched.append(request)
            for request in touched:
                request.notify(UpdateReason.LIVE)
            feed.stopping.wait(1 / self.max_rate)

    def _give_up(self, feed: _Feed, error: str) -> None:
        """
        Drop a receiver that broke. Its requests show ``error``; the next request for the
        address starts a new receiver.
        """
        with self.lock:
            if self.feeds.get(feed.address) is feed:
                del self.feeds[feed.address]
            requests = list(feed.requests)
            feed.requests.clear()
        feed.stopping.set()
        self.report(error, requests)
        feed.stream.stop()
