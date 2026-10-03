"""Opt-in retained snapshots for streams and SET_PUBLISH subscriptions."""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast
from unittest import mock

import louie
import pytest
from redis import Redis
from redis.exceptions import ConnectionError, NoPermissionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_lib.redis_connector.managed_redis_connection import ManagedRedisConnection
from bec_lib.serialization import MsgpackSerialization


def wait_for(predicate: Callable[[], bool]) -> None:
    """Wait for a listener-thread result with a bounded deadline."""
    deadline = time.monotonic() + 5
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.005)


@pytest.mark.parametrize("include_type", [False, True])
def test_pubsub_listener_accepts_untyped_messages_and_dispatch_stop(
    connected_connector: RedisConnector, include_type: bool
) -> None:
    """Downstream Pub/Sub doubles can omit type and emit the dispatcher stop sentinel."""
    managed = connected_connector._managed_connection
    topic = MessageEndpoints.device_readback("pubsub-double").endpoint
    message = messages.DeviceMessage(signals={"value": {"value": 42}})
    received: list[MessageObject] = []
    callback = lambda msg: received.append(msg)
    managed._topics_cb[topic] = [(cast(Any, louie.saferef.safe_ref(callback)), {})]
    managed._pending_replays[topic] = None
    event: dict[str, Any] = {
        "channel": topic.encode(),
        "pattern": None,
        "data": MsgpackSerialization.dumps(message),
    }
    if include_type:
        event["type"] = "message"
    events = iter([event, StopIteration])

    def poll(**_kwargs: Any) -> Any:
        event = next(events)
        if event is StopIteration:
            managed._stop_events_listener_thread.set()
        return event

    with mock.patch.object(managed._pubsub_conn, "get_message", side_effect=poll):
        managed._get_messages_loop()
    assert not managed._pending_replays
    assert managed.poll_messages() is False
    assert len(received) == 1
    assert received[0].value == message


def test_replay_only_reaches_opted_in_callbacks(connected_connector: RedisConnector) -> None:
    """Late subscribers receive retained state only when requesting replay."""
    endpoint = MessageEndpoints.device_readback("replay-test")
    connected_connector.set_and_publish(
        endpoint, messages.DeviceMessage(signals={"a": {"value": 4}})
    )
    normal: list[MessageObject[messages.DeviceMessage]] = []
    replayed: list[MessageObject[messages.DeviceMessage]] = []
    normal_cb = lambda msg: normal.append(msg)
    replay_cb = lambda msg: replayed.append(msg)
    connected_connector.register(endpoint, cb=normal_cb)
    connected_connector.register(endpoint, cb=replay_cb, replay_last=True)
    wait_for(lambda: bool(replayed))
    assert cast(messages.DeviceMessage, replayed[-1].value).signals == {"a": {"value": 4}}
    assert not normal
    connected_connector.unregister(endpoint, cb=replay_cb)
    assert endpoint.endpoint not in connected_connector._managed_connection._replay_callbacks


@pytest.mark.parametrize("during_read", [False, True])
def test_unregister_suppresses_pending_retained_delivery(
    connected_connector: RedisConnector, during_read: bool
) -> None:
    """Unregister suppresses snapshots already being read or waiting for dispatch."""
    endpoint = MessageEndpoints.device_readback("unregister-pending-replay")
    managed = connected_connector._managed_connection
    seen: list[Any] = []
    callback = lambda msg: seen.append(msg)
    message = messages.DeviceMessage(signals={"value": {"value": 42}})

    def read(_topic: str) -> messages.DeviceMessage:
        if during_read:
            connected_connector.unregister(endpoint, cb=callback)
        return message

    with mock.patch.object(managed, "_get_retained_value", side_effect=read):
        connected_connector.register(endpoint, cb=callback, replay_last=True, start_thread=False)
        wait_for(lambda: not managed._message_callbacks_queue.empty())
        if not during_read:
            connected_connector.unregister(endpoint, cb=callback)
        connected_connector.poll_messages(timeout=0)
    assert not seen


def test_live_polling_interleaves_best_effort_replays(connected_connector: RedisConnector) -> None:
    """Every retained read yields to live polling, and failed topics are dropped."""
    managed = connected_connector._managed_connection
    topics = [
        MessageEndpoints.device_readback(f"replay-fair-{index}").endpoint for index in range(3)
    ]
    callback = lambda msg: None
    for topic in topics:
        managed._replay_callbacks[topic] = [(louie.saferef.safe_ref(callback), {})]
        managed._pending_replays[topic] = None
    polls = 0
    attempts: list[tuple[str, int]] = []

    def poll(**_kwargs: Any) -> None:
        nonlocal polls
        polls += 1
        if polls == 4:
            managed._stop_events_listener_thread.set()

    def read(topic: str) -> None:
        attempts.append((topic, polls))
        raise RedisTimeoutError("retained connection timed out")

    with (
        mock.patch.object(managed._pubsub_conn, "get_message", side_effect=poll),
        mock.patch.object(managed, "_get_retained_value", side_effect=read),
    ):
        managed._get_messages_loop()
    assert attempts == list(zip(topics, [1, 2, 3]))
    assert not managed._pending_replays
    assert managed._message_callbacks_queue.empty()


def test_reregister_does_not_revive_a_removed_subscriptions_replay(
    connected_connector: RedisConnector,
) -> None:
    """A new registration of the same callable does not inherit an old queued snapshot."""
    endpoint = MessageEndpoints.device_readback("reregister-replay")
    managed = connected_connector._managed_connection
    seen: list[Any] = []
    callback = lambda msg: seen.append(msg)
    with mock.patch.object(managed, "_get_retained_value", return_value=None):
        connected_connector.register(endpoint, cb=callback, replay_last=True, start_thread=False)
        wait_for(lambda: not managed._message_callbacks_queue.empty())
        queued = managed._message_callbacks_queue.get_nowait()
        connected_connector.unregister(endpoint, cb=callback)
        connected_connector.register(endpoint, cb=callback, replay_last=True, start_thread=False)
        managed._handle_message(queued)
    assert not seen


def test_reconnect_reloads_missed_updates(connected_connector: RedisConnector) -> None:
    """Resubscription acknowledgements trigger a fresh retained read."""
    endpoint = MessageEndpoints.device_readback("replay-test")
    seen: list[MessageObject[messages.DeviceMessage]] = []
    callback = lambda msg: seen.append(msg)
    connected_connector.register(endpoint, cb=callback, replay_last=True)
    wait_for(lambda: bool(seen))
    managed = connected_connector._managed_connection
    managed._close_pubsub()
    connected_connector.set_and_publish(
        endpoint, messages.DeviceMessage(signals={"a": {"value": 9}})
    )
    managed._restart_pubsub()
    wait_for(
        lambda: any(
            isinstance(msg.value, messages.DeviceMessage)
            and msg.value.signals == {"a": {"value": 9}}
            for msg in seen
        )
    )


@pytest.mark.parametrize("patterns", [False, True])
def test_invalid_replay_does_not_start_listener(
    connected_connector: RedisConnector, patterns: bool
) -> None:
    """Invalid replay requests fail before any subscription or thread is created."""
    managed = connected_connector._managed_connection
    callback = mock.Mock()
    kwargs: dict[str, Any] = (
        {"patterns": "test*"} if patterns else {"topics": MessageEndpoints.stop_devices()}
    )
    with pytest.raises(ValueError, match="SET_PUBLISH"):
        connected_connector.register(**kwargs, cb=callback, replay_last=True)
    assert managed._events_listener_thread is None
    assert not managed._replay_callbacks


@pytest.mark.parametrize("operation", ["topic", "pattern", "stream"])
def test_unavailable_redis_on_startup_creates_no_threads_or_callbacks(
    connected_connector: RedisConnector, operation: str
) -> None:
    """Startup errors propagate before any listeners, dispatcher, or callbacks are admitted."""
    managed = connected_connector._managed_connection
    callback = lambda msg: None
    kwargs: dict[str, Any]
    if operation == "stream":
        kwargs = {"topics": MessageEndpoints.account(), "replay_last": True}
        client, method = managed._redis_conn, "xinfo_stream"
    elif operation == "pattern":
        kwargs = {"patterns": "startup*"}
        client, method = managed._pubsub_conn, "psubscribe"
    else:
        kwargs = {"topics": MessageEndpoints.device_readback("startup"), "replay_last": True}
        client, method = managed._pubsub_conn, "subscribe"
    with mock.patch.object(client, method, side_effect=ConnectionError("Redis unavailable")):
        with pytest.raises(ConnectionError, match="Redis unavailable"):
            connected_connector.register(**kwargs, cb=callback)
    assert managed._events_listener_thread is None
    assert managed._stream_events_listener_thread is None
    assert managed._events_dispatcher_thread is None
    assert not managed._topics_cb
    assert not managed._replay_callbacks
    assert not managed.any_stream_is_registered(MessageEndpoints.account(), callback)


def test_listener_can_stop_while_subscription_is_in_flight(
    connected_connector: RedisConnector,
) -> None:
    """An acknowledgement defers replay without blocking the listener on registration I/O."""
    managed = connected_connector._managed_connection
    topic = MessageEndpoints.scan_number().endpoint
    events = iter([{"type": "subscribe", "channel": topic.encode(), "data": 1}])

    def poll(**_kwargs: Any) -> dict | None:
        event = next(events, None)
        if event is None:
            managed._stop_events_listener_thread.set()
        return event

    listener = threading.Thread(target=managed._get_messages_loop)
    with mock.patch.object(managed._pubsub_conn, "get_message", side_effect=poll):
        managed._pubsub_registration_lock.acquire()
        try:
            listener.start()
            listener.join(timeout=1)
            assert not listener.is_alive()
            assert topic in managed._pending_replays
        finally:
            managed._stop_events_listener_thread.set()
            managed._pubsub_registration_lock.release()
            listener.join(timeout=5)


def test_initial_stream_replay_can_unregister_before_pubsub_listener_starts(
    connected_connector: RedisConnector,
) -> None:
    """Stream callback removal depends on its registry, even during initial dispatch."""
    managed = connected_connector._managed_connection
    endpoint = MessageEndpoints.account()
    connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="initial")})
    seen: list[Any] = []

    def callback(msg: Any) -> None:
        seen.append(msg)
        connected_connector.unregister(endpoint, cb=callback)

    def dispatch(_start_thread: bool) -> None:
        if not managed._message_callbacks_queue.empty():
            managed.poll_messages(0)

    with mock.patch.object(managed, "_start_events_dispatcher_thread", side_effect=dispatch):
        connected_connector.register(endpoint, cb=callback, replay_last=True, start_thread=False)
    assert len(seen) == 1
    assert not managed.any_stream_is_registered(endpoint, callback)


def test_live_message_before_admission_keeps_initial_replay(
    connected_connector: RedisConnector,
) -> None:
    """An early live notification cannot cancel replay before the callback is admitted."""
    managed = connected_connector._managed_connection
    topic = MessageEndpoints.device_readback("early-live").endpoint
    message = messages.DeviceMessage(signals={"value": {"value": 5}})
    seen: list[Any] = []
    callback = lambda msg: seen.append(msg)
    events = iter(
        [
            {"type": "subscribe", "channel": topic.encode(), "data": 1},
            {
                "type": "message",
                "channel": topic.encode(),
                "pattern": None,
                "data": MsgpackSerialization.dumps(message),
            },
        ]
    )

    def poll(**_kwargs: Any) -> dict | None:
        event = next(events, None)
        if event is not None:
            return event
        if managed._pubsub_registration_lock.locked():
            managed.poll_messages(0)
            assert not seen
            cb_ref = cast(louie.saferef.BoundMethodWeakref, louie.saferef.safe_ref(callback))
            item: tuple[louie.saferef.BoundMethodWeakref, dict[str, Any]] = (cb_ref, {})
            managed._topics_cb[topic].append(item)
            managed._replay_callbacks[topic] = [item]
            managed._pubsub_registration_lock.release()
        else:
            managed._stop_events_listener_thread.set()
        return None

    managed._pubsub_registration_lock.acquire()
    try:
        with (
            mock.patch.object(managed._pubsub_conn, "get_message", side_effect=poll),
            mock.patch.object(managed, "_get_retained_value", return_value=message) as read,
        ):
            managed._get_messages_loop()
        read.assert_called_once_with(topic)
        managed.poll_messages(0)
        assert len(seen) == 1
        assert seen[0].value == message
    finally:
        if managed._pubsub_registration_lock.locked():
            managed._pubsub_registration_lock.release()


def test_stream_replays_latest_then_delivers_new_entries(
    connected_connector: RedisConnector,
) -> None:
    """Stream replay delivers one latest entry followed by every subsequent update."""
    endpoint = MessageEndpoints.account()
    connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="old")})
    connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="latest")})
    seen: list[dict[str, messages.VariableMessage]] = []
    callback = lambda msg: seen.append(msg)
    connected_connector.register(endpoint, cb=callback, replay_last=True)
    wait_for(lambda: bool(seen))
    assert [msg["data"].value for msg in seen] == ["latest"]
    connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="next")})
    wait_for(lambda: len(seen) == 2)
    assert [msg["data"].value for msg in seen] == ["latest", "next"]


@pytest.mark.parametrize("patterns", [False, True])
def test_overlapping_registrations_preserve_success_after_failure(
    connected_connector: RedisConnector, patterns: bool
) -> None:
    """A failed attempt cannot roll back another successful registration of the same callback."""
    endpoint = MessageEndpoints.device_readback("overlapping-subscriptions")
    managed = connected_connector._managed_connection
    callback = lambda msg: None
    entered = threading.Event()
    release = threading.Event()
    second_started = threading.Event()
    original = managed._pubsub_conn.subscribe
    calls = 0

    def subscribe(topics: list[str]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            assert release.wait(timeout=5)
            raise ConnectionError("first attempt failed")
        original(topics)

    def register_second() -> None:
        second_started.set()
        if patterns:
            connected_connector.register(patterns=endpoint.endpoint, cb=callback)
        else:
            connected_connector.register(endpoint, cb=callback, replay_last=True)

    with (
        mock.patch.object(managed._pubsub_conn, "subscribe", side_effect=subscribe),
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        first = executor.submit(
            connected_connector.register, endpoint, cb=callback, replay_last=True
        )
        try:
            assert entered.wait(timeout=5)
            second = executor.submit(register_second)
            assert second_started.wait(timeout=5)
            with pytest.raises(TimeoutError):
                second.result(timeout=0.05)
        finally:
            release.set()
        with pytest.raises(ConnectionError, match="first attempt failed"):
            first.result(timeout=5)
        second.result(timeout=5)
    assert len(managed._topics_cb[endpoint.endpoint]) == 1
    assert managed._topics_cb[endpoint.endpoint][0][0]() is callback
    if patterns:
        assert endpoint.endpoint not in managed._replay_callbacks
    else:
        assert len(managed._replay_callbacks[endpoint.endpoint]) == 1


def test_stream_replay_preserves_existing_shared_cursor_delivery(
    connected_connector: RedisConnector,
) -> None:
    """A snapshot leaves normal buffered delivery intact, including repeated values."""
    endpoint = MessageEndpoints.account()
    connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="old")})
    normal: list[dict[str, messages.VariableMessage]] = []
    replayed: list[dict[str, messages.VariableMessage]] = []
    normal_cb = lambda msg: normal.append(msg)
    replay_cb = lambda msg: replayed.append(msg)
    connected_connector.register(endpoint, cb=normal_cb, start_thread=False)
    managed = connected_connector._managed_connection
    managed._stop_stream_events_listener_thread.set()
    listener = managed._stream_events_listener_thread
    assert listener is not None
    listener.join(timeout=5)
    assert not listener.is_alive()
    connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="pending")})
    connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="latest")})
    connected_connector.register(endpoint, cb=replay_cb, replay_last=True, start_thread=False)
    entries = cast(
        list[tuple[bytes, dict[bytes, bytes]]], managed._redis_conn.xrange(endpoint.endpoint)
    )[1:]
    managed._handle_stream_msg_list(
        [(endpoint.endpoint.encode(), entries)], managed._stream_subs.normal_subs
    )
    for _ in range(3):
        connected_connector.poll_messages(timeout=0)
    assert [msg["data"].value for msg in replayed] == ["latest", "pending", "latest"]
    assert [msg["data"].value for msg in normal] == ["pending", "latest"]


def test_empty_stream_replay_delivers_future_entries(connected_connector: RedisConnector) -> None:
    """An absent stream needs no synthetic initial entry and starts with its first write."""
    seen: list[dict[str, messages.VariableMessage]] = []
    callback = lambda msg: seen.append(msg)
    connected_connector.register(MessageEndpoints.account(), cb=callback, replay_last=True)
    connected_connector.xadd(
        MessageEndpoints.account(), {"data": messages.VariableMessage(value="first")}
    )
    wait_for(lambda: bool(seen))
    assert [msg["data"].value for msg in seen] == ["first"]


def test_stream_replay_rejects_full_history(connected_connector: RedisConnector) -> None:
    """Conflicting history options fail before listener creation."""
    callback = lambda msg: None
    with pytest.raises(ValueError, match="from_start"):
        connected_connector.register(
            MessageEndpoints.account(), cb=callback, replay_last=True, from_start=True
        )
    assert connected_connector._managed_connection._events_listener_thread is None


def test_failed_stream_snapshot_preserves_live_subscription(
    connected_connector: RedisConnector,
) -> None:
    """A failed optional snapshot emits nothing while future stream entries still arrive."""
    endpoint = MessageEndpoints.account()
    connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="current")})
    seen: list[dict[str, messages.VariableMessage]] = []
    callback = lambda msg: seen.append(msg)
    managed = connected_connector._managed_connection
    with mock.patch.object(managed, "get_last", side_effect=ConnectionError("read failed")) as read:
        connected_connector.register(endpoint, cb=callback, replay_last=True)
        assert managed.any_stream_is_registered(endpoint, callback)
        assert not seen
        connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="new")})
        wait_for(lambda: bool(seen))
        read.assert_called_once_with(endpoint.endpoint)
    assert [msg["data"].value for msg in seen] == ["new"]


@pytest.mark.parametrize("stream", [False, True])
def test_replay_preserves_callback_kwargs(
    connected_connector: RedisConnector, stream: bool
) -> None:
    """Replay uses the same callback arguments and message shape as live delivery."""
    endpoint = (
        MessageEndpoints.account() if stream else MessageEndpoints.device_readback("replay-test")
    )
    if stream:
        connected_connector.xadd(endpoint, {"data": messages.VariableMessage(value="current")})
    else:
        connected_connector.set_and_publish(
            endpoint, messages.DeviceMessage(signals={"a": {"value": 5}})
        )
    seen: list[str] = []

    def callback(msg: Any, *, tag: str) -> None:
        assert isinstance(msg, dict if stream else MessageObject)
        seen.append(tag)

    connected_connector.register(endpoint, cb=callback, replay_last=True, tag="replay")
    wait_for(lambda: bool(seen))
    assert seen[0] == "replay"


def test_failed_replay_does_not_block_live_delivery_or_other_replays(
    connected_connector: RedisConnector,
) -> None:
    """An unreadable retained key leaves healthy subscriptions operational."""
    blocked = MessageEndpoints.device_readback("blocked-replay")
    live = MessageEndpoints.device_readback("healthy-live")
    retained = MessageEndpoints.device_readback("healthy-replay")
    managed = connected_connector._managed_connection
    original = managed._get_retained_value
    attempts = 0
    live_seen: list[Any] = []
    replay_seen: list[Any] = []
    blocked_cb = lambda msg: None
    live_cb = lambda msg: live_seen.append(msg)
    replay_cb = lambda msg: replay_seen.append(msg)

    def read(topic: str) -> Any:
        nonlocal attempts
        if topic == blocked.endpoint:
            attempts += 1
            raise NoPermissionError("GET denied while SUBSCRIBE is allowed")
        return original(topic)

    connected_connector.set_and_publish(
        retained, messages.DeviceMessage(signals={"value": {"value": 3}})
    )
    with mock.patch.object(managed, "_get_retained_value", side_effect=read):
        connected_connector.register(blocked, cb=blocked_cb, replay_last=True)
        wait_for(lambda: attempts > 0)
        connected_connector.register(live, cb=live_cb)
        connected_connector.register(retained, cb=replay_cb, replay_last=True)
        connected_connector.set_and_publish(
            live, messages.DeviceMessage(signals={"value": {"value": 7}})
        )
        wait_for(lambda: bool(live_seen) and bool(replay_seen))
        assert live_seen[-1].value.signals["value"]["value"] == 7
        assert replay_seen[-1].value.signals["value"]["value"] == 3
        assert blocked.endpoint not in managed._pending_replays
        assert attempts == 1


@pytest.mark.parametrize("existing", [False, True])
def test_failed_batch_stream_replay_preserves_other_snapshots_and_live_delivery(
    connected_connector: RedisConnector, existing: bool
) -> None:
    """A failed optional snapshot leaves the batch subscribed and healthy snapshots delivered."""
    endpoints = [MessageEndpoints.device_raw("batch-a"), MessageEndpoints.device_raw("batch-b")]
    for endpoint in endpoints:
        connected_connector.xadd(
            endpoint, {"data": messages.DeviceMessage(signals={"value": {"value": 1}})}
        )
    managed = connected_connector._managed_connection
    original = managed.get_last
    seen: list[Any] = []
    callback = lambda msg: seen.append(msg)
    existing_cb = lambda msg: None
    if existing:
        connected_connector.register(endpoints[0], cb=existing_cb)

    def read(topic: str) -> Any:
        if topic == endpoints[1].endpoint:
            raise ConnectionError("second snapshot failed")
        return original(topic)

    with mock.patch.object(managed, "get_last", side_effect=read) as snapshot_read:
        connected_connector.register(endpoints, cb=callback, replay_last=True)
        for endpoint in endpoints:
            assert managed.any_stream_is_registered(endpoint, callback)
        wait_for(lambda: len(seen) == 1)
        if existing:
            assert managed.any_stream_is_registered(endpoints[0], existing_cb)
        connected_connector.xadd(
            endpoints[1], {"data": messages.DeviceMessage(signals={"value": {"value": 2}})}
        )
        wait_for(lambda: len(seen) == 2)
        assert snapshot_read.call_count == 2
    assert [msg["data"].signals["value"]["value"] for msg in seen] == [1, 2]


def test_live_poll_satisfies_pending_replay_without_retained_read(
    connected_connector: RedisConnector,
) -> None:
    """A same-topic live message after its acknowledgement replaces the pending GET."""
    endpoint = MessageEndpoints.device_readback("live-replaces-replay")
    seen: list[Any] = []
    callback = lambda msg: seen.append(msg)
    managed = connected_connector._managed_connection
    connected_connector.register(endpoint, cb=callback, replay_last=True, start_thread=False)
    managed._close_pubsub()
    while True:
        try:
            connected_connector.poll_messages(timeout=0)
        except TimeoutError:
            break
    seen.clear()
    managed._pending_replays.clear()
    managed._stop_events_listener_thread.clear()
    message = messages.DeviceMessage(signals={"value": {"value": 8}})
    events = iter(
        [
            {"type": "subscribe", "channel": endpoint.endpoint.encode(), "data": 1},
            {
                "type": "message",
                "channel": endpoint.endpoint.encode(),
                "pattern": None,
                "data": MsgpackSerialization.dumps(message),
            },
        ]
    )

    def poll(**_kwargs: Any) -> dict | None:
        event = next(events, None)
        if event is None:
            managed._stop_events_listener_thread.set()
        return event

    with (
        mock.patch.object(managed._pubsub_conn, "get_message", side_effect=poll),
        mock.patch.object(managed, "_get_retained_value") as read,
    ):
        managed._get_messages_loop()
    read.assert_not_called()
    connected_connector.poll_messages(timeout=0)
    assert len(seen) == 1
    assert seen[0].value == message
    assert endpoint.endpoint not in managed._pending_replays


def test_retained_read_times_out_on_an_unresponsive_server() -> None:
    """A server that accepts TCP but never responds cannot hold the listener indefinitely."""
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        connector = ManagedRedisConnection(f"127.0.0.1:{server.getsockname()[1]}")
        try:
            started = time.monotonic()
            with pytest.raises(RedisTimeoutError):
                connector._get_retained_value(MessageEndpoints.scan_number().endpoint)
            assert time.monotonic() - started < 3
        finally:
            connector.shutdown()


def test_replay_batch_observes_stop_between_reads(connected_connector: RedisConnector) -> None:
    """Shutdown skips remaining pending reads after the current bounded operation completes."""
    managed = connected_connector._managed_connection
    endpoints = [MessageEndpoints.device_readback(name).endpoint for name in ("stop-a", "stop-b")]
    callback = lambda msg: None
    for topic in endpoints:
        managed._replay_callbacks[topic] = [(lambda: callback, {})]
        managed._pending_replays[topic] = None

    def read(_topic: str) -> None:
        managed._stop_events_listener_thread.set()
        raise RedisTimeoutError("server stopped responding")

    with mock.patch.object(managed, "_get_retained_value", side_effect=read) as retained_read:
        managed._replay_retained_values()
    retained_read.assert_called_once_with(endpoints[0])
    assert endpoints[1] in managed._pending_replays


@pytest.mark.parametrize("timeout", [None, 0.1, 10.0])
def test_retained_read_uses_an_independent_bounded_pool(
    connected_connector: RedisConnector, timeout: float | None
) -> None:
    """Replay bounds preserve smaller configured timeouts and leave ordinary commands intact."""
    managed = connected_connector._managed_connection
    original_kwargs = managed._redis_conn.connection_pool.connection_kwargs
    original_kwargs["socket_timeout"] = timeout
    original_kwargs["socket_connect_timeout"] = timeout
    expected = 1.0 if timeout is None else min(timeout, 1.0)
    with mock.patch.object(Redis, "from_pool", wraps=Redis.from_pool) as create_client:
        assert managed._get_retained_value(MessageEndpoints.scan_number().endpoint) is None
    replay_pool = create_client.call_args.args[0]
    assert replay_pool is not managed._redis_conn.connection_pool
    assert replay_pool.connection_kwargs["socket_timeout"] == expected
    assert replay_pool.connection_kwargs["socket_connect_timeout"] == expected
    assert replay_pool.connection_kwargs["retry"].get_retries() == 0
    assert replay_pool.connection_kwargs["retry_on_timeout"] is False
    assert original_kwargs["socket_timeout"] == timeout
    assert original_kwargs["socket_connect_timeout"] == timeout
