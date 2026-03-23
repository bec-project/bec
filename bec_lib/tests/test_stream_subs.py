import time
from collections import deque
from functools import partial
from queue import Queue
from threading import Event, Thread
from unittest.mock import MagicMock

import pytest
from louie.saferef import safe_ref

from bec_lib.redis_connector import (
    DirectReadStreamSubInfo,
    RedisConnector,
    StreamSubInfo,
    StreamSubs,
)


@pytest.fixture
def stream_subs():
    return StreamSubs()


def _test_cb1(*_): ...
def _test_cb2(*_): ...


def test_add_stream_sub(stream_subs: StreamSubs):
    stream_subs.add(False, "0-0", "test", StreamSubInfo(safe_ref(_test_cb1), {}))
    assert len(stream_subs._subs["test"].subs) == 1
    stream_subs.add(False, "0-0", "test", StreamSubInfo(safe_ref(_test_cb2), {}))
    assert len(stream_subs._subs["test"].subs) == 2


def test_remove_stream_sub(stream_subs: StreamSubs):
    stream_subs.add(False, "0-0", "test", StreamSubInfo(safe_ref(_test_cb1), {}))
    stream_subs.add(False, "0-0", "test", StreamSubInfo(safe_ref(_test_cb2), {}))
    assert len(stream_subs._subs["test"].subs) == 2
    stream_subs.remove("test", _test_cb1)
    assert len(stream_subs._subs["test"].subs) == 1
    stream_subs.remove("test", _test_cb2)
    assert "test" not in stream_subs._subs


def test_add_and_remove_from_start(stream_subs: StreamSubs):
    stream_subs.add(True, "0-0", "test", StreamSubInfo(safe_ref(_test_cb1), {}))
    stream_subs.add(True, "0-0", "test", StreamSubInfo(safe_ref(_test_cb2), {}))
    assert len(stream_subs.from_start_subs["test"]) == 2
    stream_subs.remove("test", _test_cb1)
    stream_subs.remove("test", _test_cb2)
    assert "test" not in stream_subs.from_start_subs


def test_add_and_remove_direct_read(stream_subs: StreamSubs):
    connector_self = MagicMock()
    connector_self._message_callbacks_queue = Queue()
    connector_self._redis_conn.xrevrange.return_value = None
    connector_self._direct_stream_listener = partial(
        RedisConnector._direct_stream_listener, connector_self
    )
    info = RedisConnector._create_direct_stream_listener(
        connector_self, "test", safe_ref(_test_cb1), {}
    )
    stream_subs.add_direct_listener("test", info)
    start = time.monotonic()
    while not info.thread.is_alive():
        time.sleep(0.01)
        if time.monotonic() > start + 0.2:
            raise TimeoutError()
    assert len(stream_subs._direct_read_subs["test"]) == 1
    stream_subs.remove("test", _test_cb1)
    assert not info.thread.is_alive()
    assert "test" not in stream_subs._direct_read_subs
