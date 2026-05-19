import time
from collections import deque
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.devices.rate_limited_pipeline_publisher import (
    RateLimitedPipelinePublisher,
    _PublishMethod,
    _TopicState,
)


@pytest.fixture
def publisher():
    connector = ConnectorMock("")
    publisher = RateLimitedPipelinePublisher(connector_getter=lambda: connector, rate_limit_s=0.1)
    try:
        yield publisher, connector
    finally:
        publisher.shutdown()


def test_rate_limited_publish_flushes_latest_same_topic(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 0.05
    connector.message_sent.clear()

    first = messages.ProgressMessage(value=1, max_value=10, done=False, metadata={"seq": 1})
    second = messages.ProgressMessage(value=2, max_value=10, done=False, metadata={"seq": 2})
    third = messages.ProgressMessage(value=2, max_value=10, done=False, metadata={"seq": 2})

    publisher.publish_set_and_publish(MessageEndpoints.device_progress("samx"), lambda: first)
    publisher.publish_set_and_publish(MessageEndpoints.device_progress("samx"), lambda: second)
    publisher.publish_set_and_publish(MessageEndpoints.device_progress("samx"), lambda: third)

    deadline = time.time() + 1
    progress_msgs = []
    while time.time() < deadline:
        progress_msgs = [
            msg
            for msg in connector.message_sent
            if msg["queue"] == MessageEndpoints.device_progress("samx").endpoint
        ]
        if len(progress_msgs) >= 2:
            break
        time.sleep(0.01)

    assert progress_msgs == [
        {"queue": MessageEndpoints.device_progress("samx").endpoint, "msg": first, "expire": None},
        {"queue": MessageEndpoints.device_progress("samx").endpoint, "msg": third, "expire": None},
    ]


def test_rate_limited_publish_is_per_topic(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 0.5
    connector.message_sent.clear()

    publisher.publish_set_and_publish(
        MessageEndpoints.device_progress("samx"),
        lambda: messages.ProgressMessage(value=1, max_value=2, done=False, metadata={}),
    )
    publisher.publish_set_and_publish(
        MessageEndpoints.device_progress("samy"),
        lambda: messages.ProgressMessage(value=2, max_value=2, done=False, metadata={}),
    )

    deadline = time.time() + 1
    progress_msgs = []
    while time.time() < deadline:
        progress_msgs = [
            msg
            for msg in connector.message_sent
            if msg["queue"]
            in (
                MessageEndpoints.device_progress("samx").endpoint,
                MessageEndpoints.device_progress("samy").endpoint,
            )
        ]
        if len(progress_msgs) >= 2:
            break
        time.sleep(0.01)

    assert progress_msgs == [
        {
            "queue": MessageEndpoints.device_progress("samx").endpoint,
            "msg": messages.ProgressMessage(value=1, max_value=2, done=False, metadata={}),
            "expire": None,
        },
        {
            "queue": MessageEndpoints.device_progress("samy").endpoint,
            "msg": messages.ProgressMessage(value=2, max_value=2, done=False, metadata={}),
            "expire": None,
        },
    ]


def test_rate_limited_publish_warns_when_input_rate_exceeds_threshold(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 10
    publisher._high_update_rate_warning_hz = 2
    publisher._high_update_rate_warning_cycles = 1

    key = (_PublishMethod.SET_AND_PUBLISH.value, MessageEndpoints.device_progress("samx").endpoint)
    state = _TopicState(
        recent_insert_counts=deque(maxlen=publisher._high_update_rate_warning_cycles)
    )

    with mock.patch.object(connector, "raise_alarm") as raise_alarm:
        for _ in range(21):
            state.inserted_in_cooldown += 1
        publisher._start_cooldown_window_locked(key, state)

    raise_alarm.assert_called_once()
    assert raise_alarm.call_args.kwargs["severity"] == Alarms.WARNING
    info = raise_alarm.call_args.kwargs["info"]
    assert info.device == MessageEndpoints.device_progress("samx").endpoint
    assert (
        "averaged 21.0 updates per 10.000s cooldown window across the last 1 windows"
        in info.error_message
    )
    assert "rolling average ~2.1 Hz exceeds 2.0 Hz" in info.compact_error_message
    assert info.exception_type == "Warning"


def test_rate_limited_publish_does_not_warn_on_single_cycle_spike_by_default(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 10
    publisher._high_update_rate_warning_hz = 2
    publisher._high_update_rate_warning_cycles = 3

    with mock.patch.object(connector, "raise_alarm") as raise_alarm:
        for index in range(21):
            publisher.publish_set_and_publish(
                MessageEndpoints.device_progress("samx"),
                lambda index=index: messages.ProgressMessage(
                    value=index, max_value=21, done=False, metadata={"seq": index}
                ),
            )

    raise_alarm.assert_not_called()


def test_rate_limited_publish_warns_on_sustained_high_rate_across_multiple_cycles(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 10
    publisher._high_update_rate_warning_hz = 2
    publisher._high_update_rate_warning_cycles = 3

    key = (_PublishMethod.SET_AND_PUBLISH.value, MessageEndpoints.device_progress("samx").endpoint)
    state = _TopicState()

    with mock.patch.object(connector, "raise_alarm") as raise_alarm:
        for count in (21, 21):
            for _ in range(count):
                state.inserted_in_cooldown += 1
            publisher._start_cooldown_window_locked(key, state)

        for _ in range(21):
            state.inserted_in_cooldown += 1
        publisher._start_cooldown_window_locked(key, state)

    raise_alarm.assert_called_once()
    info = raise_alarm.call_args.kwargs["info"]
    assert info.device == MessageEndpoints.device_progress("samx").endpoint
    assert "across the last 3 windows" in info.error_message
    assert "rolling average ~2.1 Hz exceeds 2.0 Hz" in info.compact_error_message


def test_rate_limited_publish_includes_idle_cycles_in_rolling_average(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 10
    publisher._high_update_rate_warning_hz = 2
    publisher._high_update_rate_warning_cycles = 3

    key = (_PublishMethod.SET_AND_PUBLISH.value, MessageEndpoints.device_progress("samx").endpoint)
    state = _TopicState(
        recent_insert_counts=deque(maxlen=publisher._high_update_rate_warning_cycles - 1)
    )

    with mock.patch.object(connector, "raise_alarm") as raise_alarm:
        for _ in range(21):
            state.inserted_in_cooldown += 1
        publisher._start_cooldown_window_locked(key, state)

        # Simulate one idle cooldown window between bursts.
        publisher._start_cooldown_window_locked(key, state)

        for _ in range(21):
            state.inserted_in_cooldown += 1
        publisher._start_cooldown_window_locked(key, state)

    raise_alarm.assert_not_called()


def test_rate_limited_publish_disables_high_rate_alarm_when_threshold_is_non_positive(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 10
    publisher._high_update_rate_warning_hz = 0

    key = (_PublishMethod.SET_AND_PUBLISH.value, MessageEndpoints.device_progress("samx").endpoint)
    state = _TopicState(
        recent_insert_counts=deque(maxlen=publisher._high_update_rate_warning_cycles)
    )

    with mock.patch.object(connector, "raise_alarm") as raise_alarm:
        for _ in range(21):
            state.inserted_in_cooldown += 1
        publisher._start_cooldown_window_locked(key, state)

    raise_alarm.assert_not_called()


def test_rate_limited_publish_repeats_alarm_only_after_period(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 10
    publisher._high_update_rate_warning_hz = 2
    publisher._high_update_rate_warning_cycles = 1
    publisher._high_update_rate_warning_period_s = 30

    key = (_PublishMethod.SET_AND_PUBLISH.value, MessageEndpoints.device_progress("samx").endpoint)
    state = _TopicState(
        recent_insert_counts=deque(maxlen=publisher._high_update_rate_warning_cycles)
    )

    with (
        mock.patch.object(connector, "raise_alarm") as raise_alarm,
        mock.patch(
            "bec_server.device_server.devices.rate_limited_pipeline_publisher.time.monotonic",
            side_effect=[100.0, 120.0, 131.0],
        ),
    ):
        for _ in range(3):
            state.inserted_in_cooldown = 21
            publisher._start_cooldown_window_locked(key, state)

    assert raise_alarm.call_count == 2
