from unittest import mock

import pytest
import redis

from bec_lib.alarm_handler import Alarms
from bec_lib.connector import ConsumerConnectorError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import AlarmMessage, LogMessage
from bec_lib.redis_connector import (
    MessageObject,
    RedisConnector,
    RedisConsumer,
    RedisConsumerMixin,
    RedisConsumerThreaded,
    RedisProducer,
    RedisStreamConsumerThreaded,
)


@pytest.fixture
def producer():
    with mock.patch("bec_lib.redis_connector.redis.Redis"):
        prod = RedisProducer("localhost", 1)
        yield prod


@pytest.fixture
def connector():
    with mock.patch("bec_lib.redis_connector.redis.Redis"):
        connector = RedisConnector("localhost:1")
        yield connector


@pytest.fixture
def consumer():
    with mock.patch("bec_lib.redis_connector.redis.Redis"):
        consumer = RedisConsumer("localhost", "1", topics="topics")
        yield consumer


@pytest.fixture
def consumer_threaded():
    with mock.patch("bec_lib.redis_connector.redis.Redis"):
        consumer_threaded = RedisConsumerThreaded("localhost", "1", topics="topics")
        yield consumer_threaded


@pytest.fixture
def mixin():
    with mock.patch("bec_lib.redis_connector.redis.Redis"):
        mixin = RedisConsumerMixin
        yield mixin


def test_redis_connector_producer(connector):
    ret = connector.producer()
    assert isinstance(ret, RedisProducer)


@pytest.mark.parametrize(
    "topics, threaded", [["topics", True], ["topics", False], [None, True], [None, False]]
)
def test_redis_connector_consumer(connector, threaded, topics):
    pattern = None
    len_of_threads = len(connector._threads)

    if threaded:
        if topics is None and pattern is None:
            with pytest.raises(ValueError) as exc_info:
                ret = connector.consumer(
                    topics=topics, threaded=threaded, cb=lambda *args, **kwargs: ...
                )

            assert exc_info.value.args[0] == "Topics must be set for threaded consumer"
        else:
            ret = connector.consumer(
                topics=topics, threaded=threaded, cb=lambda *args, **kwargs: ...
            )
            assert len(connector._threads) == len_of_threads + 1
            assert isinstance(ret, RedisConsumerThreaded)

    else:
        if not topics:
            with pytest.raises(ConsumerConnectorError):
                ret = connector.consumer(
                    topics=topics, threaded=threaded, cb=lambda *args, **kwargs: ...
                )
            return
        ret = connector.consumer(topics=topics, threaded=threaded, cb=lambda *args, **kwargs: ...)
        assert isinstance(ret, RedisConsumer)


def test_redis_connector_log_warning(connector):
    connector._notifications_producer.send = mock.MagicMock()

    connector.log_warning("msg")
    connector._notifications_producer.send.assert_called_once_with(
        MessageEndpoints.log(), LogMessage(log_type="warning", content="msg").dumps()
    )


def test_redis_connector_log_message(connector):
    connector._notifications_producer.send = mock.MagicMock()

    connector.log_message("msg")
    connector._notifications_producer.send.assert_called_once_with(
        MessageEndpoints.log(), LogMessage(log_type="log", content="msg").dumps()
    )


def test_redis_connector_log_error(connector):
    connector._notifications_producer.send = mock.MagicMock()

    connector.log_error("msg")
    connector._notifications_producer.send.assert_called_once_with(
        MessageEndpoints.log(), LogMessage(log_type="error", content="msg").dumps()
    )


@pytest.mark.parametrize(
    "severity, alarm_type, source, content, metadata",
    [
        [Alarms.MAJOR, "alarm", "source", {"content": "content1"}, {"metadata": "metadata1"}],
        [Alarms.MINOR, "alarm", "source", {"content": "content1"}, {"metadata": "metadata1"}],
        [Alarms.WARNING, "alarm", "source", {"content": "content1"}, {"metadata": "metadata1"}],
    ],
)
def test_redis_connector_raise_alarm(connector, severity, alarm_type, source, content, metadata):
    connector._notifications_producer.set_and_publish = mock.MagicMock()

    connector.raise_alarm(severity, alarm_type, source, content, metadata)

    connector._notifications_producer.set_and_publish.assert_called_once_with(
        MessageEndpoints.alarm(),
        AlarmMessage(
            severity=severity,
            alarm_type=alarm_type,
            source=source,
            content=content,
            metadata=metadata,
        ).dumps(),
    )


@pytest.mark.parametrize("topic , msg", [["topic1", "msg1"], ["topic2", "msg2"]])
def test_redis_producer_send(producer, topic, msg):
    producer.send(topic, msg)
    producer.r.publish.assert_called_once_with(topic, msg)

    producer.send(topic, msg, pipe=producer.pipeline())
    producer.r.pipeline().publish.assert_called_once_with(topic, msg)


@pytest.mark.parametrize(
    "topic, msgs, max_size, expire",
    [["topic1", "msgs", None, None], ["topic1", "msgs", 10, None], ["topic1", "msgs", None, 100]],
)
def test_redis_producer_lpush(producer, topic, msgs, max_size, expire):
    pipe = None
    producer.lpush(topic, msgs, pipe, max_size, expire)

    producer.r.pipeline().lpush.assert_called_once_with(topic, msgs)

    if max_size:
        producer.r.pipeline().ltrim.assert_called_once_with(topic, 0, max_size)
    if expire:
        producer.r.pipeline().expire.assert_called_once_with(topic, expire)
    if not pipe:
        producer.r.pipeline().execute.assert_called_once()


@pytest.mark.parametrize(
    "topic , index , msgs, use_pipe", [["topic1", 1, "msg1", True], ["topic2", 4, "msg2", False]]
)
def test_redis_producer_lset(producer, topic, index, msgs, use_pipe):
    pipe = use_pipe_fcn(producer, use_pipe)

    ret = producer.lset(topic, index, msgs, pipe)

    if pipe:
        producer.r.pipeline().lset.assert_called_once_with(topic, index, msgs)
        assert ret == redis.Redis().pipeline().lset()
    else:
        producer.r.lset.assert_called_once_with(topic, index, msgs)
        assert ret == redis.Redis().lset()


@pytest.mark.parametrize(
    "topic, msgs, use_pipe", [["topic1", "msg1", True], ["topic2", "msg2", False]]
)
def test_redis_producer_rpush(producer, topic, msgs, use_pipe):
    pipe = use_pipe_fcn(producer, use_pipe)

    ret = producer.rpush(topic, msgs, pipe)

    if pipe:
        producer.r.pipeline().rpush.assert_called_once_with(topic, msgs)
        assert ret == redis.Redis().pipeline().rpush()
    else:
        producer.r.rpush.assert_called_once_with(topic, msgs)
        assert ret == redis.Redis().rpush()


@pytest.mark.parametrize(
    "topic, start, end, use_pipe", [["topic1", 0, 4, True], ["topic2", 3, 7, False]]
)
def test_redis_producer_lrange(producer, topic, start, end, use_pipe):
    pipe = use_pipe_fcn(producer, use_pipe)

    ret = producer.lrange(topic, start, end, pipe)

    if pipe:
        producer.r.pipeline().lrange.assert_called_once_with(topic, start, end)
        assert ret == redis.Redis().pipeline().lrange()
    else:
        producer.r.lrange.assert_called_once_with(topic, start, end)
        assert ret == redis.Redis().lrange()


@pytest.mark.parametrize(
    "topic, msg, pipe, expire", [["topic1", "msg1", None, 400], ["topic2", "msg2", None, None]]
)
def test_redis_producer_set_and_publish(producer, topic, msg, pipe, expire):
    producer.set_and_publish(topic, msg, pipe, expire)

    producer.r.pipeline().publish.assert_called_once_with(topic, msg)
    producer.r.pipeline().set.assert_called_once_with(topic, msg)
    if expire:
        producer.r.pipeline().expire.assert_called_once_with(topic, expire)
    if not pipe:
        producer.r.pipeline().execute.assert_called_once()


@pytest.mark.parametrize(
    "topic, msg, is_dict, expire", [["topic1", "msg1", True, None], ["topic2", "msg2", False, 400]]
)
def test_redis_producer_set(producer, topic, msg, is_dict, expire):
    pipe = None
    producer.set(topic, msg, pipe, is_dict, expire)

    if is_dict:
        producer.r.pipeline().hmset.assert_called_once_with(topic, msg)
    else:
        producer.r.pipeline().set.assert_called_once_with(topic, msg)
    if expire:
        producer.r.pipeline().expire.assert_called_once_with(topic, expire)
    if not pipe:
        producer.r.pipeline().execute.assert_called_once()


@pytest.mark.parametrize("pattern", ["samx", "samy"])
def test_redis_producer_keys(producer, pattern):
    ret = producer.keys(pattern)
    producer.r.keys.assert_called_once_with(pattern)
    assert ret == redis.Redis().keys()


def test_redis_producer_pipeline(producer):
    ret = producer.pipeline()
    producer.r.pipeline.assert_called_once()
    assert ret == redis.Redis().pipeline()


@pytest.mark.parametrize("topic,use_pipe", [["topic1", True], ["topic2", False]])
def test_redis_producer_delete(producer, topic, use_pipe):
    pipe = use_pipe_fcn(producer, use_pipe)

    producer.delete(topic, pipe)

    if pipe:
        producer.pipeline().delete.assert_called_once_with(topic)
    else:
        producer.r.delete.assert_called_once_with(topic)


@pytest.mark.parametrize(
    "topic, use_pipe, is_dict",
    [
        ["topic1", True, True],
        ["topic2", False, True],
        ["topic3", True, False],
        ["topic4", False, False],
    ],
)
def test_redis_producer_get(producer, topic, use_pipe, is_dict):
    pipe = use_pipe_fcn(producer, use_pipe)

    ret = producer.get(topic, pipe, is_dict)
    if is_dict:
        if pipe:
            producer.pipeline().hgetall.assert_called_once_with(topic)
            assert ret == redis.Redis().pipeline().hgetall()

        else:
            producer.r.hgetall.assert_called_once_with(topic)
            assert ret == redis.Redis().hgetall()

    else:
        if pipe:
            producer.pipeline().get.assert_called_once_with(topic)
            assert ret == redis.Redis().pipeline().get()

        else:
            producer.r.get.assert_called_once_with(topic)
            assert ret == redis.Redis().get()


def use_pipe_fcn(producer, use_pipe):
    if use_pipe:
        return producer.pipeline()
    return None


@pytest.mark.parametrize(
    "topics, pattern",
    [
        ["topics1", None],
        [["topics1", "topics2"], None],
        [None, "pattern1"],
        [None, ["pattern1", "pattern2"]],
    ],
)
def test_redis_consumer_init(consumer, topics, pattern):
    with mock.patch("bec_lib.redis_connector.redis.Redis"):
        consumer = RedisConsumer(
            "localhost", "1", topics, pattern, redis_cls=redis.Redis, cb=lambda *args, **kwargs: ...
        )

        if topics:
            if isinstance(topics, list):
                assert consumer.topics == topics
            else:
                assert consumer.topics == [topics]
        if pattern:
            if isinstance(pattern, list):
                assert consumer.pattern == pattern
            else:
                assert consumer.pattern == [pattern]

        assert consumer.r == redis.Redis()
        assert consumer.pubsub == consumer.r.pubsub()
        assert consumer.host == "localhost"
        assert consumer.port == "1"


@pytest.mark.parametrize("pattern, topics", [["pattern", "topics1"], [None, "topics2"]])
def test_redis_consumer_initialize_connector(consumer, pattern, topics):
    consumer.pattern = pattern

    consumer.topics = topics
    consumer.initialize_connector()

    if consumer.pattern is not None:
        consumer.pubsub.psubscribe.assert_called_once_with(consumer.pattern)
    else:
        consumer.pubsub.subscribe.assert_called_with(consumer.topics)


def test_redis_consumer_poll_messages(consumer):
    def cb_fcn(msg, **kwargs):
        print(msg)

    consumer.cb = cb_fcn

    ret = consumer.poll_messages()

    assert ret == None
    consumer.pubsub.get_message.assert_called_once_with(ignore_subscribe_messages=True)


def test_redis_consumer_shutdown(consumer):
    consumer.shutdown()
    consumer.pubsub.close.assert_called_once()


def test_redis_consumer_additional_kwargs(connector):
    cons = connector.consumer(topics="topic1", parent="here", cb=lambda *args, **kwargs: ...)
    assert "parent" in cons.kwargs


@pytest.mark.parametrize(
    "topics, pattern",
    [
        ["topics1", None],
        [["topics1", "topics2"], None],
        [None, "pattern1"],
        [None, ["pattern1", "pattern2"]],
    ],
)
def test_mixin_init_topics_and_pattern(mixin, topics, pattern):
    ret_topics, ret_pattern = mixin._init_topics_and_pattern(mixin, topics, pattern)

    if topics:
        if isinstance(topics, list):
            assert ret_topics == topics
        else:
            assert ret_topics == [topics]
    if pattern:
        if isinstance(pattern, list):
            assert ret_pattern == pattern
        else:
            assert ret_pattern == [pattern]


def test_mixin_init_redis_cls(mixin, consumer):
    mixin._init_redis_cls(consumer, None)
    assert consumer.r == redis.Redis(host="localhost", port=1)


@pytest.mark.parametrize(
    "topics, pattern",
    [
        ["topics1", None],
        [["topics1", "topics2"], None],
        [None, "pattern1"],
        [None, ["pattern1", "pattern2"]],
    ],
)
def test_redis_consumer_threaded_init(consumer_threaded, topics, pattern):
    with mock.patch("bec_lib.redis_connector.redis.Redis"):
        consumer_threaded = RedisConsumerThreaded(
            "localhost", "1", topics, pattern, redis_cls=redis.Redis, cb=lambda *args, **kwargs: ...
        )

        if topics:
            if isinstance(topics, list):
                assert consumer_threaded.topics == topics
            else:
                assert consumer_threaded.topics == [topics]
        if pattern:
            if isinstance(pattern, list):
                assert consumer_threaded.pattern == pattern
            else:
                assert consumer_threaded.pattern == [pattern]

        assert consumer_threaded.r == redis.Redis()
        assert consumer_threaded.pubsub == consumer_threaded.r.pubsub()
        assert consumer_threaded.host == "localhost"
        assert consumer_threaded.port == "1"
        assert consumer_threaded.sleep_times == [0.005, 0.1]
        assert consumer_threaded.last_received_msg == 0
        assert consumer_threaded.idle_time == 30


def test_redis_connector_xadd(producer):
    producer.xadd("topic1", {"key": "value"})
    producer.r.xadd.assert_called_once_with("topic1", {"key": "value"})


def test_redis_connector_xadd_with_maxlen(producer):
    producer.xadd("topic1", {"key": "value"}, max_size=100)
    producer.r.xadd.assert_called_once_with("topic1", {"key": "value"}, maxlen=100)


def test_redis_connector_xadd_with_expire(producer):
    producer.xadd("topic1", {"key": "value"}, expire=100)
    producer.r.pipeline().xadd.assert_called_once_with("topic1", {"key": "value"})
    producer.r.pipeline().expire.assert_called_once_with("topic1", 100)
    producer.r.pipeline().execute.assert_called_once()


def test_redis_connector_xread(producer):
    producer.xread("topic1", "id")
    producer.r.xread.assert_called_once_with({"topic1": "id"}, count=None, block=None)


def test_redis_connector_xread_without_id(producer):
    producer.xread("topic1", from_start=True)
    producer.r.xread.assert_called_once_with({"topic1": "0-0"}, count=None, block=None)
    producer.r.xread.reset_mock()

    producer.stream_keys["topic1"] = "id"
    producer.xread("topic1")
    producer.r.xread.assert_called_once_with({"topic1": "id"}, count=None, block=None)


def test_redis_connector_xread_from_end(producer):
    producer.xread("topic1", from_start=False)
    last_id = producer.r.xinfo_stream("topic1")["last-generated-id"]
    producer.r.xread.assert_called_once_with({"topic1": last_id}, count=None, block=None)


def test_redis_connector_xread_from_new_topic(producer):
    producer.r.xinfo_stream.side_effect = redis.exceptions.ResponseError(
        "NOGROUP No such key 'topic' or consumer group"
    )
    producer.xread("topic1", from_start=False)
    producer.r.xread.assert_called_once_with({"topic1": "0-0"}, count=None, block=None)


def test_redis_connector_get_last(producer):
    producer.get_last("topic1")
    producer.r.xrevrange.assert_called_once_with("topic1", "+", "-", count=1)


def test_redis_xrange(producer):
    producer.xrange("topic1", "start", "end")
    producer.r.xrange.assert_called_once_with("topic1", "start", "end", count=None)


def test_redis_xrange_topic_with_suffix(producer):
    producer.xrange("topic1", "start", "end")
    producer.r.xrange.assert_called_once_with("topic1", "start", "end", count=None)


def test_redis_consumer_threaded_no_cb_without_messages(consumer_threaded):
    with mock.patch.object(consumer_threaded.pubsub, "get_message", return_value=None):
        consumer_threaded.cb = mock.MagicMock()
        consumer_threaded.poll_messages()
        consumer_threaded.cb.assert_not_called()


def test_redis_consumer_threaded_cb_called_with_messages(consumer_threaded):
    messages = {"channel": b"topic1", "data": '{"key": "value"}'}

    with mock.patch.object(consumer_threaded.pubsub, "get_message", return_value=messages):
        consumer_threaded.cb = mock.MagicMock()
        consumer_threaded.poll_messages()
        msg = MessageObject(topic=b"topic1", value='{"key": "value"}')
        consumer_threaded.cb.assert_called_once_with(msg)


def test_redis_consumer_threaded_shutdown(consumer_threaded):
    consumer_threaded.shutdown()
    consumer_threaded.pubsub.close.assert_called_once()


def test_redis_stream_consumer_threaded_get_newest_message():
    consumer = RedisStreamConsumerThreaded(
        "localhost", "1", topics="topic1", cb=mock.MagicMock(), redis_cls=mock.MagicMock()
    )
    consumer.r.xrevrange.return_value = [(b"1691610882756-0", {b"data": b"msg"})]
    msgs = []
    consumer.get_newest_message(msgs)
    assert "topic1" in consumer.stream_keys
    assert consumer.stream_keys["topic1"] == b"1691610882756-0"


def test_redis_stream_consumer_threaded_get_newest_message_no_msg():
    consumer = RedisStreamConsumerThreaded(
        "localhost", "1", topics="topic1", cb=mock.MagicMock(), redis_cls=mock.MagicMock()
    )
    consumer.r.xrevrange.return_value = []
    msgs = []
    consumer.get_newest_message(msgs)
    assert "topic1" in consumer.stream_keys
    assert consumer.stream_keys["topic1"] == "0-0"


def test_redis_stream_consumer_threaded_get_id():
    consumer = RedisStreamConsumerThreaded(
        "localhost", "1", topics="topic1", cb=mock.MagicMock(), redis_cls=mock.MagicMock()
    )
    consumer.stream_keys["topic1"] = b"1691610882756-0"
    assert consumer.get_id("topic1") == b"1691610882756-0"
    assert consumer.get_id("doesnt_exist") == "0-0"


def test_redis_stream_consumer_threaded_poll_messages():
    consumer = RedisStreamConsumerThreaded(
        "localhost", "1", topics="topic1", cb=mock.MagicMock(), redis_cls=mock.MagicMock()
    )
    with mock.patch.object(
        consumer, "get_newest_message", return_value=None
    ) as mock_get_newest_message:
        consumer.poll_messages()
        mock_get_newest_message.assert_called_once()
        consumer.r.xread.assert_not_called()


def test_redis_stream_consumer_threaded_poll_messages_newest_only():
    consumer = RedisStreamConsumerThreaded(
        "localhost",
        "1",
        topics="topic1",
        cb=mock.MagicMock(),
        redis_cls=mock.MagicMock(),
        newest_only=True,
    )

    consumer.r.xrevrange.return_value = [(b"1691610882756-0", {b"data": b"msg"})]
    consumer.poll_messages()
    consumer.r.xread.assert_not_called()
    consumer.cb.assert_called_once_with(MessageObject(topic="topic1", value=b"msg"))


def test_redis_stream_consumer_threaded_poll_messages_read():
    consumer = RedisStreamConsumerThreaded(
        "localhost",
        "1",
        topics="topic1",
        cb=mock.MagicMock(),
        redis_cls=mock.MagicMock(),
    )
    consumer.stream_keys["topic1"] = "0-0"

    msg = [[b"topic1", [(b"1691610714612-0", {b"data": b"msg"})]]]

    consumer.r.xread.return_value = msg
    consumer.poll_messages()
    consumer.r.xread.assert_called_once_with({"topic1": "0-0"}, count=1)
    consumer.cb.assert_called_once_with(MessageObject(topic="topic1", value=b"msg"))


@pytest.mark.parametrize(
    "topics,expected",
    [
        ("topic1", ["topic1"]),
        (["topic1"], ["topic1"]),
        (["topic1", "topic2"], ["topic1", "topic2"]),
    ],
)
def test_redis_stream_consumer_threaded_init_topics(topics, expected):
    consumer = RedisStreamConsumerThreaded(
        "localhost",
        "1",
        topics=topics,
        cb=mock.MagicMock(),
        redis_cls=mock.MagicMock(),
    )
    assert consumer.topics == expected
