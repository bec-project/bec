"""ADZMQ decoding and real socket lifecycle regression tests."""

import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread

import numpy as np
import pytest
import zmq
from PIL import Image

from bec_lib.messages import ZMQSignalTransport
from bec_lib.zmq_transports import ADZMQStream

pytestmark = pytest.mark.timeout(10)


def message(**updates):
    """Build a small raw image message."""
    header = dict(htype="array-1.0", shape=[2, 3], type="uint16", frame=7, encoding="")
    header.update(updates)
    return [json.dumps(header).encode(), np.arange(6, dtype="<u2").tobytes()]


@pytest.mark.parametrize("byte_order,key", [("little", "endianness"), ("big", "endianess")])
def test_raw(byte_order, key):
    parts = message(**{key: byte_order})
    parts[1] = np.arange(6, dtype="<u2" if byte_order == "little" else ">u2").tobytes()
    frame = ADZMQStream().decode(parts)
    np.testing.assert_array_equal(frame.data, np.arange(6).reshape(2, 3))
    assert frame.metadata["frame"] == 7
    assert not frame.data.flags.writeable


def test_default_byte_order():
    parts = message()
    parts[1] = np.arange(6, dtype=">u2").tobytes()
    np.testing.assert_array_equal(ADZMQStream("big").decode(parts).data, np.arange(6).reshape(2, 3))


@pytest.mark.parametrize(
    "updates",
    [
        dict(htype="other"),
        dict(shape=[-1]),
        dict(shape=[True]),
        dict(shape="2,3"),
        dict(shape=[3, 3]),
        dict(type="object"),
        dict(type=[]),
        dict(type={}),
        dict(encoding="lz4"),
        dict(endianness="other"),
    ],
)
def test_bad_headers(updates):
    with pytest.raises(ValueError):
        ADZMQStream().decode(message(**updates))


@pytest.mark.parametrize("parts", [[], [b"{}"], [b"[]", b""], [b"{", b""]])
def test_bad_framing(parts):
    with pytest.raises(ValueError):
        ADZMQStream().decode(parts)


def test_jpeg():
    payload = io.BytesIO()
    Image.fromarray(np.zeros((2, 3), dtype=np.uint8)).save(payload, format="JPEG")
    parts = message(encoding="jpeg", type="uint8")
    parts[1] = payload.getvalue()
    assert ADZMQStream().decode(parts).data.shape == (2, 3)
    parts[0] = json.dumps(dict(htype="array-1.0", shape=[3, 2], encoding="jpeg")).encode()
    with pytest.raises(ValueError, match="dimensions"):
        ADZMQStream().decode(parts)


@pytest.fixture
def sender():
    """Provide a PUSH socket without a service or port reservation race."""
    with zmq.Context() as context:
        with context.socket(zmq.PUSH) as socket:
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.SNDTIMEO, 3000)
            port = socket.bind_to_random_port("tcp://127.0.0.1")
            yield socket, f"tcp://127.0.0.1:{port}"


def transport(address, **updates):
    """Build a receiver descriptor."""
    return ZMQSignalTransport(
        address=address, socket_type="pull", encoder="adzmq-array-1.0", **updates
    )


def test_receive_stop_restart_and_recover(sender):
    socket, address = sender
    stream = ADZMQStream()
    try:
        stream.start(transport(address))
        with pytest.raises(RuntimeError, match="already running"):
            stream.start(transport(address))
        socket.send_multipart([b"invalid", b""])
        with pytest.raises(ValueError):
            stream.take_frame()
        socket.send_multipart(message())
        frame = stream.take_frame()
        assert frame.metadata["frame"] == 7
        np.testing.assert_array_equal(stream.decode(frame).data, np.arange(6).reshape(2, 3))
        assert stream._pending is None
        stream.stop()
        stream.stop()
        stream.start(transport(address))
        assert stream._pending is None
        socket.send_multipart(message())
        assert stream.take_frame().metadata["frame"] == 7
    finally:
        stream.stop()


def test_setup_error():
    stream = ADZMQStream()
    try:
        stream.start(transport("invalid://endpoint"))
        with pytest.raises(zmq.ZMQError):
            stream.take_frame()
        # A terminal failure must not silently turn into empty reads.
        with pytest.raises(zmq.ZMQError):
            stream.take_frame()
    finally:
        stream.stop()
    assert stream.take_frame() is None


def test_stop_wakes_waiting_consumer(sender):
    _, address = sender
    entered, finished = Event(), Event()
    stream = ADZMQStream()
    results = []

    def consume():
        entered.set()
        results.append(stream.take_frame())
        finished.set()

    stream.start(transport(address))
    consumer = Thread(target=consume)
    try:
        consumer.start()
        assert entered.wait(3)
        stream.stop()
        assert finished.wait(1)
        assert results == [None]
    finally:
        stream.stop()
        consumer.join(3)


def test_latest_frame_replaces_pending_without_decoding(sender, monkeypatch):
    socket, address = sender
    stream = ADZMQStream()

    def unexpected_decode(_):
        pytest.fail("Receiver must not decode incoming images")

    monkeypatch.setattr(stream, "decode", unexpected_decode)
    try:
        stream.start(transport(address))
        for index in range(20):
            socket.send_multipart(message(frame=index))
        # Synchronize with actual reception, without consuming or a timing sleep.
        with stream._condition:
            assert stream._condition.wait_for(
                lambda: stream._pending is not None
                and json.loads(stream._pending[0])["frame"] == 19,
                timeout=3,
            )
        frame = stream.take_frame()
        assert frame.metadata["frame"] == 19
        assert isinstance(frame.data, bytes)
        assert stream._pending is None
    finally:
        stream.stop()
    # Payload ownership is independent of the socket lifetime.
    np.testing.assert_array_equal(np.frombuffer(frame.data, dtype="<u2"), np.arange(6))


def test_stop_clears_pending_frame(sender):
    socket, address = sender
    stream = ADZMQStream()
    try:
        stream.start(transport(address))
        socket.send_multipart(message())
        with stream._condition:
            assert stream._condition.wait_for(lambda: stream._pending is not None, timeout=3)
        stream.stop()
        assert stream.take_frame() is None
    finally:
        stream.stop()


def test_take_frame_waits_for_next_frame(sender):
    socket, address = sender
    stream = ADZMQStream()
    entered, finished = Event(), Event()
    frames = []

    def consume():
        entered.set()
        frames.append(stream.take_frame())
        finished.set()

    consumer = Thread(target=consume)
    try:
        stream.start(transport(address))
        socket.send_multipart(message(frame=1))
        assert stream.take_frame().metadata["frame"] == 1
        consumer.start()
        assert entered.wait(3)
        assert not finished.wait(0.02)
        socket.send_multipart(message(frame=2))
        assert finished.wait(3)
        assert frames[0].metadata["frame"] == 2
    finally:
        stream.stop()
        if consumer.ident is not None:
            consumer.join(3)


def test_take_frame_before_start():
    assert ADZMQStream().take_frame() is None


def test_jpeg_stays_encoded_until_consumed(sender):
    socket, address = sender
    stream = ADZMQStream()
    payload = io.BytesIO()
    Image.fromarray(np.zeros((2, 3), dtype=np.uint8)).save(payload, format="JPEG")
    packet = message(encoding="jpeg", type="uint8")
    packet[1] = payload.getvalue()
    try:
        stream.start(transport(address))
        socket.send_multipart(packet)
        frame = stream.take_frame()
        assert frame.data == payload.getvalue()
        assert frame.metadata["encoding"] == "jpeg"
        assert stream.decode(frame).data.shape == (2, 3)
    finally:
        stream.stop()


@pytest.mark.parametrize(
    "updates", [dict(encoder="other"), dict(topic="camera", socket_type="sub")]
)
def test_reject_descriptor(updates):
    settings = dict(address="tcp://127.0.0.1:1234", encoder="adzmq-array-1.0")
    settings.update(updates)
    with pytest.raises(ValueError):
        ADZMQStream().start(ZMQSignalTransport(**settings))


def test_subscribe_without_topic():
    stream = ADZMQStream()
    with zmq.Context() as context:
        with context.socket(zmq.XPUB) as publisher:
            publisher.setsockopt(zmq.LINGER, 0)
            publisher.setsockopt(zmq.RCVTIMEO, 3000)
            port = publisher.bind_to_random_port("tcp://127.0.0.1")
            descriptor = ZMQSignalTransport(
                address=f"tcp://127.0.0.1:{port}", encoder="adzmq-array-1.0"
            )
            try:
                stream.start(descriptor)
                # XPUB confirms the subscription, avoiding a slow-joiner sleep.
                assert publisher.recv() == b"\x01"
                publisher.send_multipart(message())
                assert stream.take_frame().metadata["frame"] == 7
            finally:
                stream.stop()


def test_receiver_bind():
    stream = ADZMQStream()
    # macOS pytest temporary paths can exceed the Unix socket path limit.
    with TemporaryDirectory(dir="/tmp") as directory, zmq.Context() as context:
        address = f"ipc://{Path(directory) / 'image.sock'}"
        with context.socket(zmq.PUSH) as publisher:
            publisher.setsockopt(zmq.LINGER, 0)
            publisher.setsockopt(zmq.SNDTIMEO, 3000)
            try:
                stream.start(transport(address, connection_mode="bind"))
                publisher.connect(address)
                publisher.send_multipart(message())
                assert stream.take_frame().metadata["frame"] == 7
            finally:
                stream.stop()
