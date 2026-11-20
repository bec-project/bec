"""ADZMQ decoding and real socket lifecycle regression tests."""

import io
import json
from importlib.metadata import EntryPoint
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pytest

from bec_lib.messages import ZMQSignalTransport
from bec_lib.zmq_transports import ADZMQStream, EncodedFrame, load_encoder_plugin
from bec_lib.zmq_transports import plugin as zmq_plugin

ADZMQ_PLUGIN = "bec_lib.zmq_transports.adzmq.ADZMQStream"

zmq = pytest.importorskip("zmq")
Image = pytest.importorskip("PIL.Image")

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
    "header_updates,expected",
    [
        ({}, None),
        ({"timeStamp": 0}, 631_152_000.0),
        ({"timeStamp": 1_000_000_000.25}, 1_631_152_000.25),
        ({"timeStamp": "invalid"}, None),
    ],
)
def test_frame_timestamp(header_updates, expected):
    parts = message(**header_updates)
    stream = ADZMQStream()
    encoded = stream._unpack(parts)
    assert encoded.timestamp == expected
    assert encoded.metadata == json.loads(parts[0])

    for frame in (parts, encoded, EncodedFrame(encoded.data, encoded.metadata)):
        decoded = stream.decode(frame)
        assert decoded.timestamp == expected
        assert decoded.metadata == encoded.metadata


@pytest.mark.parametrize("color_mode,channel_axis", [(2, 2), (3, 1), (4, 0)])
def test_raw_area_detector_color_modes(color_mode, channel_axis):
    expected = np.arange(5 * 8 * 3, dtype=np.uint8).reshape(5, 8, 3)
    raw = np.moveaxis(expected, -1, channel_axis)
    shape = list(raw.shape)
    parts = message(type="uint8", shape=shape, ndattr={"ColorMode": color_mode})
    parts[1] = raw.tobytes()

    frame = ADZMQStream().decode(parts)

    np.testing.assert_array_equal(frame.data, expected)
    assert frame.metadata["shape"] == shape
    assert not frame.data.flags.writeable
    assert np.shares_memory(frame.data, np.frombuffer(parts[1], dtype=np.uint8))


@pytest.mark.parametrize(
    "shape,ndattrs",
    [
        pytest.param((5, 8), {"ColorMode": 0}, id="mono"),
        pytest.param((5, 8), {"ColorMode": 1}, id="bayer"),
        pytest.param((3, 5, 8), {}, id="no-color-mode"),
    ],
)
def test_raw_preserves_layout_for_other_color_modes(shape, ndattrs):
    expected = np.arange(np.prod(shape), dtype=np.uint8).reshape(shape)
    parts = message(type="uint8", shape=list(shape), ndattr=ndattrs)
    parts[1] = expected.tobytes()

    frame = ADZMQStream().decode(parts)

    np.testing.assert_array_equal(frame.data, expected)
    assert frame.metadata["shape"] == list(shape)


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


@pytest.mark.parametrize("color_mode,shape", [(2, [5, 8, 3]), (3, [5, 3, 8]), (4, [3, 5, 8])])
def test_jpeg_area_detector_color_modes(color_mode, shape):
    payload = io.BytesIO()
    Image.new("RGB", (8, 5), color=(220, 40, 10)).save(payload, format="JPEG")
    parts = message(encoding="jpeg", type="uint8", shape=shape, ndattr={"ColorMode": color_mode})
    parts[1] = payload.getvalue()

    frame = ADZMQStream().decode(parts)

    with Image.open(io.BytesIO(parts[1])) as decoded:
        np.testing.assert_array_equal(frame.data, np.asarray(decoded))
    assert frame.metadata["shape"] == shape


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
        address=address,
        socket_type="pull",
        encoder="adzmq-array-1.0",
        encoder_plugin=ADZMQ_PLUGIN,
        **updates,
    )


def test_load_encoder_plugin_resolves_receiver_class():
    descriptor = transport("tcp://127.0.0.1:1234")
    descriptor.encoder_plugin = "unregistered.receivers.Stream"
    with patch("bec_lib.zmq_transports.plugin.import_module") as importer:
        assert load_encoder_plugin(descriptor) is ADZMQStream
    importer.assert_not_called()


def test_load_encoder_plugin_requires_explicit_plugin_for_unknown_encoder():
    descriptor = ZMQSignalTransport(address="tcp://127.0.0.1:1234", encoder="custom-v1")
    with pytest.raises(ValueError, match="requires an encoder_plugin"):
        load_encoder_plugin(descriptor)


@pytest.fixture
def installed_bec_plugins():
    plugins = [
        EntryPoint(name="plugin_bec", value="beamline_plugin", group="bec"),
        EntryPoint(name="plugin_bec", value="second_beamline_plugin", group="bec"),
    ]
    zmq_plugin._bec_plugin_package_names.cache_clear()
    zmq_plugin._load_encoder_plugin.cache_clear()
    try:
        with patch.object(zmq_plugin, "entry_points", return_value=plugins) as discover:
            yield discover
    finally:
        zmq_plugin._bec_plugin_package_names.cache_clear()
        zmq_plugin._load_encoder_plugin.cache_clear()


def test_load_encoder_plugin_accepts_multiple_bec_packages_and_caches(installed_bec_plugins):
    first = ZMQSignalTransport(
        address="tcp://127.0.0.1:1234",
        encoder="custom-v1",
        encoder_plugin="beamline_plugin.receivers.CustomStream",
    )
    second = first.model_copy(
        update={"encoder_plugin": "second_beamline_plugin.receivers.SecondStream"}
    )
    modules = [SimpleNamespace(CustomStream=ADZMQStream), SimpleNamespace(SecondStream=ADZMQStream)]
    with patch("bec_lib.zmq_transports.plugin.import_module", side_effect=modules) as importer:
        for _ in range(2):
            assert load_encoder_plugin(first) is ADZMQStream
            assert load_encoder_plugin(second) is ADZMQStream
    installed_bec_plugins.assert_called_once_with(group="bec")
    assert [call.args[0] for call in importer.call_args_list] == [
        "beamline_plugin.receivers",
        "second_beamline_plugin.receivers",
    ]


@pytest.mark.parametrize(
    "path",
    [
        "other_package.receivers.CustomStream",
        "beamline_plugin_extra.receivers.CustomStream",
        "bec_lib.zmq_transports.ADZMQStream",
    ],
)
def test_load_encoder_plugin_rejects_classes_outside_bec_plugin(installed_bec_plugins, path):
    descriptor = ZMQSignalTransport(
        address="tcp://127.0.0.1:1234", encoder="custom-v1", encoder_plugin=path
    )
    with (
        patch("bec_lib.zmq_transports.plugin.import_module") as importer,
        pytest.raises(ValueError, match="must be in an installed BEC plugin package"),
    ):
        load_encoder_plugin(descriptor)
    importer.assert_not_called()


def test_load_encoder_plugin_rejects_invalid_receiver(installed_bec_plugins):
    descriptor = ZMQSignalTransport(
        address="tcp://127.0.0.1:1234",
        encoder="invalid-v1",
        encoder_plugin="beamline_plugin.receivers.InvalidStream",
    )
    module = SimpleNamespace(InvalidStream=EncodedFrame)
    with (
        patch("bec_lib.zmq_transports.plugin.import_module", return_value=module),
        pytest.raises(TypeError, match="implementing SignalStream"),
    ):
        load_encoder_plugin(descriptor)


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
    settings = dict(
        address="tcp://127.0.0.1:1234", encoder="adzmq-array-1.0", encoder_plugin=ADZMQ_PLUGIN
    )
    settings.update(updates)
    with pytest.raises(ValueError):
        ADZMQStream().start(ZMQSignalTransport(**settings))


@pytest.mark.parametrize("encoder", ["adzmq-array-1.0", "array-1.0"])
def test_subscribe_without_topic(encoder):
    with zmq.Context() as context:
        with context.socket(zmq.XPUB) as publisher:
            publisher.setsockopt(zmq.LINGER, 0)
            publisher.setsockopt(zmq.RCVTIMEO, 3000)
            port = publisher.bind_to_random_port("tcp://127.0.0.1")
            descriptor = ZMQSignalTransport(address=f"tcp://127.0.0.1:{port}", encoder=encoder)
            stream = load_encoder_plugin(descriptor)()
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
