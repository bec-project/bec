"""Pull-based plugin interface for latest-frame preview streams."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import numpy as np

from bec_lib.messages import ZMQSignalTransport


@dataclass(frozen=True)
class EncodedFrame:
    """Publisher metadata and encoded image bytes, owned independently of the socket.

    JPEG payloads remain compressed. Raw payloads retain their wire byte order.
    Metadata must be treated as read-only by consumers.
    """

    data: bytes
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class DecodedFrame:
    """Image and publisher metadata; consumers must not mutate the image."""

    data: np.ndarray
    metadata: Mapping[str, Any]


class SignalStream(Protocol):
    """Single-consumer preview stream backed by a one-frame mailbox.

    Use a concrete implementation such as ``ADZMQStream``; this protocol defines
    the consumer-facing interface and is not instantiated directly. Choose the
    implementation matching the transport descriptor's ``encoder``. Pass the
    advertised descriptor to ``start``; its address, socket type, connection mode,
    and topic describe the receiving side of the connection.

    Receiving and consuming:
        ``start`` launches a background receiver and returns without waiting for a
        publisher or first frame. The receiver replaces the pending message whenever
        another arrives. ``take_frame`` waits without polling, atomically removes
        the newest pending frame, and returns it. Call it again when ready for the
        next frame. There is one consumer per stream instance; consumers sharing an
        instance would compete for frames rather than each receive a copy.

        This is a preview interface: intermediate frames may be discarded, and
        there is no replay or lossless acquisition guarantee. The mailbox contains
        the latest message received locally, not necessarily the publisher's latest
        message: upstream socket and network buffers may still contain older data.
        To limit processing to a desired refresh rate, pace the consumer before its
        next read. A tight read/decode loop processes frames as fast as it can.

    Decoding and GUI use:
        No image decoding occurs in the receiver. ``EncodedFrame.data`` contains
        the payload bytes and ``metadata`` describes their format. JPEG stays
        compressed; raw arrays retain the advertised dtype, shape, and byte order.
        Decode only after taking a frame, using the implementation's decoder or
        a suitable image library. ``decode`` is an ADZMQStream convenience method,
        not a requirement of this protocol.

        ``take_frame`` can wait indefinitely, so call it in a consumer thread,
        never in a GUI timer or the GUI thread. Process/decode there and deliver
        results to the GUI through its thread-safe dispatch mechanism. Bound or
        coalesce pending display updates as well; queuing every decoded image can
        create a second backlog even though the transport mailbox is bounded.

    Errors and shutdown:
        Receiver setup and socket failures are raised by ``take_frame``, rather
        than reported by a callback. Terminal failures remain observable until
        ``stop`` or restart; exit the consumer loop on these errors instead of
        retrying in a tight loop. Malformed individual frames can also raise;
        recovery policy belongs to the consumer.

        Always call ``stop`` in a finally block. It clears the mailbox, releases
        receiver resources, and wakes waiting consumers, whose read returns None.
        Reads before start or after stop also return None. Lifecycle calls are
        serialized, and restarting begins with an empty mailbox. A frame already
        taken remains valid after stop; stop does not wait for consumer processing,
        so join your consumer thread separately.

    Examples:
        Run an ADZMQ receiver and decode images in a consumer thread. The main
        thread remains free to stop the stream even when no publisher is sending::

            import logging
            from threading import Thread

            from bec_lib.messages import ZMQSignalTransport
            from bec_lib.zmq_transports import ADZMQStream

            stream = ADZMQStream()
            transport = ZMQSignalTransport(
                address="tcp://127.0.0.1:1234",
                socket_type="sub",
                connection_mode="connect",
                encoder="adzmq-array-1.0",
            )

            def consume():
                try:
                    while True:
                        frame = stream.take_frame()
                        if frame is None:
                            return
                        image = stream.decode(frame)
                        print(image.data.shape, image.metadata.get("frame"))
                except Exception:
                    logging.exception("Image consumer stopped after a stream/decode error")

            consumer = Thread(target=consume)
            try:
                stream.start(transport)
                consumer.start()
                input("Press Enter to stop receiving images: ")
            finally:
                stream.stop()
                if consumer.ident is not None:
                    consumer.join()

        If the consumer accepts JPEG bytes directly, use ``frame.data`` and
        ``frame.metadata`` instead of calling ``stream.decode``.
    """

    def start(self, transport: ZMQSignalTransport) -> None:
        """Start receiving asynchronously; an active stream raises RuntimeError.

        Args:
            transport (ZMQSignalTransport): Receiver connection descriptor.
        """

    def take_frame(self) -> EncodedFrame | None:
        """Wait for, atomically take, and clear the newest pending frame.

        This blocks until a frame, terminal failure, or stop notification arrives.
        It does not decode the payload and must not run in a GUI event loop.
        A subsequent call waits again if no newer frame has arrived.

        Returns:
            EncodedFrame | None: Latest frame, or None when the stream is stopped.

        Raises:
            Exception: Receiver failure or malformed frame. Terminal receiver
                failures remain available until stop or restart.
        """

    def stop(self) -> None:
        """Stop receiving, clear the mailbox, and join the worker; safe to repeat."""
