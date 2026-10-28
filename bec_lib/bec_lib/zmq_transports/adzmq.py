"""Receiver for ADZMQ's two-part array-1.0 wire format.

See https://github.com/paulscherrerinstitute/ADZMQ.
Raw arrays and JPEG are supported; other compression codecs are rejected.
"""

from __future__ import annotations

import io
import json
from collections.abc import Mapping, Sequence
from math import prod
from threading import Condition, Event, Lock, Thread
from typing import Literal

import numpy as np
import zmq

from bec_lib.messages import ZMQSignalTransport

from .protocol import DecodedFrame, EncodedFrame

# Matching protocol signatures and lazy JPEG dependency imports are intentional.
# pylint: disable=duplicate-code,import-outside-toplevel


class ADZMQStream:
    """Receive ADZMQ images; implements :class:`SignalStream`.

    The proposed BEC encoder identifier is ``adzmq-array-1.0``. ADZMQ itself
    identifies headers with ``htype=array-1.0``. There is no separate topic frame.
    Missing byte order uses ``default_byte_order`` (little endian per array-1.0).
    Configure this explicitly for publishers sending native big-endian memory.
    Mailbox and lifecycle semantics follow :class:`SignalStream`.
    The mailbox holds the latest message received by the worker; upstream ZMQ
    and network buffers can still contain older frames. This is a preview API.

    Args:
        default_byte_order (Literal["little", "big"]): Raw payload byte order
            when neither header spelling supplies it.
    """

    def __init__(self, default_byte_order: Literal["little", "big"] = "little") -> None:
        if default_byte_order not in ("little", "big"):
            raise ValueError("Byte order must be little or big")
        self._default_byte_order = default_byte_order
        self._stop = Event()
        self._lock = Lock()
        self._condition = Condition()
        self._thread: Thread | None = None
        self._pending: tuple[bytes, ...] | None = None
        self._failure: Exception | None = None
        self._active = False
        self._generation = 0

    def start(self, transport: ZMQSignalTransport) -> None:
        """Start receiving into the latest-frame mailbox without decoding.

        Args:
            transport (ZMQSignalTransport): ADZMQ connection settings.
        """
        if transport.encoder != "adzmq-array-1.0":
            raise ValueError(f"Unsupported encoder: {transport.encoder}")
        if transport.topic:
            raise ValueError("ADZMQ has no topic frame")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("Stream is already running")
            self._stop.clear()
            with self._condition:
                self._generation += 1
                self._pending = None
                self._failure = None
                self._active = True
                self._condition.notify_all()
            self._thread = Thread(
                target=self._run,
                args=(transport.model_copy(deep=True),),
                name="ADZMQStream",
                daemon=True,
            )
            try:
                self._thread.start()
            except Exception:
                with self._condition:
                    self._active = False
                    self._condition.notify_all()
                self._thread = None
                raise

    def take_frame(self) -> EncodedFrame | None:
        """Wait for and take the newest encoded frame.

        Header parsing occurs in the caller, after taking the frame. JPEG decoding
        is left to the consumer; :meth:`decode` provides an optional NumPy decoder.

        Returns:
            EncodedFrame | None: Latest frame, or None when the stream is stopped.

        Raises:
            ValueError: Invalid multipart framing or JSON header.
            Exception: Terminal socket/setup failure, retained until stop or restart.
        """
        with self._condition:
            generation = self._generation
            self._condition.wait_for(
                lambda: self._pending is not None
                or self._failure is not None
                or not self._active
                or self._generation != generation
            )
            if self._generation != generation:
                return None
            if self._failure is not None:
                raise self._failure.with_traceback(None)
            parts, self._pending = self._pending, None
        if parts is None:
            return None
        return self._unpack(parts)

    @staticmethod
    def _unpack(parts: Sequence[bytes]) -> EncodedFrame:
        if len(parts) != 2:
            raise ValueError(f"Expected 2 ADZMQ frames, received {len(parts)}")
        header = json.loads(parts[0])
        if not isinstance(header, Mapping) or header.get("htype") != "array-1.0":
            raise ValueError("Unsupported ADZMQ header type")
        return EncodedFrame(data=parts[1], metadata=header)

    def stop(self) -> None:
        """Clear the mailbox, wake consumers, and join the receiver."""
        with self._lock:
            self._stop.set()
            with self._condition:
                self._active = False
                self._pending = None
                self._failure = None
                self._condition.notify_all()
            if self._thread is not None:
                self._thread.join()
                self._thread = None

    def _run(self, transport: ZMQSignalTransport) -> None:
        try:
            with zmq.Context() as context:
                socket_type = zmq.SUB if transport.socket_type == "sub" else zmq.PULL
                with context.socket(socket_type) as socket:
                    socket.setsockopt(zmq.LINGER, 0)
                    socket.setsockopt(zmq.RCVHWM, 2)
                    if socket_type == zmq.SUB:
                        socket.setsockopt(zmq.SUBSCRIBE, b"")
                    if transport.connection_mode == "bind":
                        socket.bind(transport.address)
                    else:
                        socket.connect(transport.address)
                    while not self._stop.is_set():
                        if not socket.poll(timeout=100):
                            continue
                        parts = tuple(socket.recv_multipart())
                        with self._condition:
                            if self._stop.is_set():
                                break
                            self._pending = parts
                            self._condition.notify_all()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            with self._condition:
                if not self._stop.is_set():
                    self._failure = exc.with_traceback(None)
        finally:
            with self._condition:
                self._active = False
                self._condition.notify_all()

    def decode(self, parts: EncodedFrame | Sequence[bytes]) -> DecodedFrame:
        """Decode one complete ADZMQ message, retaining its original header.

        Args:
            parts (EncodedFrame | Sequence[bytes]): Frame from ``take_frame`` or
                a complete multipart message (JSON header and binary image payload).

        Returns:
            DecodedFrame: Image with dimensions already in NumPy order.

        Raises:
            ValueError: Invalid header, payload size, or unsupported compression.
        """
        frame = parts if isinstance(parts, EncodedFrame) else self._unpack(parts)
        header, payload = frame.metadata, frame.data
        if not isinstance(header, Mapping) or header.get("htype") != "array-1.0":
            raise ValueError("Unsupported ADZMQ header type")
        shape = header.get("shape")
        if not isinstance(shape, list) or any(
            not isinstance(dim, int) or isinstance(dim, bool) or dim < 0 for dim in shape
        ):
            raise ValueError("Shape must be a list of non-negative integers")
        encoding = header.get("encoding", "")
        if encoding == "jpeg":
            from PIL import Image

            with Image.open(io.BytesIO(payload)) as image:
                if image.format != "JPEG":
                    raise ValueError("Expected JPEG payload")
                data = np.array(image)
            if data.shape != tuple(shape):
                raise ValueError("JPEG dimensions do not match header shape")
        elif encoding == "":
            allowed_types = {
                "int8",
                "uint8",
                "int16",
                "uint16",
                "int32",
                "uint32",
                "int64",
                "uint64",
                "float16",
                "float32",
                "float64",
            }
            dtype_name = header.get("type")
            if not isinstance(dtype_name, str) or dtype_name not in allowed_types:
                raise ValueError("Unsupported image dtype")
            byte_order = header.get("endianness", header.get("endianess", self._default_byte_order))
            if byte_order not in ("little", "big"):
                raise ValueError(f"Unsupported byte order: {byte_order}")
            dtype = np.dtype(header["type"]).newbyteorder("<" if byte_order == "little" else ">")
            if len(payload) != prod(shape) * dtype.itemsize:
                raise ValueError("Payload size does not match header shape and dtype")
            data = np.frombuffer(payload, dtype=dtype).reshape(tuple(shape))
        else:
            raise ValueError(f"Unsupported ADZMQ encoding: {encoding}")
        data.setflags(write=False)
        return DecodedFrame(data=data, metadata=header)
