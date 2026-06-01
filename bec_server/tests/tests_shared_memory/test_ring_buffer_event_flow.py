from __future__ import annotations

import multiprocessing
import threading
import time
from typing import Any

import fakeredis
import numpy as np
import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_server.shared_memory.models import PayloadDescriptor, RingBufferDescriptor
from bec_server.shared_memory.ring_buffer import RingBuffer, RingBufferView


class SharedMemorySumWorker:
    """Small subprocess worker that reacts to slot-written events and publishes sums."""

    def __init__(
        self,
        bootstrap: str,
        descriptor: dict[str, Any],
        *,
        delay: float = 0,
        expected_events: int = 1,
    ):
        self.bootstrap = bootstrap
        self.descriptor = descriptor
        self.delay = delay
        self.expected_events = expected_events

    def run(self) -> None:
        connector = RedisConnector(self.bootstrap, name="SharedMemorySumWorker RedisConnector")
        view = RingBufferView(RingBufferDescriptor.model_validate(self.descriptor))
        try:
            processed = 0
            while processed < self.expected_events:
                records = connector.xread(
                    MessageEndpoints.shared_memory_slot_written(),
                    block=1000,
                    count=1,
                    from_start=processed == 0,
                )
                if not records:
                    continue
                event = records[0]["data"]
                if not isinstance(event, messages.SharedMemSlotWritten):
                    continue
                data = view.copy_data(event.slot_index)
                if self.delay:
                    time.sleep(self.delay)
                connector.xadd(
                    MessageEndpoints.shared_memory_slot_processed(),
                    {
                        "data": messages.SharedMemSlotProcessed(
                            client_id=event.client_id,
                            signal=event.signal,
                            slot_index=event.slot_index,
                            result={"sum": float(np.sum(data))},
                        )
                    },
                )
                processed += 1
        finally:
            view.close()
            connector.shutdown(per_thread_timeout_s=1)


@pytest.fixture
def fake_redis_tcp_server():
    server = fakeredis.TcpFakeServer(("127.0.0.1", 0))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"{server.server_address[0]}:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def test_two_slot_ring_buffer_event_processing_flow(fake_redis_tcp_server):
    payload = PayloadDescriptor.from_numpy(np.zeros((4,), dtype=np.float64))
    ring_buffer = RingBuffer(slots=2, payload=payload)
    connector = RedisConnector(fake_redis_tcp_server, name="SharedMemoryEventTest RedisConnector")
    worker = SharedMemorySumWorker(
        fake_redis_tcp_server, ring_buffer.descriptor.model_dump(), delay=0.01, expected_events=2
    )
    ctx = multiprocessing.get_context("spawn")
    process = ctx.Process(target=worker.run)
    process.start()

    try:
        for data in (
            np.array([1, 2, 3, 4], dtype=np.float64),
            np.array([5, 6, 7, 8], dtype=np.float64),
        ):
            slot_index = ring_buffer.write_data(data)
            connector.xadd(
                MessageEndpoints.shared_memory_slot_written(),
                {
                    "data": messages.SharedMemSlotWritten(
                        client_id="writer", signal="detector.data", slot_index=slot_index
                    )
                },
            )

        results = []
        deadline = time.monotonic() + 5
        while len(results) < 2 and time.monotonic() < deadline:
            records = connector.xread(
                MessageEndpoints.shared_memory_slot_processed(), block=100, count=1
            )
            if records:
                results.append(records[0]["data"])

        process.join(timeout=5)
        assert process.exitcode == 0
        assert [result.slot_index for result in results] == [0, 1]
        assert [result.result["sum"] for result in results] == [10.0, 26.0]
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        connector.shutdown(per_thread_timeout_s=1)
        ring_buffer.destroy()
