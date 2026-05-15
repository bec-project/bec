import numpy as np

from bec_server.shared_memory.ring_buffer import PayloadDescriptor, SharedRingBuffer


def test_shutdown_after_slot_context_releases_exported_memoryview():
    payload = PayloadDescriptor.from_numpy(np.zeros((4,), dtype=np.float64))
    ring_buffer = SharedRingBuffer.create(slots=2, payload=payload)

    with ring_buffer.write_slot(0) as view:
        array = np.ndarray(payload.shape, dtype=payload.dtype.numpy_dtype, buffer=view)
        array[:] = 1

    ring_buffer.shutdown()


def test_create_uses_fresh_shared_memory_and_lock_names():
    payload = PayloadDescriptor.from_numpy(np.zeros((4,), dtype=np.float64))
    first = SharedRingBuffer.create(slots=2, payload=payload)
    second = SharedRingBuffer.create(slots=2, payload=payload)

    try:
        assert first.name != second.name
        assert first.descriptor().lock_id != second.descriptor().lock_id
    finally:
        first.shutdown()
        second.shutdown()
