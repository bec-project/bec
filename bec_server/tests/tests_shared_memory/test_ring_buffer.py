from multiprocessing import shared_memory

import numpy as np
import posix_ipc
import pytest

from bec_server.shared_memory.models import PayloadDescriptor
from bec_server.shared_memory.ring_buffer import (
    MAX_SEMAPHORE_NAME_LENGTH,
    READER_COUNT_DTYPE,
    RingBuffer,
    RingBufferView,
)


@pytest.fixture
def payload() -> PayloadDescriptor:
    return PayloadDescriptor.from_numpy(np.zeros((4,), dtype=np.float64))


@pytest.fixture
def ring_buffer(payload: PayloadDescriptor):
    buffer = RingBuffer(slots=2, payload=payload)
    yield buffer
    buffer.destroy()


def test_descriptor_exposes_payload_counter_resources_and_rw_locks(
    ring_buffer: RingBuffer, payload: PayloadDescriptor
):
    assert ring_buffer.descriptor.name == ring_buffer.name
    assert ring_buffer.descriptor.reader_count_name == ring_buffer.reader_count_name
    assert ring_buffer.descriptor.slots == 2
    assert ring_buffer.descriptor.payload == payload
    assert len(ring_buffer.descriptor.data_lock_ids) == 2
    assert len(ring_buffer.descriptor.reader_gate_ids) == 2
    assert len(ring_buffer.descriptor.reader_count_lock_ids) == 2
    assert (
        len(
            {
                *ring_buffer.descriptor.data_lock_ids,
                *ring_buffer.descriptor.reader_gate_ids,
                *ring_buffer.descriptor.reader_count_lock_ids,
            }
        )
        == 6
    )


def test_attached_view_uses_descriptor_payload_and_counter_memory(
    ring_buffer: RingBuffer, payload: PayloadDescriptor
):
    view = RingBufferView(ring_buffer.descriptor)
    try:
        assert view.slots == ring_buffer.descriptor.slots
        assert view.bytes_per_slot == payload.nbytes
        assert view.payload_descriptor == payload
        assert view._reader_counts.shape == (2,)
        assert view._reader_counts.dtype == READER_COUNT_DTYPE
    finally:
        view.close()


def test_attached_view_rejects_incomplete_lock_descriptor(ring_buffer: RingBuffer):
    descriptor = ring_buffer.descriptor.model_copy(update={"data_lock_ids": ("only-one",)})

    with pytest.raises(ValueError, match="exactly one lock per slot"):
        RingBufferView(descriptor)


def test_write_data_uses_local_circular_position_and_returns_written_slot(ring_buffer: RingBuffer):
    first = np.array([1, 2, 3, 4], dtype=np.float64)
    second = np.array([5, 6, 7, 8], dtype=np.float64)
    third = np.array([9, 10, 11, 12], dtype=np.float64)

    assert ring_buffer.next_write_position == 0
    assert ring_buffer.write_data(first) == 0
    assert ring_buffer.next_write_position == 1
    assert ring_buffer.write_data(second) == 1
    assert ring_buffer.next_write_position == 0
    assert ring_buffer.write_data(third) == 0
    assert ring_buffer.next_write_position == 1
    np.testing.assert_array_equal(ring_buffer.copy_data(0), third)
    np.testing.assert_array_equal(ring_buffer.copy_data(1), second)


def test_explicit_write_uses_payload_only_slot_offset_without_advancing_cursor(
    ring_buffer: RingBuffer, payload: PayloadDescriptor
):
    data = np.arange(4, dtype=np.float64)

    ring_buffer.write_data_at(1, data)

    raw_payload = np.ndarray(
        payload.shape,
        dtype=payload.dtype.numpy_dtype,
        buffer=ring_buffer._shm.buf,
        offset=payload.nbytes,
    )
    np.testing.assert_array_equal(raw_payload, data)
    assert ring_buffer.next_write_position == 0


def test_attached_view_has_independent_local_write_cursor(ring_buffer: RingBuffer):
    view = RingBufferView(ring_buffer.descriptor)
    try:
        written_from_view = np.array([1, 2, 3, 4], dtype=np.float64)
        written_from_owner = np.array([5, 6, 7, 8], dtype=np.float64)

        assert view.write_data(written_from_view) == 0
        assert view.next_write_position == 1
        assert ring_buffer.next_write_position == 0
        np.testing.assert_array_equal(ring_buffer.copy_data(0), written_from_view)

        assert ring_buffer.write_data(written_from_owner) == 0
        np.testing.assert_array_equal(view.copy_data(0), written_from_owner)
    finally:
        view.close()


def test_each_buffer_has_distinct_shared_memory_and_semaphore_names(payload: PayloadDescriptor):
    first = RingBuffer(slots=2, payload=payload)
    second = RingBuffer(slots=2, payload=payload)
    try:
        assert first.name != second.name
        assert first.descriptor.reader_count_name != second.descriptor.reader_count_name
        assert first.descriptor.data_lock_ids != second.descriptor.data_lock_ids
        assert first.descriptor.reader_gate_ids != second.descriptor.reader_gate_ids
        assert first.descriptor.reader_count_lock_ids != second.descriptor.reader_count_lock_ids
    finally:
        first.destroy()
        second.destroy()


def test_slot_semaphore_name_supports_largest_header_slot_index():
    name = "bec_psm_abcdef"
    lock_name = RingBuffer._semaphore_name(name, f"_d_{(2**32) - 1}")

    assert len(lock_name) <= MAX_SEMAPHORE_NAME_LENGTH
    assert lock_name.endswith("_d_4294967295")


def test_multiple_readers_share_one_slot_lock(ring_buffer: RingBuffer):
    with ring_buffer._read_slot_lock(0, acquire_timeout=0):
        assert ring_buffer._reader_counts[0] == 1
        with ring_buffer._read_slot_lock(0, acquire_timeout=0):
            assert ring_buffer._reader_counts[0] == 2
        assert ring_buffer._reader_counts[0] == 1

    assert ring_buffer._reader_counts[0] == 0


def test_writer_waits_while_reader_is_attached_to_same_slot(ring_buffer: RingBuffer):
    with ring_buffer._read_slot_lock(0, acquire_timeout=0):
        with pytest.raises(TimeoutError, match="writing to"):
            ring_buffer.write_data_at(0, np.arange(4, dtype=np.float64), acquire_timeout=0)

    ring_buffer.write_data_at(0, np.arange(4, dtype=np.float64), acquire_timeout=0)


def test_waiting_writer_gate_blocks_new_readers(ring_buffer: RingBuffer):
    reader_gate = posix_ipc.Semaphore(ring_buffer.descriptor.reader_gate_ids[0])
    try:
        reader_gate.acquire()
        with pytest.raises(TimeoutError, match="reader gate"):
            ring_buffer.copy_data(0, acquire_timeout=0)
    finally:
        reader_gate.release()
        reader_gate.close()


def test_writer_on_one_slot_does_not_block_reader_on_other_slot(ring_buffer: RingBuffer):
    data_lock = posix_ipc.Semaphore(ring_buffer.descriptor.data_lock_ids[0])
    try:
        data_lock.acquire()
        ring_buffer.copy_data(1, acquire_timeout=0)
    finally:
        data_lock.release()
        data_lock.close()


@pytest.mark.parametrize("index", [-1, 2])
def test_copy_data_rejects_indices_outside_slots(ring_buffer: RingBuffer, index: int):
    with pytest.raises(IndexError, match="out of bounds"):
        ring_buffer.copy_data(index)


@pytest.mark.parametrize("index", [-1, 2])
def test_write_data_at_rejects_indices_outside_slots(ring_buffer: RingBuffer, index: int):
    with pytest.raises(IndexError, match="out of bounds"):
        ring_buffer.write_data_at(index, np.zeros((4,), dtype=np.float64))


@pytest.mark.parametrize(
    "data", [np.zeros((2,), dtype=np.float64), np.zeros((4,), dtype=np.float32)]
)
def test_write_data_rejects_payload_shape_or_dtype_mismatch(
    ring_buffer: RingBuffer, data: np.ndarray
):
    with pytest.raises(ValueError, match="does not match expected"):
        ring_buffer.write_data(data)


def test_destroy_is_idempotent_and_rejects_further_operations(
    ring_buffer: RingBuffer, payload: PayloadDescriptor
):
    ring_buffer.destroy()
    ring_buffer.destroy()

    with pytest.raises(RuntimeError, match="destroyed"):
        ring_buffer.write_data(np.zeros(payload.shape, dtype=payload.dtype.numpy_dtype))


def test_only_creator_owns_shared_memory_resources(ring_buffer: RingBuffer):
    view = RingBufferView(ring_buffer.descriptor)
    try:
        assert ring_buffer._owns_memory is True
        assert view._owns_memory is False
    finally:
        view.close()


def test_closing_view_keeps_owner_resources_attachable(ring_buffer: RingBuffer):
    view = RingBufferView(ring_buffer.descriptor)
    view.close()

    attached = RingBufferView(ring_buffer.descriptor)
    attached.close()
    assert ring_buffer.next_write_position == 0


def test_destroying_owner_unlinks_shared_memory_counter_memory_and_semaphores(
    ring_buffer: RingBuffer,
):
    descriptor = ring_buffer.descriptor

    ring_buffer.destroy()

    with pytest.raises(FileNotFoundError):
        shared_memory.SharedMemory(name=descriptor.name)
    with pytest.raises(FileNotFoundError):
        shared_memory.SharedMemory(name=descriptor.reader_count_name)
    with pytest.raises(posix_ipc.ExistentialError):
        posix_ipc.Semaphore(descriptor.data_lock_ids[0])
    with pytest.raises(posix_ipc.ExistentialError):
        posix_ipc.Semaphore(descriptor.reader_gate_ids[0])
    with pytest.raises(posix_ipc.ExistentialError):
        posix_ipc.Semaphore(descriptor.reader_count_lock_ids[0])
