from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
from multiprocessing import resource_tracker, shared_memory
from threading import RLock
from typing import Any, Callable, Iterator
from uuid import uuid4

import numpy as np
import posix_ipc

from bec_server.shared_memory.models import PayloadDescriptor, RingBufferDescriptor

# pylint: disable=c-extension-no-member

MAX_SEMAPHORE_NAME_LENGTH = 30
READER_COUNT_DTYPE = np.dtype(np.uint32)


def not_destroyed(method: Callable[..., Any]) -> Callable[..., Any]:
    """Check that a shared-memory handle is still open before accessing it."""

    @wraps(method)
    def wrapper(self: RingBufferView, *args: Any, **kwargs: Any) -> Any:
        if self.destroyed:
            raise RuntimeError(
                f"Cannot perform operation on a destroyed {self.__class__.__name__} object with name {self.name!r}."
            )
        return method(self, *args, **kwargs)

    return wrapper


class RingBufferView:
    """Attached handle for accessing a ring buffer without owning its resources."""

    def __init__(
        self,
        descriptor: RingBufferDescriptor,
        shm: shared_memory.SharedMemory | None = None,
        reader_count_shm: shared_memory.SharedMemory | None = None,
        *,
        owns_memory: bool = False,
    ):
        self._validate_descriptor(descriptor)
        self._descriptor = descriptor
        self._shm = shm if shm is not None else shared_memory.SharedMemory(name=descriptor.name)
        self._reader_count_shm = (
            reader_count_shm
            if reader_count_shm is not None
            else shared_memory.SharedMemory(name=descriptor.reader_count_name)
        )
        self._owns_memory = owns_memory
        if not owns_memory:
            self._unregister_attached_shared_memory(self._shm)
            self._unregister_attached_shared_memory(self._reader_count_shm)
        self._data_locks = [
            posix_ipc.Semaphore(lock_id, flags=0) for lock_id in descriptor.data_lock_ids
        ]
        self._reader_gates = [
            posix_ipc.Semaphore(lock_id, flags=0) for lock_id in descriptor.reader_gate_ids
        ]
        self._reader_count_locks = [
            posix_ipc.Semaphore(lock_id, flags=0) for lock_id in descriptor.reader_count_lock_ids
        ]
        self._reader_counts = np.ndarray(
            shape=(descriptor.slots,), dtype=READER_COUNT_DTYPE, buffer=self._reader_count_shm.buf
        )
        self._next_write_position = 0
        self.__destroyed = False
        self._lifecycle_lock = RLock()

    @staticmethod
    def _validate_descriptor(descriptor: RingBufferDescriptor) -> None:
        lock_lengths = {
            "data_lock_ids": len(descriptor.data_lock_ids),
            "reader_gate_ids": len(descriptor.reader_gate_ids),
            "reader_count_lock_ids": len(descriptor.reader_count_lock_ids),
        }
        invalid = {
            name: length for name, length in lock_lengths.items() if length != descriptor.slots
        }
        if invalid:
            raise ValueError(
                f"Ring buffer descriptor must provide exactly one lock per slot: {invalid}"
            )

    @staticmethod
    def _unregister_attached_shared_memory(shm: shared_memory.SharedMemory) -> None:
        """Let the owning manager unlink shared memory without local tracker warnings."""
        if not getattr(shared_memory, "_USE_POSIX", False):
            return
        resource_tracker.unregister(shm._name, "shared_memory")

    @contextmanager
    def _acquire(
        self, semaphore: posix_ipc.Semaphore, timeout: float | None, operation: str
    ) -> Iterator[None]:
        acquired = False
        try:
            semaphore.acquire(timeout=None if timeout is None else timeout)
            acquired = True
            yield
        except posix_ipc.BusyError:
            raise TimeoutError(
                f"Could not acquire lock for {operation} buffer {self.name!r} within {timeout} seconds."
            ) from None
        finally:
            if acquired:
                semaphore.release()

    def _acquire_lock(
        self, semaphore: posix_ipc.Semaphore, timeout: float | None, operation: str
    ) -> bool:
        try:
            semaphore.acquire(timeout=None if timeout is None else timeout)
            return True
        except posix_ipc.BusyError:
            raise TimeoutError(
                f"Could not acquire lock for {operation} buffer {self.name!r} within {timeout} seconds."
            ) from None

    def _validate_index(self, index: int) -> None:
        if index < 0 or index >= self.slots:
            raise IndexError(
                f"Index {index} is out of bounds for ring buffer with {self.slots} slots."
            )

    def _validate_payload(self, data: np.ndarray) -> None:
        descriptor = PayloadDescriptor.from_numpy(data)
        if descriptor != self.payload_descriptor:
            raise ValueError(
                f"Data shape/dtype {descriptor.shape}/{descriptor.dtype} does not match expected "
                f"shape/dtype {self.payload_descriptor.shape}/{self.payload_descriptor.dtype}"
            )

    def _array_for_slot(self, index: int) -> np.ndarray:
        return np.ndarray(
            shape=self.payload_descriptor.shape,
            dtype=self.payload_descriptor.dtype.numpy_dtype,
            buffer=self._shm.buf,
            offset=index * self.bytes_per_slot,
        )

    @contextmanager
    def _read_slot_lock(self, index: int, acquire_timeout: float | None) -> Iterator[None]:
        gate_acquired = False
        count_lock_acquired = False
        try:
            self._acquire_lock(
                self._reader_gates[index], acquire_timeout, "entering reader gate for"
            )
            gate_acquired = True
            self._acquire_lock(
                self._reader_count_locks[index], acquire_timeout, "updating reader count for"
            )
            count_lock_acquired = True
            if self._reader_counts[index] == 0:
                self._acquire_lock(self._data_locks[index], acquire_timeout, "reading from")
            self._reader_counts[index] += 1
        finally:
            if count_lock_acquired:
                self._reader_count_locks[index].release()
            if gate_acquired:
                self._reader_gates[index].release()

        try:
            yield
        finally:
            with self._acquire(
                self._reader_count_locks[index], acquire_timeout, "updating reader count for"
            ):
                if self._reader_counts[index] == 0:
                    raise RuntimeError("Reader count underflow while releasing ring buffer slot.")
                self._reader_counts[index] -= 1
                if self._reader_counts[index] == 0:
                    self._data_locks[index].release()

    @contextmanager
    def _write_slot_lock(self, index: int, acquire_timeout: float | None) -> Iterator[None]:
        gate_acquired = False
        data_lock_acquired = False
        try:
            self._acquire_lock(
                self._reader_gates[index], acquire_timeout, "entering writer gate for"
            )
            gate_acquired = True
            self._acquire_lock(self._data_locks[index], acquire_timeout, "writing to")
            data_lock_acquired = True
            yield
        finally:
            if data_lock_acquired:
                self._data_locks[index].release()
            if gate_acquired:
                self._reader_gates[index].release()

    @not_destroyed
    def copy_data(self, index: int, acquire_timeout: float | None = 0) -> np.ndarray:
        """Copy one identified payload slot while allowing concurrent readers."""
        self._validate_index(index)
        with self._read_slot_lock(index, acquire_timeout):
            return self._array_for_slot(index).copy()

    @not_destroyed
    def write_data(self, data: np.ndarray, acquire_timeout: float | None = 0) -> int:
        """Write using this writer handle's local circular slot cursor."""
        index = self._next_write_position
        self.write_data_at(index, data, acquire_timeout)
        self._next_write_position = (index + 1) % self.slots
        return index

    @not_destroyed
    def write_data_at(
        self, index: int, data: np.ndarray, acquire_timeout: float | None = 0
    ) -> None:
        """Write directly to an identified slot using the slot writer lock."""
        self._validate_index(index)
        self._validate_payload(data)
        with self._write_slot_lock(index, acquire_timeout):
            np.copyto(self._array_for_slot(index), data)

    @property
    def descriptor(self) -> RingBufferDescriptor:
        return self._descriptor

    @property
    def destroyed(self) -> bool:
        return self.__destroyed

    @property
    def name(self) -> str:
        return self._descriptor.name

    @property
    def reader_count_name(self) -> str:
        return self._descriptor.reader_count_name

    @property
    def slots(self) -> int:
        return self._descriptor.slots

    @property
    def bytes_per_slot(self) -> int:
        return self._descriptor.payload.nbytes

    @property
    def payload_descriptor(self) -> PayloadDescriptor:
        return self._descriptor.payload

    @property
    def next_write_position(self) -> int:
        return self._next_write_position

    def _close_handles(self) -> None:
        for lock in (*self._data_locks, *self._reader_gates, *self._reader_count_locks):
            lock.close()
        self._reader_count_shm.close()
        self._shm.close()

    def close(self) -> None:
        """Close local handles without unlinking owner-managed resources."""
        if self.destroyed:
            return
        with self._lifecycle_lock:
            if self.destroyed:
                return
            self._close_handles()
            self.__destroyed = True

    def destroy(self) -> None:
        """Compatibility alias for attached clients; attached handles only close resources."""
        self.close()


class RingBuffer(RingBufferView):
    """Owner of a shared ring buffer and its semaphore resources."""

    @staticmethod
    def _semaphore_name(name: str, suffix: str) -> str:
        semaphore_name = f"{name}{suffix}"
        if len(semaphore_name) > MAX_SEMAPHORE_NAME_LENGTH:
            raise ValueError(
                f"Semaphore name {semaphore_name!r} exceeds the platform limit of "
                f"{MAX_SEMAPHORE_NAME_LENGTH} characters."
            )
        return semaphore_name

    def __init__(self, slots: int, payload: PayloadDescriptor, name_suffix: str = ""):
        if not 0 < slots:
            raise ValueError("Ring buffer must contain at least one slot.")
        name = f"bec_psm_{uuid4().hex[:6]}"
        reader_count_name = f"{name}_cnt"
        data_lock_names = tuple(self._semaphore_name(name, f"_d_{index}") for index in range(slots))
        reader_gate_names = tuple(
            self._semaphore_name(name, f"_g_{index}") for index in range(slots)
        )
        reader_count_lock_names = tuple(
            self._semaphore_name(name, f"_c_{index}") for index in range(slots)
        )
        shm = shared_memory.SharedMemory(create=True, size=slots * payload.nbytes, name=name)
        reader_count_shm = shared_memory.SharedMemory(
            create=True, size=slots * READER_COUNT_DTYPE.itemsize, name=reader_count_name
        )
        reader_counts = np.ndarray(
            shape=(slots,), dtype=READER_COUNT_DTYPE, buffer=reader_count_shm.buf
        )
        reader_counts[:] = 0
        lock_names = (*data_lock_names, *reader_gate_names, *reader_count_lock_names)
        created_locks: list[posix_ipc.Semaphore] = []
        try:
            created_locks.extend(
                posix_ipc.Semaphore(
                    lock_name, flags=posix_ipc.O_CREAT | posix_ipc.O_EXCL, initial_value=1
                )
                for lock_name in lock_names
            )
            for lock in created_locks:
                lock.close()
            descriptor = RingBufferDescriptor(
                name=shm.name,
                reader_count_name=reader_count_shm.name,
                data_lock_ids=data_lock_names,
                reader_gate_ids=reader_gate_names,
                reader_count_lock_ids=reader_count_lock_names,
                slots=slots,
                payload=payload,
            )
            super().__init__(
                descriptor=descriptor, shm=shm, reader_count_shm=reader_count_shm, owns_memory=True
            )
        except Exception:
            for lock in created_locks:
                try:
                    lock.close()
                except OSError:
                    pass
                try:
                    posix_ipc.unlink_semaphore(lock.name)
                except posix_ipc.ExistentialError:
                    pass
            reader_count_shm.close()
            reader_count_shm.unlink()
            shm.close()
            shm.unlink()
            raise

    def destroy(self) -> None:
        """Close and unlink all resources created for this owned ring buffer."""
        if self.destroyed:
            return
        descriptor = self.descriptor
        self.close()
        self._reader_count_shm.unlink()
        self._shm.unlink()
        for lock_id in (
            *descriptor.data_lock_ids,
            *descriptor.reader_gate_ids,
            *descriptor.reader_count_lock_ids,
        ):
            try:
                posix_ipc.unlink_semaphore(lock_id)
            except posix_ipc.ExistentialError:
                pass

    @classmethod
    def _name_suffix(cls, name: str, suffix: str, max_length: int = 63) -> str:
        if suffix:
            name = f"{name}_{suffix}"
        return name[:max_length]
