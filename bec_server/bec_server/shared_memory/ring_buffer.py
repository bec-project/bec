from __future__ import annotations

import sys
from contextlib import contextmanager
from enum import IntEnum
from multiprocessing import shared_memory
from typing import Iterator, Literal, Tuple

import numpy as np
import posix_ipc
from pydantic import BaseModel


class SlotState(IntEnum):
    """State of the data at memory slot."""

    READY_TO_WRITE = 0
    WRITING = 1
    READY_TO_READ = 2
    READING = 3


class DTypeDescriptor(BaseModel):
    kind: Literal["uint", "int", "float", "bool"]
    itemsize: int
    byte_order: Literal["little", "big"] = "little"

    @classmethod
    def from_numpy(cls, dtype: np.dtype) -> "DTypeDescriptor":
        """Class method to create DTypeDescriptor from numpy dtype."""
        dtype = np.dtype(dtype)
        kind_map = {"u": "uint", "i": "int", "f": "float", "b": "bool"}
        if dtype.kind not in kind_map:
            raise ValueError(f"Unsupported dtype kind: {dtype.kind!r}")

        byte_order = dtype.byteorder
        if byte_order in ("=", "|"):
            byte_order = sys.byteorder
        elif byte_order == "<":
            byte_order = "little"
        elif byte_order == ">":
            byte_order = "big"
        else:
            raise ValueError(f"Unsupported byte order: {dtype.byteorder!r}")

        return cls(kind=kind_map[dtype.kind], itemsize=dtype.itemsize, byte_order=byte_order)

    @property
    def numpy_dtype(self) -> np.dtype:
        """Return the corresponding numpy dtype for this DTypeDescriptor."""
        byte_order_char = {"little": "<", "big": ">"}[self.byte_order]
        kind_char = {"uint": "u", "int": "i", "float": "f", "bool": "b"}[self.kind]
        dtype_str = f"{byte_order_char}{kind_char}{self.itemsize}"
        return np.dtype(dtype_str)


class PayloadDescriptor(BaseModel):
    """Descriptor for the data payload stored in each slot of the ring buffer."""

    nbytes: int
    shape: Tuple[int, ...]
    dtype: DTypeDescriptor
    layout: Literal["C"] = "C"

    @classmethod
    def from_numpy(cls, array: np.ndarray) -> "PayloadDescriptor":
        """Class method to create PayloadDescriptor from a numpy array."""
        return cls(
            nbytes=array.nbytes,
            shape=array.shape,
            dtype=DTypeDescriptor.from_numpy(array.dtype),
            layout="C" if array.flags.c_contiguous else "C",
        )


class SharedRingBufferDescriptor(BaseModel):
    """Descriptor for SharedRingBuffer object."""

    name: str
    lock_id: str
    slots: int
    bytes_per_slot: int
    payload: PayloadDescriptor


class SharedRingBuffer:
    """Descriptor for RingBuffer Object to share memory across processes."""

    def __init__(
        self,
        shm: shared_memory.SharedMemory,
        payload: PayloadDescriptor,
        slots: int,
        bytes_per_slot: int,
        owns_memory: bool = False,
        lock_id: str | None = None,
    ):
        self._shm = shm
        self._slots = slots
        self._bytes_per_slot = bytes_per_slot
        self._owns_memory = owns_memory
        self._payload = payload
        self._semaphore_lock = (
            posix_ipc.Semaphore(shm.name + "_lock", flags=posix_ipc.O_CREAT, initial_value=1)
            if lock_id is None
            else posix_ipc.Semaphore(lock_id, flags=0)
        )

    @property
    def name(self):
        """Name of shared ring buffer"""
        return self._shm.name

    @property
    def slots(self):
        """Max Index of shared ring buffer"""
        return self._slots

    @property
    def bytes_per_slot(self):
        """Bytes per index in shared ring buffer"""
        return self._bytes_per_slot

    @property
    def payload(self):
        return self._payload

    @classmethod
    def create(cls, slots: int, payload: PayloadDescriptor | dict) -> SharedRingBuffer:
        """Create a new shared memory location and SharedRingBuffer object."""
        if isinstance(payload, dict):
            payload = PayloadDescriptor.model_validate(payload)
        bytes_per_slot = payload.nbytes
        total_size = slots * (bytes_per_slot)
        shm = shared_memory.SharedMemory(create=True, size=total_size)
        ring_buffer = cls(
            shm, slots=slots, bytes_per_slot=bytes_per_slot, payload=payload, owns_memory=True
        )
        return ring_buffer

    @classmethod
    def attach(cls, descriptor: SharedRingBufferDescriptor) -> SharedRingBuffer:
        """Create SharedRingBuffer by attaching to an existing shared memory object by descriptor name."""
        shm = shared_memory.SharedMemory(name=descriptor.name)
        return cls(
            shm,
            slots=descriptor.slots,
            bytes_per_slot=descriptor.bytes_per_slot,
            payload=descriptor.payload,
            lock_id=descriptor.lock_id,
        )

    def descriptor(self) -> SharedRingBufferDescriptor:
        """Create a serializable descriptor for this ring buffer."""

        return SharedRingBufferDescriptor(
            name=self.name,
            slots=self.slots,
            bytes_per_slot=self.bytes_per_slot,
            payload=self.payload,
            lock_id=self._semaphore_lock.name,
        )

    def _data(self, index: int) -> memoryview:
        """Return payload memory for one slot."""
        start = self._slot_offset(index)
        stop = start + self.payload.nbytes
        return self._shm.buf[start:stop]

    def _slot_offset(self, index: int) -> int:
        self._validate_index(index)
        return index * self.bytes_per_slot

    def _validate_index(self, index: int) -> None:
        if not 0 <= index < self.slots:
            raise IndexError(f"Index {index} outside valid range 0..{self.slots - 1}.")

    def state(self, index: int) -> SlotState:
        """Return the current slot state."""
        offset = self._slot_offset(index)
        return SlotState(self._shm.buf[offset])

    @contextmanager
    def read_slot(self, index: int, timeout_lock: float = 0) -> Iterator[memoryview]:
        """Read from a slot and mark it writable afterwards."""
        try:
            self._semaphore_lock.acquire(timeout=timeout_lock)
            yield self._data(index)
        finally:
            self._semaphore_lock.release()

    @contextmanager
    def write_slot(self, index: int, timeout_lock: float = 0) -> Iterator[memoryview]:
        """Write to a slot and mark it readable afterwards."""
        try:
            self._semaphore_lock.acquire(timeout=timeout_lock)
            yield self._data(index)
        finally:
            self._semaphore_lock.release()

    def close(self):
        """Close the shared memory object."""
        self._shm.close()

    def unlink(self):
        if not self._owns_memory:
            raise RuntimeError(f"Can't unlink memory {self.name} that is not owned by this process")
        self.close()
        self._shm.unlink()
        posix_ipc.unlink_semaphore(self._semaphore_lock.name)

    def shutdown(self):
        """Close and unlink the shared memory object if owned."""
        self.close()
        if self._owns_memory:
            self.unlink()


# TODO to be tested, check if semaphore locking works
