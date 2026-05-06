from __future__ import annotations

import sys
from contextlib import contextmanager
from enum import IntEnum
from multiprocessing import shared_memory
from typing import Iterator, Literal, Tuple

import numpy as np
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


class PayloadDescriptor(BaseModel):
    nbytes: int
    shape: Tuple[int, ...]
    dtype: DTypeDescriptor
    layout: Literal["C"] = "C"

    @classmethod
    def from_numpy(cls, array: np.ndarray) -> "PayloadDescriptor":

        return cls(
            nbytes=array.nbytes,
            shape=array.shape,
            dtype=DTypeDescriptor.from_numpy(array.dtype),
            layout="C" if array.flags.c_contiguous else "C",
        )


class SharedRingBufferDescriptor(BaseModel):
    """Descriptor for SharedRingBuffer object."""

    name: str
    slots: int
    bytes_per_slot: int
    slot_state_bytes: int
    payload: PayloadDescriptor


class SharedRingBuffer:
    """Descriptor for RingBuffer Object to share memory across processes."""

    SLOT_STATE_BYTES = 1

    def __init__(
        self,
        shm: shared_memory.SharedMemory,
        payload: PayloadDescriptor,
        slots: int,
        bytes_per_slot: int,
        owns_memory: bool = False,
    ):
        self._shm = shm
        self._slots = slots
        self._bytes_per_slot = bytes_per_slot
        self._owns_memory = owns_memory
        self._payload = payload

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
        bytes_per_slot = payload.nbytes + cls.SLOT_STATE_BYTES
        total_size = slots * (bytes_per_slot)
        shm = shared_memory.SharedMemory(create=True, size=total_size)
        ring_buffer = cls(
            shm, slots=slots, bytes_per_slot=bytes_per_slot, payload=payload, owns_memory=True
        )
        for slot in range(slots):
            ring_buffer.set_state(slot, SlotState.READY_TO_WRITE.value)
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
        )

    def descriptor(self) -> SharedRingBufferDescriptor:
        """Create a serializable descriptor for this ring buffer."""

        return SharedRingBufferDescriptor(
            name=self.name,
            slots=self.slots,
            bytes_per_slot=self.bytes_per_slot,
            slot_state_bytes=self.SLOT_STATE_BYTES,
            payload=self.payload,
        )

    def _data(self, index: int) -> memoryview:
        """Return payload memory for one slot."""
        start = self._payload_offset(index)
        stop = start + self.payload.nbytes
        return self._shm.buf[start:stop]

    def _slot_offset(self, index: int) -> int:
        self._validate_index(index)
        return index * self.bytes_per_slot

    def _slot_state_offset(self, index: int) -> int:
        return self._slot_offset(index)

    def _payload_offset(self, index: int) -> int:
        return self._slot_offset(index) + self.SLOT_STATE_BYTES

    def _validate_index(self, index: int) -> None:
        if not 0 <= index < self.slots:
            raise IndexError(f"Index {index} outside valid range 0..{self.slots - 1}.")

    def state(self, index: int) -> SlotState:
        """Return the current slot state."""
        offset = self._slot_state_offset(index)
        return SlotState(self._shm.buf[offset])

    def set_state(self, index: int, state: SlotState) -> None:
        """Set the current slot state."""
        offset = self._slot_state_offset(index)
        self._shm.buf[offset] = int(state)

    @contextmanager
    def read_slot(self, index: int, force: bool = False) -> Iterator[memoryview]:
        """Read from a slot and mark it writable afterwards."""
        if force:
            valid_read_states = [SlotState.READY_TO_READ.value, SlotState.READY_TO_WRITE.value]
        else:
            valid_read_states = [SlotState.READY_TO_WRITE.value]
        while not self.state(index) in valid_read_states:
            ...
        self.set_state(index, SlotState.READING)
        try:
            yield self._data(index)
        finally:
            self.set_state(index, SlotState.READY_TO_WRITE)

    @contextmanager
    def write_slot(self, index: int, force: bool = False) -> Iterator[memoryview]:
        """Write to a slot and mark it readable afterwards."""
        if force:
            valid_write_states = [SlotState.READY_TO_READ.value, SlotState.READY_TO_WRITE.value]
        else:
            valid_write_states = [SlotState.READY_TO_WRITE.value]

        while not self.state(index) in valid_write_states:
            ...
        self.set_state(index, SlotState.WRITING)
        try:
            yield self._data(index)
        except Exception as exc:
            self.set_state(index, SlotState.READY_TO_WRITE)
            raise exc
        else:
            self.set_state(index, SlotState.READY_TO_READ)

    def close(self):
        """Close the shared memory object."""
        self._shm.close()

    def unlink(self):
        if not self._owns_memory:
            raise RuntimeError(f"Can't unlike memory {self.name} that is not owned by this process")
        self._shm.unlink()

    # TODO shutdown procedure for proper clean up of resources..
