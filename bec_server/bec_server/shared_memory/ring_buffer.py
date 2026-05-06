from __future__ import annotations

from dataclasses import dataclass
from multiprocessing import shared_memory
from typing import Any, Literal, Tuple

from pydantic import BaseModel


@dataclass(frozen=True)
class DTypeDescriptor:
    kind: Literal["uint", "int", "float", "bool"]
    itemsize: int
    byte_order: Literal["little", "big"] = "little"


@dataclass(frozen=True)
class PayloadDescriptor:
    nbytes: int
    shape: Tuple[int, ...]
    dtype: DTypeDescriptor
    layout: Literal["C"] = "C"


class SharedRingBufferDescriptor(BaseModel):
    """Descriptor for SharedRingBuffer object."""

    name: str
    max_index: int
    bytes_per_index: int
    payload: PayloadDescriptor
    # owner: Literal["device" , "client"] # To be checked if needed


class SharedRingBuffer:
    """Descriptor for RingBuffer Object to share memory across processes."""

    def __init__(
        self,
        shm: shared_memory.SharedMemory,
        max_index: int,
        bytes_per_index: int,
        owns_memory: bool = False,
    ):
        self._shm = shm
        self._max_index = max_index
        self._bytes_per_index = bytes_per_index
        self._owns_memory = owns_memory

    @property
    def name(self):
        """Name of shared ring buffer"""
        return self._shm.name

    @property
    def max_index(self):
        """Max Index of shared ring buffer"""
        return self._max_index

    @property
    def bytes_per_index(self):
        """Bytes per index in shared ring buffer"""
        return self._bytes_per_index

    @classmethod
    def create(cls, max_index: int, bytes_per_index: int) -> SharedRingBuffer:
        """Create a new shared memory location and SharedRingBuffer object."""
        total_size = max_index * bytes_per_index
        shm = shared_memory.SharedMemory(create=True, size=total_size)
        return cls(shm, max_index=max_index, bytes_per_index=bytes_per_index, owns_memory=True)

    @classmethod
    def attach(cls, descriptor: SharedRingBufferDescriptor) -> SharedRingBuffer:
        """Create SharedRingBuffer by attaching to an existing shared memory object by descriptor name."""
        shm = shared_memory.SharedMemory(name=descriptor.name)
        return cls(shm, max_index=descriptor.max_index, bytes_per_index=descriptor.bytes_per_index)

    def data(self, index: int) -> Any:
        """Get data from SharedRingBuffer from index."""
        start = index * self.bytes_per_index
        stop = start + self.bytes_per_index
        return self._shm[start:stop]

    def close(self):
        """Close the shared memory object."""
        self._shm.close()

    def unlink(self):
        if not self._owns_memory:
            raise RuntimeError(f"Can't unlike memory {self.name} that is not owned by this process")
        self._shm.unlink()
