from __future__ import annotations

from functools import wraps
from multiprocessing import resource_tracker, shared_memory
from threading import RLock
from typing import Any, Callable
from uuid import uuid4

import numpy as np
import posix_ipc

from bec_server.shared_memory.models import PayloadDescriptor, RingBufferDescriptor

# pylint: disable=c-extension-no-member


def not_destroyed(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to check if the RingBufferView has been destroyed before allowing method execution."""

    @wraps(method)
    def wrapper(self: RingBufferView, *args: Any, **kwargs: Any) -> Any:
        if self.destroyed:
            raise RuntimeError(
                f"Cannot perform operation on a destroyed {self.__class__.__name__} object with name {self.name!r}."
            )
        return method(self, *args, **kwargs)

    return wrapper


class RingBufferView:
    """
    Class for handling shared RingBuffer objects from clients, which attach to an existing shared memory object
    defined by a RingBufferDescriptor. The view can be used to read/write to the buffer without owning the memory.
    It can not be used to create new shared memory objects, which is reserved for the RingBuffer class.
    """

    def __init__(
        self, descriptor: RingBufferDescriptor, shm: shared_memory.SharedMemory | None = None
    ):
        self._descriptor = descriptor
        self._shm = shm if shm is not None else shared_memory.SharedMemory(name=descriptor.name)
        self._owns_memory = shm is None
        self._semaphore_lock = posix_ipc.Semaphore(descriptor.lock_id, flags=0)
        self.__destroyed = False
        self._lock = RLock()
        # # TODO: Check why this might be needed, but to be sure to lock is accidently kept.
        # self._semaphore_lock.release()

    ############
    # API
    ############

    @not_destroyed
    def copy_data(self, index: int, acquire_timeout: float = 0) -> np.ndarray:
        """
        Returns a copy of the data at the given slot index as a numpy array. While the data is being copied,
        the shared memory is locked to prevent concurrent modifications. Once copied, the shared memory is released.
        NOTE: The additional argument acquire_timeout can be used to specify a timeout for acquiring the lock. The
                default value of 0 means that it will wait indefinitely until the lock is acquired. If the lock cannot
                be acquired within the specified timeout, a TimeoutError will be raised. Please NOTE that this feature
                requires the underlying OS to support timeouts for posix semaphores, which is for example not the case for MAC OS.

        Args:
            index (int): The slot index to copy data from.
            acquire_timeout (float): The timeout in seconds to acquire the lock. If 0, it will wait indefinitely.

        Returns:
            np.ndarray: A copy of the data at the specified slot index.
        Raises:
            TimeoutError: If the lock cannot be acquired within the specified timeout.
        """
        if index < 0 or index >= self.slots:
            raise IndexError(
                f"Index {index} is out of bounds for ring buffer with {self.slots} slots."
            )
        with self._lock:
            try:
                self._semaphore_lock.acquire(timeout=acquire_timeout)
                array = np.ndarray(
                    shape=self.payload_descriptor.shape,
                    dtype=self.payload_descriptor.dtype.numpy_dtype,
                    buffer=self._shm.buf,
                    offset=index * self.bytes_per_slot,
                )
                local_copy = array.copy()  # Make a local copy of the data
            except posix_ipc.BusyError:
                # pylint: disable=raise-missing-from
                raise TimeoutError(
                    f"Could not acquire lock for reading from buffer {self.name!r} within {acquire_timeout} seconds."
                )
            finally:
                self._semaphore_lock.release()
            return local_copy

    @not_destroyed
    def write_data(self, index: int, data: np.ndarray, acquire_timeout: float = 0) -> None:
        """
        Writes the given numpy array data to the specified slot index in the shared memory. While the data is being
        written, the shared memory is locked to prevent concurrent modifications. Once the data is written, the shared
        memory is released.
        NOTE: The additional argument acquire_timeout can be used to specify a timeout for acquiring the lock. The
                default value of 0 means that it will wait indefinitely until the lock is acquired. If the lock cannot
                be acquired within the specified timeout, a TimeoutError will be raised. Please NOTE that this feature
                requires the underlying OS to support timeouts for posix semaphores, which is for example not the case for MAC OS.

        Args:
            index (int): The slot index to write data to.
            data (np.ndarray): The numpy array data to write to the shared memory.
            acquire_timeout (float): The timeout in seconds to acquire the lock. If 0, it will wait indefinitely.

        Raises:
            ValueError: If the size of the data does not match the expected size defined by the
                        payload descriptor.
            TimeoutError: If the lock cannot be acquired within the specified timeout.
        """
        if index < 0 or index >= self.slots:
            raise IndexError(
                f"Index {index} is out of bounds for ring buffer with {self.slots} slots."
            )
        descriptor = PayloadDescriptor.from_numpy(data)
        if descriptor != self.payload_descriptor:
            raise ValueError(
                f"Data shape/dtype {descriptor.shape}/{descriptor.dtype} does not match expected "
                f"shape/dtype {self.payload_descriptor.shape}/{self.payload_descriptor.dtype}"
            )
        with self._lock:
            try:
                self._semaphore_lock.acquire(timeout=acquire_timeout)
                array = np.ndarray(
                    shape=self.payload_descriptor.shape,
                    dtype=self.payload_descriptor.dtype.numpy_dtype,
                    buffer=self._shm.buf,
                    offset=index * self.bytes_per_slot,
                )
                np.copyto(array, data)  # Copy data into shared memory
            except posix_ipc.BusyError:
                # pylint: disable=raise-missing-from
                raise TimeoutError(
                    f"Could not acquire lock for reading from buffer {self.name!r} within {acquire_timeout} seconds."
                )
            finally:
                self._semaphore_lock.release()

    ############
    # Properties
    ############

    @property
    def descriptor(self) -> RingBufferDescriptor:
        """Return the descriptor for this RingBuffer."""
        return self._descriptor

    @property
    def destroyed(self) -> bool:
        """Indicates whether the view has been destroyed."""
        return self.__destroyed

    @property
    def name(self):
        """Name of shared ring buffer"""
        return self._descriptor.name

    @property
    def slots(self):
        """Max Index of shared ring buffer"""
        return self._descriptor.slots

    @property
    def bytes_per_slot(self):
        """Bytes per index in shared ring buffer"""
        return self._descriptor.bytes_per_slot

    @property
    def payload_descriptor(self):
        """Payload descriptor for the data stored in the ring buffer."""
        return self._descriptor.payload

    def destroy(self):
        """
        Destroy the shared memory object. The method can be called multiple times but only the first call will have an effect.
        """
        if self.destroyed:
            return
        with self._lock:
            # Semaphore lock
            self._semaphore_lock.release()  # Make sure to release upon closing to avoid deadlocks if the lock is still held by this process
            # Shared memory
            self._shm.close()
            # Cleanup depends on whether the memory is owned by this view or not.
            if self._owns_memory:
                self._semaphore_lock.unlink()
                self._shm.unlink()
            else:
                # NOTE: From Python 3.13 onwards, we can use the track=False option when creating the reference
                # For views not owning the memory, we have to manually unregister it.
                # pylint: disable=protected-access
                resource_tracker.unregister(self._shm._name, "shared_memory")
                self._semaphore_lock.close()

        # to avoid registering the shared memory with the resource tracker.
        self.__destroyed = True


class RingBuffer(RingBufferView):
    """
    RingBuffer class that owns the shared memory. If created, it will create a new sharedMemory object together with a semaphore lock.

    Args:
        slots (int): The number of slots in the ring buffer.
        payload (PayloadDescriptor): The descriptor for the data payload stored in each slot of the ring buffer.
        name_suffix (str): An optional suffix to append to the shared memory and semaphore names for identification.
    """

    def __init__(self, slots: int, payload: PayloadDescriptor, name_suffix: str = ""):
        name = f"bec_psm_{uuid4().hex[:6]}"
        shm = shared_memory.SharedMemory(create=True, size=slots * payload.nbytes, name=name)
        lock_name = f"{name}_lock"
        semaphore_lock = posix_ipc.Semaphore(lock_name, flags=posix_ipc.O_CREAT, initial_value=1)
        self._descriptor = RingBufferDescriptor(
            name=shm.name,
            lock_id=semaphore_lock.name,
            slots=slots,
            bytes_per_slot=payload.nbytes,
            payload=payload,
        )
        super().__init__(descriptor=self._descriptor, shm=shm)

    @classmethod
    def _name_suffix(cls, name: str, suffix: str, max_length: int = 63) -> str:
        if suffix:
            name = f"{name}_{suffix}"
        return name[:max_length]
