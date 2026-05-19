from __future__ import annotations

import threading
from collections import defaultdict
from typing import TYPE_CHECKING, Literal, Tuple

from bec_lib import messages
from bec_lib.bec_service import BECService
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.shared_memory.models import SharedMemInfo
from bec_server.shared_memory.ring_buffer import RingBuffer

SUPPORTED_DATATYPES = Literal["str", "float", "byte", "np.array", "list", "dict"]

if TYPE_CHECKING:
    from bec_lib.redis_connector import MessageObject, RedisConnector

logger = bec_logger.logger


class SharedMemoryManager(BECService):
    """
    Service to manage shared memory objects. It keeps track of all allocated shared memory objects and their descriptors.
    It also handles the creation and destruction of shared memory objects, and publishes the updated list of shared memory objects
    whenever a new shared memory object is created or destroyed.
    """

    def __init__(self, config, connector_cls: type[RedisConnector]) -> None:
        super().__init__(config, connector_cls, unique_service=True)
        # Shared memory objects are stored in a dictionary with the client_id and signal name tuple as key
        # and the RingBuffer instance as value
        self._shared_memory_objects: dict[Tuple[str, str], RingBuffer] = {}
        self._shared_memory_info: dict[str, dict[str, SharedMemInfo]] = defaultdict(
            dict
        )  # Nested dict with client_id as key, and dict with signal name and ShareMemInfo as value
        self.lock = threading.RLock()

    def _allocate_memory(self, request: messages.SharedMemAllocationRequest) -> None:
        """Callback function to handle shared memory allocation requests."""
        if isinstance(request, dict):
            request = messages.SharedMemAllocationRequest.model_validate(request)
        if (request.client_id, request.signal) in self._shared_memory_objects:
            logger.error(
                f"Shared memory object for client {request.client_id} and signal {request.signal} already exists. Overwriting."
            )
            # TODO should this republish the info?
            self._publish_allocation_info(self._shared_memory_info)
            return

        buff = RingBuffer(
            slots=request.slots, payload=request.payload_desc, name_suffix=request.signal
        )
        with self.lock:
            self._shared_memory_objects[(request.client_id, request.signal)] = buff
            self._shared_memory_info[request.client_id][request.signal] = SharedMemInfo(
                client_id=request.client_id, buffer_desc=buff.descriptor, signal=request.signal
            )
            self._publish_allocation_info(self._shared_memory_info)

    def _deallocate_memory(self, request: messages.SharedMemDeallocationRequest) -> None:
        """Callback function to handle shared memory deallocation requests."""
        if isinstance(request, dict):
            request = messages.SharedMemDeallocationRequest.model_validate(request)
        if (request.client_id, request.signal) not in self._shared_memory_objects:
            logger.error(
                f"Shared memory object for client {request.client_id} and signal {request.signal} does not exist. Cannot deallocate."
            )
            # TODO should this republish the info?
            self._publish_allocation_info(self._shared_memory_info)
            return

        with self.lock:
            buff = self._shared_memory_objects.pop((request.client_id, request.signal))
            buff.destroy()
            self._shared_memory_info[request.client_id].pop(request.signal, None)
            self._publish_allocation_info(self._shared_memory_info)

    def _publish_allocation_info(self, info: dict[str, dict[str, SharedMemInfo]]) -> None:
        """Publish the updated list of allocated shared memory objects."""
        self.connector.set_and_publish(
            MessageEndpoints.shared_memory_info(), messages.SharedMemAllocationInfo(info=info)
        )

    def start(self) -> None:
        """start the shared memory manager server"""
        self.connector.register(MessageEndpoints.shared_memory_allocate(), cb=self._allocate_memory)
        self.connector.register(
            MessageEndpoints.shared_memory_deallocate(), cb=self._deallocate_memory
        )

    def stop(self) -> None:
        with self.lock:
            for buff in self._shared_memory_objects.values():
                buff.destroy()
            self._shared_memory_objects.clear()
            self._shared_memory_info.clear()
            self._publish_allocation_info({})
        # Cleanup bec service related resources

    def shutdown(self) -> None:
        """Shutdown the shared memory manager server and destroy all shared memory objects."""
        self.stop()
        super().shutdown()
