from __future__ import annotations

from typing import TYPE_CHECKING

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.shared_memory.models import PayloadDescriptor
from bec_server.shared_memory.ring_buffer import RingBufferView

if TYPE_CHECKING:
    import numpy as np

    from bec_lib.connector import MessageObject
    from bec_lib.messages import SharedMemAllocationInfo
    from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger


# TODO one per service, or N per service.
class SharedMemoryClient:
    """Client for interacting with shared memory objects managed by the SharedMemoryManager."""

    def __init__(self, name: str, connector: RedisConnector):
        self.name = name
        self.connector = connector
        # signal name to ring buffer view mapping
        self._ring_buffer_views: dict[str, RingBufferView] = {}
        self.start()

    def start(self):
        """Start the client by subscribing to the shared memory object."""
        self.connector.register(MessageEndpoints.shared_memory_info(), cb=self._handle_info_update)

    def _handle_info_update(self, info: MessageObject) -> None:
        """Handle updates to the shared memory information."""
        info: SharedMemAllocationInfo = info.value
        # Any info update can potentially contain relevant information for creating or deleting ring buffer views.
        info_updates = []
        client_info = info.info.get(self.name, {})

        for signal, buff_info in client_info.items():
            info_updates.append(signal)
            if signal not in self._ring_buffer_views:  #
                self._ring_buffer_views[signal] = RingBufferView(descriptor=buff_info.buffer_desc)
            else:
                logger.error(
                    f"Ring buffer view for signal {signal} already exists, should not happend. Received info update: {buff_info}"
                )
        if len(client_info) < len(self._ring_buffer_views):
            # Some shared memory objects have been deallocated. Remove them from the local dictionary.
            to_be_removed = set(self._ring_buffer_views.keys()) - set(info_updates)
            for name in to_be_removed:
                view = self._ring_buffer_views.pop(name)
                view.close()

    def request_allocation(
        self, signal_name: str, slots: int, payload_desc: PayloadDescriptor | dict
    ) -> None:
        """Request the allocation of a shared memory object."""
        if isinstance(payload_desc, dict):
            payload_desc = PayloadDescriptor.model_validate(payload_desc)

        self.connector.xadd(
            MessageEndpoints.shared_memory_allocate(),
            {
                "client_id": self.name,
                "slots": slots,
                "payload_desc": payload_desc,
                "signal": signal_name,
            },
            max_size=1000,  # Keep history of 1000 allocation requests
        )

    def request_deallocation(self, signal_name: str) -> None:
        """Request the deallocation of a shared memory object."""
        self.connector.xadd(
            MessageEndpoints.shared_memory_deallocate(),
            {"client_id": self.name, "signal": signal_name},
            max_size=1000,  # Keep history of 1000 deallocation requests
        )

    def read_from_buffer(
        self, signal_name: str, index: int, timeout: float | None = None
    ) -> np.ndarray:
        """
        Read data from the shared memory buffer associated with the given signal name.
        If timeout is provided, the method will wait for the specified time and raise a TimeoutError if it cannot
        read the data within that time frame. Please be aware, this is meant to block during write/read operations.
        """
        # TODO add option to wait receiving an update on a specific signal in the buffer
        # Also block until there is an update on the specific index in the buffer.
        # Should there be a consume logic???
        buff = self._ring_buffer_views.get(signal_name)
        if buff is None:
            raise ValueError(f"No buffer found for signal name: {signal_name}")
        return buff.copy_data(index, timeout)

    def write_to_buffer(
        self, signal_name: str, data: np.ndarray, timeout: float | None = None
    ) -> int:
        """
        Write data to the next ring position associated with the given signal name.
        If timeout is provided, the method will wait for the specified time and raise a TimeoutError if it cannot
        write the data within that time frame. Please be aware, this is meant to block during write/read operations.

        Returns:
            int: The slot index containing the newly written payload.
        """
        buff = self._ring_buffer_views.get(signal_name)
        if buff is None:
            raise ValueError(f"No buffer found for signal name: {signal_name}")
        return buff.write_data(data=data, acquire_timeout=timeout)

    def shutdown(self) -> None:
        """Clean up resources and all shared memory views."""
        for view in self._ring_buffer_views.values():
            view.close()
        self._ring_buffer_views.clear()
        self.connector.unregister(
            MessageEndpoints.shared_memory_info(), cb=self._handle_info_update
        )


if __name__ == "__main__":
    import time

    import numpy as np

    from bec_lib.redis_connector import RedisConnector

    array = np.random.rand(5, 5)
    connector = RedisConnector(bootstrap="localhost:6379")
    client = SharedMemoryClient(name="test_client", connector=connector)
    client.request_allocation(
        signal_name="test_signal", slots=10, payload_desc=PayloadDescriptor.from_numpy(array)
    )
    time.sleep(1)  # Wait for the allocation to be processed
    print(client._ring_buffer_views)
