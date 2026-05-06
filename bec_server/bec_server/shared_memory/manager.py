from typing import Literal, TypeVar

from bec_lib.messages import BECMessage

SUPPORTED_DATATYPES = Literal["str", "float", "byte", "np.array", "list", "dict"]


class SharedMemRequestAllocation(BECMessage):
    """Message to send to the shared memory manager to create a new shared memory object."""

    sender: Literal["device", "client"]
    device: str | None = None


class SharedMemDescriptor(BECMessage):
    """Message with metadata about the shared memory created in the shared memory manager."""

    id: str
    max_index: int
    owner: Literal["device", "client"]
    device: str | None = None
    shape: tuple[int, ...]
    dtype: SUPPORTED_DATATYPES


class AvailableDataAnalysisMethods(BECMessage):
    """Message published by the DAP server on which analysis methods are available."""

    methods: list[str]


class DataAnalysisRequestWarmup(BECMessage):
    """Message to request a data analysis"""

    shared_mem: SharedMemDescriptor


class DataAnalysisRequest(BECMessage):
    """Message to request processing of a shared memory object."""

    shared_mem: SharedMemDescriptor
    index: int
    methods: list[str]


class DataAnalysisResponse(BECMessage):
    """Message to request processing of a shared memory object."""

    shared_mem: SharedMemDescriptor
    index: int
    methods: list[str]
    results: dict


class SharedMemoryManager:

    def shutdown(self):
        """Shutdown method, should clean up all shared memory objects."""

    def create_shared_mem(self, msg: SharedMemRequestAllocation) -> str:
        """Creates a shared memory object under a unique name."""

    def _publish_shared_mem_info(self, msg: SharedMemDescriptor):
        """Publish information about a shared memory object."""
