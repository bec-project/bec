from typing import Literal

from bec_lib.messages import BECMessage

SUPPORTED_DATATYPES = Literal["str", "float", "byte", "np.array", "list", "dict"]


#################
## Messages
#################
class SharedMemRequestAllocation(BECMessage):
    """Message to send to the shared memory manager to create a new shared memory object."""

    sender: Literal["device", "client"]
    device: str | None = None


class SharedMemDescriptor(BECMessage):
    """Message with metadata about the shared memory created in the shared memory manager."""

    id: str
    lock_id: str
    max_index: int
    owner: Literal["device", "client"]
    device: str | None = None
    shape: tuple[int, ...]
    dtype: SUPPORTED_DATATYPES


class AvailableDataAnalysisMethods(BECMessage):
    """Message published by the DAP server on which analysis methods are available."""

    methods: list[str]


# TODO maybe not needed to warm up, could automatically start a DAP worker once a shared memory object is created,
# Then DataAnalysisRegisterRequest is designed to register analysis methods for the shared memory object, and
# DataAnalysisTrigger is designed to trigger the analysis of the shared memory object.
# DataAnalysisResponse is designed to send the results back to the client.
class DataAnalysisRequestWarmup(BECMessage):
    """Message to request a data analysis"""

    shared_mem: SharedMemDescriptor


class DataAnalysisRegisterRequest(BECMessage):
    """Message to request processing of a shared memory object."""

    shared_mem: SharedMemDescriptor
    methods: list[str]
    client_id: str
    device: str | None = None


class DataAnalysisTrigger(BECMessage):
    """Message to request processing of a shared memory object."""

    shared_mem: SharedMemDescriptor
    index: int


class DataAnalysisResponse(BECMessage):
    """Message to request processing of a shared memory object."""

    shared_mem: SharedMemDescriptor
    index: int
    results: dict
    client_id: str
    device: str | None = None


class SharedMemoryManager:

    def shutdown(self):
        """Shutdown method, should clean up all shared memory objects."""

    def create_shared_mem(self, msg: SharedMemRequestAllocation) -> str:
        """Creates a shared memory object under a unique name."""

    def _publish_shared_mem_info(self, msg: SharedMemDescriptor):
        """Publish information about a shared memory object."""
