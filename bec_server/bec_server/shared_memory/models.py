from __future__ import annotations

import sys
from typing import Literal, Tuple

import numpy as np
from pydantic import BaseModel, ConfigDict


class SharedMemInfo(BaseModel):
    """
    Store information about the shared memory object. This message has the client_id, the buffer descriptor and
    the potentially a list of devices for which this shared memory object is relevant.
    """

    model_config = ConfigDict(validate_assignment=True)
    client_id: str
    buffer_desc: RingBufferDescriptor
    signal: str | None = None  # dotted signal name, e.g. "eiger.preview"


class DTypeDescriptor(BaseModel):
    kind: Literal["uint", "int", "float", "bool"]
    itemsize: int
    byte_order: Literal["little", "big"] = "little"

    @classmethod
    def from_numpy(cls, dtype: np.dtype) -> DTypeDescriptor:
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
    def from_numpy(cls, array: np.ndarray) -> PayloadDescriptor:
        """Class method to create PayloadDescriptor from a numpy array."""
        return cls(
            nbytes=array.nbytes,
            shape=array.shape,
            dtype=DTypeDescriptor.from_numpy(array.dtype),
            layout="C" if array.flags.c_contiguous else "C",
        )


class RingBufferDescriptor(BaseModel):
    """Descriptor for SharedRingBuffer object."""

    name: str
    lock_id: str
    slots: int
    bytes_per_slot: int
    payload: PayloadDescriptor


# class AvailableDataAnalysisMethods(messages.BECMessage):
#     """Message published by the DAP server on which analysis methods are available."""

#     methods: list[str]


# TODO maybe not needed to warm up, could automatically start a DAP worker once a shared memory object is created,
# Then DataAnalysisRegisterRequest is designed to register analysis methods for the shared memory object, and
# DataAnalysisTrigger is designed to trigger the analysis of the shared memory object.
# DataAnalysisResponse is designed to send the results back to the client.
# class DataAnalysisRequestWarmup(BECMessage):
#     """Message to request a data analysis"""

#     shared_mem: SharedMemDescriptor


# class DataAnalysisRegisterRequest(BECMessage):
#     """Message to request processing of a shared memory object."""

#     shared_mem: SharedMemDescriptor
#     methods: list[str]
#     client_id: str
#     device: str | None = None


# class DataAnalysisTrigger(BECMessage):
#     """Message to request processing of a shared memory object."""

#     shared_mem: SharedMemDescriptor
#     index: int


# class DataAnalysisResponse(BECMessage):
#     """Message to request processing of a shared memory object."""

#     shared_mem: SharedMemDescriptor
#     index: int
#     results: dict
#     client_id: str
#     device: str | None = None
