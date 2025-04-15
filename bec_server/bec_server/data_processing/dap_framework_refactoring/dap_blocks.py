from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Concatenate, Generic, Literal, Optional, ParamSpec, TypeVar,

import numpy as np
from pydantic import BaseModel, Field
from scipy.ndimage import gaussian_filter1d

from bec_lib.logger import bec_logger
from bec_lib.messages import BECMessage

logger = bec_logger.logger


Dims = TypeVar("Dims", bound=Literal[1, 2, 3])
UpdateType = TypeVar("UpdateType", bound=Literal["replace", "append", "add"])
XAxisPresent = TypeVar("XAxisPresent", bound=Literal[True, False] | None)


class Test(Generic[XAxisPresent]):
    x_axis: XAxisPresent = False


# Consider using dataclasses or Pydantic for better structure and validation
# This is basically wrong to do https://github.com/microsoft/pyright/discussions/8937
# https://github.com/microsoft/pyright/issues/9775
# https://github.com/microsoft/pyright/issues/9158
class DAPSchema(Generic[Dims, UpdateType, XAxisPresent], BaseModel):
    ndim: Dims
    max_shape: tuple[int, ...] | None = None
    async_update: UpdateType = "replace"
    x_axis: XAxisPresent = False
    dap_report: Optional[dict[str, Any]] = Field(default_factory=dict)  # TBD


SchemaType = TypeVar("SchemaType", bound=DAPSchema)


class DAPBlockMessage(BECMessage, Generic[SchemaType]):
    data: np.ndarray
    message_schema: SchemaType
    data_x: Optional[np.ndarray | None] = Field(default=None)

    model_config = {"validate_assignment": True, "arbitrary_types_allowed": True}


SchemaTypeOut = TypeVar("SchemaTypeOut", bound=DAPSchema)
P = ParamSpec("P")

# this should be doubly linked so that we can check against and add to the tail but
# run the workflow from the start
class Workflow(Generic[SchemaType, SchemaTypeOut]):
    def __init__(
        self,
        block: Callable[
            Concatenate[DAPBlockMessage[SchemaType], P], DAPBlockMessage[SchemaTypeOut]
        ],
        **kwargs,
    ):
        self._block = block
        self._kwargs = kwargs
        self._previous_workflow = None
        self.head = self

    def set_history(self, wf: Workflow[SchemaTypeOut, DAPSchema ]):
        self._previous_workflow = wf
        self._head = wf.head

    def add_block(
        self,
        block: Callable[
            Concatenate[DAPBlockMessage[SchemaTypeOut], P], DAPBlockMessage[DAPSchema]
        ],
        **kwargs,
    ):
        next = Workflow(block, **kwargs)
        next.set_history(self)
        


# What is not yet clear here is how to we handle the x,y,z


class DAPBlock(ABC):
    """
    Base class for DAP blocks.
    """

    # TODO discuss if this should be a class variable
    _input_schema: DAPSchema
    _output_schema: DAPSchema

    def __init__(self, **kwargs) -> None:
        """
        Initialize the DAP block.

        Args:
            name (str): The name of the DAP block.
        """
        self.dap_report = {}
        self.kwargs = kwargs

    @classmethod
    def validate_input(cls, output_schema: DAPSchema) -> bool:
        """
        Validate the input schema.

        Args:
            output_schema (DAPSchema): The output schema from the last block to validate against.

        Returns:
            bool: True if the input schema is valid, False otherwise.
        """
        if any(
            output_schema.ndim != cls._input_schema.ndim,
            output_schema.max_shape != cls._input_schema.max_shape,
            output_schema.async_update != cls._input_schema.async_update,
        ):
            logger.error(
                f"Invalid input schema for {cls.__name__}: {output_schema} does not match {cls._input_schema}"
            )
            return False
        return True

    @abstractmethod
    def __call__(self, msg: DAPBlockMessage, **kwargs) -> DAPBlockMessage:
        """Run method implemented by subclasses.

        Args:
            msg (DAPBlockMessage): The input message.
            **kwargs: Additional arguments.
        Returns:
            DAPBlockMessage: The output message.
        """


class SmoothBlock(DAPBlock):

    _input_schema: DAPSchema = DAPSchema(
        ndim=1, max_shape=(1,), async_update="replace", x_axis=True
    )
    _output_schema: DAPSchema = DAPSchema(
        ndim=1, max_shape=(1,), async_update="replace", x_axis=True
    )

    # pylint: disable=arguments-differ
    def __call__(self, msg: DAPBlockMessage, sigma: float = 2.0) -> DAPBlockMessage:
        """
        Run the smoothing block.

        Args:
            msg (DAPBlockMessage): The input message.
            sigma (float): The standard deviation for Gaussian kernel.

        Returns:
            DAPBlockMessage: The smoothed message.
        """
        # Fetch data
        data = msg.data
        rtr_msg = DAPBlockMessage(
            data=gaussian_filter1d(data, sigma), data_x=msg.data_x, schema=self._output_schema
        )
        return rtr_msg


class GradientBlock(DAPBlock):

    _input_schema: DAPSchema = DAPSchema(
        ndim=1, max_shape=(1,), async_update="replace", x_axis=True
    )
    _output_schema: DAPSchema = DAPSchema(
        ndim=1, max_shape=(1,), async_update="replace", x_axis=True
    )

    # pylint: disable=arguments-differ
    def __call__(self, msg: DAPBlockMessage) -> DAPBlockMessage:
        """
        Run the gradient block.

        Args:
            msg (DAPBlockMessage): The input message.

        Returns:
            DAPBlockMessage: The gradient message.
        """
        # Fetch data
        y_data = msg.data
        if msg.data_x is None:
            gradient = np.gradient(y_data)
        else:
            gradient = np.gradient(y_data, msg.data_x)
        return DAPBlockMessage(data=gradient, data_x=msg.data_x, schema=self._output_schema)
