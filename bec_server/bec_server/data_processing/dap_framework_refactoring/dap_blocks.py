from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Literal, Optional

import numpy as np
from pydantic import BaseModel, Field
from scipy.ndimage import gaussian_filter1d

from bec_lib.logger import bec_logger
from bec_lib.messages import BECMessage

logger = bec_logger.logger


# Consider using dataclasses or Pydantic for better structure and validation
class DAPSchema(BaseModel):
    ndim: Literal[1, 2, 3]
    max_shape: tuple[int, ...]
    async_update: Literal["replace", "append", "add"] = Field(default="replace")  # TBD
    x_axis: Optional[bool] = Field(default=False)  # TBD
    dap_report: Optional[dict[str, Any]] = Field(default_factory=dict)  # TBD


# What is not yet clear here is how to we handle the x,y,z


class DAPBlockMessage(BECMessage):
    data: np.ndarray
    schema: DAPSchema
    data_x: Optional[np.ndarray | None] = Field(default=None)

    model_config = {"validate_assignment": True, "arbitrary_types_allowed": True}


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
    def run(self, msg: DAPBlockMessage, **kwargs) -> DAPBlockMessage:
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
    def run(self, msg: DAPBlockMessage, sigma: float = 2.0) -> DAPBlockMessage:
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
    def run(self, msg: DAPBlockMessage) -> DAPBlockMessage:
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
