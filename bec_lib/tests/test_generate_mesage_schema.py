from typing import Annotated

import numpy as np
from pydantic import ConfigDict, WithJsonSchema

from bec_lib.messages import BECMessage


class NumpyMessage(BECMessage):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    important_value: Annotated[np.ndarray, WithJsonSchema({"type": "string"})]


def test_replace_numpy():
    schema = NumpyMessage.model_json_schema()
