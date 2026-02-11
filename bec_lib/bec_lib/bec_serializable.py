import numpy as np
from pydantic import BaseModel, ConfigDict, computed_field


class BecCodecInfo(BaseModel):
    type_name: str


class BECSerializable(BaseModel):
    """A base class for serializable BEC objects, especially BEC messages.
    Fields in subclasses which use non-primitive types must be in structured,
    type-hinted objects, and their encoders and JSON schema should be defined in
    this class."""

    model_config = ConfigDict(
        json_schema_serialization_defaults_required=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    @computed_field()
    @property
    def bec_codec(self) -> BecCodecInfo:
        return BecCodecInfo(type_name=self.__class__.__name__)

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        try:
            np.testing.assert_equal(self.model_dump(), other.model_dump())
            return True
        except AssertionError:
            return False
