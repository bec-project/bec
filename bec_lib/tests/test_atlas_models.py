import pytest
from annotated_types import Lt, MaxLen
from pydantic import BaseModel, Field, ValidationError
from pydantic_core import PydanticUndefined

from bec_lib.atlas_models import DevicePartial, make_all_fields_optional


def test_make_all_fields_optional():
    class TestModel(BaseModel):
        number: int = Field(23, lt=30)
        text: str = Field("hello", max_length=10)
        dictionary: dict = Field(default_factory=dict)
        a: float
        b: float
        c: float

    OptionalTestModel = make_all_fields_optional(TestModel, "OptionalTestModel")

    assert OptionalTestModel.model_fields["number"].default == 23
    assert OptionalTestModel.model_fields["text"].default == "hello"
    assert OptionalTestModel.model_fields["a"].default is None
    assert OptionalTestModel.model_fields["dictionary"].default is PydanticUndefined

    with pytest.raises(ValidationError):
        OptionalTestModel(number=31)

    assert Lt(lt=30) in OptionalTestModel.model_fields["number"].metadata
    assert MaxLen(max_length=10) in OptionalTestModel.model_fields["text"].metadata

    assert OptionalTestModel().model_dump() == {
        "number": 23,
        "text": "hello",
        "a": None,
        "b": None,
        "c": None,
        "dictionary": {},
    }


def test_device_partial():
    out = DevicePartial(name="test")
    assert out.model_dump(exclude_defaults=True) == {"name": "test"}

    with pytest.raises(ValidationError):
        DevicePartial(name="test", readoutPriority="invalid")
