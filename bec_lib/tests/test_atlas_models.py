import pytest
from annotated_types import Lt, MaxLen
from pydantic import BaseModel, Field, ValidationError

from bec_lib.atlas_models import Device, DevicePartial, make_all_fields_optional


def test_make_all_fields_optional():
    class TestModel(BaseModel):
        number: int = Field(23, lt=30)
        text: str = Field("hello", max_length=10)
        a: float
        b: float
        c: float

    OptionalTestModel = make_all_fields_optional(TestModel, "OptionalTestModel")

    assert OptionalTestModel.model_fields["number"].default == 23
    assert OptionalTestModel.model_fields["text"].default == "hello"
    assert OptionalTestModel.model_fields["a"].default is None

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
    }


def test_device_partial():
    out = DevicePartial(name="test")
    assert out.model_dump(exclude_defaults=True) == {"name": "test"}

    with pytest.raises(ValidationError):
        DevicePartial(name="test", readoutPriority="invalid")


def test_device_name_accepts_valid_identifier():
    device = Device(
        name="samx",
        enabled=True,
        deviceClass="ophyd_devices.SimPositioner",
        readoutPriority="monitored",
    )
    assert device.name == "samx"


@pytest.mark.parametrize("name", ["1samx", "sam-x", "for", "with space"])
def test_device_name_rejects_invalid_python_identifier(name):
    with pytest.raises(ValidationError, match="valid Python identifier"):
        Device(
            name=name,
            enabled=True,
            deviceClass="ophyd_devices.SimPositioner",
            readoutPriority="monitored",
        )


def test_device_name_rejects_private_prefix():
    with pytest.raises(ValidationError, match="must not start with '_'"):
        Device(
            name="_samx",
            enabled=True,
            deviceClass="ophyd_devices.SimPositioner",
            readoutPriority="monitored",
        )


@pytest.mark.parametrize("name", ["wm", "items", "get"])
def test_device_name_rejects_bec_namespace_conflicts(name):
    with pytest.raises(
        ValidationError, match="conflicts with an existing device namespace attribute or method"
    ):
        Device(
            name=name,
            enabled=True,
            deviceClass="ophyd_devices.SimPositioner",
            readoutPriority="monitored",
        )
