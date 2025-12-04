from pydantic import BaseModel, ValidationError

from bec_lib.utils.pydantic_pretty_print import pretty_print_pydantic_validation_error


def test_pretty_print_pydantic_validation_error(capsys):
    from pydantic import Field

    class TestModel(BaseModel):
        name: str = Field(..., min_length=3)
        age: int = Field(..., ge=0)
        email: str

    try:
        TestModel(name="Jo", age=-5)  # type: ignore
    except ValidationError as ve:
        pretty_print_pydantic_validation_error(ve, context="test model", model=TestModel)
    captured = capsys.readouterr()
    assert "Found 3 validation errors in test model" in captured.out
    assert "string_too_short" in captured.out
    assert "String should have at least 3" in captured.out
    assert "missing" in captured.out
    assert "expected type: str" in captured.out
