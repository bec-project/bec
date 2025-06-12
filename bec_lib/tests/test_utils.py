import json

from pydantic import BaseModel

from bec_lib.utils.json import ExtendedEncoder


def test_encoder_encodes_set():
    data = {"item": {"a", "b", "c"}}
    encoded = json.dumps(data, cls=ExtendedEncoder)
    decoded = json.loads(encoded)
    decoded["item"] = set(decoded["item"])
    assert decoded == data


def test_encoder_encodes_other():
    data = {"item": {"a": 1, "b": 2, "c": 3}}
    encoded = json.dumps(data, cls=ExtendedEncoder)
    decoded = json.loads(encoded)
    assert decoded == data
