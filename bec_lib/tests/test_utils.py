import json

from pydantic import BaseModel

from bec_lib.utils.json import ExtendedEncoder


def test_encoder_encodes_set():
    data = {"item": {"a", "b", "c"}}
    encoded = json.dumps(data, cls=ExtendedEncoder)
    decoded = json.loads(encoded)
    decoded["item"] = set(decoded["item"])
    assert decoded == data
