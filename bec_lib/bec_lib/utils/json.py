import json
from typing import Any


class ExtendedEncoder(json.JSONEncoder):
    # This is only really intended for pretty- printing to console - real serialization tasks
    # should use pydantic model_dump() or bec_lib msgpack serialization.
    def default(self, o: Any) -> Any:
        if isinstance(o, set):
            return list(o)
        return super().default(o)
