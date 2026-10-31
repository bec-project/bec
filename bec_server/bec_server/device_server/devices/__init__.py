import logging

import msgpack

import bec_lib
from bec_lib.numpy_encoder import numpy_encode


def is_serializable(value):
    """Return whether BEC can encode an arbitrary RPC payload."""
    try:
        msgpack.dumps(value, default=numpy_encode)
    except (TypeError, OverflowError):
        return False
    return True


loggers = logging.getLogger(__name__)
