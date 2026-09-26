"""
This module contains the numpy encoder and decoder functions for serializing and deserializing numpy objects.
Modified from https://github.com/lebedov/msgpack-numpy

Object-containing dtypes are unsupported in both MessagePack and JSON. Legacy
pickle-backed arrays are rejected without unpickling their contents.
"""

import sys

import numpy as np

_OBJECT_DTYPE_ERROR = (
    "NumPy object arrays are not supported. "
    "Use a non-object dtype or lists/dicts of supported values."
)


def _reject_object_dtype(dtype: np.dtype) -> None:
    """Reject object references, including those nested in structured dtypes."""
    if dtype.hasobject:
        raise TypeError(_OBJECT_DTYPE_ERROR)


def ndarray_to_bytes(obj):
    _reject_object_dtype(obj.dtype)
    if sys.platform == "darwin":
        return obj.tobytes()
    return obj.data if obj.flags["C_CONTIGUOUS"] else obj.tobytes()


def tostr(x):
    if isinstance(x, bytes):
        return x.decode()
    return str(x)


def numpy_encode(obj, chain=None):
    """
    Data encoder for serializing numpy data types.
    """

    if isinstance(obj, np.ndarray):
        # If the dtype is structured, store the interface description;
        # otherwise, store the corresponding array protocol type string:
        if obj.dtype.kind == "V":
            kind = bytes(obj.dtype.kind, "ascii")
            descr = obj.dtype.descr
        else:
            kind = b""
            descr = obj.dtype.str

        return {
            b"nd": True,
            b"type": descr,
            b"kind": kind,
            b"shape": obj.shape,
            b"data": ndarray_to_bytes(obj),
        }
    elif isinstance(obj, (np.bool_, np.number)):
        return {b"nd": False, b"type": obj.dtype.str, b"data": obj.data}
    elif isinstance(obj, complex):
        return {b"complex": True, b"data": repr(obj)}
    else:
        return obj if chain is None else chain(obj)


def numpy_decode(obj, chain=None):
    """
    Decoder for deserializing numpy data types.
    """

    try:
        if b"nd" in obj:
            if obj.get(b"kind") == b"O":
                raise TypeError(_OBJECT_DTYPE_ERROR)
            if obj[b"nd"] is True:
                # Check if b'kind' is in obj to enable decoding of data
                # serialized with older versions (#20):
                if b"kind" in obj and obj[b"kind"] == b"V":
                    descr = [
                        tuple(tostr(t) if type(t) is bytes else t for t in d) for d in obj[b"type"]
                    ]
                else:
                    descr = obj[b"type"]
                dtype = _unpack_dtype(descr)
                return np.ndarray(buffer=obj[b"data"], dtype=dtype, shape=obj[b"shape"])
            else:
                dtype = _unpack_dtype(obj[b"type"])
                return np.frombuffer(obj[b"data"], dtype=dtype)[0]
        elif b"complex" in obj:
            return complex(tostr(obj[b"data"]))
        else:
            return obj if chain is None else chain(obj)
    except KeyError:
        return obj if chain is None else chain(obj)


def numpy_encode_list(obj, chain=None):
    """
    Data encoder for serializing numpy data types.
    """

    if isinstance(obj, np.ndarray):
        _reject_object_dtype(obj.dtype)
        # If the dtype is structured, store the interface description;
        # otherwise, store the corresponding array protocol type string:
        if obj.dtype.kind == "V":
            kind = bytes(obj.dtype.kind, "ascii")
            descr = obj.dtype.descr
        else:
            kind = ""
            descr = obj.dtype.str

        return {"nd": True, "type": descr, "kind": kind, "shape": obj.shape, "data": obj.tolist()}
    if isinstance(obj, (np.bool_, np.number)):
        return {"nd": False, "type": obj.__class__.__name__, "data": obj.data.tolist()}
    if isinstance(obj, complex):
        return {"complex": True, "data": repr(obj)}

    return obj if chain is None else chain(obj)


def numpy_decode_list(obj, chain=None):
    """
    Decoder for deserializing numpy data types.
    """

    try:
        if "nd" in obj:
            if obj.get("kind") == "O":
                raise TypeError(_OBJECT_DTYPE_ERROR)
            if obj["nd"] is True:
                # Check if 'kind' is in obj to enable decoding of data
                # serialized with older versions:
                if "kind" in obj and obj["kind"] == "V":
                    descr = [
                        tuple(tostr(t) if type(t) is bytes else t for t in d) for d in obj["type"]
                    ]
                else:
                    descr = obj["type"]
                dtype = _unpack_dtype(descr)
                return np.array(obj["data"], dtype=dtype).reshape(obj["shape"])
            descr = obj["type"]
            numpy_dtype = getattr(np, descr)
            _reject_object_dtype(np.dtype(numpy_dtype))
            return numpy_dtype(obj["data"])
        if "complex" in obj:
            return complex(tostr(obj["data"]))
        return obj if chain is None else chain(obj)
    except KeyError:
        return obj if chain is None else chain(obj)


def _unpack_dtype(dtype):
    """
    Unpack dtype descr, recursively unpacking nested structured dtypes.
    """

    if isinstance(dtype, (list, tuple)):
        # Unpack structured dtypes of the form: (name, type, *shape)
        dtype = [
            (subdtype[0], _unpack_dtype(subdtype[1])) + tuple(subdtype[2:]) for subdtype in dtype
        ]
    dtype = np.dtype(dtype)
    _reject_object_dtype(dtype)
    return dtype
