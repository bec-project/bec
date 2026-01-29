from math import inf

import lmfit

from bec_lib.lmfit_serializer import deserialize_param_object, serialize_lmfit_params


def test_serialize_lmfit_params():
    params = lmfit.Parameters()
    params.add("a", value=1.0, vary=True)
    params.add("b", value=2.0, vary=False)
    result = serialize_lmfit_params(params)
    assert result == {
        "a": {
            "name": "a",
            "value": 1.0,
            "vary": True,
            "min": -inf,
            "max": inf,
            "expr": None,
            "brute_step": None,
        },
        "b": {
            "name": "b",
            "value": 2.0,
            "vary": False,
            "min": -inf,
            "max": inf,
            "expr": None,
            "brute_step": None,
        },
    }

    obj = deserialize_param_object(result)
    assert obj == params

    # `name` is optional for deserialization (key is the param name)
    result_without_names = {
        k: {kk: vv for kk, vv in v.items() if kk != "name"} for k, v in result.items()
    }
    obj = deserialize_param_object(result_without_names)
    assert obj == params


def test_deserialize_param_object_accepts_parameter_objects():
    params = lmfit.Parameters()
    params.add("a", value=1.0, vary=True, min=-2.0, max=3.0)
    params.add("b", value=2.0, vary=False)

    obj = deserialize_param_object({"a": params["a"], "b": params["b"]})
    assert obj == params
