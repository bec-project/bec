"""
This module contains functions for serializing and deserializing lmfit objects.
"""

from typing import TYPE_CHECKING

from bec_lib.utils.import_utils import lazy_import_from

if TYPE_CHECKING:  # pragma: no cover
    from lmfit import Parameter, Parameters
else:
    Parameter, Parameters = lazy_import_from("lmfit", ("Parameter", "Parameters"))


def serialize_param_object(param: Parameter) -> dict:
    """
    Serialize lmfit.Parameter object to JSON-serializable dictionary.

    Args:
        param (Parameter): Parameter object

    Returns:
        dict: Dictionary representation of the parameter
    """
    obj = {
        "name": param.name,
        "value": param.value,
        "vary": param.vary,
        "min": param.min,
        "max": param.max,
        "expr": param.expr,
        "brute_step": param.brute_step,
    }
    return obj


def serialize_lmfit_params(params: Parameters) -> dict:
    """
    Serialize lmfit.Parameters object to JSON-serializable dictionary.

    Args:
        params (Parameters): Parameters object containing lmfit.Parameter objects

    Returns:
        dict: Dictionary representation of the parameters
    """
    if not params:
        return {}
    if isinstance(params, Parameters):
        return {k: serialize_param_object(v) for k, v in params.items()}
    if isinstance(params, list):
        return {v.name: serialize_param_object(v) for v in params}


def deserialize_param_object(obj: dict[str, dict | Parameter]) -> Parameters:
    """
    Deserialize dictionary representation of lmfit.Parameter object.

    Args:
        obj (dict[str, dict | Parameter]): Dictionary representation of the parameters

    Returns:
        Parameters: Parameters object
    """
    param = Parameters()
    for k, v in obj.items():
        if isinstance(v, Parameter):
            param.add(
                k,
                value=v.value,
                vary=v.vary,
                min=v.min,
                max=v.max,
                expr=v.expr,
                brute_step=v.brute_step,
            )
            continue
        if isinstance(v, dict):
            v.pop("name", None)
            v_copy = v.copy()
            v_copy.pop("name", None)
            param.add(k, **v_copy)
    return param
