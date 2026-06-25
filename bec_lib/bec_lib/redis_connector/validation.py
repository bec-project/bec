from __future__ import annotations

import inspect
import traceback
import warnings
from functools import wraps
from typing import Any, Callable, Concatenate, Iterable

import redis.exceptions

from bec_lib.endpoints import EndpointInfo
from bec_lib.logger import bec_logger
from bec_lib.messages import BECMessage, BundleMessage

from .constants import IncompatibleMessageForEndpoint, IncompatibleRedisOperation, P, WrongArguments

logger = bec_logger.logger


def error_log_with_context(msg: str):
    context = "".join(traceback.format_stack(limit=5)[:-1])
    logger.error(msg + f" Context:\n{context}")


def _raise_incompatible_message(msg, endpoint):
    raise IncompatibleMessageForEndpoint(
        f"Message type {type(msg)} is not compatible with endpoint {endpoint}. Expected {endpoint.message_type}"
    )


def check_endpoint_type(endpoint: EndpointInfo | str) -> bool:
    if isinstance(endpoint, str):
        warnings.warn(
            "RedisConnector methods with a string topic are deprecated and should not be used anymore. Use RedisConnector methods with an EndpointInfo instead.",
            DeprecationWarning,
        )
        return False
    if not isinstance(endpoint, EndpointInfo):
        raise TypeError(f"Endpoint {endpoint} is not EndpointInfo")
    return True


def _validate_sequence(seq: Iterable, endpoint: EndpointInfo):
    for sub_val in seq:
        if isinstance(sub_val, BECMessage) and endpoint.message_type == Any:
            continue
        if isinstance(sub_val, BECMessage) and not isinstance(sub_val, endpoint.message_type):
            _raise_incompatible_message(sub_val, endpoint)


def _validate_all_bec_messages(values: Iterable, endpoint: EndpointInfo):
    for val in values:
        if isinstance(val, BECMessage) and endpoint.message_type == Any:
            continue
        if isinstance(val, BundleMessage):
            for msg in val.messages:
                if not isinstance(msg, endpoint.message_type):
                    _raise_incompatible_message(msg, endpoint)
        elif isinstance(val, BECMessage) and not isinstance(val, endpoint.message_type):
            _raise_incompatible_message(val, endpoint)
        if isinstance(val, dict):
            _validate_sequence(val.values(), endpoint)
        if isinstance(val, (list, tuple)):
            _validate_sequence(val, endpoint)


def _fix_docstring_for_ipython(func: Callable, arg_name: str):
    if func.__doc__ is not None:
        arg_annotation = f"    {arg_name} (str):"
        if arg_annotation in func.__doc__:
            func.__doc__ = func.__doc__.replace(arg_annotation, f"    {arg_name} (EndpointInfo):")
    func.__annotations__[arg_name] = "EndpointInfo"


def validate_endpoint(endpoint_arg_name: str):
    """Decorate an instance method to validate the first argument (named endpoint_arg_name) as
    an EndpointInfo and pass it as a str to the wrapped method. Further checks if any given BECMessage
    to the function is appropriate for the endpoint."""

    def decorator(
        func: Callable[Concatenate[Any, str, P], Any],
    ) -> Callable[Concatenate[Any, EndpointInfo, P], Any]:
        signature = inspect.signature(func)
        try:
            parameter_names = list(signature.parameters)
            argument_index = parameter_names.index(endpoint_arg_name)
            if argument_index != 1:
                raise ValueError
        except ValueError as e:
            raise WrongArguments(
                f"@validate_endpoint should be applied to an instance function which takes the named argument ('{endpoint_arg_name}') as its first non-self argument."
            ) from e

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                endpoint = args[argument_index]
                arg = list(args)
            except IndexError:
                endpoint = kwargs[endpoint_arg_name]
                arg = kwargs
            try:
                if not check_endpoint_type(endpoint):
                    return func(*args, **kwargs)
                if func.__name__ not in endpoint.message_op:
                    raise IncompatibleRedisOperation(
                        f"Endpoint {endpoint} is not compatible with {func.__name__} method"
                    )
                _validate_all_bec_messages(list(args) + list(kwargs.values()), endpoint)

                if isinstance(arg, list):
                    arg[argument_index] = endpoint.endpoint
                    return func(*tuple(arg), **kwargs)
                arg[endpoint_arg_name] = endpoint.endpoint
                return func(*args, **arg)
            except redis.exceptions.NoPermissionError as exc:
                # the default NoPermissionError message is not very informative as it does not
                # contain any information about the endpoint that caused the error
                endpoint_str = (
                    endpoint.endpoint if isinstance(endpoint, EndpointInfo) else str(endpoint)
                )
                raise redis.exceptions.NoPermissionError(
                    f"Permission denied for endpoint {endpoint_str}"
                ) from exc

        _fix_docstring_for_ipython(wrapper, endpoint_arg_name)
        return wrapper

    return decorator
