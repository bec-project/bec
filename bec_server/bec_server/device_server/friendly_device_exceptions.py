"""This module helps to reformat known exceptions raised from ophyd into less cryptic ones."""

import re

from bec_lib.messages import DeviceInstructionAction


class DeviceInstructionError(Exception):
    """An error encountered during execution of a device instruction."""


def matches(p: re.Pattern[str], e: Exception) -> bool:
    if not isinstance(msg := e.args[0], str):
        return False
    return bool(re.match(p, msg))


def create(new_msg: str, cause: Exception) -> DeviceInstructionError:
    new = DeviceInstructionError(new_msg)
    new.__cause__ = cause
    return new


def reformat_known_device_exceptions(
    e: Exception, action: DeviceInstructionAction, extra: str = ""
) -> Exception:
    """If the encountered exception is known, returns a clearer error with `extra` appended to the message.
    Otherwise, returns the original exception unchanged."""

    if action == "set":
        return _handle_set(e, extra)
    return e


INCORRECT_VALUE_FOR_TUPLE = re.compile(r"tuple indices must be integers or slices, not float")


def _handle_set(e: Exception, extra: str) -> Exception:
    if isinstance(e, TypeError) and matches(INCORRECT_VALUE_FOR_TUPLE, e):
        return create(
            "An incorrect value was provided to a .set() command. This could be, for example, providing a float rather than an int to an enum PV. "
            + extra,
            e,
        )
    return e
