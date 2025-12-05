"""
This module provides the BeamlineChecks class, which is used to perform beamline checks. It also provides the bl_check
decorator.
"""

import builtins
import datetime
import functools
import threading
import time
from collections import deque
from uuid import uuid4

from typeguard import typechecked

from bec_lib.bl_conditions import BeamlineCondition
from bec_lib.logger import bec_logger

logger = bec_logger.logger


class BeamlineCheckError(Exception):
    pass


class BeamlineCheckRepeat(Exception):
    pass


def bl_check(fcn):
    """Decorator to perform rpc calls."""

    @functools.wraps(fcn)
    def bl_check_wrapper(*args, **kwargs):
        client = builtins.__dict__.get("bec")
        bl_checks = client.bl_checks
        _run_with_bl_checks(bl_checks, fcn, *args, **kwargs)

    return bl_check_wrapper


def _run_with_bl_checks(bl_checks, fcn, *args, **kwargs):
    # pylint: disable=protected-access
    chk = {"id": str(uuid4()), "fcn": fcn, "args": args, "kwargs": kwargs}
    bl_checks._levels.append(chk)
    nested_call = len(bl_checks._levels) > 1
    if bl_checks._is_paused and bl_checks._beamline_checks:
        logger.warning(
            "Beamline checks are currently paused. Use `bec.bl_checks.resume()` to reactivate them."
        )
    try:
        if nested_call:
            # check if the beam was okay so far
            if not bl_checks.beam_is_okay:
                raise BeamlineCheckError("Beam is not okay.")
        else:
            bl_checks.reset()
            bl_checks.wait_for_beamline_checks()
        successful = False
        while not successful:
            try:
                successful, res = _run_on_failure(bl_checks, fcn, *args, **kwargs)

                if not bl_checks.beam_is_okay:
                    successful = False
                    bl_checks.wait_for_beamline_checks()
            except BeamlineCheckRepeat:
                successful = False
        return res

    finally:
        bl_checks._levels.pop()


def _run_on_failure(bl_checks, fcn, *args, **kwargs) -> tuple:
    try:
        res = fcn(*args, **kwargs)
        return (True, res)
    except BeamlineCheckError:
        bl_checks.wait_for_beamline_checks()
        return (False, None)


class BeamlineChecks:
    def __init__(self, client, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        self.send_to_scilog = True
        self._beam_is_okay = True
        self._beamline_checks = {}
        self._stop_beam_check_event = threading.Event()
        self._beam_check_thread = None
        self._started = False
        self._is_paused = False
        self._check_msgs = []
        self._levels = deque()

    @typechecked
    def register(self, check: BeamlineCondition):
        """
        Register a beamline check.

        Args:
            check (BeamlineCondition): The beamline check to register.
        """
        self._beamline_checks[check.name] = check
        setattr(self, check.name, check)

    def pause(self) -> None:
        """
        Pause beamline checks. This will disable all checks. Use `resume` to
        reactivate the checks.
        """
        self._is_paused = True

    def resume(self) -> None:
        """
        Resume all paused beamline checks.
        """
        self._is_paused = False

    def available_checks(self) -> None:
        """
        Print all available beamline checks
        """
        for name, check in self._beamline_checks.items():
            enabled = f"ENABLED: {check.enabled}"
            print(f"{name:<20} {enabled}")

    def disable_check(self, name: str) -> None:
        """
        Disable a beamline check.

        Args:
            name (str): The name of the beamline check to disable.
        """
        if name not in self._beamline_checks:
            raise ValueError(f"Beamline check {name} not registered.")
        self._beamline_checks[name].enabled = False

    def enable_check(self, name: str) -> None:
        """
        Enable a beamline check.

        Args:
            name (str): The name of the beamline check to enable.
        """
        if name not in self._beamline_checks:
            raise ValueError(f"Beamline check {name} not registered.")
        self._beamline_checks[name].enabled = True

    def disable_all_checks(self) -> None:
        """
        Disable all beamline checks.
        """
        for name in self._beamline_checks:
            self.disable_check(name)

    def enable_all_checks(self) -> None:
        """
        Enable all beamline checks.
        """
        for name in self._beamline_checks:
            self.enable_check(name)

    def _run_beamline_checks(self):
        msgs = []
        for name, check in self._beamline_checks.items():
            if not check.enabled:
                continue
            if check.run():
                continue
            msgs.append(check.on_failure_msg())
            self._beam_is_okay = False
        return msgs

    def _check_beam(self):
        while not self._stop_beam_check_event.wait(timeout=1):
            self._check_msgs = self._run_beamline_checks()

    def start(self):
        """Start the beamline checks."""
        if self._started:
            return
        self._beam_is_okay = True

        self._beam_check_thread = threading.Thread(target=self._check_beam, daemon=True)
        self._beam_check_thread.start()
        self._started = True

    def stop(self):
        """Stop the beamline checks"""
        if not self._started:
            return

        self._stop_beam_check_event.set()
        self._beam_check_thread.join()

    def reset(self):
        self._beam_is_okay = True
        self._check_msgs = []

    @property
    def beam_is_okay(self):
        return self._beam_is_okay

    def _print_beamline_checks(self):
        for msg in self._check_msgs:
            logger.warning(msg)

    def wait_for_beamline_checks(self):
        self._print_beamline_checks()
        if self.send_to_scilog and not self.beam_is_okay:
            self._send_to_scilog(
                f"Beamline checks failed at {str(datetime.datetime.now())}: {''.join(self._check_msgs)}",
                pen="red",
            )

        self._run_beamline_checks_until_okay()

        if self.send_to_scilog:
            self._send_to_scilog(
                f"Operation resumed at {str(datetime.datetime.now())}.", pen="green"
            )

    def _run_beamline_checks_until_okay(self):
        while True:
            if self._beam_status_is_okay():
                break
            self._print_beamline_checks()
            time.sleep(5)

    def _beam_status_is_okay(self) -> bool:
        self._beam_is_okay = True
        self._check_msgs = self._run_beamline_checks()
        return self._beam_is_okay

    def _send_to_scilog(self, msg, pen="red"):
        try:
            msg = self.client.logbook.LogbookMessage()
            msg.add_text(f"<p><mark class='pen-{pen}'><strong>{msg}</strong></mark></p>").add_tag(
                ["BEC", "beam_check"]
            )
            self.client.logbook.send_logbook_message(msg)
        except Exception:
            logger.warning("Failed to send update to SciLog.")
