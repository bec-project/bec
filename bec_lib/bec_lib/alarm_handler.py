"""
This module provides the alarm handler class and its related functionality.
"""

from __future__ import annotations

import enum
import threading
from collections import deque
from typing import TYPE_CHECKING

from rich.console import Console, Group
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.utils import threadlocked

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages
    from bec_lib.redis_connector import RedisConnector


logger = bec_logger.logger


class Alarms(int, enum.Enum):
    WARNING = 0
    MINOR = 1
    MAJOR = 2


class AlarmException(Exception):
    pass


class AlarmBase(Exception):
    def __init__(self, alarm: messages.AlarmMessage, severity: Alarms, handled=False) -> None:
        self.alarm = alarm
        self.severity = severity
        self.handled = handled
        self.alarm_type = alarm.info.exception_type
        super().__init__(self.alarm.content)

    def __str__(self) -> str:
        msg = self.alarm.info.compact_error_message or self.alarm.info.error_message
        return (
            f"An alarm has occured. Severity: {self.severity.name}.\n{self.alarm_type}.\n\t"
            f" {msg}"
        )

    def pretty_print(self) -> None:
        """
        Use Rich to pretty print the alarm message,
        following the same logic used in __str__().
        """
        console = Console()

        msg = self.alarm.info.compact_error_message or self.alarm.info.error_message

        text = Text()
        text.append(f"{self.alarm_type} | ", style="bold")
        text.append(f"Severity {self.severity.name}", style="bold yellow")
        if self.alarm.info.device:
            text.append(f" | Device {self.alarm.info.device}\n", style="bold")
        text.append("\n")

        renderables = []
        # Format message inside a syntax box if it looks like traceback
        if "Traceback (most recent call last):" in msg:
            renderables.append(Syntax(msg.strip(), "python", word_wrap=True))
        else:
            renderables.append(Text(msg.strip()))

        if self.alarm.info.device:
            renderables.append(
                Text(
                    f"\n\nThe error is likely unrelated to BEC. Please check the device '{self.alarm.info.device}'.",
                    style="bold",
                )
            )
        body = Group(*renderables)

        console.print(Panel(body, title=text, border_style="red", expand=True))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AlarmBase):
            return False
        return self.alarm.info.id == other.alarm.info.id


class AlarmHandler:
    def __init__(self, connector: RedisConnector) -> None:
        self.connector = connector
        self.alarms_stack = deque(maxlen=100)
        self._raised_alarms = deque(maxlen=100)
        self._lock = threading.RLock()

    def start(self):
        """start the alarm handler and its subscriptions"""
        self.connector.register(
            topics=MessageEndpoints.alarm(),
            name="AlarmHandler",
            cb=self._alarm_register_callback,
            parent=self,
        )

    @staticmethod
    def _alarm_register_callback(msg, *, parent, **_kwargs):
        parent.add_alarm(msg.value)

    @threadlocked
    def add_alarm(self, msg: messages.AlarmMessage):
        """Add a new alarm message to the stack.

        Args:
            msg (messages.AlarmMessage): Alarm message that should be added
        """
        severity = Alarms(msg.severity)
        alarm = AlarmBase(alarm=msg, severity=severity, handled=False)
        if alarm in self.alarms_stack:
            logger.debug(f"Alarm already in stack: {alarm}")
            return
        if severity > Alarms.MINOR:
            self.alarms_stack.appendleft(alarm)
            logger.debug(alarm)
        else:
            logger.warning(alarm)

    @threadlocked
    def get_unhandled_alarms(self, severity=Alarms.WARNING) -> list:
        """Get all unhandled alarms equal or above a minimum severity.

        Args:
            severity (Alarms, optional): Minimum severity. Defaults to Alarms.WARNING.

        Returns:
            list: List of unhandled alarms

        """
        return [
            alarm for alarm in self.alarms_stack if not alarm.handled and alarm.severity >= severity
        ]

    @threadlocked
    def get_alarm(self, severity=Alarms.WARNING):
        """Get the next alarm

        Args:
            severity (Alarm, optional): Minimum severity. Defaults to Alarms.WARNING.

        Yields:
            AlarmBase: Alarm
        """
        alarms = self.get_unhandled_alarms(severity=severity)
        for alarm in alarms:
            yield alarm

    def raise_alarms(self, severity=Alarms.MAJOR):
        """Raise unhandled alarms with specified severity.

        Args:
            severity (Alarm, optional): Minimum severity. Defaults to Alarms.MAJOR.

        Raises:
            alarms: Alarm exception.
        """
        alarms = self.get_unhandled_alarms(severity=severity)
        if len(alarms) > 0:
            alarm = alarms.pop(0)
            self._raised_alarms.append(alarm)
            raise alarm

    @threadlocked
    def clear(self):
        """clear all alarms from stack"""
        self.alarms_stack.clear()

    def shutdown(self):
        """shutdown the alarm handler"""
        self.connector.shutdown()
