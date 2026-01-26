import pytest

from bec_lib import messages
from bec_lib.alarm_handler import AlarmBase, Alarms


@pytest.mark.parametrize(
    "msg",
    [
        messages.AlarmMessage(
            severity=Alarms.MAJOR,
            info=messages.ErrorInfo(
                error_message="This is a test alarm message for testing purposes.",
                compact_error_message="Compact alarm content",
                exception_type="TestAlarmType",
                device="TestDevice",
            ),
            metadata={"metadata": "metadata1"},
        ),
        messages.AlarmMessage(
            severity=Alarms.MAJOR,
            info=messages.ErrorInfo(
                error_message="Another test alarm message with different content.",
                compact_error_message="Another compact alarm content",
                exception_type="AnotherTestAlarmType",
                device=None,
            ),
            metadata={"metadata": "metadata2"},
        ),
        messages.AlarmMessage(
            severity=Alarms.MAJOR,
            info=messages.ErrorInfo(
                error_message='Traceback (most recent call last):\n  File "<stdin>", line 1, in <module>\nException: Test traceback',
                compact_error_message='Traceback (most recent call last):\n  File "<stdin>", line 1, in <module>\nException: Test traceback',
                exception_type="NoCompactAlarmType",
                device="DeviceX",
            ),
            metadata={"metadata": "metadata3"},
        ),
    ],
)
def test_alarm_base_printing(msg):
    alarm_msg = AlarmBase(alarm=msg, severity=Alarms.MAJOR)

    # Test __str__ method
    expected_str = f"An alarm has occured. Severity: MAJOR.\n{msg.info.exception_type}.\n\t {msg.info.compact_error_message}"
    assert str(alarm_msg) == expected_str

    # Test pretty_print method (just ensure it runs without error)
    alarm_msg.pretty_print()
