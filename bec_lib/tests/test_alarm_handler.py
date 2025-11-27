from bec_lib import messages
from bec_lib.alarm_handler import AlarmBase, Alarms


def test_alarm_base_printing():
    msg = messages.AlarmMessage(
        severity=Alarms.MAJOR,
        alarm_type="TestAlarmType",
        source={"source": "test"},
        msg="Test alarm content",
        compact_msg="Compact alarm content",
        metadata={"metadata": "metadata1"},
    )
    alarm_msg = AlarmBase(alarm=msg, alarm_type="TestAlarmType", severity=Alarms.MAJOR)

    # Test __str__ method
    expected_str = (
        "An alarm has occured. Severity: MAJOR.\n" "TestAlarmType.\n\t Compact alarm content"
    )
    assert str(alarm_msg) == expected_str

    # Test pretty_print method (just ensure it runs without error)
    alarm_msg.pretty_print()
