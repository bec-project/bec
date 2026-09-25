import io
from unittest import mock

import pytest
from rich.console import Console

from bec_ipython_client.beamline_mixin import BeamlineMixin
from bec_ipython_client.plugins.SLS.sls_info import OperatorInfo, SLSInfo


def _get_operator_messages(num: int):
    info = {f"sls_operator_messages_message{i}": {"value": f"message{i}"} for i in range(1, num)}
    info.update({f"sls_operator_date_message{i}": {"value": f"message{i}"} for i in range(1, num)})

    for i in range(num, 6):
        info.update({f"sls_operator_messages_message{i}": {"value": ""}})
        info.update({f"sls_operator_date_message{i}": {"value": ""}})

    return info


@pytest.mark.parametrize(
    "info,out",
    (
        [
            (
                _get_operator_messages(6),
                " SLS Operator messages \n┌──────────┬──────────┐\n│ Message  │ Time     │\n├──────────┼──────────┤\n│ message1 │ message1 │\n│ message2 │ message2 │\n│ message3 │ message3 │\n│ message4 │ message4 │\n│ message5 │ message5 │\n└──────────┴──────────┘\n",
            ),
            (
                _get_operator_messages(3),
                " SLS Operator messages \n┌──────────┬──────────┐\n│ Message  │ Time     │\n├──────────┼──────────┤\n│ message1 │ message1 │\n│ message2 │ message2 │\n└──────────┴──────────┘\n",
            ),
        ]
    ),
)
def test_operator_messages(info, out):
    mixin = BeamlineMixin()
    mixin._bl_info_register(OperatorInfo)
    bl_call = mixin._bl_calls[-1]
    with mock.patch.object(bl_call, "_get_operator_messages", return_value=info) as get_op_msgs:
        console = Console(file=io.StringIO(), width=120)
        with mock.patch.object(bl_call, "_get_console", return_value=console):
            mixin.bl_show_all()
            get_op_msgs.assert_called_once()

            output = console.file.getvalue()
            assert output == out


@pytest.mark.parametrize(
    "info,out",
    (
        [
            (
                {
                    "sls_info_machine_status": {"value": "Light Available"},
                    "sls_info_injection_mode": {"value": "TOP-UP"},
                    "sls_info_current_threshold": {"value": 400.8},
                    "sls_info_current_deadband": {"value": 1.8},
                    "sls_info_filling_pattern": {"value": "Default"},
                    "sls_info_filling_life_time": {"value": 10.2},
                    "sls_info_orbit_feedback_mode": {"value": "on"},
                    "sls_info_fast_orbit_feedback": {"value": "running"},
                    "sls_info_ring_current": {"value": 401.2},
                    "sls_info_crane_usage": {"value": "OFF"},
                },
                "                 SLS Info                 \n┌──────────────────────┬─────────────────┐\n│ Key                  │ Value           │\n├──────────────────────┼─────────────────┤\n│ Machine status       │ Light Available │\n│ Injection mode       │ TOP-UP          │\n│ Ring current         │ 401.200 mA      │\n│ Current threshold    │ 400.8           │\n│ Current deadband     │ 1.8             │\n│ Filling pattern      │ Default         │\n│ SLS filling lifetime │ 10.20 h         │\n│ Orbit feedback mode  │ on              │\n│ Fast orbit feedback  │ running         │\n│ SLS crane usage      │ OFF             │\n└──────────────────────┴─────────────────┘\n",
            )
        ]
    ),
)
def test_sls_info(info, out):
    mixin = BeamlineMixin()
    mixin._bl_info_register(SLSInfo)
    bl_call = mixin._bl_calls[-1]
    with mock.patch.object(bl_call, "_get_sls_info", return_value=info) as get_sls_info:
        console = Console(file=io.StringIO(), width=120)
        with mock.patch.object(bl_call, "_get_console", return_value=console):
            mixin.bl_show_all()
            get_sls_info.assert_called_once()

            output = console.file.getvalue()
            assert output == out
