from __future__ import annotations

import inspect
import threading
import time
from unittest import mock

import numpy as np
import pytest
import yaml
from pydantic import BaseModel

from bec_lib import bl_states, messages
from bec_lib.bl_state_machine import BeamlineStateMachine
from bec_lib.bl_state_manager import (
    BeamlineStateClientBase,
    BeamlineStateManager,
    _summarize_label,
    build_signature_from_model,
)
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
from bec_lib.tests.fixtures import dm_with_devices


@pytest.fixture
def state_manager(connected_connector):
    client = mock.MagicMock()
    client.connector = connected_connector
    manager = BeamlineStateManager(client)

    yield manager


class TestHelpers:
    def test_build_signature_from_model(self):
        class DemoConfig(BaseModel):
            foo: int = 1
            bar: str = "abc"

        config = DemoConfig()
        signature = build_signature_from_model(config)

        assert list(signature.parameters) == ["foo", "bar"]
        assert signature.parameters["foo"].kind == inspect.Parameter.KEYWORD_ONLY
        assert signature.parameters["foo"].annotation is int
        assert signature.parameters["bar"].default == "abc"


class TestConfigModels:
    def test_beamline_state_config_valid_name(self):
        config = bl_states.BeamlineStateConfig(name="sample_x_limits")
        assert config.name == "sample_x_limits"

    @pytest.mark.parametrize("invalid_name", ["state-name", "class", "add", "remove", "show_all"])
    def test_beamline_state_config_invalid_name(self, invalid_name):
        with pytest.raises(ValueError):
            bl_states.BeamlineStateConfig(name=invalid_name)

    def test_device_state_config_keeps_string_device_and_signal(self):
        config = bl_states.DeviceStateConfig(name="state", device="samx", signal="samx")
        assert config.device == "samx"
        assert config.signal == "samx"

    def test_device_state_config_accepts_matching_signal_device(self, dm_with_devices):
        config = bl_states.DeviceStateConfig(
            name="state", device=dm_with_devices.devices.bpm4i, signal=dm_with_devices.devices.bpm4i
        )

        assert config.device == "bpm4i"
        assert config.signal == "bpm4i"

    def test_device_state_config_rejects_mismatched_signal_for_signal_device(self, dm_with_devices):
        with pytest.raises(ValueError, match="does not match signal device"):
            bl_states.DeviceStateConfig(
                name="state", device=dm_with_devices.devices.bpm4i, signal="bpm5i"
            )

    def test_aggregated_state_config_defaults_to_any_evaluation(self):
        config = bl_states.AggregatedStateConfig(
            name="evaluation", states={"label": {"devices": {"bpm4i": {"value": 0}}}}
        )

        assert config.evaluation_method == "any"

    @pytest.mark.parametrize("evaluation_method", ["any", "all", "exclusive", None])
    def test_aggregated_state_config_accepts_evaluation_method(self, evaluation_method):
        config = bl_states.AggregatedStateConfig(
            name="evaluation",
            evaluation_method=evaluation_method,
            states={"label": {"devices": {"bpm4i": {"value": 0}}}},
        )

        assert config.evaluation_method == evaluation_method

    @pytest.mark.parametrize("evaluation_method", ["invalid", "null", True])
    def test_aggregated_state_config_rejects_invalid_evaluation_method(self, evaluation_method):
        with pytest.raises(ValueError, match="evaluation_method"):
            bl_states.AggregatedStateConfig(
                name="evaluation",
                evaluation_method=evaluation_method,
                states={"label": {"devices": {"bpm4i": {"value": 0}}}},
            )


class TestBeamlineStateBase:
    def test_beamline_state_initialization_and_update(self):
        class ConcreteState(bl_states.BeamlineState[bl_states.BeamlineStateConfig]):
            CONFIG_CLASS = bl_states.BeamlineStateConfig

            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(
                    name=self.config.name, status="valid", label="ok"
                )

        state = ConcreteState(name="test_state")

        assert state.config.name == "test_state"
        assert state.connector is None
        assert state._last_state is None

    def test_default_config_summary(self):
        config = bl_states.DeviceStateConfig(name="state", device="samx")

        assert bl_states.BeamlineState.format_config_summary(config) == "device=samx"


class TestDeviceBeamlineState:
    def test_start_requires_connector(self, dm_with_devices):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_y_limits",
            device="samy",
            signal="samy",
            low_limit=0.0,
            high_limit=10.0,
            device_manager=dm_with_devices,
        )

        with pytest.raises(RuntimeError, match="Redis connector is not set"):
            state.start()

    def test_start_registers_device_callback(self, connected_connector, dm_with_devices):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            signal="samx",
            low_limit=0.0,
            high_limit=10.0,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        with mock.patch.object(connected_connector, "register") as register:
            state.start()

        register.assert_called_once_with(
            MessageEndpoints.device_readback("samx"), cb=state._update_device_state
        )

    def test_stop_unregisters_device_callback(self, connected_connector, dm_with_devices):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            signal="samx",
            low_limit=0.0,
            high_limit=10.0,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        with mock.patch.object(connected_connector, "unregister") as unregister:
            state.start()
            state.stop()

        unregister.assert_called_once_with(
            MessageEndpoints.device_readback("samx"), cb=state._update_device_state
        )

    def test_update_device_state_publishes_when_state_changes(
        self, connected_connector, dm_with_devices
    ):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            signal="samx",
            low_limit=0.0,
            high_limit=10.0,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        msg = messages.DeviceMessage(
            signals={"samx": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )

        state._update_device_state(MessageObject(value=msg, topic="test"))

        assert state._last_state is not None
        assert state._last_state.status == "valid"
        out = connected_connector.xread(
            MessageEndpoints.beamline_state("sample_x_limits"), from_start=True
        )
        assert out is not None
        assert out[0]["data"].status == "valid"


class TestConcreteStates:

    @pytest.fixture(scope="function")
    def aggregated_state_config(self, dm_with_devices):
        """Fixture for an test aggregated state configuration."""
        dm_with_devices.devices["samx"].user_parameter["test"] = 0
        return bl_states.AggregatedStateConfig(
            name="alignment",
            states={
                "alignment": {
                    "devices": {
                        "samx": {
                            "value": 0,
                            "abs_tol": 0.1,
                            "low_limit": {"value": -20, "abs_tol": 0.1},
                            "high_limit": {"value": 20, "abs_tol": 0.1},
                        },
                        "bpm4i": {"value": 0, "abs_tol": 0.1},
                    }
                },
                "measurement": {
                    "devices": {
                        "samx": {
                            "value": 19,
                            "abs_tol": 0.1,
                            "low_limit": {"value": -20, "abs_tol": 0.1},
                            "high_limit": {"value": 20, "abs_tol": 0.1},
                            "signals": {"velocity": {"value": 5, "abs_tol": 0.1}},
                        },
                        "bpm4i": {"value": 2, "abs_tol": 0.1},
                    }
                },
                "test": {"devices": {"bpm4i": {"value": 0, "abs_tol": 0.1}}},
                "string_state": {"devices": {"bpm3i": {"value": "ok"}}},
                "state_with_user_param": {"devices": {"samx": {"at": "test"}}},
            },
        )

    def test_device_within_limits_state_valid_and_invalid(
        self, connected_connector, dm_with_devices
    ):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            signal="samx",
            low_limit=0.0,
            high_limit=10.0,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        valid_msg = messages.DeviceMessage(
            signals={"samx": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        invalid_msg = messages.DeviceMessage(
            signals={"samx": {"value": 11.0, "timestamp": 2.0}}, metadata={"stream": "primary"}
        )

        assert state.evaluate(valid_msg).status == "valid"
        assert state.evaluate(invalid_msg).status == "invalid"

    def test_device_within_limits_state(self, connected_connector, dm_with_devices):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            low_limit=0.0,
            high_limit=10.0,
            tolerance=0.1,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        valid = messages.DeviceMessage(
            signals={"samx": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        warning = messages.DeviceMessage(
            signals={"samx": {"value": 0.05, "timestamp": 2.0}}, metadata={"stream": "primary"}
        )
        invalid = messages.DeviceMessage(
            signals={"samx": {"value": 11.0, "timestamp": 3.0}}, metadata={"stream": "primary"}
        )
        missing = messages.DeviceMessage(
            signals={"samx": {"timestamp": 4.0}}, metadata={"stream": "primary"}
        )

        assert state.evaluate(valid).status == "valid"
        assert state.evaluate(warning).status == "warning"
        assert state.evaluate(invalid).status == "invalid"
        assert state.evaluate(missing).status == "invalid"

    def test_device_within_limits_state_accepts_signal_backed_device(
        self, connected_connector, dm_with_devices
    ):
        state = bl_states.DeviceWithinLimitsState(
            name="bpm4i_within_limits",
            device="bpm4i",
            signal="bpm4i",
            low_limit=-1.0,
            high_limit=10.0,
            tolerance=0.1,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        msg = messages.DeviceMessage(
            signals={"bpm4i": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )

        assert state.signal_name == "bpm4i"
        assert state.evaluate(msg).status == "valid"

    def test_aggregated_state_init(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        """
        Test the initialization of the AggregatedState.

        Based on the provided configuration, we expect certain callbacks to be registered with the
        Redis connector. This test checks this which essentially checks the proper functionality
        of the 'start' method.
        """

        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()
        # We should now have subscriptions on samx limits, readback and read_configuration, and bpm4i & bpm4i
        info = [
            MessageEndpoints.device_readback("samx"),
            MessageEndpoints.device_read_configuration("samx"),
            MessageEndpoints.device_limits("samx"),
            MessageEndpoints.device_readback("bpm4i"),
            MessageEndpoints.device_readback("bpm3i"),
        ]
        for endpoint in info:
            assert endpoint.endpoint in state.connector._topics_cb

    def test_aggregated_state_evaluation(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        """
        Test the evaluation of the AggregatedState when receiving message updates. This should trigger a state evaluation for
        the affected labels and the current state, and if the state changes, a new state should be published.
        """
        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        with (
            mock.patch.object(state, "evaluate", return_value=None) as evaluate,
            mock.patch.object(state, "_emit_state") as emit_state,
        ):
            # Test triggering evaluation for multiple labels
            # samx affects alignment and measurement, so both should be evaluated.
            msg_with_2_states = messages.DeviceMessage(
                signals={"samx": {"value": 5.0, "timestamp": 1.0}}
            )
            msg_obj = MessageObject(
                value=msg_with_2_states, topic=MessageEndpoints.device_readback("samx").endpoint
            )
            state._update_aggregated_state(msg_obj, device="samx", source="readback")
            evaluate.assert_called_once_with(
                affected_labels=set(["state_with_user_param", "alignment", "measurement"])
            )
            emit_state.assert_not_called()  # As evaluate is mocked to return None, _emit_state should not be called

    def test_aggregated_state_evaluate(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        """
        Test the evaluate method.
        We manually cache the relevant messages and then call evaluate with the affected label.
        We then check if the output message has the expected status and label, and if the current labels are updated correctly.
        """
        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state._build_rules()
        # Assume that we are currently in test
        state._current_labels = ["test"]
        state._cache_message(
            "samx",
            "readback",
            messages.DeviceMessage(
                signals={"samx": {"value": 0, "timestamp": 1.0}}, metadata={"stream": "primary"}
            ),
        )
        state._cache_message(
            "samx",
            "configuration",
            messages.DeviceMessage(
                signals={"samx_velocity": {"value": 5, "timestamp": 1.0}},
                metadata={"stream": "baseline"},
            ),
        )
        state._cache_message(
            "samx",
            "limits",
            messages.DeviceMessage(
                signals={
                    "low": {"value": -20, "timestamp": 1.0},
                    "high": {"value": 20, "timestamp": 1.0},
                },
                metadata={"stream": "baseline"},
            ),
        )
        state._cache_message(
            "bpm4i",
            "readback",
            messages.DeviceMessage(
                signals={"bpm4i": {"value": 0, "timestamp": 1.0}}, metadata={"stream": "primary"}
            ),
        )

        msg = state.evaluate(affected_labels={"alignment"})

        assert msg.status == "valid"
        assert msg.label == "alignment|test"
        assert set(state._current_labels) == set(["alignment", "test"])
        dm_with_devices.devices["samx"].user_parameter["test"] = 0
        msg = state.evaluate(affected_labels={"alignment", "state_with_user_param"})
        assert msg.status == "valid"
        assert msg.label == "alignment|state_with_user_param|test"
        assert set(state._current_labels) == set(["alignment", "state_with_user_param", "test"])

        dm_with_devices.devices["samx"].user_parameter["test"] = 1
        msg = state.evaluate(affected_labels={"state_with_user_param"})
        assert msg.status == "valid"
        assert msg.label == "alignment|test"
        assert state._current_labels == ["alignment", "test"]

        state._cache_message(
            "samx",
            "readback",
            messages.DeviceMessage(
                signals={"samx": {"value": 3, "timestamp": 2.0}}, metadata={"stream": "primary"}
            ),
        )

        msg = state.evaluate(affected_labels={"alignment"})

        assert msg.status == "valid"
        assert msg.label == "test"
        assert state._current_labels == ["test"]

        state._cache_message(
            "bpm4i",
            "readback",
            messages.DeviceMessage(
                signals={"bpm4i": {"value": 2, "timestamp": 2.0}}, metadata={"stream": "primary"}
            ),
        )

        msg = state.evaluate(affected_labels={"alignment", "test", "measurement"})

        assert msg.status == "invalid"
        assert msg.label == "No matching state"
        assert state._current_labels == []

    @pytest.mark.parametrize(
        ("evaluation_method", "targets", "expected_status", "expected_label"),
        [
            ("any", (0, 1), "valid", "first"),
            ("any", (0, 0), "valid", "first|second"),
            ("any", (1, 2), "invalid", "No matching state"),
            ("all", (0, 0), "valid", "first|second"),
            ("all", (0, 1), "invalid", "first"),
            ("all", (1, 2), "invalid", "No matching state"),
            ("exclusive", (0, 1), "valid", "first"),
            ("exclusive", (0, 0), "invalid", "first|second"),
            ("exclusive", (1, 2), "invalid", "No matching state"),
            (None, (0, 0), "valid", "first|second"),
            (None, (1, 2), "valid", "No matching state"),
        ],
    )
    def test_aggregated_state_evaluation_method(
        self,
        connected_connector,
        dm_with_devices,
        evaluation_method,
        targets,
        expected_status,
        expected_label,
    ):
        config = bl_states.AggregatedStateConfig(
            name="evaluation",
            evaluation_method=evaluation_method,
            states={
                "first": {"devices": {"bpm4i": {"value": targets[0]}}},
                "second": {"devices": {"bpm4i": {"value": targets[1]}}},
            },
        )
        state = bl_states.AggregatedState(
            config=config, redis_connector=connected_connector, device_manager=dm_with_devices
        )
        state._build_rules()
        state._cache_message(
            "bpm4i",
            "readback",
            messages.DeviceMessage(signals={"bpm4i": {"value": 0, "timestamp": 1}}),
        )

        msg = state.evaluate(affected_labels={"first", "second"})

        assert msg.status == expected_status
        assert msg.label == expected_label
        assert state._current_labels == (
            [] if expected_label == "No matching state" else expected_label.split("|")
        )

    def test_aggregated_state_preserves_incremental_label_evaluation(
        self, connected_connector, dm_with_devices
    ):
        config = bl_states.AggregatedStateConfig(
            name="evaluation",
            states={
                "affected": {"devices": {"samx": {"value": 1}}},
                "current": {"devices": {"bpm4i": {"value": 0}}},
                "unrelated": {"devices": {"bpm3i": {"value": "ok"}}},
            },
        )
        state = bl_states.AggregatedState(
            config=config, redis_connector=connected_connector, device_manager=dm_with_devices
        )
        state._build_rules()
        state._signal_value_cache.update(
            {
                ("samx", "readback", "samx"): 1,
                ("bpm4i", "readback", "bpm4i"): 0,
                ("bpm3i", "readback", "bpm3i"): "ok",
            }
        )
        state._current_labels = ["current"]

        with mock.patch.object(
            state, "_label_matches", wraps=state._label_matches
        ) as label_matches:
            msg = state.evaluate(affected_labels={"affected"})

        assert {call.args[0] for call in label_matches.call_args_list} == {"affected", "current"}
        assert msg.status == "valid"
        assert msg.label == "affected|current"

    def test_aggregated_state_runtime_evaluation_method_update(
        self, connected_connector, dm_with_devices
    ):
        config = bl_states.AggregatedStateConfig(
            name="evaluation",
            evaluation_method="any",
            states={
                "alignment": {"devices": {"bpm4i": {"value": 0}}},
                "duplicate": {"devices": {"bpm4i": {"value": 0}}},
            },
        )
        state = bl_states.AggregatedState(
            config=config, redis_connector=connected_connector, device_manager=dm_with_devices
        )
        connected_connector.set_and_publish(
            MessageEndpoints.device_readback("bpm4i"),
            messages.DeviceMessage(signals={"bpm4i": {"value": 0, "timestamp": 1}}),
        )
        state.start()
        initial = connected_connector.xread(
            MessageEndpoints.beamline_state("evaluation"), from_start=True
        )[-1]["data"]
        assert initial.status == "valid"
        assert initial.label == "alignment|duplicate"

        state.update_parameters(evaluation_method="exclusive")
        state.restart()

        updated = connected_connector.xread(
            MessageEndpoints.beamline_state("evaluation"), from_start=True
        )[-1]["data"]
        assert updated.status == "invalid"
        assert updated.label == "alignment|duplicate"

    def test_aggregated_state_exception_handling(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        """
        Test that if an exception is raised during the evaluation of the state, this is properly handled and an alarm is raised.
        We check that the evaluate method is called and that if it raises an exception, the raise_alarm method of the connector
        is called, and a state with status "unknown" and label "broken state" is published.
        """
        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()
        msg = messages.DeviceMessage(
            signals={"samx": {"value": 0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        msg_obj = MessageObject(value=msg, topic=MessageEndpoints.device_readback("samx").endpoint)

        with (
            mock.patch.object(
                state, "evaluate", side_effect=RuntimeError("broken state")
            ) as evaluate,
            mock.patch.object(connected_connector, "raise_alarm") as raise_alarm,
        ):
            state._update_aggregated_state(msg_obj, device="samx", source="readback")

        evaluate.assert_called_once_with(
            affected_labels={"state_with_user_param", "alignment", "measurement"}
        )
        raise_alarm.assert_called_once()
        out = connected_connector.xread(
            MessageEndpoints.beamline_state("alignment"), from_start=True
        )
        assert out[-1]["data"].status == "unknown"
        assert out[-1]["data"].label == "broken state"
        assert state.raised_warning is True

    def test_aggregated_state_transitions_between_labels(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        """
        Test the transitions between different labels of the aggregated state. We simulate the messages that would trigger
        the transitions and check that the output message has the expected status and label, and that the current labels are updated correctly.
        """
        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        def update(device, source, signals):
            msg = messages.DeviceMessage(signals=signals, metadata={"stream": "primary"})
            msg_obj = MessageObject(value=msg, topic=state._endpoint(device, source).endpoint)
            state._update_aggregated_state(msg_obj, device=device, source=source)
            out = connected_connector.xread(
                MessageEndpoints.beamline_state("alignment"), from_start=True
            )
            return out[-1]["data"]

        msg = update("samx", "configuration", {"samx_velocity": {"value": 5, "timestamp": 1.0}})
        assert msg.status == "invalid"

        update(
            "samx",
            "limits",
            {"low": {"value": -20, "timestamp": 1.0}, "high": {"value": 20, "timestamp": 1.0}},
        )
        update("samx", "readback", {"samx": {"value": 0, "timestamp": 1.0}})
        msg = update("bpm4i", "readback", {"bpm4i": {"value": 0, "timestamp": 1.0}})
        assert msg.status == "valid"
        assert msg.label == "alignment|state_with_user_param|test"

        msg = update("samx", "readback", {"samx": {"value": 19, "timestamp": 2.0}})
        assert msg.status == "valid"
        assert msg.label == "test"

        msg = update("bpm4i", "readback", {"bpm4i": {"value": 2, "timestamp": 2.0}})
        assert msg.status == "valid"
        assert msg.label == "measurement"

    @pytest.mark.parametrize(
        ("cached_value", "expected_value", "at", "abs_tolerance", "matches"),
        [
            (1.05, 1.0, None, 0.1, True),
            (1.2, 1.0, None, 0.1, False),
            (5, 5, None, 0.0, True),
            (np.int64(5), 5, None, 0.0, True),
            (np.float64(1.05), 1.0, None, 0.1, True),
            ("ok", "ok", None, 0.0, True),
            ("not-ok", "ok", None, 0.0, False),
            ([1, 2], 1, None, 0.0, False),
            (np.array([1.0, 2.0]), 1.0, None, 0.1, False),
            (np.array([1.0, 2.0]), np.array([1.0, 2.0]), None, 0.0, False),
        ],
    )
    def test_aggregated_state_requirement_matches(
        self,
        connected_connector,
        dm_with_devices,
        aggregated_state_config,
        cached_value,
        expected_value,
        at,
        abs_tolerance,
        matches,
    ):
        """
        Test the evaluation of requirements in the aggregated state. We manually set the signal value
        cache and then call the _requirement_matches method with a requirement, and check if the output is as expected.
        """
        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        requirement = bl_states.ResolvedStateSignal(
            label="alignment",
            device_name="bpm4i",
            signal_name="bpm4i",
            expected_value=expected_value,
            at=at,
            abs_tolerance=abs_tolerance,
            source="readback",
        )
        state._signal_value_cache[("bpm4i", "readback", "bpm4i")] = cached_value

        assert state._requirement_matches(requirement) is matches

    def test_device_config_requires_at_least_one_target(self):
        with pytest.raises(ValueError, match="At least one of value"):
            bl_states.DeviceConfig()

    @pytest.mark.parametrize("config_class", [bl_states.SignalConfig, bl_states.DeviceConfig])
    def test_target_config_rejects_negative_abs_tolerance(self, config_class):
        with pytest.raises(ValueError, match="greater than or equal to 0"):
            config_class(value=1, abs_tol=-0.1)

    @pytest.mark.parametrize("config_class", [bl_states.SignalConfig, bl_states.DeviceConfig])
    def test_target_config_rejects_value_and_at(self, config_class):
        with pytest.raises(ValueError, match="Cannot specify both 'value' and 'at'"):
            config_class(value=1, at="target")

    def test_signal_config_requires_value_or_at(self):
        with pytest.raises(ValueError, match="Either 'value' or 'at'"):
            bl_states.SignalConfig()

    def test_aggregated_state_rejects_unknown_target_fields(self):
        with pytest.raises(ValueError, match="low_limt"):
            bl_states.SubDeviceStateConfig(devices={"samx": {"value": 0, "low_limt": -10}})

    @pytest.mark.parametrize("target", [0, False, ""])
    def test_aggregated_state_resolves_falsy_user_parameter(self, dm_with_devices, target):
        dm_with_devices.devices["samx"].user_parameter["target"] = target
        config = bl_states.SubDeviceStateConfig(devices={"samx": {"at": "target"}})

        requirements = bl_states.AggregatedState.get_state_requirements(
            "test", config, dm_with_devices, "test"
        )

        assert len(requirements) == 1
        resolved = bl_states.AggregatedState.get_expected_value(requirements[0], dm_with_devices)
        assert resolved == target
        assert type(resolved) is type(target)

    @pytest.mark.parametrize("target", [0, False, ""])
    def test_aggregated_state_keeps_falsy_literal_target(self, dm_with_devices, target):
        config = bl_states.SubDeviceStateConfig(devices={"samx": {"value": target}})

        requirements = bl_states.AggregatedState.get_state_requirements(
            "test", config, dm_with_devices, "test"
        )

        assert len(requirements) == 1
        assert requirements[0].expected_value == target
        assert type(requirements[0].expected_value) is type(target)

    def test_aggregated_state_resolves_nested_user_parameters(self, dm_with_devices):
        dm_with_devices.devices["samx"].user_parameter.update({"low": -5, "high": 5, "speed": 2})
        config = bl_states.SubDeviceStateConfig(
            devices={
                "samx": {
                    "low_limit": {"at": "low"},
                    "high_limit": {"at": "high"},
                    "signals": {"velocity": {"at": "speed"}},
                }
            }
        )

        requirements = bl_states.AggregatedState.get_state_requirements(
            "test", config, dm_with_devices, "test"
        )

        assert [(req.signal_name, req.at) for req in requirements] == [
            ("low", "low"),
            ("high", "high"),
            ("samx_velocity", "speed"),
        ]
        assert [
            bl_states.AggregatedState.get_expected_value(req, dm_with_devices)
            for req in requirements
        ] == [-5, 5, 2]

    @pytest.mark.parametrize(
        ("target_config", "user_parameters"),
        [
            ({"low_limit": {"at": "missing"}}, {}),
            ({"low_limit": {"at": "missing"}}, {"missing": None}),
            ({"high_limit": {"at": "missing"}}, {}),
            ({"high_limit": {"at": "missing"}}, {"missing": None}),
            ({"signals": {"velocity": {"at": "missing"}}}, {}),
            ({"signals": {"velocity": {"at": "missing"}}}, {"missing": None}),
        ],
    )
    def test_aggregated_state_rejects_missing_nested_user_parameter(
        self, dm_with_devices, target_config, user_parameters
    ):
        dm_with_devices.devices["samx"].user_parameter.update(user_parameters)
        config = bl_states.SubDeviceStateConfig(devices={"samx": target_config})

        with pytest.raises(ValueError, match="User parameter 'missing'"):
            bl_states.AggregatedState.get_state_requirements(
                "test", config, dm_with_devices, "test"
            )

    def test_aggregated_state_endpoint_rejects_unknown_source(self):
        with pytest.raises(ValueError, match="Invalid signal source"):
            bl_states.AggregatedState._endpoint("samx", "unknown")

    def test_aggregated_state_get_device_manager_falls_back_to_client(self):
        state = bl_states.AggregatedState(
            name="alignment", states={"label": {"devices": {"samx": {"value": 0}}}}
        )
        client = mock.MagicMock()

        with mock.patch("bec_lib.client.BECClient", return_value=client):
            assert state._get_device_manager() is client.device_manager

    def test_aggregated_state_get_signal_source_rejects_unsupported_kind(self):
        with pytest.raises(ValueError, match="Unsupported kind"):
            bl_states.AggregatedState._get_signal_source(
                {"kind_str": "omitted", "obj_name": "samx_unused"}, "test"
            )

    def test_aggregated_state_resolve_signal_edge_cases(self, dm_with_devices):
        assert bl_states.AggregatedState._resolve_signal(
            "samx", "low_limit_travel", dm_with_devices, "test"
        ) == ("low", "limits")
        assert bl_states.AggregatedState._resolve_signal(
            "samx", "high_limit_travel", dm_with_devices, "test"
        ) == ("high", "limits")
        assert bl_states.AggregatedState._resolve_signal(
            "samx", "samx_velocity", dm_with_devices, "test"
        ) == ("samx_velocity", "configuration")

        with pytest.raises(ValueError, match="Device 'missing' not found"):
            bl_states.AggregatedState._resolve_signal("missing", "missing", dm_with_devices, "test")
        with pytest.raises(ValueError, match="Device name must be a string"):
            bl_states.AggregatedState._resolve_signal(1, "samx", dm_with_devices, "test")
        with pytest.raises(ValueError, match="Signal 'missing_signal' not found"):
            bl_states.AggregatedState._resolve_signal(
                "samx", "missing_signal", dm_with_devices, "test"
            )
        with pytest.raises(ValueError, match="Unsupported kind"):
            bl_states.AggregatedState._resolve_signal("samx", "unused", dm_with_devices, "test")

    def test_aggregated_state_resolve_dotted_signal_edge_cases(self, dm_with_devices):
        assert bl_states.AggregatedState._resolve_signal(
            "samx", "samx.velocity", dm_with_devices, "test"
        ) == ("samx_velocity", "configuration")

        with pytest.raises(ValueError, match="does not belong"):
            bl_states.AggregatedState._resolve_signal(
                "samx", "samy.velocity", dm_with_devices, "test"
            )

        devices = mock.MagicMock()
        devices.__getitem__.side_effect = [dm_with_devices.devices["samx"], AttributeError]
        manager = mock.MagicMock(devices=devices)
        with pytest.raises(ValueError, match="Signal 'samx.missing' not found"):
            bl_states.AggregatedState._resolve_signal("samx", "samx.missing", manager, "test")

    def test_aggregated_state_start_edge_cases(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.started = True
        with mock.patch.object(state, "_build_rules") as build_rules:
            state.start()
        build_rules.assert_not_called()

        state = bl_states.AggregatedState(
            config=aggregated_state_config, redis_connector=None, device_manager=dm_with_devices
        )
        with pytest.raises(RuntimeError, match="Redis connector is not set"):
            state.start()

    def test_aggregated_state_start_handles_rule_build_error(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        with (
            mock.patch.object(state, "_build_rules", side_effect=RuntimeError("bad rules")),
            mock.patch.object(state, "_handle_state_exception") as handle_exception,
            mock.patch.object(connected_connector, "register") as register,
        ):
            state.start()

            handle_exception.assert_called_once()
            register.assert_not_called()
            assert state.started is False
            assert state._subscriptions == set()
            assert state._requirements_for_label == {}
            assert state._signal_info_to_labels == {}

    def test_aggregated_state_fill_cache_uses_existing_messages(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state._build_rules()
        connected_connector.set_and_publish(
            MessageEndpoints.device_readback("samx"),
            messages.DeviceMessage(signals={"samx": {"value": 0, "timestamp": 1.0}}),
        )

        affected_labels = state._fill_cache()

        assert affected_labels == {"alignment", "measurement", "state_with_user_param"}
        assert state._signal_value_cache[("samx", "readback", "samx")] == 0

    def test_aggregated_state_cache_ignores_irrelevant_signals(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state._build_rules()

        affected_labels = state._cache_message(
            "samx",
            "readback",
            messages.DeviceMessage(
                signals={"samx_unused": {"value": 1, "timestamp": 1.0}},
                metadata={"stream": "primary"},
            ),
        )

        assert affected_labels == set()
        assert ("samx", "readback", "samx_unused") not in state._signal_value_cache

    def test_aggregated_state_stop_unregisters_subscriptions(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        with mock.patch.object(connected_connector, "unregister") as unregister:
            state.stop()

        assert unregister.call_count == len(state._subscriptions)
        assert state.started is False

    def test_aggregated_state_stop_is_noop_before_start(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        with mock.patch.object(connected_connector, "unregister") as unregister:
            state.stop()

        unregister.assert_not_called()

    def test_aggregated_state_evaluate_without_affected_labels(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        assert state.evaluate() is None


class TestBeamlineStateManager:
    def test_manager_registers_for_state_updates(self, connected_connector):
        client = mock.MagicMock()
        client.connector = connected_connector

        with mock.patch.object(connected_connector, "register") as register:
            BeamlineStateManager(client)

        register.assert_called_once_with(MessageEndpoints.available_beamline_states(), cb=mock.ANY)

    def test_manager_is_ready_when_no_state_update_exists(self, connected_connector):
        client = mock.MagicMock()
        client.connector = connected_connector

        manager = BeamlineStateManager(client)

        assert manager.ready is True
        assert manager._states == {}

    def test_manager_loads_existing_state_update_on_init(self, connected_connector):
        config = messages.BeamlineStateConfig(
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
        )
        connected_connector.xadd(
            MessageEndpoints.available_beamline_states(),
            {"data": messages.AvailableBeamlineStatesMessage(states=[config])},
            max_size=1,
        )
        client = mock.MagicMock()
        client.connector = connected_connector

        manager = BeamlineStateManager(client)

        assert manager.ready is True
        assert "sample_y_limits" in manager._states
        assert isinstance(getattr(manager, "sample_y_limits"), BeamlineStateClientBase)

    def test_manager_rejects_abstract_state_type_on_init(self, connected_connector):
        config = messages.BeamlineStateConfig(
            name="generic_device_state",
            state_type="DeviceBeamlineState",
            parameters={"name": "generic_device_state", "device": "samy"},
        )
        connected_connector.xadd(
            MessageEndpoints.available_beamline_states(),
            {"data": messages.AvailableBeamlineStatesMessage(states=[config])},
            max_size=1,
        )
        client = mock.MagicMock()
        client.connector = connected_connector

        with pytest.raises(ValueError, match="not a concrete beamline state"):
            BeamlineStateManager(client)

    def test_on_state_update_creates_client_attribute(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])

        state_manager._on_state_update({"data": update}, parent=state_manager)

        assert "sample_y_limits" in state_manager._states
        assert isinstance(
            state_manager._states["sample_y_limits"], bl_states.DeviceWithinLimitsStateConfig
        )
        assert isinstance(getattr(state_manager, "sample_y_limits"), BeamlineStateClientBase)

    def test_update_parameters_from_client_updates_state_and_publishes(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="limits",
            state_type="DeviceWithinLimitsState",
            parameters={"name": "limits", "device": "samx", "low_limit": 0.0, "high_limit": 10.0},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager.limits.update_parameters(tolerance=0.25)

        assert state_manager._states["limits"].tolerance == 0.25

        out = state_manager._connector.xread(
            MessageEndpoints.available_beamline_states(), from_start=True
        )
        assert out
        assert isinstance(out[-1]["data"], messages.AvailableBeamlineStatesMessage)

    def test_runtime_evaluation_method_is_published(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="evaluation",
            state_type="AggregatedState",
            parameters={
                "name": "evaluation",
                "evaluation_method": "any",
                "states": {"alignment": {"devices": {"bpm4i": {"value": 0}}}},
            },
        )
        state_manager._on_state_update(
            {"data": messages.AvailableBeamlineStatesMessage(states=[config])}, parent=state_manager
        )

        state_manager.evaluation.update_parameters(evaluation_method="exclusive")

        update = state_manager._connector.xread(
            MessageEndpoints.available_beamline_states(), from_start=True
        )[-1]["data"]
        published = next(state for state in update.states if state.name == "evaluation")
        assert published.parameters["evaluation_method"] == "exclusive"

    def test_external_parameter_update_refreshes_existing_client_state(self, state_manager):
        initial = messages.BeamlineStateConfig(
            name="limits",
            state_type="DeviceWithinLimitsState",
            parameters={"name": "limits", "device": "samx", "low_limit": 0.0, "high_limit": 10.0},
        )
        state_manager._on_state_update(
            {"data": messages.AvailableBeamlineStatesMessage(states=[initial])},
            parent=state_manager,
        )

        updated = messages.BeamlineStateConfig(
            name="limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "limits",
                "device": "samx",
                "low_limit": 1.0,
                "high_limit": 9.0,
                "tolerance": 0.25,
            },
        )
        state_manager._on_state_update(
            {"data": messages.AvailableBeamlineStatesMessage(states=[updated])},
            parent=state_manager,
        )

        assert state_manager._states["limits"].low_limit == 1.0
        assert state_manager._states["limits"].high_limit == 9.0
        assert state_manager._states["limits"].tolerance == 0.25
        assert state_manager.limits._state.model_dump(exclude_none=True) == updated.parameters

    def test_client_get_returns_unknown_without_status_message(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        result = state_manager.sample_y_limits.get()
        assert result == {"status": "unknown", "label": "No state information available."}

    def test_client_get_returns_latest_status_message(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager._connector.xadd(
            MessageEndpoints.beamline_state("sample_y_limits"),
            {
                "data": messages.BeamlineStateMessage(
                    name="sample_y_limits", status="valid", label="ok"
                )
            },
            max_size=1,
        )

        result = state_manager.sample_y_limits.get()
        assert result == {"status": "valid", "label": "ok"}

    def test_add_waits_for_initial_state_message(self, state_manager):
        state = bl_states.DeviceWithinLimitsStateConfig(
            name="sample_y_limits", device="samy", low_limit=0.0, high_limit=10.0
        )

        def publish_initial_state():
            time.sleep(0.05)
            state_manager._connector.xadd(
                MessageEndpoints.beamline_state("sample_y_limits"),
                {
                    "data": messages.BeamlineStateMessage(
                        name="sample_y_limits", status="valid", label="ok"
                    )
                },
                max_size=1,
            )

        publisher = threading.Thread(target=publish_initial_state)
        publisher.start()
        try:
            state_manager.add(state)
        finally:
            publisher.join()

        assert state_manager.sample_y_limits.get() == {"status": "valid", "label": "ok"}

    def test_add_rejects_abstract_device_state_config(self, state_manager):
        state = bl_states.DeviceStateConfig(name="generic_device_state", device="samy")

        with pytest.raises(ValueError, match="not a concrete beamline state"):
            state_manager.add(state)

    def test_add_and_delete_publish_updates(self, state_manager):
        state = bl_states.DeviceWithinLimitsStateConfig(
            name="sample_y_limits", device="samy", low_limit=0.0, high_limit=10.0
        )

        with mock.patch.object(state_manager, "_wait_for_initial_state"):
            state_manager.add(state)
        assert "sample_y_limits" in state_manager._states

        state_manager.delete("sample_y_limits")
        assert "sample_y_limits" not in state_manager._states

    def test_client_remove_state(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager.sample_y_limits.remove()

        assert "sample_y_limits" not in state_manager._states

    def test_show_all_prints_table(self, state_manager, capsys):
        state = bl_states.DeviceWithinLimitsStateConfig(
            name="sample_y_limits", device="samy", low_limit=0.0, high_limit=10.0
        )
        assert bl_states.DeviceWithinLimitsState.format_config_summary(state) == (
            "samy · signal=samy · limits=[0.0, 10.0] · tolerance=0.1"
        )
        with mock.patch.object(state_manager, "_wait_for_initial_state"):
            state_manager.add(state)

        state_manager.show_all()

        captured = capsys.readouterr()
        assert "sample_y_limits" in (captured.out + captured.err)

    @staticmethod
    def _aggregated_display_config():
        return bl_states.AggregatedStateConfig(
            name="beamline_mode",
            evaluation_method="all",
            states={
                "alignment": {
                    "devices": {"samx": {"value": 0, "signals": {"velocity": {"value": 5}}}},
                    "transition_metadata": {"description": "Prepare alignment"},
                },
                "measurement": {"devices": {"bpm4i": {"value": 100}}},
                "parked": {
                    "devices": {"samx": {"low_limit": {"value": -10}, "high_limit": {"value": 10}}}
                },
                "test": {"devices": {"samy": {"at": "in"}}},
            },
        )

    def test_show_all_summarizes_aggregated_state(self, state_manager, capsys):
        state = self._aggregated_display_config()
        state_manager._add_state(state)

        assert bl_states.AggregatedState.format_config_summary(state) == (
            "all · 4 labels · 3 devices · 6 requirements · 1 transition"
        )
        assert _summarize_label("alignment|measurement|parked|test") == (
            "alignment|measurement|parked|… (+1)"
        )

        with mock.patch.object(
            state_manager.beamline_mode,
            "get",
            return_value={"status": "valid", "label": "alignment|measurement|parked|test"},
        ):
            state_manager.show_all()

        output = capsys.readouterr().out
        assert "beamline_mode" in output
        assert "4 labels" in output
        assert "requirements" in output
        assert "transition_metadata" not in output
        assert "velocity" not in output

    def test_state_client_describe_prints_nested_parameters(self, state_manager, capsys):
        state_manager._add_state(self._aggregated_display_config())

        state_manager.beamline_mode.describe()

        output = capsys.readouterr().out
        for expected in (
            "beamline_mode",
            "AggregatedState",
            "evaluation_method",
            "'all'",
            "states",
            "alignment",
            "devices",
            "samx",
            "signals",
            "velocity",
            "transition_metadata",
            "Prepare alignment",
        ):
            assert expected in output


class TestStateMachine:

    @pytest.fixture()
    def state_machine(self, state_manager):
        state_machine = BeamlineStateMachine(manager=state_manager)
        return state_machine

    @pytest.fixture()
    def config_dict(self):
        return {
            "alignment": {
                "bl_state_class": "AggregatedState",
                "config": {
                    "evaluation_method": "any",
                    "states": {
                        "alignment": {
                            "devices": {
                                "samx": {
                                    "value": 0,
                                    "abs_tol": 0.1,
                                    "signals": {"velocity": {"value": 5, "abs_tol": 0.1}},
                                }
                            }
                        }
                    },
                },
            }
        }

    @pytest.mark.timeout(30)
    def test_load_from_config_with_dict(
        self, state_machine: BeamlineStateMachine, tmp_path, config_dict
    ):
        """Test loading configuration from a dictionary or file."""

        # Load valid configuration from dictionary
        with mock.patch.object(state_machine._manager, "add") as manager_add:
            state_machine.load_from_config(config_path=None, config_dict=config_dict)
            manager_add.assert_called_once_with(
                bl_states.AggregatedStateConfig(
                    name="alignment", states=config_dict["alignment"]["config"]["states"]
                ),
                skip_existing=False,
            )
            # Loading with both config_path and config_dict should raise an error
            with pytest.raises(ValueError):
                state_machine.load_from_config(
                    config_path="path/to/config.yaml", config_dict=config_dict
                )
            # Loading with neither config_path nor config_dict should raise an error
            with pytest.raises(ValueError):
                state_machine.load_from_config(config_path=None, config_dict=None)

            # Loading from file should work.
            config_path = tmp_path / "config.yaml"
            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(config_dict, f)
            state_machine.load_from_config(config_path=str(config_path))
            manager_add.assert_called_with(
                bl_states.AggregatedStateConfig(
                    name="alignment", states=config_dict["alignment"]["config"]["states"]
                ),
                skip_existing=False,
            )

    def test_load_from_config_rejects_invalid_evaluation_method(self, state_machine, config_dict):
        config_dict["alignment"]["config"]["evaluation_method"] = "invalid"

        with (
            mock.patch.object(state_machine._manager, "clear_all") as clear_all,
            pytest.raises(ValueError, match="evaluation_method"),
        ):
            state_machine.load_from_config(config_dict=config_dict)

        clear_all.assert_not_called()

    def test_load_from_yaml_accepts_null_evaluation_method(
        self, state_machine, config_dict, tmp_path
    ):
        config_dict["alignment"]["config"]["evaluation_method"] = None
        config_path = tmp_path / "state_config.yaml"
        with open(config_path, "w", encoding="utf-8") as stream:
            yaml.safe_dump(config_dict, stream)

        with mock.patch.object(state_machine._manager, "add") as manager_add:
            state_machine.load_from_config(config_path=str(config_path))

        loaded_config = manager_add.call_args.args[0]
        assert loaded_config.evaluation_method is None

    def test_load_from_yaml_preserves_scalar_aggregated_limits(
        self, state_machine, config_dict, tmp_path
    ):
        samx_config = config_dict["alignment"]["config"]["states"]["alignment"]["devices"]["samx"]
        samx_config.update({"low_limit": -10, "high_limit": 10})
        config_path = tmp_path / "state_config.yaml"
        with open(config_path, "w", encoding="utf-8") as stream:
            yaml.safe_dump(config_dict, stream)

        with mock.patch.object(state_machine._manager, "add") as manager_add:
            state_machine.load_from_config(config_path=str(config_path))

        loaded_config = manager_add.call_args.args[0]
        loaded_samx = loaded_config.states["alignment"].devices["samx"]
        assert isinstance(loaded_samx, bl_states.DeviceConfig)
        assert loaded_samx.low_limit == bl_states.SignalConfig(value=-10)
        assert loaded_samx.high_limit == bl_states.SignalConfig(value=10)
        assert loaded_samx.signals == {"velocity": bl_states.SignalConfig(value=5, abs_tol=0.1)}

    def test_load_from_config_forwards_skip_existing(
        self, state_machine: BeamlineStateMachine, config_dict
    ):
        """Test that skip_existing is forwarded to the state manager."""
        with mock.patch.object(state_machine._manager, "add") as manager_add:
            state_machine.load_from_config(config_dict=config_dict, flush=False, skip_existing=True)

        manager_add.assert_called_once_with(
            bl_states.AggregatedStateConfig(
                name="alignment", states=config_dict["alignment"]["config"]["states"]
            ),
            skip_existing=True,
        )
