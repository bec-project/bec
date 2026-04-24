from __future__ import annotations

import inspect
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
        config = bl_states.BeamlineStateConfig(name="shutter_open", title="Shutter")
        assert config.name == "shutter_open"

    @pytest.mark.parametrize("invalid_name", ["state-name", "class", "add", "remove", "show_all"])
    def test_beamline_state_config_invalid_name(self, invalid_name):
        with pytest.raises(ValueError):
            bl_states.BeamlineStateConfig(name=invalid_name)

    def test_device_state_config_keeps_string_device_and_signal(self):
        config = bl_states.DeviceStateConfig(name="state", device="samx", signal="samx")
        assert config.device == "samx"
        assert config.signal == "samx"


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

        state.update_parameters(title="Test State")
        assert state.config.title == "Test State"


class TestDeviceBeamlineState:
    def test_start_requires_connector(self, dm_with_devices):
        state = bl_states.ShutterState(
            name="shutter_open", device="samy", signal="samy", device_manager=dm_with_devices
        )

        with pytest.raises(RuntimeError, match="Redis connector is not set"):
            state.start()

    def test_start_registers_device_callback(self, connected_connector, dm_with_devices):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="samx",
            signal="samx",
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        with mock.patch.object(connected_connector, "register") as register:
            state.start()

        register.assert_called_once_with(
            MessageEndpoints.device_readback("samx"), cb=state._update_device_state
        )

    def test_stop_unregisters_device_callback(self, connected_connector, dm_with_devices):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="samx",
            signal="samx",
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
        state = bl_states.ShutterState(
            name="shutter_open",
            device="samx",
            signal="samx",
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        msg = messages.DeviceMessage(
            signals={"samx": {"value": "open", "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        msg_obj = MessageObject(value=msg, topic="test")

        state._update_device_state(MessageObject(value=msg, topic="test"))

        assert state._last_state is not None
        assert state._last_state.status == "valid"
        out = connected_connector.xread(
            MessageEndpoints.beamline_state("shutter_open"), from_start=True
        )
        assert out is not None
        assert out[0]["data"].status == "valid"


class TestConcreteStates:
    def test_shutter_state_open_and_closed(self, connected_connector, dm_with_devices):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="samx",
            signal="samx",
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        open_msg = messages.DeviceMessage(
            signals={"samx": {"value": "OPEN", "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        closed_msg = messages.DeviceMessage(
            signals={"samx": {"value": "closed", "timestamp": 2.0}}, metadata={"stream": "primary"}
        )

        assert state.evaluate(open_msg).status == "valid"
        assert state.evaluate(closed_msg).status == "invalid"

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

    @pytest.fixture(scope="function")
    def aggregated_state_config(self):
        """Fixture for an test aggregated state configuration."""
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
            },
        )

    def test_aggregated_state_init_and_start(
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
            evaluate.assert_called_once_with(affected_labels=set(["alignment", "measurement"]))
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
        # The order of the labels is not guaranteed
        assert msg.label in ["alignment|test", "test|alignment"]
        assert set(state._current_labels) == set(["alignment", "test"])

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

        evaluate.assert_called_once_with(affected_labels={"alignment", "measurement"})
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
        assert set(msg.label.split("|")) == {"alignment", "test"}

        msg = update("samx", "readback", {"samx": {"value": 19, "timestamp": 2.0}})
        assert msg.status == "valid"
        assert msg.label == "test"

        msg = update("bpm4i", "readback", {"bpm4i": {"value": 2, "timestamp": 2.0}})
        assert msg.status == "valid"
        assert msg.label == "measurement"

    @pytest.mark.parametrize(
        ("cached_value", "expected_value", "abs_tolerance", "matches"),
        [
            (1.05, 1.0, 0.1, True),
            (1.2, 1.0, 0.1, False),
            (5, 5, 0.0, True),
            (np.int64(5), 5, 0.0, True),
            (np.float64(1.05), 1.0, 0.1, True),
            ("ok", "ok", 0.0, True),
            ("not-ok", "ok", 0.0, False),
            ([1, 2], 1, 0.0, False),
            (np.array([1.0, 2.0]), 1.0, 0.1, False),
            (np.array([1.0, 2.0]), np.array([1.0, 2.0]), 0.0, False),
        ],
    )
    def test_aggregated_state_requirement_matches(
        self,
        connected_connector,
        dm_with_devices,
        aggregated_state_config,
        cached_value,
        expected_value,
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
            abs_tolerance=abs_tolerance,
            source="readback",
        )
        state._signal_value_cache[("bpm4i", "readback", "bpm4i")] = cached_value

        assert state._requirement_matches(requirement) is matches


class TestBeamlineStateManager:
    def test_manager_registers_for_state_updates(self, connected_connector):
        client = mock.MagicMock()
        client.connector = connected_connector

        with mock.patch.object(connected_connector, "register") as register:
            BeamlineStateManager(client)

        register.assert_called_once_with(
            MessageEndpoints.available_beamline_states(), cb=mock.ANY, from_start=True
        )

    def test_on_state_update_creates_client_attribute(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            title="Shutter Open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "title": "Shutter Open", "device": "samy"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])

        state_manager._on_state_update({"data": update}, parent=state_manager)

        assert "shutter_open" in state_manager._states
        assert isinstance(state_manager._states["shutter_open"], bl_states.DeviceStateConfig)
        assert isinstance(getattr(state_manager, "shutter_open"), BeamlineStateClientBase)

    def test_update_parameters_from_client_updates_state_and_publishes(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="limits",
            title="Limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "limits",
                "title": "Limits",
                "device": "samx",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
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

    def test_client_get_returns_unknown_without_status_message(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            title="Shutter Open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "title": "Shutter Open", "device": "samy"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        result = state_manager.shutter_open.get()
        assert result == {"status": "unknown", "label": "No state information available."}

    def test_client_get_returns_latest_status_message(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            title="Shutter Open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "title": "Shutter Open", "device": "samy"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager._connector.xadd(
            MessageEndpoints.beamline_state("shutter_open"),
            {
                "data": messages.BeamlineStateMessage(
                    name="shutter_open", status="valid", label="ok"
                )
            },
            max_size=1,
        )

        result = state_manager.shutter_open.get()
        assert result == {"status": "valid", "label": "ok"}

    def test_add_and_delete_publish_updates(self, state_manager):
        state = bl_states.DeviceStateConfig(
            name="shutter_open", title="Shutter Open", device="samy"
        )

        state_manager.add(state)
        assert "shutter_open" in state_manager._states

        state_manager.delete("shutter_open")
        assert "shutter_open" not in state_manager._states

    def test_client_remove_state(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            title="Shutter Open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "title": "Shutter Open", "device": "samy"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager.shutter_open.remove()

        assert "shutter_open" not in state_manager._states

    def test_show_all_prints_table(self, state_manager, capsys):
        state = bl_states.DeviceStateConfig(
            name="shutter_open", title="Shutter Open", device="samy"
        )
        state_manager.add(state)

        state_manager.show_all()

        captured = capsys.readouterr()
        assert "shutter_open" in (captured.out + captured.err)


class TestStateMachine:

    @pytest.fixture()
    def state_machine(self, state_manager):
        state_machine = BeamlineStateMachine(manager=state_manager)
        return state_machine

    @pytest.fixture()
    def config_dict(self):
        return {
            "alignment": {
                "devices": {
                    "samx": {
                        "value": 0,
                        "abs_tol": 0.1,
                        "signals": {"velocity": {"value": 5, "abs_tol": 0.1}},
                    }
                }
            }
        }

    def test_load_from_config_with_dict(
        self, state_machine: BeamlineStateMachine, tmp_path, config_dict
    ):
        """Test loading configuration from a dictionary or file."""

        # Load valid configuration from dictionary
        with mock.patch.object(state_machine._manager, "add") as manager_add:
            state_machine.load_from_config(
                name="alignment", config_path=None, config_dict=config_dict
            )
            manager_add.assert_called_once_with(
                bl_states.AggregatedStateConfig(name="alignment", states=config_dict)
            )
            # Loading with both config_path and config_dict should raise an error
            with pytest.raises(ValueError):
                state_machine.load_from_config(
                    name="alignment", config_path="path/to/config.yaml", config_dict=config_dict
                )
            # Loading with neither config_path nor config_dict should raise an error
            with pytest.raises(ValueError):
                state_machine.load_from_config(name="alignment", config_path=None, config_dict=None)

            # Loading from file should work.
            config_path = tmp_path / "config.yaml"
            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(config_dict, f)
            state_machine.load_from_config(name="alignment", config_path=str(config_path))
            manager_add.assert_called_with(
                bl_states.AggregatedStateConfig(name="alignment", states=config_dict)
            )

    def test_update_config(self, state_machine: BeamlineStateMachine, config_dict, tmp_path):
        """Test update method of state machine."""
        with mock.patch.object(state_machine._manager, "_update_state") as manager_update:
            config = bl_states.AggregatedStateConfig(name="alignment", states=config_dict)
            state_machine.update_config(name="alignment", config_dict=config_dict)
            manager_update.assert_called_once_with(config)

            manager_update.reset_mock()

            # Invalid updates should raise an error
            with pytest.raises(ValueError):
                state_machine.update_config(name="alignment", config_dict=None)
                manager_update.assert_not_called()

            with pytest.raises(ValueError):
                state_machine.update_config(
                    name="alignment", config_path="path/to/config.yaml", config_dict=config_dict
                )
                manager_update.assert_not_called()
            manager_update.reset_mock()
            # Updating from file should work.
            config_path = tmp_path / "config.yaml"
            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(config_dict, f)
            state_machine.update_config(name="alignment", config_path=str(config_path))
            manager_update.assert_called_once_with(config)
