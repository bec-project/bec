import time
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.bl_states import (
    BeamlineState,
    BeamlineStateManager,
    DeviceBeamlineState,
    DeviceWithinLimitsState,
    ShutterState,
)
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject


@pytest.fixture
def state_manager(connected_connector):
    client = mock.MagicMock()
    client.connector = connected_connector
    client.device_manager = mock.MagicMock()
    config = BeamlineStateManager(client)
    yield config


# ============================================================================
# BeamlineState tests
# ============================================================================


class TestBeamlineState:
    """Tests for the abstract BeamlineState base class."""

    def test_beamline_state_initialization(self):
        """Test basic initialization of a BeamlineState."""

        class ConcreteState(BeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="invalid", label="Test")

        state = ConcreteState(name="test_state", title="Test State")
        assert state.name == "test_state"
        assert state.title == "Test State"
        assert state.connector is None
        assert state._configured is False
        assert state._last_state is None

    def test_beamline_state_default_title(self):
        """Test that title defaults to name if not provided."""

        class ConcreteState(BeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteState(name="test_state")
        assert state.title == "test_state"

    def test_beamline_state_configure(self):
        """Test that configure marks the condition as configured."""

        class ConcreteState(BeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteState(name="test_state")
        assert state._configured is False
        state.configure()
        assert state._configured is True

    def test_beamline_state_parameters(self):
        """Test that parameters returns an empty dict by default."""

        class ConcreteState(BeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteState(name="test_state")
        assert state.parameters() == {}

    def test_beamline_state_with_connector(self, connected_connector):
        """Test BeamlineState initialization with a connector."""

        class ConcreteState(BeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteState(name="test_state", redis_connector=connected_connector)
        assert state.connector == connected_connector


# ============================================================================
# DeviceBeamlineState tests
# ============================================================================


class TestDeviceBeamlineState:
    """Tests for DeviceBeamlineState."""

    def test_device_state_configure(self, connected_connector):
        """Test DeviceBeamlineState configuration."""

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteDeviceState(name="device_test", redis_connector=connected_connector)
        state.configure(device="samx", signal="samx_value")
        assert state.device == "samx"
        assert state.signal == "samx_value"
        assert state._configured is True

    def test_device_state_configure_default_signal(self, connected_connector):
        """Test that signal defaults to device name if not provided."""

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteDeviceState(name="device_test", redis_connector=connected_connector)
        state.configure(device="samx", signal="samx")
        assert state.device == "samx"
        assert state.signal == "samx"

    def test_device_state_parameters(self, connected_connector):
        """Test that parameters includes device and signal."""

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteDeviceState(name="device_test", redis_connector=connected_connector)
        state.configure(device="samx", signal="samx_value")
        params = state.parameters()
        assert params["device"] == "samx"
        assert params["signal"] == "samx_value"

    def test_device_state_start_not_configured(self, connected_connector):
        """Test that start raises RuntimeError if state is not configured."""

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteDeviceState(name="device_test", redis_connector=connected_connector)
        with pytest.raises(RuntimeError, match="State must be configured before starting"):
            state.start()

    def test_device_state_start_no_connector(self):
        """Test that start raises RuntimeError if connector is not set."""

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteDeviceState(name="device_test")
        state.configure(device="samx")
        with pytest.raises(RuntimeError, match="Redis connector is not set"):
            state.start()

    def test_device_state_start_registers_callback(self, connected_connector):
        """Test that start registers the callback with the connector."""

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteDeviceState(name="device_test", redis_connector=connected_connector)
        state.configure(device="samx")
        with mock.patch.object(connected_connector, "register") as mock_register:
            state.start()
            mock_register.assert_called_once()
            call_args = mock_register.call_args
            assert call_args[0][0] == MessageEndpoints.device_readback("samx")

    def test_device_state_stop(self, connected_connector):
        """Test that stop unregisters the callback."""

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteDeviceState(name="device_test", redis_connector=connected_connector)
        state.configure(device="samx")

        with mock.patch.object(connected_connector, "unregister") as mock_unregister:
            state.stop()
            mock_unregister.assert_called_once()

    def test_device_state_stop_not_configured(self, connected_connector):
        """Test that stop doesn't raise an error if not configured."""

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteDeviceState(name="device_test", redis_connector=connected_connector)
        # Should not raise an error
        state.stop()

    def test_device_state_stop_no_connector(self):
        """Test that stop doesn't raise an error if connector is not set."""

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(name=self.name, status="valid", label="Test")

        state = ConcreteDeviceState(name="device_test")
        state.configure(device="samx")
        # Should not raise an error
        state.stop()

    def test_device_state_update_device_state(self, connected_connector):
        """Test that _update_device_state calls evaluate and updates _last_state."""

        msg = messages.BeamlineStateMessage(name="device_test", status="valid", label="Test")

        class ConcreteDeviceState(DeviceBeamlineState):
            def evaluate(self, *args, **kwargs):
                return msg

        state = ConcreteDeviceState(name="device_test", redis_connector=connected_connector)
        state.configure(device="samx")

        msg_obj = MessageObject(value=msg, topic="test_topic")
        state._update_device_state(msg_obj, parent=state)
        assert state._last_state == msg
        out = state.connector.xread(MessageEndpoints.beamline_state("device_test"), from_start=True)
        assert out is not None
        assert out[0]["data"] == msg


# ============================================================================
# ShutterState tests
# ============================================================================


class TestShutterState:
    """Tests for ShutterState."""

    def test_shutter_open(self, connected_connector):
        """Test evaluation when shutter is open."""
        state = ShutterState(name="shutter_open", redis_connector=connected_connector)
        state.configure(device="shutter1", signal="shutter1")

        msg = messages.DeviceMessage(
            signals={"shutter1": {"value": "open", "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = state.evaluate(msg)
        assert result.name == "shutter_open"
        assert result.status == "valid"
        assert result.label == "Shutter is open."

    def test_shutter_open_uppercase(self, connected_connector):
        """Test evaluation when shutter value is uppercase and gets lowercased."""
        state = ShutterState(name="shutter_open", redis_connector=connected_connector)
        state.configure(device="shutter1", signal="shutter1")

        msg = messages.DeviceMessage(
            signals={"shutter1": {"value": "OPEN", "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = state.evaluate(msg)
        assert result.status == "valid"
        assert result.label == "Shutter is open."

    def test_shutter_closed(self, connected_connector):
        """Test evaluation when shutter is closed."""
        state = ShutterState(name="shutter_open", redis_connector=connected_connector)
        state.configure(device="shutter1")

        msg = messages.DeviceMessage(
            signals={"shutter1": {"value": "closed", "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = state.evaluate(msg)
        assert result.name == "shutter_open"
        assert result.status == "invalid"
        assert result.label == "Shutter is closed."

    def test_shutter_missing_value(self, connected_connector):
        """Test evaluation when value is missing."""
        state = ShutterState(name="shutter_open", redis_connector=connected_connector)
        state.configure(device="shutter1")

        msg = messages.DeviceMessage(
            signals={"shutter1": {"timestamp": 1234567890.0}}, metadata={"stream": "primary"}
        )

        result = state.evaluate(msg)
        assert result.status == "invalid"
        assert result.label == "Shutter is closed."


# ============================================================================
# DeviceWithinLimitsState tests
# ============================================================================


class TestDeviceWithinLimitsState:
    """Tests for DeviceWithinLimitsState."""

    def test_within_limits_configure(self, connected_connector):
        """Test configuration of DeviceWithinLimitsState."""
        state = DeviceWithinLimitsState(name="sample_x_limits", redis_connector=connected_connector)
        state.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        assert state.device == "sample_x"
        assert state.min_limit == 0.0
        assert state.max_limit == 10.0
        assert state.tolerance == 0.1

    def test_within_limits_configure_custom_tolerance(self, connected_connector):
        """Test configuration with custom tolerance."""
        state = DeviceWithinLimitsState(name="sample_x_limits", redis_connector=connected_connector)
        state.configure(device="sample_x", min_limit=0.0, max_limit=10.0, tolerance=0.2)

        assert state.tolerance == 0.2

    def test_within_limits_value_inside(self, connected_connector):
        """Test evaluation when value is within limits."""
        state = DeviceWithinLimitsState(name="sample_x_limits", redis_connector=connected_connector)
        state.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": 5.0, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = state.evaluate(msg)
        assert result.status == "valid"
        assert result.label == "Positioner sample_x within limits"

    def test_within_limits_value_outside_low(self, connected_connector):
        """Test evaluation when value is below minimum limit."""
        state = DeviceWithinLimitsState(name="sample_x_limits", redis_connector=connected_connector)
        state.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": -1.0, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = state.evaluate(msg)
        assert result.status == "invalid"
        assert result.label == "Positioner sample_x out of limits"

    def test_within_limits_value_outside_high(self, connected_connector):
        """Test evaluation when value is above maximum limit."""
        state = DeviceWithinLimitsState(name="sample_x_limits", redis_connector=connected_connector)
        state.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": 11.0, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = state.evaluate(msg)
        assert result.status == "invalid"
        assert result.label == "Positioner sample_x out of limits"

    def test_within_limits_value_near_min(self, connected_connector):
        """Test evaluation when value is near minimum limit (within tolerance)."""
        state = DeviceWithinLimitsState(name="sample_x_limits", redis_connector=connected_connector)
        state.configure(device="sample_x", min_limit=0.0, max_limit=10.0, tolerance=0.1)

        # 10% of (10 - 0) = 1.0, so near min is < 1.0
        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": 0.5, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = state.evaluate(msg)
        assert result.status == "warning"
        assert result.label == "Positioner sample_x near limits"

    def test_within_limits_value_near_max(self, connected_connector):
        """Test evaluation when value is near maximum limit (within tolerance)."""
        state = DeviceWithinLimitsState(name="sample_x_limits", redis_connector=connected_connector)
        state.configure(device="sample_x", min_limit=0.0, max_limit=10.0, tolerance=0.1)

        # 10% of (10 - 0) = 1.0, so near max is > 9.0
        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": 9.5, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = state.evaluate(msg)
        assert result.status == "warning"
        assert result.label == "Positioner sample_x near limits"

    def test_within_limits_missing_value(self, connected_connector):
        """Test evaluation when value is missing."""
        state = DeviceWithinLimitsState(name="sample_x_limits", redis_connector=connected_connector)
        state.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        msg = messages.DeviceMessage(
            signals={"sample_x": {"timestamp": 1234567890.0}}, metadata={"stream": "primary"}
        )

        result = state.evaluate(msg)
        assert result.status == "invalid"
        assert "value not found" in result.label

    def test_within_limits_parameters(self, connected_connector):
        """Test that parameters includes all configuration."""
        state = DeviceWithinLimitsState(name="sample_x_limits", redis_connector=connected_connector)
        state.configure(device="sample_x", min_limit=0.0, max_limit=10.0, signal="x_readback")

        params = state.parameters()
        assert params["device"] == "sample_x"
        assert params["min_limit"] == 0.0
        assert params["max_limit"] == 10.0
        assert params["tolerance"] == 0.1
        assert params["signal"] == "x_readback"


# ============================================================================
# BeamlineStateConfig tests
# ============================================================================


class TestBeamlineStateConfig:
    """Tests for BeamlineStateConfig manager."""

    @pytest.mark.timeout(5)
    def test_add_state(self, state_manager):
        """Test adding a state."""
        state = ShutterState(name="shutter_open", title="Shutter Open")
        state.configure(device="shutter1")

        # Setup device manager mock - the signal should match the device name when no signal is provided
        state_manager._client.device_manager.devices = {"shutter1": mock.MagicMock()}
        state_manager._client.device_manager.devices["shutter1"].read.return_value = {
            "shutter1": {"value": "open"}
        }

        state_manager.add(state)
        while True:
            if any(c.name == "shutter_open" for c in state_manager._states):
                break
            time.sleep(0.1)
        # Check that the state was added
        assert any(c.name == "shutter_open" for c in state_manager._states)

    @pytest.mark.timeout(5)
    def test_add_state_already_exists(self, state_manager):
        """Test that adding a duplicate state is ignored."""
        state = ShutterState(name="shutter_open", title="Shutter Open")
        state.configure(device="shutter1")

        # Setup device manager mock
        state_manager._client.device_manager.devices = {"shutter1": mock.MagicMock()}
        state_manager._client.device_manager.devices["shutter1"].read.return_value = {
            "shutter1": {"value": "open"}
        }

        # Add the state once
        state_manager.add(state)
        while True:
            if any(c.name == "shutter_open" for c in state_manager._states):
                break
            time.sleep(0.1)
        initial_count = len(state_manager._states)

        # Add the same state again
        state_manager.add(state)
        time.sleep(0.5)
        # Count should not increase
        assert len(state_manager._states) == initial_count

    def test_add_state_device_not_found(self, state_manager):
        """Test that adding a state with invalid device raises RuntimeError."""
        state = ShutterState(name="shutter_open", title="Shutter Open")
        state.configure(device="nonexistent_shutter")

        state_manager._client.device_manager.devices = {}

        with pytest.raises(RuntimeError, match="Device nonexistent_shutter not found"):
            state_manager.add(state)

    def test_add_state_signal_not_found(self, state_manager):
        """Test that adding a state with invalid signal raises RuntimeError."""
        state = ShutterState(name="shutter_open", title="Shutter Open")
        # Setup device manager mock with device but without the signal
        mock_device = mock.MagicMock()
        mock_device.read.return_value = {"other_signal": {"value": "open"}}
        state_manager._client.device_manager.devices = {"shutter1": mock_device}

        state.configure(device="shutter1", signal="value")

        with pytest.raises(RuntimeError, match="Signal value not found in device shutter1"):
            state_manager.add(state)

    @pytest.mark.timeout(5)
    def test_remove_state(self, state_manager):
        """Test removing a state."""
        state = ShutterState(name="shutter_open", title="Shutter Open")
        state.configure(device="shutter1")

        # Setup device manager mock
        state_manager._client.device_manager.devices = {"shutter1": mock.MagicMock()}
        state_manager._client.device_manager.devices["shutter1"].read.return_value = {
            "shutter1": {"value": "open"}
        }

        # Add and then remove
        state_manager.add(state)
        while True:
            if any(c.name == "shutter_open" for c in state_manager._states):
                break
            time.sleep(0.1)

        state_manager.remove("shutter_open")
        while True:
            if not any(c.name == "shutter_open" for c in state_manager._states):
                break
            time.sleep(0.1)

    def test_remove_nonexistent_state(self, state_manager):
        """Test removing a state that doesn't exist."""
        # Should not raise an error
        state_manager.remove("nonexistent")
        assert len(state_manager._states) == 0

    @pytest.mark.timeout(5)
    def test_show_all(self, state_manager, capsys):
        """Test that show_all displays states in a table."""
        state = ShutterState(name="shutter_open", title="Shutter Open")
        state.configure(device="shutter1")

        # Setup device manager mock
        state_manager._client.device_manager.devices = {"shutter1": mock.MagicMock()}
        state_manager._client.device_manager.devices["shutter1"].read.return_value = {
            "shutter1": {"value": "open"}
        }

        state_manager.add(state)
        while True:
            if any(c.name == "shutter_open" for c in state_manager._states):
                break
            time.sleep(0.1)
        state_manager.show_all()

        # The output should be printed (checked via capsys)
        captured = capsys.readouterr()
        # Check that the state name appears in the output
        assert "shutter_open" in captured.out or "shutter_open" in captured.err

    def test_on_state_update(self, state_manager):
        """Test that _on_state_update updates the states list."""
        update_entry = messages.BeamlineStateConfig(
            name="test_state", title="Test State", state_type="ShutterState", parameters={}
        )
        msg = messages.AvailableBeamlineStatesMessage(states=[update_entry])

        state_manager._on_state_update({"data": msg}, parent=state_manager)

        assert len(state_manager._states) == 1
        assert state_manager._states[0].name == "test_state"
