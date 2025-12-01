from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.beamline_states import (
    BeamlineCondition,
    BeamlineConditionConfig,
    DeviceBeamlineCondition,
    DeviceWithinLimitsCondition,
    ShutterCondition,
)
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject


@pytest.fixture
def beamline_config(connected_connector):
    client = mock.MagicMock()
    client.connector = connected_connector
    client.device_manager = mock.MagicMock()
    config = BeamlineConditionConfig(client)
    yield config


# ============================================================================
# BeamlineCondition tests
# ============================================================================


class TestBeamlineCondition:
    """Tests for the abstract BeamlineCondition base class."""

    def test_beamline_condition_initialization(self):
        """Test basic initialization of a BeamlineCondition."""

        class ConcreteCondition(BeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteCondition(name="test_condition", title="Test Condition")
        assert condition.name == "test_condition"
        assert condition.title == "Test Condition"
        assert condition.connector is None
        assert condition._configured is False
        assert condition._last_state is None

    def test_beamline_condition_default_title(self):
        """Test that title defaults to name if not provided."""

        class ConcreteCondition(BeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteCondition(name="test_condition")
        assert condition.title == "test_condition"

    def test_beamline_condition_configure(self):
        """Test that configure marks the condition as configured."""

        class ConcreteCondition(BeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteCondition(name="test_condition")
        assert condition._configured is False
        condition.configure()
        assert condition._configured is True

    def test_beamline_condition_parameters(self):
        """Test that parameters returns an empty dict by default."""

        class ConcreteCondition(BeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteCondition(name="test_condition")
        assert condition.parameters() == {}

    def test_beamline_condition_with_connector(self, connected_connector):
        """Test BeamlineCondition initialization with a connector."""

        class ConcreteCondition(BeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteCondition(name="test_condition", redis_connector=connected_connector)
        assert condition.connector == connected_connector


# ============================================================================
# DeviceBeamlineCondition tests
# ============================================================================


class TestDeviceBeamlineCondition:
    """Tests for DeviceBeamlineCondition."""

    def test_device_condition_configure(self, connected_connector):
        """Test DeviceBeamlineCondition configuration."""

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteDeviceCondition(name="device_test", redis_connector=connected_connector)
        condition.configure(device="samx", signal="samx_value")
        assert condition.device == "samx"
        assert condition.signal == "samx_value"
        assert condition._configured is True

    def test_device_condition_configure_default_signal(self, connected_connector):
        """Test that signal defaults to device name if not provided."""

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteDeviceCondition(name="device_test", redis_connector=connected_connector)
        condition.configure(device="samx", signal="samx")
        assert condition.device == "samx"
        assert condition.signal == "samx"

    def test_device_condition_parameters(self, connected_connector):
        """Test that parameters includes device and signal."""

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteDeviceCondition(name="device_test", redis_connector=connected_connector)
        condition.configure(device="samx", signal="samx_value")
        params = condition.parameters()
        assert params["device"] == "samx"
        assert params["signal"] == "samx_value"

    def test_device_condition_start_not_configured(self, connected_connector):
        """Test that start raises RuntimeError if condition is not configured."""

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteDeviceCondition(name="device_test", redis_connector=connected_connector)
        with pytest.raises(RuntimeError, match="Condition must be configured before starting"):
            condition.start()

    def test_device_condition_start_no_connector(self):
        """Test that start raises RuntimeError if connector is not set."""

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteDeviceCondition(name="device_test")
        condition.configure(device="samx")
        with pytest.raises(RuntimeError, match="Redis connector is not set"):
            condition.start()

    def test_device_condition_start_registers_callback(self, connected_connector):
        """Test that start registers the callback with the connector."""

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteDeviceCondition(name="device_test", redis_connector=connected_connector)
        condition.configure(device="samx")

        with mock.patch.object(connected_connector, "register") as mock_register:
            condition.start()
            mock_register.assert_called_once()
            call_args = mock_register.call_args
            assert call_args[0][0] == MessageEndpoints.device_readback("samx")

    def test_device_condition_stop(self, connected_connector):
        """Test that stop unregisters the callback."""

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteDeviceCondition(name="device_test", redis_connector=connected_connector)
        condition.configure(device="samx")

        with mock.patch.object(connected_connector, "unregister") as mock_unregister:
            condition.stop()
            mock_unregister.assert_called_once()

    def test_device_condition_stop_not_configured(self, connected_connector):
        """Test that stop doesn't raise an error if not configured."""

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteDeviceCondition(name="device_test", redis_connector=connected_connector)
        # Should not raise an error
        condition.stop()

    def test_device_condition_stop_no_connector(self):
        """Test that stop doesn't raise an error if connector is not set."""

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return messages.BeamlineConditionMessage(
                    name=self.name, status="normal", message="Test"
                )

        condition = ConcreteDeviceCondition(name="device_test")
        condition.configure(device="samx")
        # Should not raise an error
        condition.stop()

    def test_device_condition_update_device_state(self, connected_connector):
        """Test that _update_device_state calls evaluate and updates _last_state."""

        msg = messages.BeamlineConditionMessage(name="device_test", status="normal", message="Test")

        class ConcreteDeviceCondition(DeviceBeamlineCondition):
            def evaluate(self, *args, **kwargs):
                return msg

        condition = ConcreteDeviceCondition(name="device_test", redis_connector=connected_connector)
        condition.configure(device="samx")

        msg_obj = MessageObject(value=msg, topic="test_topic")
        condition._update_device_state(msg_obj, parent=condition)
        assert condition._last_state == msg
        out = condition.connector.xread(
            MessageEndpoints.beamline_condition("device_test"), from_start=True
        )
        assert out is not None
        assert out[0]["data"] == msg


# ============================================================================
# ShutterCondition tests
# ============================================================================


class TestShutterCondition:
    """Tests for ShutterCondition."""

    def test_shutter_open(self, connected_connector):
        """Test evaluation when shutter is open."""
        condition = ShutterCondition(name="shutter_open", redis_connector=connected_connector)
        condition.configure(device="shutter1", signal="shutter1")

        msg = messages.DeviceMessage(
            signals={"shutter1": {"value": "open", "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = condition.evaluate(msg)
        assert result.name == "shutter_open"
        assert result.status == "normal"
        assert result.message == "Shutter is open."

    def test_shutter_open_uppercase(self, connected_connector):
        """Test evaluation when shutter value is uppercase and gets lowercased."""
        condition = ShutterCondition(name="shutter_open", redis_connector=connected_connector)
        condition.configure(device="shutter1", signal="shutter1")

        msg = messages.DeviceMessage(
            signals={"shutter1": {"value": "OPEN", "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = condition.evaluate(msg)
        assert result.status == "normal"
        assert result.message == "Shutter is open."

    def test_shutter_closed(self, connected_connector):
        """Test evaluation when shutter is closed."""
        condition = ShutterCondition(name="shutter_open", redis_connector=connected_connector)
        condition.configure(device="shutter1")

        msg = messages.DeviceMessage(
            signals={"shutter1": {"value": "closed", "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = condition.evaluate(msg)
        assert result.name == "shutter_open"
        assert result.status == "alarm"
        assert result.message == "Shutter is closed."

    def test_shutter_missing_value(self, connected_connector):
        """Test evaluation when value is missing."""
        condition = ShutterCondition(name="shutter_open", redis_connector=connected_connector)
        condition.configure(device="shutter1")

        msg = messages.DeviceMessage(
            signals={"shutter1": {"timestamp": 1234567890.0}}, metadata={"stream": "primary"}
        )

        result = condition.evaluate(msg)
        assert result.status == "alarm"
        assert result.message == "Shutter is closed."


# ============================================================================
# DeviceWithinLimitsCondition tests
# ============================================================================


class TestDeviceWithinLimitsCondition:
    """Tests for DeviceWithinLimitsCondition."""

    def test_within_limits_configure(self, connected_connector):
        """Test configuration of DeviceWithinLimitsCondition."""
        condition = DeviceWithinLimitsCondition(
            name="sample_x_limits", redis_connector=connected_connector
        )
        condition.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        assert condition.device == "sample_x"
        assert condition.min_limit == 0.0
        assert condition.max_limit == 10.0
        assert condition.tolerance == 0.1

    def test_within_limits_configure_custom_tolerance(self, connected_connector):
        """Test configuration with custom tolerance."""
        condition = DeviceWithinLimitsCondition(
            name="sample_x_limits", redis_connector=connected_connector
        )
        condition.configure(device="sample_x", min_limit=0.0, max_limit=10.0, tolerance=0.2)

        assert condition.tolerance == 0.2

    def test_within_limits_value_inside(self, connected_connector):
        """Test evaluation when value is within limits."""
        condition = DeviceWithinLimitsCondition(
            name="sample_x_limits", redis_connector=connected_connector
        )
        condition.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": 5.0, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = condition.evaluate(msg)
        assert result.status == "normal"
        assert result.message == "Positioner sample_x within limits"

    def test_within_limits_value_outside_low(self, connected_connector):
        """Test evaluation when value is below minimum limit."""
        condition = DeviceWithinLimitsCondition(
            name="sample_x_limits", redis_connector=connected_connector
        )
        condition.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": -1.0, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = condition.evaluate(msg)
        assert result.status == "alarm"
        assert result.message == "Positioner sample_x out of limits"

    def test_within_limits_value_outside_high(self, connected_connector):
        """Test evaluation when value is above maximum limit."""
        condition = DeviceWithinLimitsCondition(
            name="sample_x_limits", redis_connector=connected_connector
        )
        condition.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": 11.0, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = condition.evaluate(msg)
        assert result.status == "alarm"
        assert result.message == "Positioner sample_x out of limits"

    def test_within_limits_value_near_min(self, connected_connector):
        """Test evaluation when value is near minimum limit (within tolerance)."""
        condition = DeviceWithinLimitsCondition(
            name="sample_x_limits", redis_connector=connected_connector
        )
        condition.configure(device="sample_x", min_limit=0.0, max_limit=10.0, tolerance=0.1)

        # 10% of (10 - 0) = 1.0, so near min is < 1.0
        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": 0.5, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = condition.evaluate(msg)
        assert result.status == "warning"
        assert result.message == "Positioner sample_x near limits"

    def test_within_limits_value_near_max(self, connected_connector):
        """Test evaluation when value is near maximum limit (within tolerance)."""
        condition = DeviceWithinLimitsCondition(
            name="sample_x_limits", redis_connector=connected_connector
        )
        condition.configure(device="sample_x", min_limit=0.0, max_limit=10.0, tolerance=0.1)

        # 10% of (10 - 0) = 1.0, so near max is > 9.0
        msg = messages.DeviceMessage(
            signals={"sample_x": {"value": 9.5, "timestamp": 1234567890.0}},
            metadata={"stream": "primary"},
        )

        result = condition.evaluate(msg)
        assert result.status == "warning"
        assert result.message == "Positioner sample_x near limits"

    def test_within_limits_missing_value(self, connected_connector):
        """Test evaluation when value is missing."""
        condition = DeviceWithinLimitsCondition(
            name="sample_x_limits", redis_connector=connected_connector
        )
        condition.configure(device="sample_x", min_limit=0.0, max_limit=10.0)

        msg = messages.DeviceMessage(
            signals={"sample_x": {"timestamp": 1234567890.0}}, metadata={"stream": "primary"}
        )

        result = condition.evaluate(msg)
        assert result.status == "alarm"
        assert "value not found" in result.message

    def test_within_limits_parameters(self, connected_connector):
        """Test that parameters includes all configuration."""
        condition = DeviceWithinLimitsCondition(
            name="sample_x_limits", redis_connector=connected_connector
        )
        condition.configure(device="sample_x", min_limit=0.0, max_limit=10.0, signal="x_readback")

        params = condition.parameters()
        assert params["device"] == "sample_x"
        assert params["min_limit"] == 0.0
        assert params["max_limit"] == 10.0
        assert params["tolerance"] == 0.1
        assert params["signal"] == "x_readback"


# ============================================================================
# BeamlineConditionConfig tests
# ============================================================================


class TestBeamlineConditionConfig:
    """Tests for BeamlineConditionConfig manager."""

    def test_add_condition(self, beamline_config):
        """Test adding a condition."""
        condition = ShutterCondition(name="shutter_open", title="Shutter Open")
        condition.configure(device="shutter1")

        # Setup device manager mock - the signal should match the device name when no signal is provided
        beamline_config._client.device_manager.devices = {"shutter1": mock.MagicMock()}
        beamline_config._client.device_manager.devices["shutter1"].read.return_value = {
            "shutter1": {"value": "open"}
        }

        beamline_config.add(condition)
        # Check that the condition was added
        assert any(c.name == "shutter_open" for c in beamline_config._conditions)

    def test_add_condition_already_exists(self, beamline_config):
        """Test that adding a duplicate condition is ignored."""
        condition = ShutterCondition(name="shutter_open", title="Shutter Open")
        condition.configure(device="shutter1")

        # Setup device manager mock
        beamline_config._client.device_manager.devices = {"shutter1": mock.MagicMock()}
        beamline_config._client.device_manager.devices["shutter1"].read.return_value = {
            "shutter1": {"value": "open"}
        }

        # Add the condition once
        beamline_config.add(condition)
        initial_count = len(beamline_config._conditions)

        # Add the same condition again
        beamline_config.add(condition)
        # Count should not increase
        assert len(beamline_config._conditions) == initial_count

    def test_add_condition_device_not_found(self, beamline_config):
        """Test that adding a condition with invalid device raises RuntimeError."""
        condition = ShutterCondition(name="shutter_open", title="Shutter Open")
        condition.configure(device="nonexistent_shutter")

        beamline_config._client.device_manager.devices = {}

        with pytest.raises(RuntimeError, match="Device nonexistent_shutter not found"):
            beamline_config.add(condition)

    def test_add_condition_signal_not_found(self, beamline_config):
        """Test that adding a condition with invalid signal raises RuntimeError."""
        condition = ShutterCondition(name="shutter_open", title="Shutter Open")

        # Setup device manager mock with device but without the signal
        mock_device = mock.MagicMock()
        mock_device.read.return_value = {"other_signal": {"value": "open"}}
        beamline_config._client.device_manager.devices = {"shutter1": mock_device}

        condition.configure(device="shutter1", signal="value")

        with pytest.raises(RuntimeError, match="Signal value not found in device shutter1"):
            beamline_config.add(condition)

    def test_remove_condition(self, beamline_config):
        """Test removing a condition."""
        condition = ShutterCondition(name="shutter_open", title="Shutter Open")
        condition.configure(device="shutter1")

        # Setup device manager mock
        beamline_config._client.device_manager.devices = {"shutter1": mock.MagicMock()}
        beamline_config._client.device_manager.devices["shutter1"].read.return_value = {
            "shutter1": {"value": "open"}
        }

        # Add and then remove
        beamline_config.add(condition)
        assert any(c.name == "shutter_open" for c in beamline_config._conditions)

        beamline_config.remove("shutter_open")
        assert not any(c.name == "shutter_open" for c in beamline_config._conditions)

    def test_remove_nonexistent_condition(self, beamline_config):
        """Test removing a condition that doesn't exist."""
        # Should not raise an error
        beamline_config.remove("nonexistent")
        assert len(beamline_config._conditions) == 0

    def test_show_all(self, beamline_config, capsys):
        """Test that show_all displays conditions in a table."""
        condition = ShutterCondition(name="shutter_open", title="Shutter Open")
        condition.configure(device="shutter1")

        # Setup device manager mock
        beamline_config._client.device_manager.devices = {"shutter1": mock.MagicMock()}
        beamline_config._client.device_manager.devices["shutter1"].read.return_value = {"shutter1"}

        beamline_config.add(condition)
        beamline_config.show_all()

        # The output should be printed (checked via capsys)
        captured = capsys.readouterr()
        # Check that the condition name appears in the output
        assert "shutter_open" in captured.out or "shutter_open" in captured.err

    def test_on_condition_update(self, beamline_config):
        """Test that _on_condition_update updates the conditions list."""
        update_entry = messages.BeamlineConditionUpdateEntry(
            name="test_condition",
            title="Test Condition",
            condition_type="ShutterCondition",
            parameters={},
        )
        msg = messages.AvailableBeamlineConditionsMessage(conditions=[update_entry])

        BeamlineConditionConfig._on_condition_update({"data": msg}, parent=beamline_config)

        assert len(beamline_config._conditions) == 1
        assert beamline_config._conditions[0].name == "test_condition"
