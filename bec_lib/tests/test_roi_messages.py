"""ROI geometry validation and cross-service serialization compatibility."""

import pytest
from pydantic import BaseModel, ValidationError

from bec_lib import messages
from bec_lib.serialization import MsgpackSerialization, json_ext


def make_message(config):
    """Build a configuration message for a geometry payload."""
    return messages.ROIConfigurationMessage(
        device="camera",
        signal="roi",
        name="sample",
        active=True,
        roi_config=config,
        selected_operations=["sum"],
    )


@pytest.mark.parametrize("serializer", [MsgpackSerialization, json_ext])
@pytest.mark.parametrize(
    "config, model",
    [
        ({"type": "rect", "x": 1, "y": 2, "width": 3, "height": 4}, messages.RectROIConfig),
        (
            {"type": "rotated_rect", "x": 1.5, "y": 2, "width": 3, "height": 4, "angle": 30},
            messages.RotatedRectROIConfig,
        ),
        ({"type": "circle", "x": 1, "y": 2, "radius": 3}, messages.CircleROIConfig),
        (
            {"type": "ellipse", "x": 1, "y": 2, "width": 3, "height": 4, "angle": -45},
            messages.EllipseROIConfig,
        ),
        ({"type": "pixelmap", "mask": [[True, False], [False, True]]}, messages.PixelMapROIConfig),
        (
            {"type": "custom_anchor", "vertices": [[0, 0], [2, 0], [1, 3]]},
            messages.CustomAnchorROIConfig,
        ),
    ],
)
def test_roi_geometries_roundtrip(config, model, serializer):
    msg = make_message(config)
    assert isinstance(msg.roi_config, model)
    restored = serializer.loads(serializer.dumps(msg))
    assert restored == msg
    assert isinstance(restored.roi_config, model)
    assert restored.roi_config.model_dump() == msg.roi_config.model_dump()
    assert make_message(msg.roi_config).roi_config == msg.roi_config


def test_legacy_rectangle_payload_remains_supported():
    class LegacyROIConfig(BaseModel):
        """Rectangle schema used before the geometry discriminator was added."""

        x: int
        y: int
        width: int
        height: int

    config = {"x": 1, "y": 2, "width": 3, "height": 4}
    msg = make_message(config)
    assert isinstance(msg.roi_config, messages.RectROIConfig)
    assert msg.roi_config.type == "rect"
    assert config == {"x": 1, "y": 2, "width": 3, "height": 4}
    assert make_message(messages.RectROIConfig(**config)) == msg
    # Older readers ignore the newly added rectangle discriminator.
    assert LegacyROIConfig.model_validate(msg.roi_config.model_dump()).model_dump() == config


@pytest.mark.parametrize(
    "config",
    [
        {"type": "unknown", "x": 1, "y": 2, "width": 3, "height": 4},
        {"type": "rect", "x": 1, "y": 2, "width": 3, "height": 4, "angle": 30},
        {"type": "rotated_rect", "x": 1, "y": 2, "width": 3, "height": 4},
        {"type": "ellipse", "x": 1, "y": 2, "width": 0, "height": 4, "angle": 0},
        {"type": "circle", "x": 1, "y": 2, "radius": -1},
        {"type": "circle", "x": float("nan"), "y": 2, "radius": 1},
        {"type": "pixelmap", "mask": []},
        {"type": "pixelmap", "mask": [[]]},
        {"type": "pixelmap", "mask": [[True], [True, False]]},
        {"type": "pixelmap", "mask": [[2]]},
        {"type": "custom_anchor", "vertices": [[0, 0], [1, 1]]},
        {"type": "custom_anchor", "vertices": [[0, 0], [1, 1], [0, 0]]},
        {"type": "custom_anchor", "vertices": [[0, 0, 1], [1, 1], [1, 0]]},
        {"type": "custom_anchor", "vertices": [[0, 0], [1, 1], [float("inf"), 0]]},
    ],
)
def test_invalid_roi_geometry_is_rejected(config):
    with pytest.raises(ValidationError):
        make_message(config)
