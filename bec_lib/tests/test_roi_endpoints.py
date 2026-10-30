"""ROI clients send requests; devices publish the current configuration."""

from bec_lib.endpoints import MessageEndpoints, MessageOp
from bec_lib.messages import ROIConfigurationMessage


def test_roi_request_and_current_configuration_are_separate():
    request = MessageEndpoints.roi_config_request("camera", "roi_processing2", "alice")
    current = MessageEndpoints.roi_config("camera", "roi_processing2")
    assert request.endpoint != current.endpoint
    assert request.endpoint == "personal/alice/devices/roi_config_request/camera/roi_processing2"
    assert (
        request.endpoint
        != MessageEndpoints.roi_config_request("camera", "roi_processing2", "bob").endpoint
    )
    assert request.message_type is ROIConfigurationMessage
    assert current.message_type is ROIConfigurationMessage
    assert request.message_op == MessageOp.SEND
    assert current.message_op == MessageOp.SET_PUBLISH


def test_roi_request_subscription_can_match_all_users():
    request = MessageEndpoints.roi_config_request("camera", "roi_processing2", "*")
    assert request.endpoint == "personal/*/devices/roi_config_request/camera/roi_processing2"
