from bec_lib.client import BECClient
from bec_lib.endpoints import EndpointInfo, MessageOp
from bec_lib.messages import RawMessage

from bec_server.actors.actor import PollingActor, SubscriptionActor


def _test_condition(client: BECClient):
    return True


def _test_action(client: BECClient):
    client.connector.set_and_publish(ep, RawMessage(data={"test": "result"}))


ep = EndpointInfo(
    endpoint="test_endpoint", message_type=RawMessage, message_op=MessageOp.SET_PUBLISH
)


class PollingTestActor(PollingActor):
    action_table = {_test_condition: _test_action}


sub_ep = EndpointInfo(
    endpoint="test_subscription_actor_endpoint",
    message_type=RawMessage,
    message_op=MessageOp.SET_PUBLISH,
)


class SubscriptionTestActor(SubscriptionActor):
    action_table = {_test_condition: _test_action}

    def default_monitor_endpoints(self) -> set[EndpointInfo]:
        return {sub_ep}
