import fakeredis
import pytest

from bec_lib import messages
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector

# overwrite threads_check fixture from bec_lib,
# to have it in autouse


def fake_redis_server(host, port, **kwargs):
    redis = fakeredis.FakeRedis()
    return redis


@pytest.fixture
def connected_connector():
    connector = RedisConnector("localhost:1", redis_cls=fake_redis_server)
    connector._managed_connection.flushall()
    try:
        yield connector
    finally:
        connector.shutdown()


@pytest.fixture(autouse=True)
def threads_check(threads_check):
    yield
    bec_logger.logger.remove()


@pytest.fixture
def deployment_info_factory():
    def _make(
        *,
        title: str = "Experiment Title",
        firstname: str = "John",
        lastname: str = "Doe",
        realm_id: str = "TestRealm",
        proposal: str = "20250001",
        account: str = "doe_j",
        pgroup: str = "p12345",
    ) -> messages.DeploymentInfoMessage:
        return messages.DeploymentInfoMessage(
            deployment_id="test-deployment-id",
            metadata={},
            name="Demo Deployment 1",
            active_session=messages.SessionInfoMessage(
                metadata={},
                name="_default_",
                experiment=messages.ExperimentInfoMessage(
                    metadata={},
                    realm_id=realm_id,
                    proposal=proposal,
                    title=title,
                    firstname=firstname,
                    lastname=lastname,
                    email="john.doe@example.com",
                    account=account,
                    pi_firstname="John",
                    pi_lastname="Doe",
                    pi_email="john.doe@example.com",
                    pi_account=account,
                    eaccount="e12345",
                    pgroup=pgroup,
                    abstract="",
                    schedule=[{"start": "01/01/2025 08:00:00", "end": "05/01/2025 18:00:00"}],
                    proposal_submitted="15/12/2024",
                    proposal_expire="31/12/2025",
                    proposal_status="Accepted",
                    delta_last_schedule=30,
                    mainproposal="",
                ),
                messaging_services=[],
            ),
            messaging_config=messages.MessagingConfig(
                scilog=messages.MessagingServiceScopeConfig(enabled=True, default=None),
                signal=messages.MessagingServiceScopeConfig(enabled=True, default=None),
                teams=messages.MessagingServiceScopeConfig(enabled=False, default=None),
            ),
            messaging_services=[],
        )

    return _make
