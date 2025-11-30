from unittest import mock

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints


def create_dummy_deployment_info():
    """Create anonymized dummy deployment info for testing"""
    return messages.DeploymentInfoMessage(
        metadata={},
        name="Demo Deployment 1",
        active_session=messages.SessionInfoMessage(
            metadata={},
            name="_default_",
            experiment=messages.ExperimentInfoMessage(
                metadata={},
                realm_id="TestRealm",
                proposal="20250001",
                title="Test Experiment Title",
                firstname="John",
                lastname="Doe",
                email="john.doe@example.com",
                account="doe_j",
                pi_firstname="John",
                pi_lastname="Doe",
                pi_email="john.doe@example.com",
                pi_account="doe_j",
                eaccount="e12345",
                pgroup="p12345",
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
        messaging_services=[
            messages.MessagingServiceConfig(
                metadata={}, service_name="signal", scopes=["*"], enabled=True
            )
        ],
    )


def test_atlas_metadata_handler(atlas_connector):

    msg = messages.ScanStatusMessage(
        scan_id="adlk-jalskdjs",
        status="open",
        info={
            "scan_motors": ["samx"],
            "readout_priority": {"monitored": ["samx"], "baseline": [], "on_request": []},
            "queue_id": "my-queue-ID",
            "scan_number": 5,
            "scan_type": "step",
        },
    )
    msg_obj = MessageObject(topic="internal/scan/status", value=msg)
    with mock.patch.object(atlas_connector, "ingest_data") as mock_ingest_data:
        atlas_connector.metadata_handler._handle_scan_status(
            msg_obj, parent=atlas_connector.metadata_handler
        )
        mock_ingest_data.assert_called_once_with({"scan_status": msg})

    with mock.patch.object(
        atlas_connector.metadata_handler, "send_atlas_update", side_effect=ValueError
    ):
        atlas_connector.metadata_handler._handle_scan_status(
            msg_obj, parent=atlas_connector.metadata_handler
        )
        assert True


def test_handle_account_info_valid(atlas_connector):
    msg = {"data": messages.VariableMessage(value="account2")}
    with mock.patch.object(
        atlas_connector.metadata_handler, "send_atlas_update"
    ) as mock_send_update:
        atlas_connector.metadata_handler._handle_account_info(
            msg, parent=atlas_connector.metadata_handler
        )
        mock_send_update.assert_called_once()


def test_handle_account_info_invalid(atlas_connector):
    msg = {"invalid": "data"}
    with mock.patch("bec_lib.logger.bec_logger.logger.error") as mock_logger_error:
        atlas_connector.metadata_handler._handle_account_info(
            msg, parent=atlas_connector.metadata_handler
        )
        mock_logger_error.assert_called()


def test_handle_scan_history(atlas_connector):
    msg = {"data": {"history": "test"}}
    with mock.patch.object(
        atlas_connector.metadata_handler, "send_atlas_update"
    ) as mock_send_update:
        atlas_connector.metadata_handler._handle_scan_history(
            msg, parent=atlas_connector.metadata_handler
        )
        mock_send_update.assert_called_once_with({"scan_history": {"history": "test"}})


def test_send_atlas_update(atlas_connector):
    handler = atlas_connector.metadata_handler
    with mock.patch.object(handler.atlas_connector, "ingest_data") as mock_ingest_data:
        handler.send_atlas_update({"key": "value"})
        mock_ingest_data.assert_called_once_with({"key": "value"})


def test_update_deployment_info(atlas_connector):
    """Test that deployment info updates are correctly handled"""
    handler = atlas_connector.metadata_handler
    deployment_info = create_dummy_deployment_info()

    with (
        mock.patch.object(handler, "update_messaging_services") as mock_update_messaging,
        mock.patch.object(handler, "update_local_account") as mock_update_account,
    ):
        handler.update_deployment_info(deployment_info)

        # Verify that deployment info is stored in local redis
        stored_info = handler.atlas_connector.connector.get_last(MessageEndpoints.deployment_info())
        assert stored_info is not None
        assert stored_info["data"] == deployment_info

        # Verify that messaging services are updated
        mock_update_messaging.assert_called_once_with(deployment_info.messaging_services)

        # Verify that account is updated
        mock_update_account.assert_called_once_with(deployment_info)


def test_update_messaging_services(atlas_connector):
    """Test that messaging services are correctly updated"""
    handler = atlas_connector.metadata_handler
    services = [
        messages.MessagingServiceConfig(
            metadata={}, service_name="signal", scopes=["*"], enabled=True
        )
    ]

    handler.update_messaging_services(services)

    # Verify the services were stored in redis
    stored_services = handler.atlas_connector.connector.get_last(
        MessageEndpoints.available_messaging_services()
    )
    assert stored_services is not None
    assert isinstance(stored_services["data"], messages.AvailableResourceMessage)
    assert stored_services["data"].resource == services


def test_update_local_account_new_account(atlas_connector):
    """Test that local account is updated when it changes"""
    handler = atlas_connector.metadata_handler
    handler._account = "old_account"
    deployment_info = create_dummy_deployment_info()

    handler.update_local_account(deployment_info)

    # Verify that account update was sent
    stored_account = handler.atlas_connector.connector.get_last(MessageEndpoints.account())
    assert stored_account is not None
    assert isinstance(stored_account["data"], messages.VariableMessage)
    assert stored_account["data"].value == "p12345"


def test_update_local_account_same_account(atlas_connector):
    """Test that local account is not updated when it's the same"""
    handler = atlas_connector.metadata_handler
    handler._account = "p12345"
    deployment_info = create_dummy_deployment_info()

    # Store initial account
    handler.atlas_connector.connector.xadd(
        MessageEndpoints.account(), {"data": messages.VariableMessage(value="p12345")}, max_size=1
    )
    with mock.patch.object(handler.atlas_connector.connector, "xadd") as mock_xadd:
        handler.update_local_account(deployment_info)
        # Verify that no account update was sent
        mock_xadd.assert_not_called()


def test_update_local_account_no_session(atlas_connector):
    """Test that local account update handles missing session gracefully"""
    handler = atlas_connector.metadata_handler
    deployment_info = messages.DeploymentInfoMessage(
        metadata={}, name="Test Deployment", active_session=None, messaging_services=[]
    )

    with mock.patch.object(handler.atlas_connector.connector, "xadd") as mock_xadd:
        handler.update_local_account(deployment_info)
        # Verify that no account update was sent
        mock_xadd.assert_not_called()


def test_update_local_account_no_experiment(atlas_connector):
    """Test that local account update handles missing experiment gracefully"""
    handler = atlas_connector.metadata_handler
    deployment_info = messages.DeploymentInfoMessage(
        metadata={},
        name="Test Deployment",
        active_session=messages.SessionInfoMessage(
            metadata={}, name="_default_", experiment=None, messaging_services=[]
        ),
        messaging_services=[],
    )

    with mock.patch.object(handler.atlas_connector.connector, "xadd") as mock_xadd:
        handler.update_local_account(deployment_info)
        # Verify that no account update was sent
        mock_xadd.assert_not_called()


def test_handle_deployment_info_valid(atlas_connector):
    """Test handling of valid deployment info message"""
    deployment_info = create_dummy_deployment_info()
    msg = {"data": deployment_info}

    with mock.patch.object(
        atlas_connector.metadata_handler, "update_deployment_info"
    ) as mock_update:
        atlas_connector.metadata_handler._update_deployment_info(
            msg, parent=atlas_connector.metadata_handler
        )
        mock_update.assert_called_once_with(deployment_info)


def test_handle_deployment_info_invalid(atlas_connector):
    """Test handling of invalid deployment info message"""
    msg = {"invalid": "data"}

    with mock.patch("bec_lib.logger.bec_logger.logger.error") as mock_logger_error:
        atlas_connector.metadata_handler._update_deployment_info(
            msg, parent=atlas_connector.metadata_handler
        )
        mock_logger_error.assert_called()


def test_handle_messaging(atlas_connector):
    """Test handling of messaging service messages"""
    msg = {"data": {"message": "test"}}

    with mock.patch.object(
        atlas_connector.metadata_handler.atlas_connector, "ingest_message"
    ) as mock_ingest:
        atlas_connector.metadata_handler._handle_messaging(
            msg, parent=atlas_connector.metadata_handler
        )
        mock_ingest.assert_called_once_with(msg)


def test_handle_messaging_error(atlas_connector):
    """Test handling of messaging service messages with error"""
    msg = {"data": {"message": "test"}}

    with (
        mock.patch.object(
            atlas_connector.metadata_handler.atlas_connector,
            "ingest_message",
            side_effect=ValueError("Test error"),
        ),
        mock.patch("bec_lib.logger.bec_logger.logger.exception") as mock_logger_error,
    ):
        atlas_connector.metadata_handler._handle_messaging(
            msg, parent=atlas_connector.metadata_handler
        )
        mock_logger_error.assert_called()
