from unittest import mock

from bec_lib import messages
from bec_lib.connector import MessageObject


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


def test_handle_atlas_account_update_valid(atlas_connector):
    msg = {"data": messages.VariableMessage(value="account1")}
    with mock.patch.object(
        atlas_connector.metadata_handler, "_update_local_account"
    ) as mock_update_local_account:
        atlas_connector.metadata_handler._handle_atlas_account_update(
            msg, parent=atlas_connector.metadata_handler
        )
        mock_update_local_account.assert_called_once_with("account1")


def test_handle_atlas_account_update_invalid(atlas_connector):
    msg = {"invalid": "data"}
    with mock.patch("bec_lib.logger.bec_logger.logger.error") as mock_logger_error:
        atlas_connector.metadata_handler._handle_atlas_account_update(
            msg, parent=atlas_connector.metadata_handler
        )
        mock_logger_error.assert_called()


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


def test_update_local_account(atlas_connector):
    handler = atlas_connector.metadata_handler
    handler._account = "old_account"
    with mock.patch.object(handler.atlas_connector.connector, "xadd") as mock_xadd:
        handler._update_local_account("new_account")

        expected_msg = messages.VariableMessage(value="new_account")
        assert any(
            c.args[1] == {"data": expected_msg} and c.kwargs.get("max_size", 1) == 1
            for c in mock_xadd.call_args_list
        )


def test_send_atlas_update(atlas_connector):
    handler = atlas_connector.metadata_handler
    with mock.patch.object(handler.atlas_connector, "ingest_data") as mock_ingest_data:
        handler.send_atlas_update({"key": "value"})
        mock_ingest_data.assert_called_once_with({"key": "value"})
