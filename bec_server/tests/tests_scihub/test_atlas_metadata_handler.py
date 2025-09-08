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
        atlas_connector.metadata_handler, "update_scan_status", side_effect=ValueError
    ):
        atlas_connector.metadata_handler._handle_scan_status(
            msg_obj, parent=atlas_connector.metadata_handler
        )
        assert True
