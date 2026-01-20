from unittest import mock

import pytest

from bec_messages import messages
from bec_lib.redis_connector import MessageObject
from bec_server.data_processing.dap_service import DAPServiceBase
from bec_server.data_processing.dap_service_manager import DAPServiceManager


class ServiceMock(DAPServiceBase):
    def configure(self, *args, **kwargs):
        pass

    def process(self, *args, **kwargs):
        pass

    def on_scan_status_update(self, *args, **kwargs):
        pass

    @classmethod
    def get_class_doc_string(cls, *args, **kwargs):
        return "class_doc"


@pytest.fixture
def service_manager():
    dap_service_manager = DAPServiceManager(services=ServiceMock)
    dap_service_manager.start(client=mock.MagicMock())
    yield dap_service_manager
    dap_service_manager.shutdown()


def test_DAPServiceManager_init(service_manager):
    assert service_manager.services == [ServiceMock]
    assert service_manager.available_dap_services == {
        "ServiceMock": {
            "class": "ServiceMock",
            "user_friendly_name": "ServiceMock",
            "class_doc": "class_doc",
            "run_doc": None,
            "run_name": "run",
            "signature": [
                {
                    "name": "args",
                    "kind": "VAR_POSITIONAL",
                    "default": "_empty",
                    "annotation": "_empty",
                },
                {
                    "name": "kwargs",
                    "kind": "VAR_KEYWORD",
                    "default": "_empty",
                    "annotation": "_empty",
                },
            ],
            "params": {},
            "auto_run_supported": False,
            "class_args": [],
            "class_kwargs": {},
        }
    }


@pytest.mark.parametrize(
    "msg, process_called",
    [
        (messages.DAPRequestMessage(dap_cls="dap_cls", dap_type="on_demand", config={}), True),
        (messages.ScanStatusMessage(scan_id="scan_id", status="open", info={}), False),
    ],
)
def test_DAPServiceManager_request_callback(service_manager, msg, process_called):
    msg_obj = MessageObject(value=msg, topic="topic")
    with mock.patch.object(service_manager, "process_dap_request") as mock_process_dap_request:
        service_manager._dap_request_callback(msg_obj)
        if process_called:
            mock_process_dap_request.assert_called_once_with(msg)


@pytest.mark.parametrize(
    "msg, raised_exception, error_msg",
    [
        (
            messages.DAPRequestMessage(
                dap_cls="ServiceMock",
                dap_type="continuous",
                config={"auto_fit": True, "class_args": [], "class_kwargs": {}},
            ),
            False,
            "",
        ),
        (
            messages.DAPRequestMessage(
                dap_cls="ServiceMock", dap_type="continuous", config={"auto_fit": False}
            ),
            False,
            "",
        ),
        (
            messages.DAPRequestMessage(dap_cls="ServiceMock", dap_type="on_demand", config={}),
            False,
            "",
        ),
    ],
)
def test_process_dap_request(service_manager, msg, raised_exception, error_msg):
    if raised_exception:
        with mock.patch.object(service_manager, "send_dap_response") as mock_send_dap_response:
            service_manager.process_dap_request(msg)
            mock_send_dap_response.assert_called_once_with(
                msg, success=False, error=error_msg, metadata=msg.metadata
            )
        return

    service_manager.process_dap_request(msg)
