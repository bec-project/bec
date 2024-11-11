from unittest import mock

import lmfit
import numpy as np
import pytest

from bec_server.data_processing.dap_service import DAPError
from bec_server.data_processing.lmfit1d_service import LmfitService1D


@pytest.fixture
def lmfit_service():
    yield LmfitService1D(model="GaussianModel", continuous=False, client=mock.MagicMock())


@pytest.mark.parametrize("model, exists", [("GaussianModel", True), ("ModelDoesntExist", False)])
def test_LmfitService1D(model, exists):
    client = mock.MagicMock()
    if exists:
        service = LmfitService1D(model=model, client=client)
        return
    with pytest.raises(AttributeError):
        service = LmfitService1D(model=model, client=client)


def test_LmfitService1D_available_models(lmfit_service):
    models = lmfit_service.available_models()
    assert len(models) > 0
    assert all(issubclass(model, lmfit.model.Model) for model in models)
    assert all(
        model.__name__ not in ["Gaussian2dModel", "ExpressionModel", "Model", "SplineModel"]
        for model in models
    )


def test_LmfitService1D_get_provided_services(lmfit_service):
    services = lmfit_service.get_provided_services()
    assert isinstance(services, dict)
    assert len(services) > 0
    for model, service in services.items():
        assert isinstance(service, dict)
        assert "class" in service
        assert "user_friendly_name" in service
        assert "class_doc" in service
        assert "run_doc" in service


def test_LmfitService1D_get_data_from_current_scan_without_devices(lmfit_service):
    scan_item = mock.MagicMock()
    scan_item.live_data = mock.MagicMock()
    scan_item.live_data[0].metadata = {"scan_report_devices": ["device_x", "device_y"]}

    data = lmfit_service.get_data_from_current_scan(scan_item)
    assert data is None


def test_LmfitService1D_get_data_from_current_scan(lmfit_service):
    scan_item = mock.MagicMock()
    scan_item.live_data = mock.MagicMock()
    lmfit_service.device_x = "device_x"
    lmfit_service.signal_x = "signal_x"
    lmfit_service.device_y = "device_y"
    lmfit_service.signal_y = "signal_y"

    scan_item.live_data = {
        "device_x": {"signal_x": {"value": [1, 2, 3], "timestamp": 0}},
        "device_y": {"signal_y": {"value": [4, 5, 6], "timestamp": 0}},
    }
    data = lmfit_service.get_data_from_current_scan(scan_item)
    assert all(data["x"] == np.array([1, 2, 3]))
    assert all(data["y"] == np.array([4, 5, 6]))


@pytest.mark.parametrize(
    "scan_data",
    [
        {
            "device_x": {"signal_x": {"value": [1, 2], "timestamp": 0}},
            "device_y": {"signal_y": {"value": [4, 5], "timestamp": 0}},
        },
        {"device_y": {"signal_y": {"value": [4, 5, 6], "timestamp": 0}}},
        {"device_x": {"signal_x": {"value": [1, 2], "timestamp": 0}}},
        None,
    ],
)
def test_LmfitService1D_get_data_from_current_scan_returns_None(lmfit_service, scan_data):
    scan_item = mock.MagicMock()
    scan_item.live_data = mock.MagicMock()
    lmfit_service.device_x = "device_x"
    lmfit_service.signal_x = "signal_x"
    lmfit_service.device_y = "device_y"
    lmfit_service.signal_y = "signal_y"
    scan_item.live_data = scan_data
    data = lmfit_service.get_data_from_current_scan(scan_item)
    assert data is None


def test_LmfitService1D_process(lmfit_service):
    lmfit_service.data = {"x": [1, 2, 3], "y": [4, 5, 6], "x_lim": False, "x_original": [1, 2, 3]}
    lmfit_service.model = mock.MagicMock()

    result = lmfit_service.process()
    assert isinstance(result, tuple)
    assert isinstance(result[0], dict)
    assert isinstance(result[1], dict)
    lmfit_service.model.fit.assert_called_once_with([4, 5, 6], x=[1, 2, 3])


def test_LmfitService1D_on_scan_status_update(lmfit_service):
    with mock.patch.object(lmfit_service, "process_until_finished") as process_until_finished:
        lmfit_service.on_scan_status_update({"status": "running"}, {})
        process_until_finished.assert_called_once()


def test_LmfitService1D_on_scan_status_update_finishes_on_closed_scans(lmfit_service):
    with mock.patch("bec_server.data_processing.lmfit1d_service.threading") as threading:
        event = threading.Event()
        lmfit_service.finish_event = event
        with mock.patch.object(lmfit_service, "process_until_finished") as process_until_finished:
            lmfit_service.on_scan_status_update({"status": "closed"}, {})
            event.set.assert_called_once()
            process_until_finished.assert_not_called()
            assert lmfit_service.finish_event is None


def test_LmfitService1D_process_until_finished(lmfit_service):
    event = mock.MagicMock()
    event.is_set.side_effect = [False, False, True]

    with mock.patch.object(lmfit_service, "get_data_from_current_scan") as get_data:
        get_data.return_value = {"x": [1, 2, 3], "y": [4, 5, 6]}
        with mock.patch.object(lmfit_service, "process") as process:
            process.return_value = ({"result": "result"}, {"metadata": "metadata"})
            lmfit_service.process_until_finished(event)
            assert get_data.call_count == 3
            assert process.call_count == 3
            assert lmfit_service.client.connector.xadd.call_count == 3


def test_LmfitService1D_configure(lmfit_service):
    with pytest.raises(DAPError):
        lmfit_service.configure()


def test_LmfitService1D_configure_selected_devices(lmfit_service):
    lmfit_service.continuous = False
    with pytest.raises(DAPError):
        lmfit_service.configure(selected_device=["bpm4i", "bpm4i"])

    with mock.patch.object(lmfit_service, "get_data_from_current_scan") as get_data:
        get_data.return_value = {"x": [1, 2, 3], "y": [4, 5, 6]}
        lmfit_service.configure(
            selected_device=["bpm4i", "bpm4i"], device_x="samx", signal_x="samx"
        )
        get_data.assert_called_once()


def test_LmfitService1D_get_model(lmfit_service):
    model = lmfit_service.get_model("GaussianModel")
    assert model.__name__ == "GaussianModel"
    assert issubclass(model, lmfit.model.Model)

    with pytest.raises(ValueError):
        lmfit_service.get_model("ModelDoesntExist")
