from unittest import mock

import numpy as np
import pytest

from bec_server.data_processing.image_analysis_service import (
    DAPError,
    ImageAnalysisService,
    ReturnType,
)


@pytest.fixture
def image_analysis_service():
    yield ImageAnalysisService(client=mock.MagicMock())


def test_image_analysis_configure(image_analysis_service):
    """Test the configure method of the image analysis service."""

    # Test with scan item
    scan_item = mock.MagicMock()
    dummy_data = [np.linspace(0, 1, 100) for _ in range(10)]
    scan_item.return_value = "mock_scan_id"
    with mock.patch.object(
        image_analysis_service, "get_images_for_scan_item", return_value=dummy_data
    ):
        image_analysis_service.configure(scan_item=scan_item, device="eiger")
        assert image_analysis_service.data == dummy_data
        assert image_analysis_service.device == "eiger"
        assert image_analysis_service.return_type == ReturnType.CENTER_OF_MASS
        # Reset the imageanalysisService
        image_analysis_service.data = []
        image_analysis_service.device = None
        image_analysis_service.return_type = None

        # Missing device argument
        with pytest.raises(DAPError):
            image_analysis_service.configure(scan_item=scan_item)

        # Reset the imageanalysisService
        image_analysis_service.data = []
        image_analysis_service.device = None
        image_analysis_service.return_type = None

        # Missing scan item
        with pytest.raises(DAPError):
            image_analysis_service.configure(device="eiger")

        # Reset the imageanalysisService
        image_analysis_service.data = []
        image_analysis_service.device = None
        image_analysis_service.return_type = None

        # Test with images
        image_analysis_service.configure(images=dummy_data)
        assert image_analysis_service.data == dummy_data


def test_get_images_for_scan_item(image_analysis_service):
    """Test the get_images_for_scan_item method of the image analysis service."""

    dummy_data = [np.linspace(0, 1, 100) for _ in range(10)]

    with mock.patch.object(
        image_analysis_service.device_monitor_plugin,
        "get_data_for_scan",
        side_effect=[dummy_data, []],
    ):
        # Test with existing scan id
        scan_id = "mock_scan_id"
        image_analysis_service.scan_id = scan_id
        data = image_analysis_service.get_images_for_scan_item(scan_id)
        assert data == dummy_data
        assert image_analysis_service.scan_id == scan_id

        # Test with empty return
        data = image_analysis_service.get_images_for_scan_item(scan_id)
        assert data == []


def test_compute_statistics(image_analysis_service):
    """Test the compute_statistics method of the image analysis service."""

    dummy_data = [np.zeros((10, 10)), np.ones((10, 10))]
    stats = image_analysis_service.compute_statistics(dummy_data)
    assert stats["min"].shape == (2,)
    assert stats["max"].shape == (2,)
    assert np.isclose(stats["mean"], np.array([0, 1])).all()
    assert np.isclose(stats["min"], np.array([0, 1])).all()


def test_get_stream_output(image_analysis_service):
    """Test the get_stream_output method of the image analysis service."""
    dummy_data = [np.ones((10, 10)), np.ones((10, 10))]
    stats = image_analysis_service.compute_statistics(dummy_data)
    image_analysis_service.return_type = ReturnType.MIN
    stream_output = image_analysis_service._compute_stream_output(stats)
    assert np.isclose(stream_output["x"], np.linspace(0, 1, 2)).all()
    assert (stream_output["y"] == stats["min"]).all()
    # Test center of Mass
    image_analysis_service.return_type = ReturnType.CENTER_OF_MASS
    stream_output = image_analysis_service._compute_stream_output(stats)
    assert (stream_output["x"] == stats["center_of_mass"].T[0]).all()
    assert (stream_output["y"] == stats["center_of_mass"].T[1]).all()
