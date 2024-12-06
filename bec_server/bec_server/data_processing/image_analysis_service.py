from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

import numpy as np
from scipy.ndimage import center_of_mass
from typeguard import typechecked

from bec_lib.device_monitor_plugin import DeviceMonitorPlugin
from bec_lib.logger import bec_logger
from bec_server.data_processing.dap_service import DAPError, DAPServiceBase

if TYPE_CHECKING:
    from bec_lib.client import BECClient
    from bec_lib.device import DeviceBase
    from bec_lib.scan_items import ScanItem

logger = bec_logger.logger


class ReturnType(str, Enum):
    """The possible return data types for the image analysis service."""

    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    MEDIAN = "median"
    STD = "std"
    CENTER_OF_MASS = "center_of_mass"


class ImageAnalysisService(DAPServiceBase):
    """Service for image analysis."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.device_monitor_plugin = DeviceMonitorPlugin(self.client.connector)
        self.data = []
        self.device = None
        self.return_type = None

    def configure(self, scan_item=None, device=None, images=None, return_type=None, **kwargs):
        # TODO Add type hints for np.ndarray and list[np.ndarray] do not work yet in the signature_serializer
        # This will be adressed in a different MR, issue is created #395
        # scan_item: ScanItem | str = None,
        # device: DeviceBase | str = None,
        # images: np.ndarray | list[np.ndarray] | None = None,
        # **kwargs,
        # ):
        """Configure the image analysis service. Either provide a scan item and a device which
        has a 2D monitor active, or provide the images directly. If no data is found for the input
        the service will return an empty stream output.

        Args:
            scan_item: Scan Item to consider for the analysis
            device: The device for the 2D monitor data
            images: Alternatively, you can provide the images directly
            return_type: The type of data to return, can be "min", "max", "mean", "median", "std", "center_of_mass"
        """
        if return_type is None:
            return_type = ReturnType.CENTER_OF_MASS
        else:
            return_type = ReturnType(return_type)
        self.return_type = return_type
        # If images are provided, use them
        if images is not None:
            if isinstance(images, np.ndarray):
                self.data = [images]
            elif isinstance(images, list) and all(
                isinstance(image, np.ndarray) for image in images
            ):
                self.data = images
            return
        # Else if scan item is provided, get the images
        if device is None or scan_item is None:
            raise DAPError(
                f"Either provide a device: {device} and scan_id {scan_item} or images {images}"
            )
        self.device = str(device)
        self.data = self.get_images_for_scan_item(scan_id=scan_item)

    def get_images_for_scan_item(self, scan_id: str) -> list[np.ndarray]:
        """Get the data for the scan item."""
        self.scan_id = scan_id
        data = self.device_monitor_plugin.get_data_for_scan(device=self.device, scan=self.scan_id)
        if len(data) == 0:
            logger.warning(f"No data found for scan {scan_id} and device {self.device}")
        return data

    @typechecked
    def compute_statistics(self, images: list[np.ndarray]) -> dict:
        """Compute the image analysis.

        Args:
            images: The images to analyze

        Returns:
            dict: The statistics
        """
        stacked_images = np.stack(images, axis=0)
        stats = {
            "min": stacked_images.min(axis=(1, 2)),
            "max": stacked_images.max(axis=(1, 2)),
            "mean": stacked_images.mean(axis=(1, 2)),
            "median": np.median(stacked_images, axis=(1, 2)),
            "std": np.std(stacked_images, axis=(1, 2)),
            "center_of_mass": np.array([center_of_mass(img) for img in images]),
        }
        return stats

    def process(self):
        """Process the image analysis."""
        stats = self.compute_statistics(self.data)
        metadata = {"scan_id": self.scan_id, "device": self.device, "stats": stats}
        stream_output = self._compute_stream_output(stats)
        return stream_output, metadata

    @classmethod
    def get_user_friendly_name(cls, *args, **kwargs):
        """
        Get the user friendly name for the service.
        """
        return "image_analysis"

    def _compute_stream_output(self, stats: dict) -> dict:
        """
        Compute the stream output from the statistics.

        Args:
            stats (dict): Statistics

        Returns:
            dict: Stream output
        """
        if self.return_type == ReturnType.CENTER_OF_MASS:
            return {"x": stats["center_of_mass"].T[0], "y": stats["center_of_mass"].T[1]}
        else:
            ret_type = self.return_type.value
            values = stats[ret_type]
            length = len(values)
            return {"x": np.linspace(0, length - 1, length), "y": values}
