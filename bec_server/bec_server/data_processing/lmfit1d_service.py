from __future__ import annotations

import inspect
import threading
import time
from typing import TYPE_CHECKING

import lmfit
import numpy as np

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.lmfit_serializer import deserialize_param_object, serialize_lmfit_params
from bec_lib.logger import bec_logger
from bec_server.data_processing.dap_service import DAPError, DAPServiceBase

if TYPE_CHECKING:
    from bec_lib.scan_items import ScanItem

logger = bec_logger.logger


class LmfitService1D(DAPServiceBase):
    """
    Lmfit service for 1D data.
    """

    AUTO_FIT_SUPPORTED = True

    def __init__(self, model: str, *args, continuous: bool = False, **kwargs):
        """
        Initialize the lmfit service. This is a multiplexer service that provides
        access to multiple lmfit models.

        Args:
            model (str): Model name
            continuous (bool, optional): Continuous processing. Defaults to False.
        """
        super().__init__(*args, **kwargs)
        self.scan_id = None
        self.device_x = None
        self.signal_x = None
        self.device_y = None
        self.signal_y = None
        self.parameters = None
        self.current_scan_item = None
        self.finished_id = None
        self.model = getattr(lmfit.models, model)()
        self.finish_event = None
        self.data = None
        self.continuous = continuous
        self.oversample = 1

    @staticmethod
    def available_models():
        models = []
        for name, model_cls in inspect.getmembers(lmfit.models):
            try:
                is_model = issubclass(model_cls, lmfit.model.Model)
            except TypeError:
                is_model = False
            if is_model and name not in [
                "Gaussian2dModel",
                "ExpressionModel",
                "Model",
                "SplineModel",
            ]:
                models.append(model_cls)
        return set(models)

    @classmethod
    def get_provided_services(cls):
        services = {
            model.__name__: {
                "class": cls.__name__,
                "user_friendly_name": model.__name__,
                "class_doc": cls.get_class_doc_string(model),
                "run_doc": cls.get_run_doc_string(model),
                "run_name": cls.get_user_friendly_run_name(),
                "signature": cls.get_signature(),
                "auto_run_supported": getattr(cls, "AUTO_FIT_SUPPORTED", False),
                "params": serialize_lmfit_params(cls.get_model(model)().make_params()),
                "class_args": [],
                "class_kwargs": {"model": model.__name__},
            }
            for model in cls.available_models()
        }
        return services

    @classmethod
    def get_class_doc_string(cls, model: str, *args, **kwargs):
        """
        Get the public doc string for the model.
        """
        model = cls.get_model(model)
        return model.__doc__ or model.__init__.__doc__

    @classmethod
    def get_run_doc_string(cls, model: str, *args, **kwargs):
        """
        Get the fit doc string.
        """
        return cls.get_class_doc_string(model) + cls.configure.__doc__

    @classmethod
    def get_user_friendly_run_name(cls):
        """
        Get the user friendly run name.
        """
        return "fit"

    @staticmethod
    def get_model(model: str) -> lmfit.Model:
        """Get the model from the config and convert it to an lmfit model."""

        if isinstance(model, str):
            model = getattr(lmfit.models, model, None)
        if not model:
            raise ValueError(f"Unknown model {model}")

        return model

    def on_scan_status_update(self, status: dict, metadata: dict):
        """
        Process a scan segment.

        Args:
            data (dict): Scan segment data
            metadata (dict): Scan segment metadata
        """
        if self.finish_event is None:
            self.finish_event = threading.Event()
            threading.Thread(target=self.process_until_finished, args=(self.finish_event,)).start()

        if status.get("status") != "open":
            time.sleep(0.2)
            self.finish_event.set()
            self.finish_event = None

    def process_until_finished(self, event: threading.Event):
        """
        Process until the scan is finished.
        """
        while True:
            data = self.get_data_from_current_scan(scan_item=self.current_scan_item)
            if not data:
                time.sleep(0.1)
                continue
            self.data = data
            out = self.process()
            if out:
                stream_output, metadata = out
                self.client.connector.xadd(
                    MessageEndpoints.processed_data(self.model.__class__.__name__),
                    msg_dict={
                        "data": messages.ProcessedDataMessage(data=stream_output, metadata=metadata)
                    },
                )
            if event.is_set():
                break
            time.sleep(0.1)

    def configure(
        self,
        scan_item: ScanItem | str = None,
        device_x: DeviceBase | str = None,
        signal_x: DeviceBase | str = None,
        device_y: DeviceBase | str = None,
        signal_y: DeviceBase | str = None,
        x_min: float = None,
        x_max: float = None,
        amplitude: lmfit.Parameter = None,
        center: lmfit.Parameter = None,
        sigma: lmfit.Parameter = None,
        oversample: int = 1,
        **kwargs,
    ):
        """
        Args:
            scan_item (ScanItem): Scan item or scan ID
            device_x (DeviceBase | str): Device name for x
            signal_x (DeviceBase | str): Signal name for x
            device_y (DeviceBase | str): Device name for y
            signal_y (DeviceBase | str): Signal name for y
            x_min (float): Minimum x value
            x_max (float): Maximum x value
            parameters (dict): Fit parameters
            oversample (int): Oversample factor
        """
        # we only receive scan IDs from the client. However, users may
        # pass in a scan item in the CLI which is converted to a scan ID
        # within BEC lib.

        self.oversample = oversample

        selected_device = kwargs.get("selected_device")
        if selected_device:
            device_y, signal_y = selected_device

        scan_id = scan_item
        if scan_id != self.scan_id or not self.current_scan_item:
            scan_item = self.client.queue.scan_storage.find_scan_by_ID(scan_id)
            self.scan_id = scan_id
        else:
            scan_item = self.current_scan_item

        self.parameters = {}

        if device_x:
            self.device_x = device_x
        if signal_x:
            self.signal_x = signal_x
        elif device_x and self.client.device_manager.devices.get(device_x):
            if len(self.client.device_manager.devices[device_x]._hints) == 1:
                self.signal_x = self.client.device_manager.devices[device_x]._hints[0]
        if device_y:
            self.device_y = device_y
        if signal_y:
            self.signal_y = signal_y
        elif device_y and self.client.device_manager.devices.get(device_y):
            if len(self.client.device_manager.devices[device_y]._hints) == 1:
                self.signal_y = self.client.device_manager.devices[device_y]._hints[0]
        if amplitude:
            self.parameters["amplitude"] = amplitude
        if center:
            self.parameters["center"] = center
        if sigma:
            self.parameters["sigma"] = sigma

        self.parameters = deserialize_param_object(self.parameters)

        if not self.continuous:
            if not scan_item:
                logger.warning("Failed to access scan item")
                return
            if not self.device_x or not self.signal_x or not self.device_y or not self.signal_y:
                raise DAPError("Device and signal names are required")
            self.data = self.get_data_from_current_scan(
                scan_item=scan_item, x_min=x_min, x_max=x_max
            )

    def get_data_from_current_scan(
        self, scan_item: ScanItem, devices: dict = None, x_min: float = None, x_max: float = None
    ) -> dict | None:
        """
        Get the data from the current scan.

        Args:
            scan_item (ScanItem): Scan item
            devices (dict): Device names for x and y axes. If not provided, the default values will be used.
            x_min (float): Minimum x value
            x_max (float): Maximum x value

        Returns:
            dict: Data for the x and y axes, limited to the specified range
        """

        MIN_DATA_POINTS = 3

        if not scan_item:
            logger.warning("Failed to access scan item")
            return None
        if not devices:
            devices = {}
        device_x = devices.get("device_x", self.device_x)
        signal_x = devices.get("signal_x", self.signal_x)
        device_y = devices.get("device_y", self.device_y)
        signal_y = devices.get("signal_y", self.signal_y)

        if not device_x:
            if not scan_item.live_data:
                return None
            scan_report_devices = scan_item.live_data[0].metadata.get("scan_report_devices", [])
            if not scan_report_devices:
                logger.warning("Failed to find scan report devices")
                return None
            device_x = scan_report_devices[0]
            bec_device_x = self.client.device_manager.devices.get(device_x)
            if not bec_device_x:
                logger.warning(f"Failed to find device {device_x}")
                return None
            # pylint: disable=protected-access
            hints = bec_device_x._hints
            if not hints:
                logger.warning(f"Failed to find hints for device {device_x}")
                return None
            if len(hints) > 1:
                logger.warning(f"Multiple hints found for device {device_x}")
                return None
            signal_x = hints[0]

        # get the event data
        if not scan_item.live_data:
            return None
        x = scan_item.live_data.get(device_x, {}).get(signal_x, {}).get("value")
        if not x:
            logger.warning(f"Failed to find signal {device_x}.{signal_x}")
            return None
        y = scan_item.live_data.get(device_y, {}).get(signal_y, {}).get("value")
        if not y:
            logger.warning(f"Failed to find signal {device_y}.{signal_y}")
            return None

        if len(x) < MIN_DATA_POINTS or len(y) < MIN_DATA_POINTS:
            return None

        # limit the data to the specified range
        if x_min is None:
            x_min = -np.inf
        if x_max is None:
            x_max = np.inf

        x_original = np.asarray(x)
        x = np.asarray(x)
        y = np.asarray(y)

        indices = np.where((x >= x_min) & (x <= x_max))
        x = x[indices]
        y = y[indices]

        # check if the filtered data is still long enough to fit
        if len(x) < MIN_DATA_POINTS or len(y) < MIN_DATA_POINTS:
            return None

        return {
            "x": x,
            "y": y,
            "x_original": x_original,
            "x_lim": (x_min is not None or x_max is not None),
        }

    def process(self) -> tuple[dict, dict]:
        """
        Process data and return the result.
        """
        # get the data
        if not self.data:
            return None

        x = self.data["x"]
        y = self.data["y"]

        # fit the data
        if self.parameters:
            result = self.model.fit(y, x=x, params=self.parameters)
        else:
            result = self.model.fit(y, x=x)

        # if the fit was only on a subset of the data, add the original x values to the output
        if self.data["x_lim"] or self.oversample != 1:
            x_data = self.data["x_original"]
            x_out = np.linspace(x_data.min(), x_data.max(), int(len(x_data) * self.oversample))
            y_out = np.asarray(self.model.eval(**result.best_values, x=x_out))
        else:
            x_out = self.data["x_original"]
            y_out = np.asarray(result.best_fit)

        # add the fit result to the output
        stream_output = {"x": x_out, "y": y_out}

        # add the fit parameters to the metadata
        metadata = {}
        metadata["input"] = {
            "scan_id": self.scan_id,
            "device_x": self.device_x,
            "signal_x": self.signal_x,
            "device_y": self.device_y,
            "signal_y": self.signal_y,
            "parameters": serialize_lmfit_params(self.parameters),
        }
        metadata["fit_parameters"] = result.best_values
        metadata["fit_summary"] = result.summary()
        logger.info(f"fit summary: {metadata['fit_summary']}")

        return (stream_output, metadata)
