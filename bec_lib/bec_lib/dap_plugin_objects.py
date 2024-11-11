"""
This module contains the base classes for DAP plugin objects. These classes should be used to create custom DAP plugin objects.
"""

from __future__ import annotations

import builtins
import time
import uuid
from typing import TYPE_CHECKING

import numpy as np
from typeguard import typechecked

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.lmfit_serializer import serialize_param_object
from bec_lib.scan_items import ScanItem
from bec_lib.scan_report import ScanReport
from bec_lib.utils.import_utils import lazy_import

lmfit = lazy_import("lmfit")

if TYPE_CHECKING:
    from bec_lib.client import BECClient


class DAPPluginObjectBase:
    """
    Base class for DAP plugin objects. This class should not be used directly. Instead, use one of the derived classes.
    """

    _result_cls = None

    def __init__(
        self,
        service_name: str,
        plugin_info: dict,
        client: BECClient = None,
        auto_run_supported: bool = False,
        service_info: dict = None,
    ) -> None:
        """
        Args:
            service_name (str): The name of the service.
            plugin_info (dict): Information about the plugin.
            client (BECClient, optional): The BEC client. Defaults to None.
            auto_run_supported (bool, optional): Whether the plugin supports auto run. Defaults to False.
            service_info (dict, optional): Information about the service. Defaults to None.
            result_cls (type, optional): The class to use for the result of the plugin. Defaults to None.
        """
        self._service_name = service_name
        self._plugin_info = plugin_info
        self._client = client
        self._auto_run_supported = auto_run_supported
        self._plugin_config = {}
        self._service_info = service_info

        # run must be an anonymous function to allow for multiple doc strings
        self._user_run = lambda *args, **kwargs: self._run(*args, **kwargs)

    def _run(self, *args, **kwargs):
        converted_args = []
        for arg in args:
            if isinstance(arg, ScanItem):
                converted_args.append(arg.scan_id)
            elif isinstance(arg, ScanReport):
                converted_args.append(arg.scan.scan_id)
            else:
                converted_args.append(arg)
        args = converted_args
        converted_kwargs = {}
        for key, val in kwargs.items():
            if isinstance(val, ScanItem):
                converted_kwargs[key] = val.scan_id
            elif isinstance(arg, ScanReport):
                converted_kwargs[key] = arg.scan.scan_id
            elif isinstance(val, lmfit.Parameter):
                converted_kwargs[key] = serialize_param_object(val)
            else:
                converted_kwargs[key] = val
        kwargs = converted_kwargs
        request_id = str(uuid.uuid4())
        self._client.connector.set_and_publish(
            MessageEndpoints.dap_request(),
            messages.DAPRequestMessage(
                dap_cls=self._plugin_info["class"],
                dap_type="on_demand",
                config={
                    "args": args,
                    "kwargs": kwargs,
                    "class_args": self._plugin_info.get("class_args"),
                    "class_kwargs": self._plugin_info.get("class_kwargs"),
                },
                metadata={"RID": request_id},
            ),
        )

        response = self._wait_for_dap_response(request_id)
        if isinstance(response, dict):
            response = response.get("data")
        return self._convert_result(response)

    def _convert_result(self, result: messages.BECMessage):
        if not result.content["data"]:
            return None
        if not callable(self._result_cls):
            return result.content["data"]
        # pylint: disable=not-callable
        return self._result_cls(result, self._plugin_info["user_friendly_name"])

    def _wait_for_dap_response(self, request_id: str, timeout: float = 5.0):
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError("Timeout waiting for DAP response.")
            response = self._client.connector.get(MessageEndpoints.dap_response(request_id))
            if not response:
                time.sleep(0.005)
                continue

            if response.metadata["RID"] != request_id:
                time.sleep(0.005)
                continue

            if response.content["success"]:
                return response
            raise RuntimeError(response.content["error"])

    def _update_dap_config(self, request_id: str = None):
        if not self._plugin_config.get("selected_device"):
            return
        self._plugin_config["class_args"] = self._plugin_info.get("class_args")
        self._plugin_config["class_kwargs"] = self._plugin_info.get("class_kwargs")
        self._client.connector.set_and_publish(
            MessageEndpoints.dap_request(),
            messages.DAPRequestMessage(
                dap_cls=self._plugin_info["class"],
                dap_type="continuous",
                config=self._plugin_config,
                metadata={"RID": request_id},
            ),
        )


class DAPPluginObject(DAPPluginObjectBase):
    """
    Default DAP plugin object. This class should be used for plugins that do not support auto run.
    To customize a plugin, create a new class that inherits from this class and override the methods as needed.
    """

    def get_data(self):
        """
        Get the data from last run.
        """
        msg = self._client.connector.get_last(MessageEndpoints.processed_data(self._service_name))
        if not msg:
            return None
        if isinstance(msg, dict):
            msg = msg.get("data")
        return self._convert_result(msg)


class DAPPluginObjectAutoRun(DAPPluginObject):
    """
    DAP plugin object that supports auto run. This class should be used for plugins that support auto run.
    To customize a plugin, create a new class that inherits from this class and override the methods as needed.
    """

    @property
    def auto_run(self):
        """
        Set to True to start a continously running worker.
        """
        return self._plugin_config.get("auto_run", False)

    @auto_run.setter
    @typechecked
    def auto_run(self, val: bool):
        self._plugin_config["auto_run"] = val
        request_id = str(uuid.uuid4())
        self._update_dap_config(request_id=request_id)


class LmfitService1DResult:
    """
    Result of fitting 1D data using lmfit.
    """

    def __init__(
        self,
        result: list[dict] | messages.ProcessedDataMessage,
        model_name: str = None,
        client: BECClient = None,
    ):
        if isinstance(result, messages.ProcessedDataMessage):
            result = [result.content["data"], result.metadata]
        elif isinstance(result, messages.DAPResponseMessage):
            result = result.data
        self._data = result[0]
        self._report = result[1]
        self._model = model_name
        if client:
            self._client = client
        else:
            self._client = builtins.__dict__.get("bec")
        if "amplitude" in self.params:
            self.amplitude = self.params["amplitude"]
        if "center" in self.params:
            self.center = self.params["center"]
        if "sigma" in self.params:
            self.sigma = self.params["sigma"]

    @property
    def params(self):
        """
        The parameters of the fit.
        """
        return self._report["fit_parameters"]

    @property
    def data(self):
        """
        The data from the fit.
        """
        return self._data

    @property
    def report(self):
        """
        The report of the fit.
        """
        return self._report

    def eval(self, x: np.ndarray):
        """
        Evaluate the fit at the given x values.

        Args:
            x (array_like): The x values to evaluate the fit at.

        Returns:
            array_like: The y values of the fit at the given x values.
        """
        if not isinstance(x, np.ndarray):
            x = np.array(x)
        model = getattr(lmfit.models, self._model)()
        params = model.make_params(**self.params)
        return {"x": x, "y": model.eval(params=params, x=x)}

    @property
    def min(self):
        """
        Get the minimum value of the fit.

        Returns:
            float: The minimum value of the fit.
        """
        # get the index of the minimum value
        min_index = np.argmin(self._data["y"])
        return {"x": self._data["x"][min_index], "y": self._data["y"][min_index]}

    @property
    def max(self):
        """
        Get the maximum value of the fit.

        Returns:
            float: The maximum value of the fit.
        """
        # get the index of the maximum value
        max_index = np.argmax(self._data["y"])
        return {"x": self._data["x"][max_index], "y": self._data["y"][max_index]}

    @property
    def input_data(self):
        """
        Get the input data used for the fit.

        Returns:
            dict: The input data used for the fit.
        """
        input_data = self._report.get("input")
        scan_id = input_data.get("scan_id")
        if not scan_id:
            return None

        scan_item = self._client.queue.scan_storage.find_scan_by_ID(scan_id)
        if not scan_item:
            return None

        x = scan_item.live_data[input_data["device_x"]][input_data["signal_x"]].val
        y = scan_item.live_data[input_data["device_y"]][input_data["signal_y"]].val

        return {"x": x, "y": y}

    def plot(self):
        """
        Plot the fit.
        """
        # move this to BECWidgets once it's available
        try:
            # pylint: disable=import-outside-toplevel
            import matplotlib.pyplot as plt

            plt.ion()
        except ImportError:
            raise ImportError(
                "matplotlib is not installed. Cannot plot. Please install matplotlib using 'pip install matplotlib'."
            )

        input_data = self.input_data
        plt.figure()
        plt.plot(input_data["x"], input_data["y"], label="data", color="black", marker="o")
        plt.plot(self._data["x"], self._data["y"], label=f"{self._model}", color="red")
        plt.legend()
        plt.show()

    def __str__(self) -> str:
        return f"{self._model} fit result: \n Params: {self.params} \n Min: {self.min} \n Max: {self.max}"


class LmfitService1D(DAPPluginObjectAutoRun):
    """
    Plugin for fitting 1D data using lmfit.
    """

    _result_cls = LmfitService1DResult

    def __init__(
        self,
        service_name: str,
        plugin_info: dict,
        client: BECClient = None,
        auto_run_supported: bool = False,
        service_info: dict = None,
    ) -> None:
        super().__init__(
            service_name,
            plugin_info,
            client=client,
            auto_run_supported=auto_run_supported,
            service_info=service_info,
        )
        self._params = None

    def select(self, device: DeviceBase | str, signal: str = None):
        """
        Select the device and signal to use for fitting.

        Args:
            device (DeviceBase | str): The device to use for fitting. Can be either a DeviceBase object or the name of the device.
            signal (str, optional): The signal to use for fitting. If not provided, the first signal in the device's hints will be used.
        """
        bec_device = (
            device
            if isinstance(device, DeviceBase)
            else self._client.device_manager.devices.get(device)
        )
        if not bec_device:
            raise AttributeError(f"Device {device} not found.")
        if signal:
            self._plugin_config["selected_device"] = [bec_device.name, signal]
        else:
            # pylint: disable=protected-access
            hints = bec_device._hints
            if not hints:
                raise AttributeError(
                    f"Device {bec_device.name} has no hints. Cannot select device without signal."
                )
            if len(hints) > 1:
                raise AttributeError(
                    f"Device {bec_device.name} has multiple hints. Please specify a signal."
                )
            self._plugin_config["selected_device"] = [bec_device.name, hints[0]]

        request_id = str(uuid.uuid4())
        self._update_dap_config(request_id=request_id)

    def get_params(self) -> lmfit.Parameters:
        """
        Create a set of parameters for the model.

        Returns:
            lmfit.Parameters: The parameters available for the model.
        """
        if not self._params:
            model = getattr(lmfit.models, self._plugin_info["user_friendly_name"])()
            self._params = model.make_params()
        return self._params

    def reset_params(self):
        """
        Reset the parameters to the default values.
        """
        self._params = None

    def _run(self, *args, **kwargs):
        if self._params:
            return super()._run(*args, **self._params, **kwargs)
        return super()._run(*args, **kwargs)
