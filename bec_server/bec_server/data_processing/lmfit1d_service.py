from __future__ import annotations

import inspect
import threading
import time
from collections.abc import Iterable, Sequence

import lmfit
import numpy as np

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.lmfit_serializer import deserialize_param_object, serialize_lmfit_params
from bec_lib.logger import bec_logger
from bec_lib.scan_items import ScanItem
from bec_server.data_processing.dap_service import DAPError, DAPServiceBase

logger = bec_logger.logger


class LmfitService1D(DAPServiceBase):
    """
    Lmfit service for 1D data.
    """

    AUTO_FIT_SUPPORTED = True

    def __init__(
        self, model: str | list[str] | tuple[str, ...], *args, continuous: bool = False, **kwargs
    ):
        """
        Initialize the lmfit service. This is a multiplexer service that provides
        access to multiple lmfit models.

        Args:
            model (str | list[str]): Model name or list of model names for a composite.
            continuous (bool, optional): Continuous processing. Defaults to False.
        """
        super().__init__(*args, **kwargs)
        self.scan_id = None
        self.device_x = None
        self.signal_x = None
        self.device_y = None
        self.signal_y = None
        self.parameters = None
        self.override_params = None
        self.model_sequence: list[str] | None = None
        self.model_name_sequence: list[str] | None = None
        self.model_name_to_component: dict[str, str] | None = None
        self._parameter_override_names = []
        self.current_scan_item = None
        self.finished_id = None
        self.model_components: dict[str, lmfit.Model] | None = None
        self.model_prefixes: dict[str, str] | None = None
        self.model = self._build_model(model)
        self.finish_event = None
        self.data = None
        self.continuous = continuous
        self.oversample = 1

    def _build_model(self, model: str | list[str] | tuple[str, ...]) -> lmfit.Model:
        if isinstance(model, (list, tuple)):
            return self._build_composite_model(self._coerce_model_list(model))
        if isinstance(model, str):
            return self._build_single_model(model)
        raise ValueError(f"Unknown model {model}")

    def _build_single_model(self, model_name: str) -> lmfit.Model:
        model_cls = self._get_model_cls(model_name)
        return model_cls()

    def _build_composite_model(self, model_list: Sequence[str]) -> lmfit.Model:
        if not model_list:
            raise ValueError("Composite model list cannot be empty.")
        self.model_components = {}
        self.model_prefixes = {}
        self.model_sequence = []
        self.model_name_sequence = list(model_list)
        composite_model: lmfit.model.Model | None = None
        for index, model_name in enumerate(model_list):
            component_name = self._component_name(model_name, index)
            component = self._create_component(model_name, component_name)
            composite_model = component if composite_model is None else composite_model + component
        self._build_component_lookup()
        logger.debug(
            f"Initialized composite lmfit model with components={list(self.model_components.keys())} "
            f"prefixes={self.model_prefixes}"
        )
        return composite_model

    @staticmethod
    def _get_model_cls(model_name: str):
        model_cls = getattr(lmfit.models, model_name, None)
        if not model_cls:
            raise ValueError(f"Unknown model {model_name}")
        return model_cls

    @staticmethod
    def _component_name(model_name: str, index: int) -> str:
        return f"{model_name}_{index}"

    def _create_component(self, model_name: str, component_name: str):
        model_cls = self._get_model_cls(model_name)
        prefix = f"{component_name}_"
        self.model_sequence.append(component_name)
        self.model_prefixes[component_name] = prefix
        component = model_cls(prefix=prefix)
        self.model_components[component_name] = component
        return component

    @staticmethod
    def _coerce_model_list(model_list: Iterable[str]) -> list[str]:
        return list(model_list)

    def _build_component_lookup(self) -> None:
        unique_names = len(set(self.model_name_sequence)) == len(self.model_name_sequence)
        if unique_names:
            self.model_name_to_component = {
                name: self.model_sequence[idx] for idx, name in enumerate(self.model_name_sequence)
            }
        else:
            self.model_name_to_component = None

    def _expand_composite_parameters(self, parameters: dict | list | tuple) -> dict:
        if self.model_components is None or self.model_prefixes is None:
            return parameters
        if isinstance(parameters, (list, tuple)):
            return self._expand_composite_list(parameters)
        if isinstance(parameters, dict):
            return self._expand_composite_dict(parameters)
        raise DAPError("Composite parameters must be a dict or list.")

    def _expand_composite_list(self, parameters: list | tuple) -> dict:
        if self.model_sequence is None or len(parameters) != len(self.model_sequence):
            raise DAPError(
                "Composite parameters list must match the length of the composite model list."
            )
        expanded: dict = {}
        for index, param_map in enumerate(parameters):
            if param_map is None:
                continue
            if not isinstance(param_map, dict):
                raise DAPError(
                    f"Composite parameters list item {index} must be a dict of parameter overrides."
                )
            component_name = self.model_sequence[index]
            expanded.update(self._expand_param_map(component_name, param_map))
        return expanded

    def _expand_composite_dict(self, parameters: dict) -> dict:
        expanded: dict = {}
        component_keys = set(self.model_components.keys())
        if set(parameters.keys()).issubset(component_keys):
            for component_name, param_map in parameters.items():
                if param_map is None:
                    continue
                if not isinstance(param_map, dict):
                    raise DAPError(
                        f"Composite parameters for '{component_name}' must be a dict of parameter overrides."
                    )
                expanded.update(self._expand_param_map(component_name, param_map))
            return expanded

        component_map = self._resolve_model_name_map(parameters)
        for model_name, param_map in parameters.items():
            if param_map is None:
                continue
            if not isinstance(param_map, dict):
                raise DAPError(
                    f"Composite parameters for '{model_name}' must be a dict of parameter overrides."
                )
            component_name = component_map[model_name]
            expanded.update(self._expand_param_map(component_name, param_map))
        return expanded

    def _resolve_model_name_map(self, parameters: dict) -> dict[str, str]:
        if self.model_name_to_component is None:
            raise DAPError(
                "Composite parameters are ambiguous with duplicate model names. "
                "Use a list aligned to the model list or keys like 'ModelName_0'."
            )
        invalid_models = set(parameters.keys()) - set(self.model_name_to_component.keys())
        if invalid_models:
            raise DAPError(
                f"Invalid parameter groups for composite model: {sorted(invalid_models)}"
            )
        return self.model_name_to_component

    def _expand_param_map(self, component_name: str, param_map: dict) -> dict:
        prefix = self.model_prefixes[component_name]
        expanded = {}
        for param_name, spec in param_map.items():
            expanded[f"{prefix}{param_name}"] = spec
        return expanded

    def _guess_parameters(self, x: np.ndarray, y: np.ndarray) -> lmfit.Parameters:
        guessed_params = self.model.make_params()
        if self.model_components is not None:
            for name, component in self.model_components.items():
                self._update_guess_from_component(guessed_params, component, name, x, y)
        else:
            self._update_guess_from_component(guessed_params, self.model, None, x, y)
        self._log_guess(guessed_params)
        return guessed_params

    @staticmethod
    def _update_guess_from_component(
        params: lmfit.Parameters,
        component: lmfit.Model,
        component_name: str | None,
        x: np.ndarray,
        y: np.ndarray,
    ) -> None:
        guess_fn = getattr(component, "guess", None)
        if not callable(guess_fn):
            return
        try:
            component_guess = guess_fn(y, x=x)
            params.update(component_guess)
        except Exception as guess_exc:
            name = component_name or component.__class__.__name__
            logger.debug(f"lmfit guess failed for component={name}: {guess_exc}")

    def _log_guess(self, guessed_params: lmfit.Parameters) -> None:
        logger.debug(
            f"Using lmfit guess params for model={self.model.__class__.__name__}: "
            f"{list(guessed_params.keys())}"
        )
        logger.debug(
            f"lmfit initial params for model={self.model.__class__.__name__}: "
            f"{serialize_lmfit_params(guessed_params)}"
        )

    @staticmethod
    def _apply_override_params(
        params: lmfit.Parameters, overrides: lmfit.Parameters
    ) -> lmfit.Parameters:
        for name, override in overrides.items():
            params[name].set(
                value=override.value,
                vary=override.vary,
                min=override.min,
                max=override.max,
                expr=override.expr,
                brute_step=getattr(override, "brute_step", None),
            )
        return params

    def _coerce_parameters(self, parameters: dict | list | tuple | lmfit.Parameters | None) -> dict:
        raw_parameters: dict = {}
        if not parameters:
            return raw_parameters
        if isinstance(parameters, lmfit.Parameters):
            if self.model_components is not None:
                raise DAPError(
                    "Composite models require parameters to be passed as a dict keyed by model name."
                )
            raw_parameters.update({name: param for name, param in parameters.items()})
            return raw_parameters
        if isinstance(parameters, (dict, list, tuple)):
            if self.model_components is not None:
                raw_parameters.update(self._expand_composite_parameters(parameters))
            elif isinstance(parameters, dict):
                raw_parameters.update(parameters)
            else:
                raise DAPError("Non-dict parameters are only supported for composite models.")
            return raw_parameters
        raise DAPError(
            f"Invalid parameters type {type(parameters)}. Expected dict or lmfit.Parameters."
        )

    def _filter_override_params(self, override_params: lmfit.Parameters) -> lmfit.Parameters:
        if not override_params:
            return override_params
        param_names = set(getattr(self.model, "param_names", []))
        model_params = self.model.make_params()
        model_param_names = set(model_params.keys())
        if not model_param_names:
            return override_params
        invalid_names = set(override_params.keys()) - model_param_names
        derived_names = model_param_names - param_names
        for name in list(override_params.keys()):
            if name in invalid_names:
                logger.warning(
                    f"Ignoring unknown lmfit parameter '{name}' for model '{self.model.__class__.__name__}'."
                )
                override_params.pop(name, None)
            elif name in derived_names:
                logger.debug(
                    f"Ignoring derived lmfit parameter '{name}' for model '{self.model.__class__.__name__}'."
                )
                override_params.pop(name, None)
        return override_params

    def _build_parameters_from_overrides(self, overrides: lmfit.Parameters) -> lmfit.Parameters:
        full_params = self.model.make_params()
        return self._apply_override_params(full_params, overrides)

    def _prepare_fit_params(self, x: np.ndarray, y: np.ndarray) -> lmfit.Parameters:
        if self.parameters is None:
            return self._guess_parameters(x, y)
        if self.override_params is not None and len(self.override_params) > 0:
            guessed_params = self._guess_parameters(x, y)
            return self._apply_override_params(guessed_params, self.override_params)
        return self.parameters

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
            status: (dict): Scan segment data
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
                    max_size=100,
                    expire=60,
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
        data_x: np.ndarray = None,
        data_y: np.ndarray = None,
        x_min: float = None,
        x_max: float = None,
        parameters: dict | list | None = None,
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
            data_x (np.ndarray): Data for x instead of a scan item
            data_y (np.ndarray): Data for y instead of a scan item
            x_min (float): Minimum x value
            x_max (float): Maximum x value
            parameters (dict | list): Fit parameters. For composite models, pass either
                a list aligned to the model list (each item is a param dict), or
                `{"ModelName": {"param": {...}}}` per model (unique model names only).
            oversample (int): Oversample factor
        """
        # we only receive scan IDs from the client. However, users may
        # pass in a scan item in the CLI which is converted to a scan ID
        # within BEC lib.

        self.oversample = oversample

        raw_parameters = self._coerce_parameters(parameters)
        if amplitude:
            raw_parameters["amplitude"] = amplitude
        if center:
            raw_parameters["center"] = center
        if sigma:
            raw_parameters["sigma"] = sigma

        override_params = deserialize_param_object(raw_parameters)
        override_params = self._filter_override_params(override_params)

        self._parameter_override_names = list(override_params.keys())
        self.override_params = override_params
        if len(override_params) > 0:
            self.parameters = self._build_parameters_from_overrides(override_params)
            logger.debug(
                f"Configured lmfit model={self.model.__class__.__name__} with override_params={serialize_lmfit_params(override_params)}"
            )
        else:
            self.parameters = None
            if parameters or amplitude or center or sigma:
                logger.debug(
                    f"No usable lmfit parameter overrides after validation for model={self.model.__class__.__name__} "
                    f"(input_keys={list(raw_parameters.keys())})"
                )

        if data_x is not None and data_y is not None:
            self.data = {
                "x": data_x,
                "y": data_y,
                "x_original": data_x,
                "x_lim": False,
                "scan_data": False,
            }
            return

        selected_device = kwargs.get("selected_device")
        if selected_device:
            device_y, signal_y = selected_device

        scan_id = scan_item
        if scan_id != self.scan_id or not self.current_scan_item:
            scan_item = self.client.queue.scan_storage.find_scan_by_ID(scan_id)
            self.scan_id = scan_id
        else:
            scan_item = self.current_scan_item

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
            "scan_data": True,
        }

    def process(self) -> tuple[dict, dict] | None:
        """
        Process data and return the result.

        Returns:
            tuple[dict, dict]: Processed data and metadata if successful, None otherwise.
        """
        # get the data
        if not self.data:
            return None

        x = self.data["x"]
        y = self.data["y"]

        # fit the data
        model_name = self.model.__class__.__name__
        if self.parameters:
            logger.debug(
                f"Running lmfit fit: model={model_name} points={len(x)} fixed/override_params={self._parameter_override_names}"
            )
        else:
            logger.debug(f"Running lmfit fit: model={model_name} points={len(x)} params=<default>")

        try:
            fit_params = self._prepare_fit_params(x, y)
            result = self.model.fit(y, x=x, params=fit_params)
        except Exception as exc:  # pylint: disable=broad-except
            if self.parameters is not None:
                try:
                    params_str = serialize_lmfit_params(self.parameters)
                except Exception as ser_exc:
                    params_str = f"<serialization failed: {ser_exc}>"
            else:
                params_str = "<None>"
            logger.warning(
                f"lmfit fit failed: model={model_name} points={len(x)} parameters={params_str} error={exc}"
            )
            return

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
        if self.data["scan_data"]:
            metadata["input"] = {
                "scan_id": self.scan_id,
                "device_x": self.device_x,
                "signal_x": self.signal_x,
                "device_y": self.device_y,
                "signal_y": self.signal_y,
                "parameters": serialize_lmfit_params(self.parameters),
            }
        else:
            metadata["input"] = {"parameters": serialize_lmfit_params(self.parameters)}
        metadata["fit_parameters"] = result.best_values
        metadata["fit_summary"] = result.summary()
        logger.debug(
            "fit summary: "
            f"model={model_name} chi-square={result.chisqr:.6g} "
            f"redchi={result.redchi:.6g} aic={result.aic:.6g} bic={result.bic:.6g}"
        )
        if self.model_components is not None:
            logger.debug(
                f"Composite lmfit best params for model={model_name}: {metadata['fit_parameters']}"
            )

        return (stream_output, metadata)
