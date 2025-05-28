"""
This module contains the DeviceManagerDS class, which is a subclass of
the DeviceManagerBase class and is the main device manager for devices
in BEC. It is the only place where devices are initialized and managed.
"""

from __future__ import annotations

import inspect
import time
import traceback
from typing import Callable

import numpy as np
import ophyd
import ophyd_devices as opd
from ophyd.ophydobj import OphydObject
from ophyd.signal import EpicsSignalBase
from typeguard import typechecked

from bec_lib import messages, plugin_helper
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.bec_service import BECService
from bec_lib.device import DeviceBase
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.utils.rpc_utils import rgetattr
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.device_serializer import get_device_info

logger = bec_logger.logger


class DSDevice(DeviceBase):
    def __init__(self, name, obj, config, parent=None):
        super().__init__(name=name, config=config, parent=parent)
        self.obj = obj
        self.metadata = {}
        self.initialized = False

    def __getattr__(self, name: str) -> inspect.Any:
        if hasattr(self.obj, name):
            # compatibility with ophyd devices accessed on the client side
            return rgetattr(self.obj, name)
        return super().__getattr__(name)

    def initialize_device_buffer(self, connector):
        """initialize the device read and readback buffer on redis with a new reading"""
        dev_msg = messages.DeviceMessage(signals=self.obj.read(), metadata={})

        if hasattr(self.obj, "low_limit_travel") and hasattr(self.obj, "high_limit_travel"):
            limits = {
                "low": {"value": self.obj.low_limit_travel.get()},
                "high": {"value": self.obj.high_limit_travel.get()},
            }
        else:
            limits = None
        pipe = connector.pipeline()
        connector.set_and_publish(MessageEndpoints.device_readback(self.name), dev_msg, pipe=pipe)
        connector.set_and_publish(
            topic=MessageEndpoints.device_read(self.name), msg=dev_msg, pipe=pipe
        )
        if not isinstance(self.obj, ophyd.Signal):
            # signals have the same read and read_configuration values; no need to publish twice
            dev_config_msg = messages.DeviceMessage(
                signals=self.obj.read_configuration(), metadata={}
            )
            connector.set_and_publish(
                MessageEndpoints.device_read_configuration(self.name), dev_config_msg, pipe=pipe
            )
        if limits is not None:
            connector.set_and_publish(
                MessageEndpoints.device_limits(self.name),
                messages.DeviceMessage(signals=limits),
                pipe=pipe,
            )
        pipe.execute()
        self.initialized = True


class DeviceManagerDS(DeviceManagerBase):
    def __init__(
        self,
        service: BECService,
        config_update_handler: ConfigUpdateHandler | None = None,
        status_cb: list[Callable] | Callable | None = None,
    ):
        super().__init__(service, status_cb)
        self._config_request_connector = None
        self._device_instructions_connector = None
        self._config_update_handler_cls = config_update_handler
        self.config_update_handler = None
        self.failed_devices = {}

    def initialize(self, bootstrap_server) -> None:
        self.config_update_handler = (
            self._config_update_handler_cls
            if self._config_update_handler_cls is not None
            else ConfigUpdateHandler(device_manager=self)
        )
        super().initialize(bootstrap_server)

    def _reload_action(self) -> None:
        pass

    def _add_action(self, msg: messages.DeviceConfigMessage) -> None:
        pass

    def _remove_action(self, msg: messages.DeviceConfigMessage) -> None:
        pass

    @staticmethod
    def _get_device_class(dev_type: str) -> type:
        """Get the device class from the device type"""
        return plugin_helper.get_plugin_class(dev_type, [opd, ophyd])

    def _load_session(self, *_args, **_kwargs):
        delayed_init = []
        if not self._is_config_valid():
            self._reset_config()
            return

        try:
            self.failed_devices = {}
            for dev in self._session["devices"]:
                name = dev.get("name")
                enabled = dev.get("enabled")
                logger.info(f"Adding device {name}: {'ENABLED' if enabled else 'DISABLED'}")
                dev_cls = self._get_device_class(dev.get("deviceClass"))
                if issubclass(dev_cls, (opd.DeviceProxy, opd.ComputedSignal)):
                    delayed_init.append(dev)
                    continue
                obj, config = self.construct_device_obj(dev, device_manager=self)
                try:
                    self.initialize_device(dev, config, obj)
                # pylint: disable=broad-except
                except Exception:
                    msg = traceback.format_exc()
                    logger.warning(f"Failed to initialize device {name}: {msg}")
                    self.failed_devices[name] = msg

            for dev in delayed_init:
                name = dev.get("name")
                obj, config = self.construct_device_obj(dev, device_manager=self)
                try:
                    self.initialize_delayed_devices(dev, config, obj)
                # pylint: disable=broad-except
                except Exception:
                    msg = traceback.format_exc()
                    logger.warning(f"Failed to initialize device {name}: {msg}")
                    self.failed_devices[name] = msg
            self.config_update_handler.handle_failed_device_inits()
        except Exception as exc:
            content = traceback.format_exc()
            logger.error(
                f"Failed to initialize device: {dev}: {content}. The config will be reset."
            )
            self._reset_config()
            raise DeviceConfigError(
                f"Failed to initialize device: {dev}: {content}. The config will be reset."
            ) from exc

    def initialize_delayed_devices(self, dev: dict, config: dict, obj: OphydObject) -> None:
        """Initialize delayed device after all other devices have been initialized."""
        name = dev.get("name")
        enabled = dev.get("enabled")
        logger.info(f"Adding device {name}: {'ENABLED' if enabled else 'DISABLED'}")

        obj = self.initialize_device(dev, config, obj)

        if hasattr(obj.obj, "lookup"):
            self._register_device_proxy(name)

    def _register_device_proxy(self, name: str) -> None:
        obj_lookup = self.devices.get(name).obj.lookup
        for key in obj_lookup.keys():
            signal_name = obj_lookup[key].get("signal_name")
            if key not in self.devices:
                raise DeviceConfigError(
                    f"Failed to init DeviceProxy {name}, no device {key} found in device manager."
                )
            dev_obj = self.devices[key].obj
            registered_proxies = dev_obj.registered_proxies
            if not hasattr(dev_obj, signal_name):
                raise DeviceConfigError(
                    f"Failed to init DeviceProxy {name}, no signal {signal_name} found for device {key}."
                )
            if key not in registered_proxies:
                # pylint: disable=protected-access
                self.devices[key].obj._registered_proxies.update({name: signal_name})
                continue
            if key in registered_proxies and signal_name not in registered_proxies[key]:
                # pylint: disable=protected-access
                self.devices[key].obj._registered_proxies.update({name: signal_name})
                continue
            if key in registered_proxies.keys() and signal_name in registered_proxies[key]:
                raise RuntimeError(
                    f"Failed to init DeviceProxy {name}, device {key} already has a registered DeviceProxy for {signal_name}. Only one DeviceProxy can be active per signal."
                )

    def _reset_config(self):
        """
        Reset the device config in redis and add the current config to the history.
        """
        current_config = self._session["devices"]
        if current_config:
            # store the current config in the history
            current_config_msg = messages.AvailableResourceMessage(
                resource=current_config, metadata={"removed_at": time.time()}
            )
            self.connector.lpush(
                MessageEndpoints.device_config_history(), current_config_msg, max_size=50
            )
        msg = messages.AvailableResourceMessage(resource=[])
        self.connector.set(MessageEndpoints.device_config(), msg)
        reload_msg = messages.DeviceConfigMessage(action="reload", config={})
        self.connector.send(MessageEndpoints.device_config_update(), reload_msg)

    @staticmethod
    def update_config(obj: OphydObject, config: dict) -> None:
        """Update an ophyd device's config

        Args:
            obj (Ophydobj): Ophyd object that should be updated
            config (dict): Config dictionary

        """
        if hasattr(obj, "_update_device_config"):
            obj._update_device_config(config)
        else:
            for config_key, config_value in config.items():
                # first handle the ophyd exceptions...
                if config_key == "limits":
                    if hasattr(obj, "low_limit_travel") and hasattr(obj, "high_limit_travel"):
                        low_limit_status = obj.low_limit_travel.set(config_value[0])
                        high_limit_status = obj.high_limit_travel.set(config_value[1])
                        low_limit_status.wait()
                        high_limit_status.wait()
                        continue
                if config_key == "labels":
                    if not config_value:
                        config_value = set()
                    # pylint: disable=protected-access
                    obj._ophyd_labels_ = set(config_value)
                    continue
                if not hasattr(obj, config_key):
                    raise DeviceConfigError(
                        f"Unknown config parameter {config_key} for device of type"
                        f" {obj.__class__.__name__}."
                    )

                config_attr = getattr(obj, config_key)
                if isinstance(config_attr, ophyd.Signal):
                    config_attr.set(config_value)
                elif callable(config_attr):
                    config_attr(config_value)
                else:
                    setattr(obj, config_key, config_value)

    @staticmethod
    def construct_device_obj(dev: dict, device_manager: DeviceManagerDS) -> (OphydObject, dict):
        """
        Construct a device object from a device config dictionary.

        Args:
            dev (dict): device config dictionary
            device_manager (DeviceManagerDS): device manager instance

        Returns:
            (OphydObject, dict): device object and updated config dictionary
        """
        name = dev.get("name")
        dev_cls = DeviceManagerDS._get_device_class(dev["deviceClass"])
        device_config = dev.get("deviceConfig")
        device_config = device_config if device_config is not None else {}
        config = device_config.copy()
        config["name"] = name

        # pylint: disable=protected-access
        device_classes = [dev_cls]
        if issubclass(dev_cls, ophyd.Signal):
            device_classes.append(ophyd.Signal)
        if issubclass(dev_cls, EpicsSignalBase):
            device_classes.append(EpicsSignalBase)
        if issubclass(dev_cls, ophyd.OphydObject):
            device_classes.append(ophyd.OphydObject)

        # get all init parameters of the device class and its parents
        class_params = set()
        for device_class in device_classes:
            class_params.update(inspect.signature(device_class)._parameters)
        class_params_and_config_keys = class_params & config.keys()

        init_kwargs = {key: config.pop(key) for key in class_params_and_config_keys}
        device_access = config.pop("device_access", None)
        if device_access or (device_access is None and config.get("device_mapping")):
            init_kwargs["device_manager"] = device_manager

        signature = inspect.signature(dev_cls)
        if "device_manager" in signature.parameters:
            init_kwargs["device_manager"] = device_manager
        if "scan_info" in signature.parameters:
            # Additional device_manager != None is needed for static_device_test which
            # uses the static method with device_manager=None
            init_kwargs["scan_info"] = device_manager.scan_info if device_manager else None

        # initialize the device object
        obj = dev_cls(**init_kwargs)
        return obj, config

    def initialize_device(self, dev: dict, config: dict, obj: OphydObject) -> DSDevice:
        """
        Prepares a device for later usage.
        This includes inspecting the device class signature,
        initializing the object, refreshing the device info and buffer,
        as well as adding subscriptions.

        Args:
            dev (dict): device config dictionary
            config (dict): device config dictionary
            obj (OphydObject): device object

        Returns:
            DSDevice: initialized device object
        """
        name = dev.get("name")
        enabled = dev.get("enabled")

        # refresh the device info
        pipe = self.connector.pipeline()
        self.reset_device_data(obj, pipe)
        raised_exc = self.publish_device_info(obj, pipe)
        pipe.execute()

        # insert the created device obj into the device manager
        opaas_obj = DSDevice(name=name, obj=obj, config=dev, parent=self)

        # pylint:disable=protected-access # this function is shared with clients and it is currently not foreseen that clients add new devices
        self.devices._add_device(name, opaas_obj)

        if raised_exc:
            raise raised_exc

        if not enabled:
            return opaas_obj

        self.initialize_enabled_device(opaas_obj)

        self.update_config(obj, config)

        obj = opaas_obj.obj
        # add subscriptions
        if not hasattr(obj, "event_types"):
            return opaas_obj
        self._subscribe_to_device_events(obj, opaas_obj)

        return opaas_obj

    def _subscribe_to_device_events(self, obj: OphydObject, opaas_obj: DSDevice):
        """Subscribe to device events"""

        if "readback" in obj.event_types:
            obj.subscribe(self._obj_callback_readback, event_type="readback", run=opaas_obj.enabled)
        elif "value" in obj.event_types:
            obj.subscribe(self._obj_callback_readback, event_type="value", run=opaas_obj.enabled)
        if "device_monitor_2d" in obj.event_types:
            obj.subscribe(
                self._obj_callback_device_monitor_2d, event_type="device_monitor_2d", run=False
            )
        if "device_monitor_1d" in obj.event_types:
            obj.subscribe(
                self._obj_callback_device_monitor_1d, event_type="device_monitor_1d", run=False
            )
        if "file_event" in obj.event_types:
            obj.subscribe(self._obj_callback_file_event, event_type="file_event", run=False)
        if "done_moving" in obj.event_types:
            obj.subscribe(self._obj_callback_done_moving, event_type="done_moving", run=False)
        if "flyer" in obj.event_types:
            obj.subscribe(self._obj_flyer_callback, event_type="flyer", run=False)
        if "progress" in obj.event_types:
            obj.subscribe(self._obj_callback_progress, event_type="progress", run=False)

        if hasattr(obj, "motor_is_moving"):
            obj.motor_is_moving.subscribe(self._obj_callback_is_moving, run=opaas_obj.enabled)

        if hasattr(obj, "component_names"):
            for component_name in obj.component_names:
                component = getattr(obj, component_name)
                if not getattr(component, "_auto_monitor", False):
                    continue
                if component.kind in (ophyd.Kind.normal, ophyd.Kind.hinted):
                    component.subscribe(self._obj_callback_readback, run=False)
                elif component.kind == ophyd.Kind.config:
                    component.subscribe(self._obj_callback_configuration, run=False)

    def initialize_enabled_device(self, opaas_obj):
        """connect to an enabled device and initialize the device buffer"""
        self.connect_device(opaas_obj.obj)
        if hasattr(opaas_obj.obj, "on_connected"):
            opaas_obj.obj.on_connected()
        opaas_obj.initialize_device_buffer(self.connector)

    @staticmethod
    def disconnect_device(obj):
        """disconnect from a device"""
        if not obj.connected:
            return
        if hasattr(obj, "controller"):
            obj.controller.off()
            return
        obj.destroy()

    def reset_device(self, obj: DSDevice):
        """reset a device"""
        obj.initialized = False

    @staticmethod
    def connect_device(obj, wait_for_all=False):
        """establish a connection to a device"""
        try:
            if obj.connected:
                return
            if hasattr(obj, "controller"):
                obj.controller.on()
                return
            if hasattr(obj, "wait_for_connection"):
                try:
                    obj.wait_for_connection(all_signals=wait_for_all, timeout=30)
                except TypeError:
                    obj.wait_for_connection(timeout=30)
                return
            logger.error(
                f"Device {obj.name} does not implement the socket controller interface nor"
                " wait_for_connection and cannot be turned on."
            )
            raise ConnectionError(f"Failed to establish a connection to device {obj.name}")
        except Exception:
            error_traceback = traceback.format_exc()
            logger.error(f"{error_traceback}. Failed to connect to {obj.name}.")
            raise ConnectionError(f"Failed to establish a connection to device {obj.name}")

    def publish_device_info(self, obj: OphydObject, pipe=None) -> None | Exception:
        """
        Publish the device info to redis. The device info contains
        inter alia the class name, user functions and signals.

        Args:
            obj (_type_): _description_
        """
        try:
            interface = get_device_info(obj)
            self.connector.set(
                MessageEndpoints.device_info(obj.name),
                messages.DeviceInfoMessage(device=obj.name, info=interface),
                pipe,
            )
        except Exception as exc:
            logger.error(f"Failed to get device info for {obj.name}: {exc}")
            interface = get_device_info(obj, connect=False)
            self.connector.set(
                MessageEndpoints.device_info(obj.name),
                messages.DeviceInfoMessage(device=obj.name, info=interface),
                pipe,
            )
            return exc

    def reset_device_data(self, obj: OphydObject, pipe=None) -> None:
        """delete all device data and device info"""
        self.connector.delete(MessageEndpoints.device_status(obj.name), pipe)
        self.connector.delete(MessageEndpoints.device_read(obj.name), pipe)
        self.connector.delete(MessageEndpoints.device_read_configuration(obj.name), pipe)
        self.connector.delete(MessageEndpoints.device_info(obj.name), pipe)

    def _obj_callback_readback(self, *_args, obj: OphydObject, **kwargs):
        if not obj.connected:
            return
        name = obj.root.name
        signals = obj.root.read()
        metadata = self.devices.get(obj.root.name).metadata
        dev_msg = messages.DeviceMessage(signals=signals, metadata=metadata)
        pipe = self.connector.pipeline()
        self.connector.set_and_publish(MessageEndpoints.device_readback(name), dev_msg, pipe)
        pipe.execute()

    def _obj_callback_configuration(self, *_args, obj: OphydObject, **kwargs):
        if not obj.connected:
            return
        if isinstance(obj.root, ophyd.Signal):
            # we don't need to publish the configuration of a signal
            return
        name = obj.root.name
        signals = obj.root.read_configuration()
        metadata = self.devices.get(obj.root.name).metadata
        dev_msg = messages.DeviceMessage(signals=signals, metadata=metadata)
        pipe = self.connector.pipeline()
        self.connector.set_and_publish(
            MessageEndpoints.device_read_configuration(name), dev_msg, pipe
        )
        pipe.execute()

    @typechecked
    def _obj_callback_device_monitor_2d(
        self, *_args, obj: OphydObject, value: np.ndarray, timestamp: float | None = None, **kwargs
    ):
        """
        Callback for ophyd monitor events. Sends the data to redis.
        Introduces a check of the data size, and incoporates a limit which is defined in max_size (in MB)

        Args:
            obj (OphydObject): ophyd object
            value (np.ndarray): data from ophyd device

        """
        # Convert sizes from bytes to MB
        dsize = len(value.tobytes()) / 1e6
        max_size = 1000
        if dsize > max_size:
            logger.warning(
                f"Data size of single message is too large to send, current max_size {max_size}."
            )
            return
        if obj.connected:
            name = obj.root.name
            metadata = self.devices[name].metadata
            msg = messages.DeviceMonitor2DMessage(
                device=name,
                data=value,
                metadata=metadata,
                timestamp=timestamp if timestamp else time.time(),
            )
            stream_msg = {"data": msg}
            self.connector.xadd(
                MessageEndpoints.device_monitor_2d(name),
                stream_msg,
                max_size=min(100, int(max_size // dsize)),
            )

    def _obj_callback_device_monitor_1d(
        self, *_args, obj: OphydObject, value: np.ndarray, timestamp: float | None = None, **kwargs
    ):
        """
        Callback for ophyd monitor events. Sends the data to redis.
        Introduces a check of the data size, and incoporates a limit which is defined in max_size (in MB)

        Args:
            obj (OphydObject): ophyd object
            value (np.ndarray): data from ophyd device

        """
        # Convert sizes from bytes to MB
        dsize = len(value.tobytes()) / 1e6
        max_size = 1000
        if dsize > max_size:
            logger.warning(
                f"Data size of single message is too large to send, current max_size {max_size}."
            )
            return
        if obj.connected:
            name = obj.root.name
            metadata = self.devices[name].metadata
            msg = messages.DeviceMonitor1DMessage(
                device=name,
                data=value,
                metadata=metadata,
                timestamp=timestamp if timestamp else time.time(),
            )
            stream_msg = {"data": msg}
            self.connector.xadd(
                MessageEndpoints.device_monitor_1d(name),
                stream_msg,
                max_size=min(100, int(max_size // dsize)),
            )

    def _obj_callback_acq_done(self, *_args, **kwargs):
        device = kwargs["obj"].root.name
        status = 0
        metadata = self.devices[device].metadata
        self.connector.set(
            MessageEndpoints.device_status(device),
            messages.DeviceStatusMessage(device=device, status=status, metadata=metadata),
        )

    def _obj_callback_done_moving(self, *args, **kwargs):
        self._obj_callback_readback(*args, **kwargs)
        # self._obj_callback_acq_done(*args, **kwargs)

    def _obj_callback_is_moving(self, *_args, **kwargs):
        device = kwargs["obj"].root.name
        status = int(kwargs.get("value"))
        metadata = self.devices[device].metadata
        self.connector.set(
            MessageEndpoints.device_status(device),
            messages.DeviceStatusMessage(device=device, status=status, metadata=metadata),
        )

    def _obj_flyer_callback(self, *_args, **kwargs):
        obj = kwargs["obj"]
        logger.warning(
            f"Flyer callback will be deprecated in future, please refactor your device {obj.root.name} in favor of an async devices as soon as possible."
        )
        data = kwargs["value"].get("data")
        ds_obj = self.devices[obj.root.name]
        metadata = ds_obj.metadata
        if "scan_id" not in metadata:
            return

        if not hasattr(ds_obj, "emitted_points"):
            ds_obj.emitted_points = {}

        emitted_points = ds_obj.emitted_points.get(metadata["scan_id"], 0)

        # make sure all arrays are of equal length
        max_points = min(len(d) for d in data.values())

        pipe = self.connector.pipeline()
        for ii in range(emitted_points, max_points):
            timestamp = time.time()
            signals = {}
            for key, val in data.items():
                signals[key] = {"value": val[ii], "timestamp": timestamp}
            msg = messages.DeviceMessage(signals=signals, metadata={"point_id": ii, **metadata})
            self.connector.set_and_publish(
                MessageEndpoints.device_read(obj.root.name), msg, pipe=pipe
            )

        ds_obj.emitted_points[metadata["scan_id"]] = max_points
        msg = messages.DeviceStatusMessage(
            device=obj.root.name, status=max_points, metadata=metadata
        )
        self.connector.set(MessageEndpoints.device_status(obj.root.name), msg, pipe=pipe)
        pipe.execute()

    def _obj_callback_progress(self, *_args, obj, value, max_value, done, **kwargs):
        metadata = self.devices[obj.root.name].metadata
        msg = messages.ProgressMessage(
            value=value, max_value=max_value, done=done, metadata=metadata
        )
        self.connector.set_and_publish(MessageEndpoints.device_progress(obj.root.name), msg)

    def _obj_callback_file_event(
        self,
        *_args,
        obj,
        file_path: str,
        done: bool,
        successful: bool,
        file_type: str = "h5",
        hinted_h5_entries: dict[str, str] | None = None,
        **kwargs,
    ):
        """Callback for file events on devices. This callback set and publishes
        a file message to the file_event and public_file endpoints in Redis to inform
        the file writer and other services about externally created files.

        Args:
            obj (OphydObject): ophyd object
            file_path (str): file path to the created file
            done (bool): if the file is done
            successfull (bool): if the file was created successfully
            file_type (str): Optional, file type. Default is h5.
            hinted_h5_entry (dict[str, str] | None): Optional, hinted h5 entry. Please check FileMessage for more details
        """
        device_name = obj.root.name
        metadata = self.devices[device_name].metadata
        if kwargs.get("metadata") is not None:
            metadata.update(kwargs.get("metadata"))
        scan_id = metadata.get("scan_id")
        msg = messages.FileMessage(
            file_path=file_path,
            done=done,
            successful=successful,
            file_type=file_type,
            device_name=device_name,
            is_master_file=False,
            hinted_h5_entries=hinted_h5_entries,
            metadata=metadata,
        )
        pipe = self.connector.pipeline()
        self.connector.set_and_publish(MessageEndpoints.file_event(device_name), msg, pipe=pipe)
        self.connector.set_and_publish(
            MessageEndpoints.public_file(scan_id=scan_id, name=device_name), msg, pipe=pipe
        )
        pipe.execute()
