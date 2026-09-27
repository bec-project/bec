"""
This module contains the DeviceManagerDS class, which is a subclass of
the DeviceManagerBase class and is the main device manager for devices
in BEC. It is the only place where devices are initialized and managed.
"""

from __future__ import annotations

import inspect
import threading
import time
import traceback
from collections import deque
from collections.abc import Callable
from contextlib import ExitStack, nullcontext
from typing import TYPE_CHECKING, Any

import numpy as np
import ophyd
import ophyd_devices as opd
from ophyd.ophydobj import OphydObject
from ophyd.signal import EpicsSignalBase
from ophyd_devices.utils.bec_signals import BECMessageSignal
from typeguard import typechecked

from bec_lib import messages, plugin_helper
from bec_lib.alarm_handler import Alarms
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.bec_service import BECService
from bec_lib.device import DeviceBaseWithConfig
from bec_lib.devicemanager import CancelledError, DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.utils.rpc_utils import rgetattr
from bec_server.device_server.bec_message_handler import BECMessageHandler
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.device_serializer import (
    disable_lazy_wait_for_connection,
    get_device_info,
)
from bec_server.device_server.devices.event_dispatcher import DeviceEventDispatcher

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.redis_connector import RedisConnector
    from bec_server.device_server.devices.event_dispatcher import Domain, Readings, ReadToken


logger = bec_logger.logger
StatusCallback = Callable[[messages.BECStatus], None]


class DeviceProgress:
    """
    Class to track and publish device initialization progress.
    """

    def __init__(self, connector: RedisConnector, all_devices: list[dict]):
        """
        Initialize the DeviceProgress class.

        Args:
            connector (RedisConnector): Redis connector to publish progress messages.
            all_devices (list[dict]): List of all device configurations.
        """
        self.connector = connector
        self.all_devices = all_devices
        self.total_devices = len(all_devices)
        self.initialized_devices = 0

    def update_progress(self, device_name: str, finished: bool, success: bool) -> None:
        """
        Update the device initialization progress and publish a progress message.

        Args:
            device_name (str): Name of the device being initialized.
            finished (bool): Whether the device initialization is finished.
            success (bool): Whether the device initialization was successful.
        """
        if finished:
            self.initialized_devices += 1

        progress_msg = messages.DeviceInitializationProgressMessage(
            device=device_name,
            finished=finished,
            index=self.initialized_devices,
            total=self.total_devices,
            success=success,
        )
        self.connector.set_and_publish(
            MessageEndpoints.device_initialization_progress(), progress_msg, expire=3600
        )


class DSDevice(DeviceBaseWithConfig):
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

    def initialize_device_buffer(
        self, connector: RedisConnector, dispatcher: DeviceEventDispatcher | None = None
    ) -> None:
        """Seed Redis and event snapshots together from the same baseline reads.

        Args:
            connector (RedisConnector): Connector used to publish the initial readings.
            dispatcher (DeviceEventDispatcher | None): Dispatcher whose snapshots are seeded
                after the Redis pipeline succeeds. Defaults to None.
        """
        readers: dict[Domain, Callable[[], Readings]] = {"readback": self.obj.read}
        if not isinstance(self.obj, ophyd.Signal):
            readers["configuration"] = self.obj.read_configuration
        low_limit = getattr(self.obj, "low_limit_travel", None)
        high_limit = getattr(self.obj, "high_limit_travel", None)
        if low_limit is not None and high_limit is not None:
            readers["limits"] = lambda: {
                "low": {"value": low_limit.get()},
                "high": {"value": high_limit.get()},
            }
        endpoints = {
            "readback": MessageEndpoints.device_readback,
            "configuration": MessageEndpoints.device_read_configuration,
            "limits": MessageEndpoints.device_limits,
        }
        with ExitStack() as stack:
            snapshots: dict[Domain, ReadToken | None] = {
                domain: stack.enter_context(
                    dispatcher.read_context(self.obj, domain) if dispatcher else nullcontext()
                )
                for domain in readers
            }
            pipe = connector.pipeline()
            readings: dict[Domain, Readings] = {}
            for domain, read in readers.items():
                readings[domain] = read()
                msg = messages.DeviceMessage(signals=readings[domain], metadata={})
                connector.set_and_publish(endpoints[domain](self.name), msg, pipe=pipe)
                if domain == "readback":
                    connector.set_and_publish(
                        MessageEndpoints.device_read(self.name), msg, pipe=pipe
                    )
            pipe.execute()
            for domain, snapshot in snapshots.items():
                if snapshot is not None:
                    snapshot.update(readings[domain], {})
        self.initialized = True


class DeviceManagerDS(DeviceManagerBase):
    def __init__(
        self,
        service: BECService,
        config_update_handler: ConfigUpdateHandler | None = None,
        status_cb: list[StatusCallback] | StatusCallback | None = None,
    ) -> None:
        """Initialize the device manager and its event dispatcher.

        Args:
            service (BECService): Service that owns this device manager.
            config_update_handler (ConfigUpdateHandler | None): Optional existing handler
                for device configuration requests. Defaults to None.
            status_cb (list[StatusCallback] | StatusCallback | None): Callbacks receiving each new
                BECStatus. Defaults to None.
        """
        super().__init__(service, status_cb)
        self._use_proxy_objects = False
        self._config_request_connector = None
        self._device_instructions_connector = None
        self._config_update_handler_cls = config_update_handler
        self.config_update_handler = None
        self.failed_devices = {}
        self._bec_message_handler = BECMessageHandler(self)
        self._device_order_map = {}
        self.event_dispatcher = DeviceEventDispatcher(lambda: self.connector)

    def initialize(self, bootstrap_server) -> None:
        self.config_update_handler = (
            self._config_update_handler_cls
            if self._config_update_handler_cls is not None
            else ConfigUpdateHandler(device_manager=self)
        )
        super().initialize(bootstrap_server)

    @property
    def current_session(self) -> dict:
        """
        Get the current device session.
        Please note that the internal _session variable is private as it is shared across
        multiple services and typically should not be accessed directly.
        """
        return self._session

    @staticmethod
    def _get_device_class(dev_type: str) -> type:
        """Get the device class from the device type"""
        return plugin_helper.get_plugin_class(dev_type, [opd, ophyd])

    def _init_device(self, device_info: dict, delayed: bool, progress: DeviceProgress) -> None:
        """
        Initialize a device from its configuration dictionary.

        Args:
            device_info (dict): Device configuration dictionary.
            delayed (bool): Whether to initialize the device in delayed mode.
            progress (DeviceProgress): DeviceProgress instance to track initialization progress.
        """

        name = device_info.get("name")
        success = True
        progress.update_progress(device_name=name, finished=False, success=success)

        obj, config = self.construct_device_obj(device_info, device_manager=self)
        try:
            if delayed:
                self.initialize_delayed_devices(device_info, config, obj)
            else:
                self.initialize_device(device_info, config, obj)
        # pylint: disable=broad-except
        except Exception:
            if name not in self.devices:
                raise
            msg = traceback.format_exc()
            logger.warning(f"Failed to initialize device {name}: {msg}")
            self.failed_devices[name] = msg
            success = False
        finally:
            progress.update_progress(device_name=name, finished=True, success=success)

    def _load_session(self, *_args, cancel_event: threading.Event | None = None, **_kwargs):
        delayed_init = []
        if not self._is_config_valid():
            self._reset_config()
            return

        progress = DeviceProgress(self.connector, self._session["devices"])
        current_device_name = None
        try:
            devices = self.resolve_device_dependencies(self.current_session["devices"])
            self.failed_devices = {}
            for dev in devices:
                name = dev.get("name")
                enabled = dev.get("enabled")

                if cancel_event and cancel_event.is_set():
                    raise CancelledError("Device initialization cancelled.")
                logger.info(f"Adding device {name}: {'ENABLED' if enabled else 'DISABLED'}")
                current_device_name = name

                dev_cls = self._get_device_class(dev.get("deviceClass"))
                if issubclass(dev_cls, (opd.DeviceProxy, opd.ComputedSignal)):
                    delayed_init.append(dev)
                    continue

                self._init_device(dev, delayed=False, progress=progress)
                current_device_name = None

            for dev in delayed_init:
                name = dev.get("name")
                if cancel_event and cancel_event.is_set():
                    raise CancelledError("Device initialization cancelled.")
                current_device_name = name
                self._init_device(dev, delayed=True, progress=progress)
                current_device_name = None

            self.config_update_handler.handle_failed_device_inits()
        except CancelledError:
            self._reset_config()
            raise
        except Exception as exc:
            content = traceback.format_exc()
            logger.error(
                f"Failed to initialize device: {current_device_name}: {content}. The config will be reset."
            )
            self._reset_config()
            raise DeviceConfigError(
                f"Failed to initialize device: {current_device_name}: {content}. The config will be reset."
            ) from exc

    def resolve_device_dependencies(self, devices: list[dict]) -> list[dict]:
        """
        Resolve device dependencies and return a sorted list of devices. It uses
        the device's "needs" field of the config to determine dependencies.

        Using Kahn's algorithm for topological sorting.

        Args:
            devices (list[dict]): List of device config dictionaries
        Returns:
            list[dict]: Sorted list of device config dictionaries
        """
        device_dict = {dev["name"]: dev for dev in devices}
        in_degree = {dev["name"]: 0 for dev in devices}
        adj_list = {dev["name"]: [] for dev in devices}

        for dev in devices:
            needs = dev.get("needs", [])
            for dep in needs:
                if dep not in device_dict:
                    raise DeviceConfigError(f"Device {dev['name']} needs unknown device {dep}.")
                if device_dict[dep].get("enabled") is False:
                    self.connector.raise_alarm(
                        Alarms.WARNING,
                        messages.ErrorInfo(
                            error_message=f"Device {dev['name']} depends on disabled device {dep}.",
                            compact_error_message=f"Dependency on disabled device {dep}.",
                            exception_type="Warning",
                            device=dev["name"],
                        ),
                    )
                adj_list[dep].append(dev["name"])
                in_degree[dev["name"]] += 1

        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        sorted_devices = []

        while queue:
            current = queue.popleft()
            sorted_devices.append(device_dict[current])
            for neighbor in adj_list[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        cyclic_devices = [name for name, degree in in_degree.items() if degree > 0]
        if cyclic_devices:
            raise DeviceConfigError(f"Cyclic dependency detected among devices: {cyclic_devices}")

        self._device_order_map = {dev["name"]: idx for idx, dev in enumerate(sorted_devices)}

        return sorted_devices

    def get_device_order(self, device_names: list[str]) -> list[str]:
        """
        Get the device names sorted by their initialization order.

        Args:
            device_names (list[str]): List of device names to sort.

        Returns:
            list[str]: Sorted list of device names.

        Raises:
            RuntimeError: If the device order map is not initialized.
        """
        if not self._device_order_map:
            raise RuntimeError("Device order map is not initialized.")
        return sorted(
            device_names,
            key=lambda name: self._device_order_map.get(name.split(".")[0], float("inf")),
        )

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

    def update_config(
        self, obj: OphydObject, config: dict, *, refresh_subscriptions: bool = False
    ) -> None:
        """Update a device's configuration and refresh its event subscriptions.

        Args:
            obj (OphydObject): Device whose configuration should be updated.
            config (dict): Device-specific configuration values to apply.
            refresh_subscriptions (bool): Rebuild subscriptions even for an empty rollback
                configuration. Defaults to False.

        Raises:
            DeviceConfigError: A configuration key does not exist on the device.
            TimeoutError: A signal update or event-subscription retirement times out.
        """
        if hasattr(obj, "_update_device_config"):
            # If the device has implemented its own config update method, use it
            # pylint: disable=protected-access
            obj._update_device_config(config)  # type: ignore
            self._refresh_event_subscriptions(obj)
            return

        for config_key, config_value in config.items():
            # first handle the ophyd exceptions...
            if config_key == "limits":
                if hasattr(obj, "low_limit_travel") and hasattr(obj, "high_limit_travel"):
                    low_limit_status = obj.low_limit_travel.set(config_value[0])  # type: ignore
                    high_limit_status = obj.high_limit_travel.set(config_value[1])  # type: ignore
                    # Respect Timeout to avoid blocking the device server indefinitely
                    low_limit_status.wait(timeout=2)
                    high_limit_status.wait(timeout=2)
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
                config_attr.set(config_value).wait(timeout=2)
            elif callable(config_attr):
                config_attr(config_value)
            else:
                setattr(obj, config_key, config_value)

        if config or refresh_subscriptions:
            self._refresh_event_subscriptions(obj)

        self.connector.publish_metrics("device_server", {"num_devices": len(self.devices)})

    @staticmethod
    def construct_device_obj(
        dev: dict, device_manager: DeviceManagerDS
    ) -> tuple[OphydObject, dict]:
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
        """Prepare a device and its subscriptions for later use.

        Register the device, refresh its information and buffers, then apply its
        initial settings after enabling it.

        Args:
            dev (dict): Device configuration record, including its name and enabled state.
            config (dict): Initial device attributes and signal values.
            obj (OphydObject): Constructed device object to register.

        Returns:
            DSDevice: Initialized device-server wrapper.

        Raises:
            TimeoutError: The previous device instance still has an active event operation.
        """
        name = dev.get("name")
        enabled = dev.get("enabled")

        previous = self.devices.get(name)
        if previous is not None and not self.event_dispatcher.remove(previous.obj):
            raise TimeoutError(f"Device {name} still has an active event operation")

        # refresh the device info
        pipe = self.connector.pipeline()
        self.reset_device_data(obj, pipe)
        raised_exc = None
        connect = False
        if enabled:
            # Try to connect to the device, needs wait_for_all to include lazy signals e.g. AD detectors
            raised_exc = self.connect_device(
                obj, wait_for_all=True, timeout=dev.get("connectionTimeout", 5)
            )
            # Publish device info with connect = True if no exception was raised during connection
            # Otherwise publish with connect = False
            connect = raised_exc is None
        # If .describe() fails for connect=True, we rerun with connect=False
        # and return the exception. This will later be raised even if
        # connect_device succeeded.
        publish_device_exc = self.publish_device_info(obj, connect=connect, pipe=pipe)
        pipe.execute()

        # insert the created device obj into the device manager
        opaas_obj = DSDevice(name=name, obj=obj, config=dev, parent=self)

        # pylint:disable=protected-access # this function is shared with clients and it is currently not foreseen that clients add new devices
        self.devices._add_device(name, opaas_obj)

        if raised_exc:
            raise raised_exc

        if publish_device_exc:
            raise publish_device_exc

        if not enabled:
            return opaas_obj

        self.initialize_enabled_device(opaas_obj)

        # Update the config at last as this may also set signals
        self.update_config(obj, config)

        return opaas_obj

    def _subscribe_to_limit_updates(self, obj: OphydObject) -> None:
        """Subscribe to each available low and high travel-limit signal.

        Args:
            obj (OphydObject): Device whose travel-limit signals should be monitored.
        """
        for attr in ("low_limit_travel", "high_limit_travel"):
            signal = getattr(obj, attr, None)
            if signal is not None and hasattr(signal, "subscribe"):
                self._subscribe(signal, self._obj_callback_auto_monitor_limits, run=False)

    def _subscribe_to_device_events(self, obj: OphydObject, opaas_obj: DSDevice) -> None:
        """Subscribe to the device's readback and moving-state events.

        Args:
            obj (OphydObject): Device that emits the events.
            opaas_obj (DSDevice): Managed device whose enabled state controls initial callbacks.
        """
        if "readback" in obj.event_types:
            self._subscribe(
                obj, self._obj_callback_readback, event_type="readback", run=opaas_obj.enabled
            )
        elif "value" in obj.event_types:
            self._subscribe(
                obj, self._obj_callback_readback, event_type="value", run=opaas_obj.enabled
            )
        moving_signal = getattr(obj, "motor_is_moving", None)
        if moving_signal is not None:
            self._subscribe(moving_signal, self._obj_callback_is_moving, run=opaas_obj.enabled)

    def _subscribe_to_bec_device_events(self, obj: OphydObject) -> None:
        """Subscribe to legacy BEC device events.

        Monitor, file, movement, flyer, and progress events are deprecated in favor
        of the signals handled by ``_subscribe_to_bec_signals``.

        Args:
            obj (OphydObject): Device whose supported legacy events should be subscribed.
        """
        if "device_monitor_2d" in obj.event_types:
            self._subscribe(
                obj, self._obj_callback_device_monitor_2d, event_type="device_monitor_2d", run=False
            )
        if "device_monitor_1d" in obj.event_types:
            self._subscribe(
                obj, self._obj_callback_device_monitor_1d, event_type="device_monitor_1d", run=False
            )
        if "file_event" in obj.event_types:
            self._subscribe(obj, self._obj_callback_file_event, event_type="file_event", run=False)
        if "done_moving" in obj.event_types:
            self._subscribe(
                obj, self._obj_callback_done_moving, event_type="done_moving", run=False
            )
        if "flyer" in obj.event_types:
            self._subscribe(obj, self._obj_flyer_callback, event_type="flyer", run=False)
        if "progress" in obj.event_types:
            self._subscribe(obj, self._obj_callback_progress, event_type="progress", run=False)

    def _subscribe_to_auto_monitors(self, obj: OphydObject) -> None:
        """Subscribe to components that enable automatic monitoring.

        Use each component's kind to select readback and configuration callbacks.

        Args:
            obj (OphydObject): Device whose components should be inspected recursively.
        """
        if not hasattr(obj, "component_names"):
            return

        for component_name in obj.component_names:  # type: ignore
            component = getattr(obj, component_name)
            if hasattr(component, "component_names"):
                self._subscribe_to_auto_monitors(component)
                continue
            if not getattr(component, "_auto_monitor", False):
                continue
            if component.kind & ophyd.Kind.normal:
                self._subscribe(component, self._obj_callback_auto_monitor_readback, run=False)
            if component.kind & ophyd.Kind.config:
                self._subscribe(component, self._obj_callback_auto_monitor_configuration, run=False)

    def _subscribe_to_bec_signals(self, obj: OphydObject) -> None:
        """Subscribe to BEC preview, progress, file, and other message signals.

        Args:
            obj (OphydObject): Device whose BEC message signals should be subscribed.
        """
        if not hasattr(obj, "walk_signals"):
            # If the object does not have walk_components, it is likely a simple signal
            return
        signal_walk = obj.walk_signals()  # type: ignore
        for _ancestor, _signal_name, signal in signal_walk:
            if isinstance(signal, BECMessageSignal):
                self._subscribe(signal, callback=self._obj_callback_bec_message_signal, run=False)

    def _subscribe(
        self,
        obj: OphydObject,
        callback: Callable[..., None],
        *,
        event_type: str | None = None,
        run: bool = True,
    ) -> int:
        """Track a subscription so retired device instances cannot dispatch again.

        Args:
            obj (OphydObject): Device or signal that emits the event.
            callback (Callable[..., None]): Callback registered with the object.
            event_type (str | None): Event name, or None to use the object's default event.
            run (bool): Whether to immediately replay the object's cached event.

        Returns:
            int: Subscription identifier retained for cleanup.
        """
        domain: Domain = "status"
        domain_handlers: tuple[tuple[Domain, tuple[Callable[..., None], ...]], ...] = (
            ("readback", (self._obj_callback_readback, self._obj_callback_auto_monitor_readback)),
            (
                "configuration",
                (self._obj_callback_configuration, self._obj_callback_auto_monitor_configuration),
            ),
            ("limits", (self._obj_callback_limit_change, self._obj_callback_auto_monitor_limits)),
        )
        for event_domain, handlers in domain_handlers:
            if callback in handlers:
                domain = event_domain
                break
        return self.event_dispatcher.subscribe(
            obj, callback, event_type=event_type, run=run, domain=domain
        )

    def initialize_enabled_device(self, opaas_obj: DSDevice) -> None:
        """Subscribe before seeding snapshots to retain racing monitor updates.

        Args:
            opaas_obj (DSDevice): Enabled device to connect and initialize.
        """
        obj = opaas_obj.obj
        if hasattr(obj, "on_connected"):
            obj.on_connected()
        self._bind_event_subscriptions(opaas_obj)

    def _refresh_event_subscriptions(self, obj: OphydObject) -> None:
        """Refresh snapshots and reconcile bindings without removing unchanged callbacks.

        Args:
            obj (OphydObject): Device or component whose initialized root should be refreshed.

        Raises:
            TimeoutError: The root still has an active event operation.
        """
        device = self.devices.get(obj.root.name)
        if device is not None and device.obj is obj.root and device.initialized:
            with self.event_dispatcher.reconfigure(obj.root):
                self._bind_event_subscriptions(device)

    def _bind_event_subscriptions(self, opaas_obj: DSDevice) -> None:
        """Reuse existing callbacks and seed snapshots, retaining failed baselines for retry.

        Args:
            opaas_obj (DSDevice): Managed device to register with the dispatcher.
        """
        obj = opaas_obj.obj
        self.event_dispatcher.register(opaas_obj)
        try:
            if hasattr(obj, "event_types"):
                self._subscribe_to_device_events(obj, opaas_obj)
                self._subscribe_to_bec_device_events(obj)
                self._subscribe_to_auto_monitors(obj)
                self._subscribe_to_limit_updates(obj)
                self._subscribe_to_bec_signals(obj)
            opaas_obj.initialize_device_buffer(self.connector, self.event_dispatcher)
        finally:
            # A failed first initialization stays inactive. Existing devices recover
            # missing baselines through the dispatcher's ordinary refresh retries.
            if opaas_obj.initialized:
                self.event_dispatcher.activate(obj)

    def disconnect_device(self, obj: OphydObject | DSDevice) -> None:
        """Retire callbacks before destroying a device.

        Args:
            obj (OphydObject | DSDevice): Device object or managed device to disconnect.

        Raises:
            TimeoutError: The device still has an active event operation.
        """
        device_obj: OphydObject = obj.obj if isinstance(obj, DSDevice) else obj
        if not self.event_dispatcher.remove(device_obj):
            raise TimeoutError(
                f"Cannot destroy {device_obj.name} during an active device operation"
            )
        device_obj.destroy()

    def reset_device(self, obj: DSDevice) -> None:
        """Reset a device and discard its event state.

        Args:
            obj (DSDevice): Managed device to mark as uninitialized.
        """
        self.event_dispatcher.remove(obj.obj)
        obj.initialized = False

    @staticmethod
    def connect_device(
        obj: ophyd.OphydObject, wait_for_all: bool = False, timeout: float = 5, **kwargs
    ) -> None | Exception:
        """
        Establish a connection to a device.

        Args:
            obj (OphydObject): The device object to connect to.
            wait_for_all (bool): Whether to wait for all signals to connect.
                                 Default is False
            timeout (float): Timeout in seconds for the connection attempt to all signals.
                             Default is 5 seconds.

        Raises:
            ConnectionError: If the connection could not be established.
        """

        try:
            if hasattr(obj, "wait_for_connection"):
                try:
                    with disable_lazy_wait_for_connection(obj):
                        obj.wait_for_connection(all_signals=wait_for_all, timeout=timeout)  # type: ignore
                except TypeError:
                    with disable_lazy_wait_for_connection(obj):
                        obj.wait_for_connection(timeout=timeout)  # type: ignore
                return
            # Check connected last, as an ophyd device with only lazy signals will always
            # be obj.connected == True. Therefore, we have to call wait_for_connection first
            # for any ophyd devices. This anyways falls back to checking obj.connected.
            # For simulated devices or non-ophyd devices that do not implement wait_for_connection
            # we still want to check obj.connected to allow for them to load.
            if obj.connected:
                return

            logger.error(
                f"Device {obj.name} does not implement the socket controller interface nor"
                " wait_for_connection and cannot be turned on."
            )
            return ConnectionError(f"Failed to establish a connection to device {obj.name}")
        except Exception as exc:
            logger.error(f"Failed to connect for {obj.name}: {exc}")
            return exc

    def publish_device_info(
        self, obj: OphydObject, connect: bool = True, pipe=None
    ) -> None | Exception:
        """
        Publish the device info to redis. The device info contains
        inter alia the class name, user functions and signals.

        Args:
            obj (_type_): _description_
            connect (bool): Whether to connect to the device before getting the info. Defaults to True.
        """
        try:
            interface = get_device_info(obj, connect=connect)
            self.connector.set(
                MessageEndpoints.device_info(obj.name),
                messages.DeviceInfoMessage(device=obj.name, info=interface),
                pipe,
            )
        except Exception as exc:
            logger.error(f"Failed to publish device info for {obj.name}: {exc}")
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

    def _obj_callback_limit_change(self, *_args: Any, obj: OphydObject, **kwargs: Any) -> None:
        """Queue a limit snapshot update from a device event.

        Args:
            *_args (Any): Unused positional callback arguments.
            obj (OphydObject): Device or signal that emitted the event.
            **kwargs (Any): Event payload forwarded to the dispatcher.
        """
        self.event_dispatcher.enqueue(obj, "limits", **kwargs)

    def _obj_callback_readback(self, *_args: Any, obj: OphydObject, **kwargs: Any) -> None:
        """Queue a readback snapshot update from a device event.

        Args:
            *_args (Any): Unused positional callback arguments.
            obj (OphydObject): Device or signal that emitted the event.
            **kwargs (Any): Event payload forwarded to the dispatcher.
        """
        self.event_dispatcher.enqueue(obj, "readback", **kwargs)

    def _obj_callback_configuration(self, *_args: Any, obj: OphydObject, **kwargs: Any) -> None:
        """Queue a configuration snapshot update for a root device.

        Args:
            *_args (Any): Unused positional callback arguments.
            obj (OphydObject): Device or signal that emitted the event.
            **kwargs (Any): Event payload forwarded to the dispatcher.
        """
        if not isinstance(obj.root, ophyd.Signal):
            self.event_dispatcher.enqueue(obj, "configuration", **kwargs)

    def _obj_callback_auto_monitor_readback(
        self, *_args: Any, obj: OphydObject, **kwargs: Any
    ) -> None:
        """Queue a readback snapshot update from a monitored component.

        Args:
            *_args (Any): Unused positional callback arguments.
            obj (OphydObject): Signal that emitted the monitor update.
            **kwargs (Any): Event payload forwarded to the dispatcher.
        """
        self.event_dispatcher.enqueue(obj, "readback", **kwargs)

    def _obj_callback_auto_monitor_configuration(
        self, *_args: Any, obj: OphydObject, **kwargs: Any
    ) -> None:
        """Queue a configuration snapshot update from a monitored component.

        Args:
            *_args (Any): Unused positional callback arguments.
            obj (OphydObject): Signal that emitted the monitor update.
            **kwargs (Any): Event payload forwarded to the dispatcher.
        """
        self.event_dispatcher.enqueue(obj, "configuration", **kwargs)

    def _obj_callback_auto_monitor_limits(
        self, *_args: Any, obj: OphydObject, **kwargs: Any
    ) -> None:
        """Queue a limit snapshot update from a monitored limit signal.

        Args:
            *_args (Any): Unused positional callback arguments.
            obj (OphydObject): Limit signal that emitted the monitor update.
            **kwargs (Any): Event payload forwarded to the dispatcher.
        """
        self.event_dispatcher.enqueue(obj, "limits", **kwargs)

    @typechecked
    def _obj_callback_device_monitor_2d(
        self, *_args, obj: OphydObject, value: np.ndarray, timestamp: float | None = None, **kwargs
    ):
        """
        DEPRECATED: Use _obj_callback_preview instead.

        Callback for ophyd monitor events. Sends the data to redis.
        Introduces a check of the data size, and incorporates a limit which is defined in max_size (in MB)

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
                expire=3600,
            )

    def _obj_callback_device_monitor_1d(
        self, *_args, obj: OphydObject, value: np.ndarray, timestamp: float | None = None, **kwargs
    ):
        """
        DEPRECATED: Use _obj_callback_preview instead.

        Callback for ophyd monitor events. Sends the data to redis.
        Introduces a check of the data size, and incorporates a limit which is defined in max_size (in MB)

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
                expire=3600,
            )

    def _obj_callback_acq_done(self, *_args, **kwargs):
        device = kwargs["obj"].root.name
        status = 0
        metadata = self.devices[device].metadata
        self.connector.set(
            MessageEndpoints.device_status(device),
            messages.DeviceStatusMessage(device=device, status=status, metadata=metadata),
        )

    def _obj_callback_done_moving(self, *_args: Any, obj: OphydObject, **_kwargs: Any) -> None:
        """Queue the final readback after a device finishes moving.

        Args:
            *_args (Any): Unused positional callback arguments.
            obj (OphydObject): Device that completed its motion.
            **_kwargs (Any): Unused event payload; this event requests a snapshot refresh.
        """
        self.event_dispatcher.enqueue(obj, "readback")

    def _obj_callback_is_moving(
        self, *_args: Any, obj: OphydObject, value: Any, **_kwargs: Any
    ) -> None:
        """Queue a moving-state update without publishing on the monitor thread.

        Args:
            *_args (Any): Unused positional callback arguments.
            obj (OphydObject): Signal that emitted the moving-state update.
            value (Any): Moving-state value supplied by the signal callback.
            **_kwargs (Any): Unused additional callback payload.
        """
        self.event_dispatcher.enqueue(obj, "status", value=value)

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
        """
        DEPRECATED: Use _obj_callback_progress_signal instead.

        Callback for progress events. Sends the data to redis.
        """
        metadata = self.devices[obj.root.name].metadata
        msg = messages.ProgressMessage(
            value=value, max_value=max_value, done=done, metadata=metadata
        )
        self.connector.set_and_publish(
            MessageEndpoints.device_progress(obj.root.name), msg, expire=3600
        )

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
        """
        DEPRECATED: Use _obj_callback_file_event_signal instead.

        Callback for file events on devices. This callback set and publishes
        a file message to the file_event and public_file endpoints in Redis to inform
        the file writer and other services about externally created files.

        Args:
            obj (OphydObject): ophyd object
            file_path (str): file path to the created file
            done (bool): if the file is done
            successful (bool): if the file was created successfully
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
        self.connector.set_and_publish(
            MessageEndpoints.file_event(device_name), msg, pipe=pipe, expire=3600
        )
        self.connector.set_and_publish(
            MessageEndpoints.public_file(scan_id=scan_id, name=device_name),
            msg,
            pipe=pipe,
            expire=3600,
        )
        pipe.execute()

    def _obj_callback_bec_message_signal(
        self, *_args, obj: OphydObject, value: messages.BECMessage, **kwargs
    ):
        """
        Callback for BECMessageSignal events. Sends the data to redis.

        Args:
            obj (OphydObject): ophyd object
            value (BECMessageSignal): data from ophyd device
        """
        if not obj.connected:
            return
        if not isinstance(value, messages.BECMessage):
            return
        self._bec_message_handler.emit(obj, value)

    def shutdown(self) -> None:
        """Stop configuration and event workers, then disconnect all devices."""
        if self.config_update_handler:
            self.config_update_handler.shutdown()
        self.event_dispatcher.shutdown()
        for device in self.devices.values():
            try:
                logger.info(f"Disconnecting device {device.name}")
                self.disconnect_device(device.obj)
            except Exception:
                logger.error(f"Failed to disconnect device {device.name}: {traceback.format_exc()}")
        self.devices.flush()
        super().shutdown()
