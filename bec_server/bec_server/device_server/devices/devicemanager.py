"""
This module contains the DeviceManagerDS class, which is a subclass of
the DeviceManagerBase class and is the main device manager for devices
in BEC. It is the only place where devices are initialized and managed.
"""

from __future__ import annotations

import threading
import time
import traceback
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import ophyd
from ophyd.ophydobj import OphydObject
from ophyd_devices.utils.bec_signals import BECMessageSignal

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.bec_service import BECService
from bec_lib.devicemanager import CancelledError, DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.device_server.bec_message_handler import BECMessageHandler
from bec_server.device_server.device_providers.device_interfaces import (
    DeviceTarget,
    RefreshSection,
    wait_for_completion,
)
from bec_server.device_server.device_providers.device_provider_manager import DeviceProviderManager
from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
    connect_native_device,
    construct_native_device,
    get_native_device_class,
)
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.ds_device import DSDevice, DSDeviceConfig
from bec_server.device_server.refresh_scheduler import RefreshScheduler

if TYPE_CHECKING:  # pragma: no cover
    from redis.client import Pipeline

    from bec_lib.redis_connector import RedisConnector


logger = bec_logger.logger


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


class DeviceManagerDS(DeviceManagerBase):
    def __init__(
        self,
        service: BECService,
        config_update_handler: ConfigUpdateHandler | None = None,
        status_cb: list[Callable] | Callable | None = None,
    ):
        super().__init__(service, status_cb)
        self.device_provider_manager = DeviceProviderManager(self)
        self._use_proxy_objects = False
        self._config_request_connector = None
        self._device_instructions_connector = None
        self._config_update_handler_cls = config_update_handler
        self.config_update_handler = None
        self.failed_devices = {}
        self._bec_message_handler = BECMessageHandler(self)
        self._device_order_map = {}
        self._refresh_scheduler = RefreshScheduler()
        self._shutdown_event = threading.Event()
        self._auto_monitor_start_lock = threading.RLock()
        self._auto_monitor_update_thread = self._create_auto_monitor_update_thread()

    def _create_auto_monitor_update_thread(self) -> threading.Thread:
        return threading.Thread(
            target=self._auto_monitor_update_loop, daemon=True, name="AutoMonitorUpdateThread"
        )

    def _ensure_auto_monitor_update_thread(self) -> None:
        with self._auto_monitor_start_lock:
            if self._auto_monitor_update_thread is None:
                self._auto_monitor_update_thread = self._create_auto_monitor_update_thread()
            elif (
                not self._auto_monitor_update_thread.is_alive()
                and self._auto_monitor_update_thread.ident is not None
            ):
                self._auto_monitor_update_thread = self._create_auto_monitor_update_thread()

            if not self._auto_monitor_update_thread.is_alive():
                self._auto_monitor_update_thread.start()

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
        return get_native_device_class(dev_type)

    @staticmethod
    def construct_device_obj(dev: dict, device_manager: DeviceManagerDS | None) -> tuple[Any, dict]:
        """Construct a classic native device for callers outside the device server."""
        device_class = DeviceManagerDS._get_device_class(dev["deviceClass"])
        return construct_native_device(
            device_class, dev.get("name"), dev.get("deviceConfig"), device_manager
        )

    @staticmethod
    def connect_device(
        obj: Any, wait_for_all: bool = False, timeout: float = 5, **kwargs
    ) -> Exception | None:
        """Connect a native object for legacy offline device checkers.

        Args:
            obj (Any): Previously constructed native device.
            wait_for_all (bool): Connect all declared signals. Defaults to False.
            timeout (float): Connection timeout in seconds. Defaults to 5.
            **kwargs: Additional legacy connection options.

        Returns:
            Exception | None: Connection error, or None on success.
        """
        return connect_native_device(obj, wait_for_all=wait_for_all, timeout=timeout, **kwargs)

    def _init_device(self, device_info: dict, delayed: bool, progress: DeviceProgress) -> None:
        """
        Initialize a device from its configuration dictionary.

        Args:
            device_info (dict): Device configuration dictionary.
            delayed (bool): Whether this device is initialized in the delayed phase.
            progress (DeviceProgress): DeviceProgress instance to track initialization progress.
        """

        name = device_info.get("name")
        success = True
        progress.update_progress(device_name=name, finished=False, success=success)

        device_config = DSDeviceConfig.model_validate(device_info)
        device, config = self.device_provider_manager.construct_device(device_config)
        try:
            self.device_provider_manager.initialize_device(device, config)
            if delayed:
                self.device_provider_manager.register_device_proxy(device)
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

                if self.device_provider_manager.requires_delayed_initialization(dev["deviceClass"]):
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

    def on_configuration_applied(self, device: DSDevice, *, refresh_buffer: bool) -> None:
        """Publish BEC state after the provider applies a standard configuration update.

        Args:
            device (DSDevice): Updated device wrapper.
            refresh_buffer (bool): Whether unmonitored changes require a synchronous buffer refresh.
        """
        if refresh_buffer:
            device.initialize_device_buffer(self.connector)
        self.connector.publish_metrics("device_server", {"num_devices": len(self.devices)})

    def _subscribe_to_limit_updates(self, obj: OphydObject):
        """
        Subscribe to limit updates if the device has low_limit_travel and high_limit_travel signals.

        Args:
            obj (OphydObject): Ophyd object to subscribe to limit updates
        """
        if hasattr(obj, "low_limit_travel") and hasattr(obj.low_limit_travel, "subscribe"):
            self._ensure_auto_monitor_update_thread()
            obj.low_limit_travel.subscribe(self._obj_callback_auto_monitor_limits, run=False)
        if hasattr(obj, "high_limit_travel") and hasattr(obj.high_limit_travel, "subscribe"):
            self._ensure_auto_monitor_update_thread()
            obj.high_limit_travel.subscribe(self._obj_callback_auto_monitor_limits, run=False)

    def _subscribe_to_device_events(self, obj: OphydObject, opaas_obj: DSDevice):
        """Subscribe to device events"""

        if "readback" in obj.event_types:
            obj.subscribe(self._obj_callback_readback, event_type="readback", run=opaas_obj.enabled)
        elif "value" in obj.event_types:
            obj.subscribe(self._obj_callback_readback, event_type="value", run=opaas_obj.enabled)
        if hasattr(obj, "motor_is_moving"):
            obj.motor_is_moving.subscribe(self._obj_callback_is_moving, run=opaas_obj.enabled)  # type: ignore
        if "done_moving" in obj.event_types:
            obj.subscribe(self._obj_callback_readback, event_type="done_moving", run=False)

    def _subscribe_to_auto_monitors(self, obj: OphydObject):
        """
        If the component has set the _auto_monitor attribute to True,
        subscribe to the readback or configuration signals.

        Args:
            obj (OphydObject): Ophyd object to subscribe to auto monitors
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
            self._ensure_auto_monitor_update_thread()
            if component.kind in (ophyd.Kind.normal, ophyd.Kind.hinted):
                component.subscribe(self._obj_callback_auto_monitor_readback, run=False)
            elif component.kind == ophyd.Kind.config:
                component.subscribe(self._obj_callback_auto_monitor_configuration, run=False)

    def _subscribe_to_bec_signals(self, obj: OphydObject):
        """
        Subscribe to BEC signals, such as PreviewSignal, ProgressSignal, FileEventSignal, etc.

        Args:
            obj (OphydObject): Ophyd object to subscribe to BEC signals

        """
        if not hasattr(obj, "walk_signals"):
            # If the object does not have walk_components, it is likely a simple signal
            return
        signal_walk = obj.walk_signals()  # type: ignore
        for _ancestor, _signal_name, signal in signal_walk:
            if isinstance(signal, BECMessageSignal):
                signal.subscribe(callback=self._obj_callback_bec_message_signal, run=False)

    def initialize_enabled_device(self, device: DSDevice) -> None:
        """Initialize the data buffer for a connected device.

        Args:
            device (DSDevice): Connected device wrapper to initialize.
        """
        device.initialize_device_buffer(self.connector)

    def publish_device_info(
        self, device: DSDevice, info: dict, pipe: Pipeline | None = None
    ) -> None:
        """Publish already extracted device metadata to Redis.

        Args:
            device (DSDevice): Device wrapper whose metadata should be published.
            info (dict): Metadata returned by the provider.
            pipe (Pipeline | None): Optional Redis pipeline. Defaults to None.
        """
        self.connector.set(
            MessageEndpoints.device_info(device.name),
            messages.DeviceInfoMessage(device=device.name, info=info),
            pipe,
        )

    def reset_device_data(self, device: DSDevice, pipe=None) -> None:
        """Delete all data and metadata for a device wrapper."""
        self.connector.delete(MessageEndpoints.device_status(device.name), pipe)
        self.connector.delete(MessageEndpoints.device_read(device.name), pipe)
        self.connector.delete(MessageEndpoints.device_read_configuration(device.name), pipe)
        self.connector.delete(MessageEndpoints.device_info(device.name), pipe)

    def _publish_refresh(
        self, device: DSDevice, section: RefreshSection, pipe: Pipeline | None = None
    ) -> None:
        """Read through the owning provider and publish one cache section."""
        if self.devices.get(device.name) is not device:
            return
        provider = self.device_provider_manager.get_provider(type(device.obj))
        target = DeviceTarget(device)
        if section is RefreshSection.READBACK:
            readings = wait_for_completion(provider.read(target))
            endpoint = MessageEndpoints.device_readback
        elif section is RefreshSection.CONFIGURATION:
            readings = wait_for_completion(provider.read_configuration(target, separate_only=True))
            endpoint = MessageEndpoints.device_read_configuration
        else:
            readings = provider.get_device_limits(device)
            endpoint = MessageEndpoints.device_limits
        if readings is None or self.devices.get(device.name) is not device:
            return
        metadata = {} if section is RefreshSection.LIMITS else device.metadata
        local_pipe = pipe if pipe is not None else self.connector.pipeline()
        self.connector.set_and_publish(
            endpoint(device.name),
            messages.DeviceMessage(signals=readings, metadata=metadata),
            pipe=local_pipe,
        )
        if pipe is None:
            local_pipe.execute()

    def _callback_refresh(self, obj: OphydObject, section: RefreshSection, pipe=None) -> None:
        if not obj.connected:
            return
        device = self.devices.get(obj.root.name)
        if device is None or device.obj is not obj.root:
            return
        self._publish_refresh(device, section, pipe)

    def _obj_callback_limit_change(
        self, *_args, obj: OphydObject, pipe: Pipeline | None = None, **kwargs
    ):
        """Acquire and publish limits without exposing Redis to the provider."""
        self._callback_refresh(obj, RefreshSection.LIMITS, pipe)

    def _obj_callback_readback(
        self, *_args, obj: OphydObject, pipe: Pipeline | None = None, **kwargs
    ):
        self._callback_refresh(obj, RefreshSection.READBACK, pipe)

    def _obj_callback_configuration(
        self, *_args, obj: OphydObject, pipe: Pipeline | None = None, **kwargs
    ):
        self._callback_refresh(obj, RefreshSection.CONFIGURATION, pipe)

    def request_refresh(self, device: DSDevice, sections: frozenset[RefreshSection]) -> None:
        """Queue a backend notification for the shared refresh worker without acquiring data."""
        with self._auto_monitor_start_lock:
            if self._shutdown_event.is_set() or self.devices.get(device.name) is not device:
                return
            self._refresh_scheduler.request_refresh(device, sections)
            self._ensure_auto_monitor_update_thread()

    def _request_refresh_scheduler(self, obj: OphydObject, section: RefreshSection) -> None:
        if not obj.connected:
            return
        device = self.devices.get(obj.root.name)
        if device is not None and device.obj is obj.root:
            self._refresh_scheduler.request_refresh(device, frozenset({section}))

    def _obj_callback_auto_monitor_readback(self, *_args, obj: OphydObject, **kwargs):
        self._request_refresh_scheduler(obj, RefreshSection.READBACK)

    def _obj_callback_auto_monitor_configuration(self, *_args, obj: OphydObject, **kwargs):
        self._request_refresh_scheduler(obj, RefreshSection.CONFIGURATION)

    def _obj_callback_auto_monitor_limits(self, *_args, obj: OphydObject, **kwargs):
        self._request_refresh_scheduler(obj, RefreshSection.LIMITS)

    def _auto_monitor_update_loop(self):
        while not self._shutdown_event.wait(0.01):
            try:
                requests = self._refresh_scheduler.drain()
                if not requests:
                    continue
                pipe = self.connector.pipeline()
                for device, section in requests:
                    if self.devices.get(device.name) is not device:
                        continue
                    if not self.device_provider_manager.is_connected(device):
                        continue
                    self._publish_refresh(device, section, pipe)
                pipe.execute()
            except Exception as exc:
                logger.error(f"Error in auto monitor update loop: {exc}")
                logger.error(traceback.format_exc())

    def _obj_callback_is_moving(self, *_args, **kwargs):
        device = kwargs["obj"].root.name
        status = int(kwargs.get("value"))
        metadata = self.devices[device].metadata
        self.connector.set(
            MessageEndpoints.device_status(device),
            messages.DeviceStatusMessage(device=device, status=status, metadata=metadata),
        )

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

    def shutdown(self):
        """Shutdown the device manager and disconnect all devices"""
        with self._auto_monitor_start_lock:
            self._shutdown_event.set()
        if self._auto_monitor_update_thread.is_alive():
            self._auto_monitor_update_thread.join(timeout=5)
        self._refresh_scheduler.clear()
        for device in self.devices.values():
            try:
                logger.info(f"Disconnecting device {device.name}")
                self.device_provider_manager.disconnect_device(device)
            except Exception:
                logger.error(f"Failed to disconnect device {device.name}: {traceback.format_exc()}")
        self.devices.flush()
        if self.config_update_handler:
            self.config_update_handler.shutdown()
        super().shutdown()
