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
from typing import TYPE_CHECKING, Any

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.bec_service import BECService
from bec_lib.device import DeviceBaseWithConfig
from bec_lib.device_backend import (
    Capability,
    DeviceEvent,
    DeviceHandler,
    DeviceProvider,
    EventKind,
    MessageChannel,
)
from bec_lib.devicemanager import CancelledError, DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.utils.rpc_utils import rgetattr
from bec_server.device_server.backend import create_device_provider
from bec_server.device_server.bec_message_handler import BECMessageHandler
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler

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


class DSDevice(DeviceBaseWithConfig):
    def __init__(self, name, obj, config, parent=None):
        super().__init__(name=name, config=config, parent=parent)
        self.obj = obj
        self._handler_object = None
        self._handler = None
        self._device_provider = (
            parent.device_provider
            if parent is not None and hasattr(type(parent), "handler_for")
            else create_device_provider()
        )
        self.metadata = {}
        self.initialized = False

    @property
    def handler(self) -> DeviceHandler:
        """Service dispatch interface; .obj remains raw for existing device code."""
        if self._handler_object is not self.obj:
            self._handler = self._device_provider.wrap(self.obj)
            self._handler_object = self.obj
        return self._handler

    def __getattr__(self, name: str) -> Any:
        if hasattr(self.obj, name):
            # compatibility with ophyd devices accessed on the client side
            return rgetattr(self.obj, name)
        return super().__getattr__(name)

    def initialize_device_buffer(self, connector):
        """initialize the device read and readback buffer on redis with a new reading"""
        dev_msg = messages.DeviceMessage(signals=self.handler.read(), metadata={})

        limits = self.handler.limits()
        pipe = connector.pipeline()
        connector.set_and_publish(MessageEndpoints.device_readback(self.name), dev_msg, pipe=pipe)
        connector.set_and_publish(
            topic=MessageEndpoints.device_read(self.name), msg=dev_msg, pipe=pipe
        )
        if Capability.SIGNAL not in self.handler.capabilities:
            # signals have the same read and read_configuration values; no need to publish twice
            dev_config_msg = messages.DeviceMessage(
                signals=self.handler.read_configuration(), metadata={}
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
        device_provider: DeviceProvider | None = None,
    ):
        self.device_provider = (
            device_provider or vars(service).get("device_provider") or create_device_provider()
        )
        super().__init__(service, status_cb)
        self._use_proxy_objects = False
        self._config_request_connector = None
        self._device_instructions_connector = None
        self._config_update_handler_cls = config_update_handler
        self.config_update_handler = None
        self.failed_devices = {}
        self._bec_message_handler = BECMessageHandler(self)
        self._device_order_map = {}
        self._auto_monitor_readback_updates = set()
        self._auto_monitor_configuration_updates = set()
        self._limit_change_updates = set()
        self._auto_monitor_update_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._auto_monitor_update_thread = self._create_auto_monitor_update_thread()

    def handler_for(self, obj: object) -> DeviceHandler:
        """Adapt raw objects only at documented legacy helper boundaries."""
        if isinstance(obj, DeviceHandler):
            return obj
        return self.device_provider.wrap(obj)

    def _create_auto_monitor_update_thread(self) -> threading.Thread:
        return threading.Thread(
            target=self._auto_monitor_update_loop, daemon=True, name="AutoMonitorUpdateThread"
        )

    def _ensure_auto_monitor_update_thread(self) -> None:
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
    def _get_device_class(dev_type: str, *, provider: DeviceProvider | None = None) -> type:
        """Get the device class from the device type"""
        return (provider or create_device_provider()).load_class(dev_type)

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

                dev_cls = self._get_device_class(
                    dev.get("deviceClass"), provider=self.device_provider
                )
                if self.device_provider.requires_dependencies(dev_cls):
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

    def initialize_delayed_devices(self, dev: dict, config: dict, obj: object) -> None:
        """Initialize delayed device after all other devices have been initialized."""
        name = dev.get("name")
        enabled = dev.get("enabled")
        logger.info(f"Adding device {name}: {'ENABLED' if enabled else 'DISABLED'}")

        obj = self.initialize_device(dev, config, obj)

        if Capability.PROXY in obj.handler.capabilities:
            self._register_device_proxy(name)

    def _register_device_proxy(self, name: str) -> None:
        self.device_provider.register_proxy(
            self.devices[name].handler, {key: item.handler for key, item in self.devices.items()}
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

    def update_config(self, obj: object, config: dict) -> None:
        """Update an ophyd device's config

        Args:
            obj (Ophydobj): Ophyd object that should be updated
            config (dict): Config dictionary

        """
        result = self.handler_for(obj).configure(config)
        if result.delegated:
            return
        signal_updated = result.refresh
        if signal_updated:
            # re-initialize the device buffer to reflect the updated signal values
            self.devices[self.handler_for(obj).identity.root].initialize_device_buffer(
                self.connector
            )

        self.connector.publish_metrics("device_server", {"num_devices": len(self.devices)})

    @staticmethod
    def construct_device_obj(dev: dict, device_manager: DeviceManagerDS) -> tuple[object, dict]:
        """
        Construct a device object from a device config dictionary.

        Args:
            dev (dict): device config dictionary
            device_manager (DeviceManagerDS): device manager instance

        Returns:
            (object, dict): device object and updated config dictionary
        """
        provider = (
            device_manager.device_provider
            if isinstance(device_manager, DeviceManagerDS)
            else create_device_provider()
        )
        dev_cls = DeviceManagerDS._get_device_class(dev["deviceClass"], provider=provider)
        config = dict(dev.get("deviceConfig") or {})
        config["name"] = dev.get("name")
        return provider.construct(dev_cls, config, device_manager)

    def initialize_device(self, dev: dict, config: dict, obj: object) -> DSDevice:
        """
        Prepares a device for later usage.
        This includes inspecting the device class signature,
        initializing the object, refreshing the device info and buffer,
        as well as adding subscriptions.

        Args:
            dev (dict): device config dictionary
            config (dict): device config dictionary
            obj (object): device object

        Returns:
            DSDevice: initialized device object
        """
        name = dev.get("name")
        enabled = dev.get("enabled")

        handler = self.handler_for(obj)
        # refresh the device info
        pipe = self.connector.pipeline()
        self.reset_device_data(obj, pipe)
        raised_exc = None
        connect = False
        if enabled:
            # Try to connect to the device, needs wait_for_all to include lazy signals e.g. AD detectors
            raised_exc = self.connect_device(
                handler, wait_for_all=True, timeout=dev.get("connectionTimeout", 5)
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

        opaas_obj.handler.subscribe(self._handle_device_event)
        self.update_config(opaas_obj.handler, config)

        return opaas_obj

    def _handle_device_event(self, event: DeviceEvent) -> None:
        """Receive a typed event outside the HAL owner context."""
        name = event.source.root
        if name not in self.devices:
            return
        handler = self.devices[name].handler
        if event.kind == EventKind.MESSAGE:
            self._bec_message_handler.emit_event(event)
            return
        if not handler.connected:
            return
        if event.kind == EventKind.MOTION:
            self._publish_motion(name, event.value)
        elif event.batched:
            self._ensure_auto_monitor_update_thread()
            with self._auto_monitor_update_lock:
                if event.kind == EventKind.READBACK:
                    self._auto_monitor_readback_updates.add(name)
                elif event.kind == EventKind.CONFIGURATION:
                    self._auto_monitor_configuration_updates.add(name)
                elif event.kind == EventKind.LIMITS:
                    self._limit_change_updates.add(name)
        elif event.kind == EventKind.READBACK:
            self._publish_readback(handler)
        elif event.kind == EventKind.CONFIGURATION:
            self._publish_configuration(handler)
        elif event.kind == EventKind.LIMITS:
            self._publish_limits(handler)

    def initialize_enabled_device(self, opaas_obj):
        """connect to an enabled device and initialize the device buffer"""
        opaas_obj.handler.on_connected()
        opaas_obj.initialize_device_buffer(self.connector)

    @staticmethod
    def disconnect_device(obj):
        """disconnect from a device"""
        if isinstance(obj, DeviceHandler):
            obj.close()
        else:
            create_device_provider().wrap(obj).close()

    def reset_device(self, obj: DSDevice):
        """reset a device"""
        obj.initialized = False

    @staticmethod
    def connect_device(
        obj: object, wait_for_all: bool = False, timeout: float = 5, **kwargs
    ) -> None | Exception:
        """
        Establish a connection to a device.

        Args:
            obj (object): The device object to connect to.
            wait_for_all (bool): Whether to wait for all signals to connect.
                                 Default is False
            timeout (float): Timeout in seconds for the connection attempt to all signals.
                             Default is 5 seconds.

        Raises:
            ConnectionError: If the connection could not be established.
        """

        try:
            handler = obj if isinstance(obj, DeviceHandler) else create_device_provider().wrap(obj)
            handler.connect(all_signals=wait_for_all, timeout=timeout)
        except Exception as exc:
            logger.error(f"Failed to connect device: {exc}")
            return exc
        return None

    def publish_device_info(self, obj: object, connect: bool = True, pipe=None) -> None | Exception:
        """
        Publish the device info to redis. The device info contains
        inter alia the class name, user functions and signals.

        Args:
            obj (_type_): _description_
            connect (bool): Whether to connect to the device before getting the info. Defaults to True.
        """
        try:
            handler = self.handler_for(obj)
            interface = handler.describe(connected=connect)
            self.connector.set(
                MessageEndpoints.device_info(self.handler_for(obj).identity.root),
                messages.DeviceInfoMessage(
                    device=self.handler_for(obj).identity.root, info=interface
                ),
                pipe,
            )
        except Exception as exc:
            logger.error(
                f"Failed to publish device info for {self.handler_for(obj).identity.root}: {exc}"
            )
            interface = handler.describe(connected=False)
            self.connector.set(
                MessageEndpoints.device_info(self.handler_for(obj).identity.root),
                messages.DeviceInfoMessage(
                    device=self.handler_for(obj).identity.root, info=interface
                ),
                pipe,
            )
            return exc

    def reset_device_data(self, obj: object, pipe=None) -> None:
        """delete all device data and device info"""
        self.connector.delete(
            MessageEndpoints.device_status(self.handler_for(obj).identity.root), pipe
        )
        self.connector.delete(
            MessageEndpoints.device_read(self.handler_for(obj).identity.root), pipe
        )
        self.connector.delete(
            MessageEndpoints.device_read_configuration(self.handler_for(obj).identity.root), pipe
        )
        self.connector.delete(
            MessageEndpoints.device_info(self.handler_for(obj).identity.root), pipe
        )

    def _publish_limits(self, handler: DeviceHandler, pipe=None) -> None:
        if not handler.connected:
            return
        limits = handler.limits()
        if limits is None:
            return
        target = pipe if pipe is not None else self.connector.pipeline()
        self.connector.set_and_publish(
            MessageEndpoints.device_limits(handler.identity.root),
            messages.DeviceMessage(signals=limits),
            pipe=target,
        )
        if pipe is None:
            target.execute()

    def _publish_readback(self, handler: DeviceHandler, pipe=None) -> None:
        if not handler.connected:
            return
        name = handler.identity.root
        target = pipe if pipe is not None else self.connector.pipeline()
        message = messages.DeviceMessage(
            signals=handler.read(), metadata=self.devices[name].metadata
        )
        self.connector.set_and_publish(MessageEndpoints.device_readback(name), message, pipe=target)
        if pipe is None:
            target.execute()

    def _publish_configuration(self, handler: DeviceHandler, pipe=None) -> None:
        if not handler.connected or Capability.SIGNAL in handler.capabilities:
            return
        name = handler.identity.root
        target = pipe if pipe is not None else self.connector.pipeline()
        message = messages.DeviceMessage(
            signals=handler.read_configuration(), metadata=self.devices[name].metadata
        )
        self.connector.set_and_publish(
            MessageEndpoints.device_read_configuration(name), message, pipe=target
        )
        if pipe is None:
            target.execute()

    def _obj_callback_limit_change(self, *_args, obj: object, pipe=None, **_kwargs):
        return self._publish_limits(self.devices[self.handler_for(obj).identity.root].handler, pipe)

    def _obj_callback_readback(self, *_args, obj: object, pipe=None, **_kwargs):
        return self._publish_readback(
            self.devices[self.handler_for(obj).identity.root].handler, pipe
        )

    def _obj_callback_configuration(self, *_args, obj: object, pipe=None, **_kwargs):
        return self._publish_configuration(
            self.devices[self.handler_for(obj).identity.root].handler, pipe
        )

    def _queue_monitor(self, obj: object, updates: set) -> None:
        handler = self.handler_for(obj)
        if handler.connected:
            with self._auto_monitor_update_lock:
                updates.add(handler.identity.root)

    def _obj_callback_auto_monitor_readback(self, *_args, obj: object, **_kwargs):
        self._queue_monitor(obj, self._auto_monitor_readback_updates)

    def _obj_callback_auto_monitor_configuration(self, *_args, obj: object, **_kwargs):
        self._queue_monitor(obj, self._auto_monitor_configuration_updates)

    def _obj_callback_auto_monitor_limits(self, *_args, obj: object, **_kwargs):
        self._queue_monitor(obj, self._limit_change_updates)

    def _auto_monitor_update_loop(self):
        while not self._shutdown_event.wait(0.01):
            try:
                if (
                    not self._auto_monitor_readback_updates
                    and not self._auto_monitor_configuration_updates
                    and not self._limit_change_updates
                ):
                    continue
                with self._auto_monitor_update_lock:
                    readback_updates = list(self._auto_monitor_readback_updates)
                    configuration_updates = list(self._auto_monitor_configuration_updates)
                    limits_updates = list(self._limit_change_updates)
                    self._auto_monitor_readback_updates.clear()
                    self._auto_monitor_configuration_updates.clear()
                    self._limit_change_updates.clear()

                pipe = self.connector.pipeline()
                for name in readback_updates:
                    if name not in self.devices:
                        continue
                    self._publish_readback(self.devices[name].handler, pipe=pipe)

                for name in configuration_updates:
                    if name not in self.devices:
                        continue
                    self._publish_configuration(self.devices[name].handler, pipe=pipe)

                for name in limits_updates:
                    if name not in self.devices:
                        continue
                    self._publish_limits(self.devices[name].handler, pipe=pipe)

                pipe.execute()
            except Exception as exc:
                logger.error(f"Error in auto monitor update loop: {exc}")
                logger.error(traceback.format_exc())

    def _publish_motion(self, device: str, value: Any) -> None:
        self.connector.set(
            MessageEndpoints.device_status(device),
            messages.DeviceStatusMessage(
                device=device, status=int(value), metadata=self.devices[device].metadata
            ),
        )

    def _obj_callback_is_moving(self, *_args, **kwargs):
        self._publish_motion(self.handler_for(kwargs["obj"]).identity.root, kwargs.get("value"))

    def _obj_callback_bec_message_signal(
        self, *_args, obj: object, value: messages.BECMessage, **_kwargs
    ):
        """Compatibility callback; classify native message signals in the provider."""
        if isinstance(value, messages.BECMessage) and self.handler_for(obj).connected:
            self._bec_message_handler.emit(obj, value)

    def shutdown(self):
        """Shutdown the device manager and disconnect all devices"""
        self._shutdown_event.set()
        if self._auto_monitor_update_thread.is_alive():
            self._auto_monitor_update_thread.join(timeout=5)
        for device in self.devices.values():
            try:
                logger.info(f"Disconnecting device {device.name}")
                self.disconnect_device(device.handler)
            except Exception:
                logger.error(f"Failed to disconnect device {device.name}: {traceback.format_exc()}")
        provider_error = None
        try:
            self.device_provider.close()
        except Exception as error:
            provider_error = error
            logger.error(f"Failed to close device provider: {error}")
        finally:
            self.devices.flush()
            try:
                if self.config_update_handler:
                    self.config_update_handler.shutdown()
            finally:
                super().shutdown()
        if provider_error is not None:
            raise provider_error
