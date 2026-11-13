"""Device-server wrapper for constructed device objects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from bec_lib import messages
from bec_lib.atlas_models import _DeviceModelCore
from bec_lib.device import DeviceBaseWithConfig
from bec_lib.endpoints import MessageEndpoints
from bec_lib.utils.rpc_utils import rgetattr
from bec_server.device_server.device_providers.device_interfaces import (
    DeviceTarget,
    wait_for_completion,
)

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector


class DSDeviceConfig(_DeviceModelCore, extra="allow"):
    """Validate runtime configuration while preserving existing device names and metadata."""

    name: str


class DSDevice(DeviceBaseWithConfig):
    """Wrap a device object and manage its device-server data buffer."""

    def __init__(self, name, obj, config, parent=None):
        super().__init__(name=name, config=config, parent=parent)
        self.obj = obj
        self.metadata = {}
        self.initialized = False
        self.device_info_active = True

    def __getattr__(self, name: str) -> Any:
        if hasattr(self.obj, name):
            # Expose native attributes for callers using the device wrapper.
            return rgetattr(self.obj, name)
        return super().__getattr__(name)

    def initialize_device_buffer(self, connector: RedisConnector) -> None:
        """
        Seed the Redis device buffers with readings from the owning provider.

        Args:
            connector (RedisConnector): Connection used to publish the initial readings.
        """
        provider = self.parent.device_provider_manager.get_provider(type(self.obj))
        target = DeviceTarget(self)
        readings = wait_for_completion(provider.read(target))
        limits = provider.get_device_limits(self)
        dev_msg = messages.DeviceMessage(signals=readings, metadata={})
        pipe = connector.pipeline()
        connector.set_and_publish(MessageEndpoints.device_readback(self.name), dev_msg, pipe=pipe)
        connector.set_and_publish(
            topic=MessageEndpoints.device_read(self.name), msg=dev_msg, pipe=pipe
        )
        configuration = wait_for_completion(provider.read_configuration(target, separate_only=True))
        if configuration is not None:
            dev_config_msg = messages.DeviceMessage(signals=configuration, metadata={})
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
