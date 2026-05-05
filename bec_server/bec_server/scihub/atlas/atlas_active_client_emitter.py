from __future__ import annotations

from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:  # pragma: no cover
    from bec_server.scihub.atlas.atlas_connector import AtlasConnector

_INTERNAL_SERVICE_PREFIXES = (
    "DeviceServer",
    "FileWriterManager",
    "DAPServer",
    "ScanBundler",
    "ScanServer",
    "SciHub",
)


class AtlasActiveClientEmitter:

    def __init__(self, atlas_connector: AtlasConnector) -> None:
        self.atlas_connector = atlas_connector
        self._last_active_client_metrics: dict[str, messages.DynamicMetricValue] | None = None
        self._start_service_status_subscription()
        self._emit_active_client_metrics()

    def _start_service_status_subscription(self):
        self.atlas_connector.connector.register(
            patterns=MessageEndpoints.service_status("*"), cb=self._handle_service_status
        )

    def _handle_service_status(self, _msg, **_kwargs) -> None:
        self._emit_active_client_metrics()

    def _emit_active_client_metrics(self) -> None:
        metrics = self._get_active_client_metrics()
        if metrics == self._last_active_client_metrics:
            return
        self._last_active_client_metrics = metrics
        self.atlas_connector.connector.publish_metrics("active_clients", metrics)

    def _get_active_client_metrics(self) -> dict[str, str]:
        active_clients = {}
        for service_name, status_msg in self.atlas_connector.scihub.service_status.items():
            if self._is_internal_service(service_name):
                continue
            if status_msg.status != messages.BECStatus.RUNNING:
                continue
            info = status_msg.info
            hostname = (
                info.hostname if isinstance(info, messages.ServiceInfo) else info.get("hostname")
            )
            if hostname:
                active_clients[service_name] = hostname
        return active_clients

    @classmethod
    def _is_internal_service(cls, service_name: str) -> bool:
        return service_name.startswith(_INTERNAL_SERVICE_PREFIXES)

    def shutdown(self):
        self.atlas_connector.connector.unregister(
            patterns=MessageEndpoints.service_status("*"), cb=self._handle_service_status
        )
