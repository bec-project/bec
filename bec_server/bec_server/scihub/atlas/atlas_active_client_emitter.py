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
        self._last_active_client_metrics: messages.DynamicMetricDict = {}
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

    def _get_active_client_metrics(self) -> messages.DynamicMetricDict:
        clients = [
            (name, status)
            for (name, status) in self.atlas_connector.scihub.service_status.items()
            if not (self._is_internal_service(name) or status.status != messages.BECStatus.RUNNING)
        ]
        ipython_clients = len(list(filter(lambda ns: "BECIPythonClient" in ns[0], clients)))
        bec_clients = len(list(filter(lambda ns: "BECClient" in ns[0], clients)))

        def _from_console(name_msg: tuple[str, messages.StatusMessage]):
            msg = name_msg[1]
            if not isinstance(msg.info, messages.ServiceInfo):
                return False
            return "cons" in msg.info.hostname

        from_consoles = len(list(filter(_from_console, clients)))

        return {
            "total_connected": len(clients),
            "bec_clients": bec_clients,
            "ipython_clients": ipython_clients,
            "from_consoles": from_consoles,
        }

    @classmethod
    def _is_internal_service(cls, service_name: str) -> bool:
        return service_name.startswith(_INTERNAL_SERVICE_PREFIXES)

    def shutdown(self):
        self.atlas_connector.connector.unregister(
            patterns=MessageEndpoints.service_status("*"), cb=self._handle_service_status
        )
