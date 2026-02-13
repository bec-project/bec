from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import MessageObject

logger = bec_logger.logger


class DAPServiceManager:
    """Base class for data processing services."""

    def __init__(self, services: list) -> None:
        self.connector = None
        self._started = False
        self.client = None
        self._dap_request_thread = None
        self.available_dap_services = {}
        self.dap_services = {}
        self.continuous_dap = None
        self.services = services if isinstance(services, list) else [services]
        self.threadpool = ThreadPoolExecutor(max_workers=4)

    def _start_dap_request_consumer(self) -> None:
        """
        Start the dap request consumer.
        """
        self.connector.register(
            topics=MessageEndpoints.dap_request(), cb=self._dap_request_callback
        )

    def _dap_request_callback(self, msg: MessageObject) -> None:
        """
        Callback function for dap request consumer.

        Args:
            msg (MessageObject): MessageObject instance
            parent (DAPService): DAPService instance
        """
        dap_request_msg = msg.value
        if not dap_request_msg:
            return
        self.threadpool.submit(self.process_dap_request, dap_request_msg)

    def process_dap_request(self, dap_request_msg: messages.DAPRequestMessage) -> None:
        """
        Process a dap request.

        Args:
            dap_request_msg (DAPRequestMessage): DAPRequestMessage instance
        """
        try:
            dap_cls = self._get_dap_cls(dap_request_msg)
            if not dap_cls:
                return
            dap_type = dap_request_msg.content["dap_type"]
            result = None
            if dap_type == "continuous":
                self._start_continuous_dap(dap_cls, dap_request_msg)
            elif dap_type == "on_demand":
                result = self._start_on_demand_dap(dap_cls, dap_request_msg)
            else:
                raise ValueError(f"Unknown dap type {dap_type}")

        # pylint: disable=broad-except
        except Exception as e:
            logger.exception(f"Failed to process dap request {dap_request_msg}: {e}")
            self.send_dap_response(
                dap_request_msg, success=False, error=str(e), metadata=dap_request_msg.metadata
            )
            return

        self.send_dap_response(
            dap_request_msg, success=True, data=result, metadata=dap_request_msg.metadata
        )

    def _start_continuous_dap(
        self, dap_cls: type, dap_request_msg: messages.DAPRequestMessage
    ) -> None:
        if not self.client:
            return
        if self.continuous_dap is not None:
            self.client.callbacks.remove(self.continuous_dap["id"])

        dap_config = dap_request_msg.content["config"]
        if not dap_config.get("auto_run"):
            return

        config = dap_request_msg.content["config"]
        cls_args = config["class_args"]
        cls_kwargs = config["class_kwargs"]
        dap_instance = dap_cls(*cls_args, client=self.client, **cls_kwargs, continuous=True)
        dap_instance.configure(**dap_config)
        self.continuous_dap = {
            "id": self.client.callbacks.register(
                # pylint: disable=protected-access
                event_type="scan_status",
                callback=dap_instance._process_scan_status_update,
            ),
            "instance": dap_instance,
        }

    def _start_on_demand_dap(
        self, dap_cls: type, dap_request_msg: messages.DAPRequestMessage
    ) -> dict:
        """
        Start an on demand dap.
        """
        config = dap_request_msg.content["config"]
        cls_args = config["class_args"]
        cls_kwargs = config["class_kwargs"]
        dap_instance = dap_cls(*cls_args, client=self.client, **cls_kwargs)
        config = dap_request_msg.content["config"]
        dap_instance.configure(*config["args"], **config["kwargs"])
        result = dap_instance.process()
        return result

    def _get_dap_cls(self, dap_request_msg: messages.DAPRequestMessage) -> type:
        """
        Get the dap class.

        Args:
            dap_request_msg (DAPRequestMessage): DAPRequestMessage instance

        Returns:
            type: DAP class
        """
        dap_cls = dap_request_msg.content["dap_cls"]
        if dap_cls in self.dap_services:
            return self.dap_services[dap_cls]
        # raise ValueError(f"Unknown dap class {dap_cls}")

    def send_dap_response(
        self,
        dap_request_msg: messages.DAPRequestMessage,
        success: bool,
        data=None,
        error: str = None,
        metadata: dict = None,
    ) -> None:
        """
        Send a dap response.

        Args:
            dap_request_msg (DAPRequestMessage): DAPRequestMessage instance
            success (bool): Success flag
            error (str, optional): Error message. Defaults to None.
            data (dict, optional): Data. Defaults to None.
            metadata (dict, optional): Metadata. Defaults to None.
        """
        dap_response_msg = messages.DAPResponseMessage(
            success=success, data=data, error=error, dap_request=dap_request_msg, metadata=metadata
        )
        self.connector.set_and_publish(
            MessageEndpoints.dap_response(metadata.get("RID")), dap_response_msg, expire=60
        )

    def start(self, client: BECClient) -> None:
        """
        Start the data processing service.

        Args:
            connector (RedisConnector): RedisConnector instance
        """
        if self._started:
            return
        self.client = client
        self.connector = client.connector
        self._start_dap_request_consumer()
        self.update_available_dap_services()
        self.publish_available_services()
        self._started = True

    def update_available_dap_services(self):
        """
        Update the available dap services.
        """
        for service in self.services:
            self.dap_services[service.__name__] = service
            provided_services = service.get_provided_services()
            self._validate_provided_services(provided_services)
            self.available_dap_services.update(service.get_provided_services())

        # members = inspect.getmembers(dap_plugins)

        # for name, service_cls in members:
        #     if name in ["DAPServiceBase", "LmfitService"]:
        #         continue
        #     try:
        #         is_service = issubclass(service_cls, dap_plugins.DAPServiceBase)
        #     except TypeError:
        #         is_service = False

        #     if not is_service:
        #         logger.debug(f"Ignoring {name}")
        #         continue
        #     if name in self.available_dap_services:
        #         logger.error(f"{service_cls.scan_name} already exists. Skipping.")
        #         continue

        #     self.dap_services[name] = service_cls
        #     provided_services = service_cls.get_provided_services()
        #     self._validate_provided_services(provided_services)
        #     self.available_dap_services.update(service_cls.get_provided_services())

    def _validate_provided_services(self, provided_services: dict) -> None:
        """
        Validate the provided services.
        {
                "class": cls.__name__,
                "user_friendly_name": model.__name__,
                "class_doc": cls.public_doc_string(model),
                "fit_doc": cls.get_fit_doc(model),
                "signature": cls.get_signature(),
                "auto_fit_supported": getattr(cls, "AUTO_FIT_SUPPORTED", False),
                "params": serialize_lmfit_params(
                    cls.get_model(model)().make_params()
                ),
                "class_args": [],
                "class_kwargs": {"model": model.__name__},
            }

        Args:
            provided_services (dict): Provided services
        """
        for service_name, service in provided_services.items():
            if not isinstance(service, dict):
                raise ValueError(f"Invalid service {service_name}: {service}. Must be a dict.")
            self._check_service_keys_exists(
                service,
                [
                    "class",
                    "user_friendly_name",
                    "class_doc",
                    "run_doc",
                    "run_name",
                    "signature",
                    "auto_run_supported",
                    "params",
                    "class_args",
                    "class_kwargs",
                ],
            )

    def _check_service_keys_exists(self, service: dict, keys: list) -> bool:
        """
        Check if the service keys exists.

        Args:
            service (dict): Service
            keys (list): Keys

        Returns:
            bool: True if the keys exists
        """
        for key in keys:
            if key not in service:
                raise ValueError(f"Invalid service {service}. Must have a {key} key.")

    def publish_available_services(self):
        """send all available dap services to the broker"""
        msg = messages.AvailableResourceMessage(resource=self.available_dap_services)
        # pylint: disable=protected-access
        self.connector.set(
            MessageEndpoints.dap_available_plugins(f"DAPServer/{self.client._service_id}"), msg
        )

    def shutdown(self) -> None:
        self.threadpool.shutdown()
        if not self._started:
            return
        self.connector.shutdown()
        self._started = False
