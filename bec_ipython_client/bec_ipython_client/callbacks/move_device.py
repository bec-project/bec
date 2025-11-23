from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

import numpy as np

from bec_ipython_client.progressbar import DeviceProgressBar
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject

from .utils import LiveUpdatesBase, check_alarms

if TYPE_CHECKING:
    from bec_lib.client import BECClient
    from bec_lib.devicemanager import DeviceManagerBase


class ReadbackDataHandler:
    """Helper class to get the current device values and request-done messages."""

    def __init__(
        self, device_manager: DeviceManagerBase, devices: list[str], request_id: str
    ) -> None:
        """Helper class to get the current device values and request-done messages.

        Args:
            device_manager (DeviceManagerBase): device manager
            devices (list): list of devices to monitor
            request_id (str): request ID
        """
        self.device_manager = device_manager
        self.devices = devices
        self.connector = device_manager.connector
        self.request_id = request_id
        self._devices_received = {dev: False for dev in devices}
        self.data: dict[str, messages.DeviceMessage] = {}
        self._devices_done_state: dict[str, tuple[bool, bool]] = {
            dev: (False, False) for dev in devices
        }
        self.requests_done = threading.Event()
        self._register_callbacks()

    def _register_callbacks(self):
        """register callbacks for device readback messages."""
        for dev in self.devices:
            self.connector.register(
                MessageEndpoints.device_readback(dev), cb=self.on_readback, parent=self, device=dev
            )
        self.connector.register(
            MessageEndpoints.device_req_status(self.request_id),
            cb=self.on_req_status,
            from_start=True,
            parent=self,
        )

    def _unregister_callbacks(self):
        """unregister callbacks for device readback messages."""
        for dev in self.devices:
            self.connector.unregister(MessageEndpoints.device_readback(dev), cb=self.on_readback)
        self.connector.unregister(
            MessageEndpoints.device_req_status(self.request_id), cb=self.on_req_status
        )

    @staticmethod
    def on_req_status(
        msg_obj: dict[str, messages.DeviceReqStatusMessage], parent: ReadbackDataHandler
    ):
        """Callback for device request status messages to track which devices are done.

        Args:
            msg_obj (dict[str, messages.DeviceReqStatusMessage]): message object or device request status message
            parent (ReadbackDataHandler): parent instance
        """
        # pylint: disable=protected-access
        msg = msg_obj["data"]
        if msg.request_id != parent.request_id:
            return
        device = msg.device
        parent._devices_done_state[device] = (True, msg.success)

        if (
            all(done for done, _ in parent._devices_done_state.values())
            and not parent.requests_done.is_set()
        ):
            parent._on_request_done()

    @staticmethod
    def on_readback(msg_obj: MessageObject, parent: ReadbackDataHandler, device: str):
        """Callback for updating device readback data.

        Args:
            msg_obj (MessageObject): message object
            parent (ReadbackDataHandler): parent instance
            device (str): device name
        """
        # pylint: disable=protected-access
        msg: messages.DeviceMessage = cast(messages.DeviceMessage, msg_obj.value)
        parent._devices_received[device] = True
        parent.data[device] = msg

    def _on_request_done(self):
        """Callback for when all requests are done."""
        self.requests_done.set()
        self._unregister_callbacks()

    def get_device_values(self) -> list:
        """get the current device values

        Returns:
            list: list of device values
        """
        values = []
        for dev in self.devices:
            val = self.data.get(dev)
            if val is None:
                signal_data = self.device_manager.devices[dev].read(cached=True)
            else:
                signal_data = val.signals
            # pylint: disable=protected-access
            hints = self.device_manager.devices[dev]._hints
            # if we have hints, use them to get the value, otherwise just use the first value
            if hints:
                values.append(signal_data.get(hints[0]).get("value"))
            else:
                values.append(signal_data.get(list(signal_data.keys())[0]).get("value"))
        return values

    def done(self) -> bool:
        """check if all devices are done

        Returns:
            bool: True if all devices are done, False otherwise
        """
        return self.requests_done.is_set()

    def device_states(self) -> dict[str, tuple[bool, bool]]:
        """
        Return the current device done states.

        Returns:
            dict: dictionary with device names as keys and tuples of (done, success) as values
        """
        return self._devices_done_state


class LiveUpdatesReadbackProgressbar(LiveUpdatesBase):
    """Live feedback on motor movements using a progressbar.

    Args:
        bec (BECClient): BECClient instance
        report_instruction (list, optional): report instruction for the scan. Defaults to None.
        request (messages.ScanQueueMessage, optional): scan queue request message. Defaults to None.
        callbacks (list[Callable], optional): list of callbacks to register. Defaults to None.

    """

    def __init__(
        self,
        bec: BECClient,
        report_instruction: list | None = None,
        request: messages.ScanQueueMessage | None = None,
        callbacks: list[Callable] | None = None,
    ) -> None:
        super().__init__(
            bec, report_instruction=report_instruction, request=request, callbacks=callbacks
        )
        if report_instruction:
            self.devices = report_instruction["readback"]["devices"]
        else:
            self.devices = list(request.content["parameter"]["args"].keys())

    def core(self):
        """core function to monitor the device values and update the progressbar accordingly."""
        request_id = self.request.metadata["RID"]
        if self.report_instruction:
            self.devices = self.report_instruction["readback"]["devices"]
            request_id = self.report_instruction["readback"]["RID"]
        data_source = ReadbackDataHandler(self.bec.device_manager, self.devices, request_id)
        start_values = data_source.get_device_values()
        self.wait_for_request_acceptance()

        if self.report_instruction:
            target_values = self.report_instruction["readback"]["end"]

            start_instr = self.report_instruction["readback"].get("start")
            if start_instr:
                start_values = self.report_instruction["readback"]["start"]
        else:
            target_values = [
                x for xs in self.request.content["parameter"]["args"].values() for x in xs
            ]
            if self.request.content["parameter"]["kwargs"].get("relative"):
                target_values = np.asarray(target_values) + np.asarray(start_values)

        with DeviceProgressBar(
            self.devices, start_values=start_values, target_values=target_values
        ) as progress:

            while not progress.finished or not data_source.done():
                check_alarms(self.bec)

                values = data_source.get_device_values()
                progress.update(values=values)
                self._print_client_msgs_asap()

                for dev, (done, success) in data_source.device_states().items():
                    if done and success:
                        progress.set_finished(dev)
                # pylint: disable=protected-access
                progress._progress.refresh()
        self._print_client_msgs_all()

    def run(self):
        """run the progressbar."""
        self.core()
