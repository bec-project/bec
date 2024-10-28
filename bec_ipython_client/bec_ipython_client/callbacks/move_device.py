from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import numpy as np

from bec_ipython_client.progressbar import DeviceProgressBar
from bec_lib.endpoints import MessageEndpoints

from .utils import LiveUpdatesBase, check_alarms

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_lib.client import BECClient
    from bec_lib.devicemanager import DeviceManagerBase


class ReadbackDataMixin:
    def __init__(self, device_manager: DeviceManagerBase, devices: list) -> None:
        """Mixin to get the current device values and request-done messages.

        Args:
            device_manager (DeviceManagerBase): device manager
            devices (list): list of devices to monitor
        """
        self.device_manager = device_manager
        self.devices = devices

    def get_device_values(self) -> list:
        """get the current device values

        Returns:
            list: list of device values
        """
        values = []
        for dev in self.devices:
            val = self.device_manager.devices[dev].read(cached=True)
            if not val:
                values.append(np.nan)
                continue
            # pylint: disable=protected-access
            hints = self.device_manager.devices[dev]._hints
            # if we have hints, use them to get the value, otherwise just use the first value
            if hints:
                values.append(val.get(hints[0]).get("value"))
            else:
                values.append(val.get(list(val.keys())[0]).get("value"))
        return values

    def get_request_done_msgs(self):
        """get all request-done messages"""
        pipe = self.device_manager.connector.pipeline()
        for dev in self.devices:
            self.device_manager.connector.get(MessageEndpoints.device_req_status(dev), pipe)
        return self.device_manager.connector.execute_pipeline(pipe)

    def wait_for_RID(self, request: messages.ScanQueueMessage) -> None:
        """wait for the readback's metadata to match the request ID

        Args:
            request (messages.ScanQueueMessage): request message
        """
        while True:
            msgs = [
                self.device_manager.connector.get(MessageEndpoints.device_readback(dev))
                for dev in self.devices
            ]
            if all(msg.metadata.get("RID") == request.metadata["RID"] for msg in msgs if msg):
                break
            check_alarms(self.device_manager.parent)


class LiveUpdatesReadbackProgressbar(LiveUpdatesBase):
    """Live feedback on motor movements using a progressbar.

    Args:
        dm (DeviceManagerBase): device_manager
        request (ScanQueueMessage): request that should be monitored

    """

    def __init__(
        self,
        bec: BECClient,
        report_instruction: list = None,
        request: messages.ScanQueueMessage = None,
        callbacks: list[Callable] = None,
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
        data_source = ReadbackDataMixin(self.bec.device_manager, self.devices)
        start_values = data_source.get_device_values()
        self.wait_for_request_acceptance()
        data_source.wait_for_RID(self.request)
        if self.report_instruction:
            self.devices = self.report_instruction["readback"]["devices"]
            target_values = self.report_instruction["readback"]["end"]

            start_instr = self.report_instruction["readback"].get("start")
            if start_instr:
                start_values = self.report_instruction["readback"]["start"]
            data_source = ReadbackDataMixin(self.bec.device_manager, self.devices)
        else:
            target_values = [
                x for xs in self.request.content["parameter"]["args"].values() for x in xs
            ]
            if self.request.content["parameter"]["kwargs"].get("relative"):
                target_values = np.asarray(target_values) + np.asarray(start_values)

        with DeviceProgressBar(
            self.devices, start_values=start_values, target_values=target_values
        ) as progress:
            req_done = False
            while not progress.finished or not req_done:
                check_alarms(self.bec)

                values = data_source.get_device_values()
                progress.update(values=values)
                self._print_client_msgs_asap()

                msgs = data_source.get_request_done_msgs()
                request_ids = [
                    msg.metadata["RID"] if (msg and msg.metadata.get("RID")) else None
                    for msg in msgs
                ]

                if self.report_instruction:
                    compare_rids = set([self.report_instruction["readback"]["RID"]])
                else:
                    compare_rids = set([self.request.metadata["RID"]])
                if set(request_ids) != set(compare_rids):
                    progress.sleep()
                    continue

                req_done = True
                for dev, msg in zip(self.devices, msgs):
                    if not msg:
                        continue
                    if msg.content.get("success", False):
                        progress.set_finished(dev)
                # pylint: disable=protected-access
                progress._progress.refresh()
        self._print_client_msgs_all()

    def run(self):
        """run the progressbar."""
        self.core()
