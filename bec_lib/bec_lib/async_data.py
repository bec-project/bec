"""
This module contains the AsyncDataHandler class which is used to receive and store async device data from the BEC.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import numpy as np

from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_lib.connector import ConnectorBase


class AsyncDataHandler:
    def __init__(self, connector: ConnectorBase):
        self.connector = connector

    def get_async_data_for_scan(self, scan_id: str) -> dict[list]:
        """
        Get the async data for a given scan.

        Args:
            scan_id(str): the scan id to get the async data for

        Returns:
            dict[list]: the async data for the scan sorted by device name
        """
        async_device_keys = self.connector.keys(
            MessageEndpoints.device_async_readback(scan_id, "*")
        )
        async_data = {}
        for device_key in async_device_keys:
            key = device_key.decode()
            device_name = key.split(MessageEndpoints.device_async_readback(scan_id, "").endpoint)[
                -1
            ].split(":")[0]
            data = self.get_async_data_for_device(scan_id, device_name)
            if not data:
                continue
            async_data[device_name] = data
        return async_data

    def get_async_data_for_device(self, scan_id: str, device_name: str) -> list:
        """
        Get the async data for a given device in a scan.

        Args:
            scan_id(str): the scan id to get the async data for
            device_name(str): the device name to get the async data for

        Returns:
            list: the async data for the device
        """
        key = MessageEndpoints.device_async_readback(scan_id, device_name)
        msgs = self.connector.xrange(key, min="-", max="+")
        if not msgs:
            return []
        return self.process_async_data(msgs)

    @staticmethod
    def process_async_data(
        msgs: list[dict[Literal["data"], messages.DeviceMessage]]
    ) -> dict | list[dict]:
        """
        Process the async data.

        Args:
            msgs(list[messages.DeviceMessage]): the async data to process

        Returns:
            list: the processed async data
        """
        concat_type = None
        data = []
        async_data = {}
        for msg in msgs:
            msg = msg["data"]
            if not concat_type:
                concat_type = msg.metadata.get("async_update", "append")
            data.append(msg.content["signals"])
        if len(data) == 1:
            async_data = data[0]
            return async_data
        if concat_type == "extend":
            # concatenate the dictionaries
            for signal in data[0].keys():
                async_data[signal] = {}
                for key in data[0][signal].keys():
                    if hasattr(data[0][signal][key], "__iter__"):
                        async_data[signal][key] = np.concatenate([d[signal][key] for d in data])
                    else:
                        async_data[signal][key] = [d[signal][key] for d in data]
            return async_data
        if concat_type == "append":
            # concatenate the lists
            for key in data[0].keys():
                async_data[key] = {"value": [], "timestamp": []}
                for d in data:
                    async_data[key]["value"].append(d[key]["value"])
                    if "timestamp" in d[key]:
                        async_data[key]["timestamp"].append(d[key]["timestamp"])
            return async_data
        if concat_type == "replace":
            # replace the dictionaries
            async_data = data[-1]
            return async_data
        raise ValueError(f"Unknown async update type: {concat_type}")
