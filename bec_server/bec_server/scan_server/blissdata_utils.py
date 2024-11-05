import datetime
import itertools
from dataclasses import dataclass
from importlib.metadata import version as pkg_version

import numpy
from blissdata.schemas.scan_info import ChainDict, ChannelDict, DeviceDict, ScanInfoDict

from bec_lib.endpoints import MessageEndpoints


@dataclass
class ChannelInfo:
    name: str
    channel: ChannelDict
    dtype: type
    shape: tuple


def _dev_desc_to_channels(dev_name, dev_info, scan_motors):
    try:
        dev_description = dev_info["describe"]
    except KeyError:
        # print("NO DESC FOR", dev_name, dev_description)
        pass
    else:
        # description like { [devname: {}], devname_signalname: { }, ... }
        chans = []
        for signal_name, data_desc in dev_description.items():
            if dev_name in scan_motors:
                channel_name = f"axis:{dev_name}"
            else:
                channel_name = dev_name
            signal_name = signal_name[len(dev_name) + 1 :]
            if signal_name:
                channel_name = f"{channel_name}:{signal_name}"
            dtype = data_desc["dtype"]
            if dtype == "integer":
                dtype = numpy.dtype(int).name
            else:
                dtype = numpy.dtype(dtype).name
            shape = data_desc["shape"]
            dim = len(shape)
            decimals = data_desc["precision"]
            # if signal_name in ("setpoint", "motor_is_moving"):
            #    continue
            channel_info = ChannelInfo(
                channel_name, ChannelDict(dim=dim, decimals=decimals), dtype, shape
            )
            chans.append(channel_info)
    return chans


def bec_scan_info_to_blissdata_scan_info(bec_scan_info, connector):
    scan_motors = bec_scan_info["scan_motors"]
    top_master = "axis" if scan_motors else "time"
    scan_devices = bec_scan_info["readout_priority"].get("monitored", [])

    devname_channels = {}
    for dev_name in scan_devices:
        dev_info = connector.get(MessageEndpoints.device_info(dev_name)).info["device_info"]
        devname_channels[dev_name] = _dev_desc_to_channels(dev_name, dev_info, scan_motors)

    # this 'channels' dict is for Blissdata clients,
    # it is indexed by the Blissdata channel name regardless of the BEC device they belong to
    channels = {
        channel_info.name: channel_info.channel
        for channel_info in itertools.chain(*devname_channels.values())
    }

    devices = {}
    for dev_name in scan_devices:
        if dev_name in scan_motors:
            continue
        # devices[dev_name] = DeviceDict(name=dev_name, type="mca" if "mca" in dev_name else None, channels=[channel_info.name for channel_info in devname_channels[dev_name]])
        devices[dev_name] = DeviceDict(
            name=dev_name,
            type=None,
            channels=[channel_info.name for channel_info in devname_channels[dev_name]],
        )
    devices[top_master] = DeviceDict(
        name=top_master,
        channels=[
            channel_info.name
            for channel_info in itertools.chain(*devname_channels.values())
            if channel_info.name.startswith(top_master)
        ],
        triggered_devices=list(set(scan_devices) - set(scan_motors)),
    )

    # scan_title=f"{self.current_scan_info['scan_name']}({','.join(itertools.chain(self.current_scan_info['args'])
    save = "scan_file_path" in bec_scan_info
    if save:
        scan_file_path = bec_scan_info["scan_file_path"]
    else:
        scan_file_path = ""

    scan_info_dict = ScanInfoDict(
        name=bec_scan_info["scan_name"],
        publisher="bec",
        publisher_version=pkg_version("bec_server"),
        start_time=datetime.datetime.now().astimezone().isoformat(),
        scan_nb=bec_scan_info["scan_number"],
        type=bec_scan_info["scan_type"],
        title=bec_scan_info[
            "scan_name"
        ],  # scan_title,  # title in SPEC was the command line, tradition is kept
        stab_time=bec_scan_info["settling_time"],
        sleep_time=bec_scan_info["readout_time"],
        count_time=bec_scan_info["exp_time"],
        npoints=bec_scan_info["num_points"],
        # 'devices' is deprecated in ChainDict, but still used by Writer
        # see BLISS issue #4458
        acquisition_chain={"scan": ChainDict(top_master=top_master, devices=list(devices))},
        channels=channels,
        devices=devices,
        # keys below to have NexusWriter working
        filename=scan_file_path,
        save=save,
        data_writer="external",
        scan_meta_categories=[],
        writer_options={},
    )
    import pprint

    pprint.pprint(scan_info_dict)
    return devname_channels, scan_info_dict
