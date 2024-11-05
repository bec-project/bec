import datetime
import itertools
import threading
import time
import traceback

import numpy
from blissdata.redis_engine.store import DataStore
from blissdata.streams.base import Stream

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.file_utils import FileWriter
from bec_lib.logger import bec_logger

from .blissdata_utils import bec_scan_info_to_blissdata_scan_info
from .device_validation import DeviceValidation
from .errors import DeviceMessageError, ScanAbortion
from .scan_queue import InstructionQueueItem, InstructionQueueStatus, RequestBlock

logger = bec_logger.logger


class ScanWorker(threading.Thread):
    """
    Scan worker receives device instructions and pre-processes them before sending them to the device server
    """

    def __init__(self, *, parent, queue_name: str = "primary"):
        super().__init__(daemon=True)
        self._data_store = None
        self._blissdata_scan = {}
        self.queue_name = queue_name
        self.name = f"ScanWorker-{queue_name}"
        self.parent = parent
        self.device_manager = self.parent.device_manager
        self.connector = self.parent.connector
        self.status = InstructionQueueStatus.IDLE
        self.signal_event = threading.Event()
        self.scan_id = None
        self.scan_motors = []
        self.readout_priority = {}
        self.scan_type = None
        self.current_scan_id = None
        self.current_scan_info = None
        self._staged_devices = set()
        self.max_point_id = 0
        self._exposure_time = None
        self.current_instruction_queue_item = None
        self._last_trigger = {}
        self._groups = {}
        self.interception_msg = None
        self.reset()
        self.validate = DeviceValidation(self.device_manager.connector, self)

    def open_scan(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Open a new scan and emit a scan status message.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler

        """
        if not self.scan_id:
            self.scan_id = instr.metadata.get("scan_id")
            if instr.content["parameter"].get("scan_motors") is not None:
                self.scan_motors = [
                    self.device_manager.devices[dev]
                    for dev in instr.content["parameter"].get("scan_motors")
                ]
                self.readout_priority = instr.content["parameter"].get("readout_priority", {})
            self.scan_type = instr.content["parameter"].get("scan_type")

        if not instr.metadata.get("scan_def_id"):
            self.max_point_id = 0
        instr_num_points = instr.content["parameter"].get("num_points", 0)
        if instr_num_points is None:
            instr_num_points = 0
        num_points = self.max_point_id + instr_num_points
        if self.max_point_id:
            num_points += 1

        active_rb = self.current_instruction_queue_item.active_request_block

        self._initialize_scan_info(active_rb, instr, num_points)

        # from here, self.current_scan_info is populated
        if self._data_store is None:
            # new scan
            service = self.device_manager._service
            redis_data_url = f"redis://{service._service_config.redis_data}"
            self._data_store = DataStore(redis_data_url)

        bliss_scan_id = {
            "name": self.current_scan_info["scan_name"],
            "number": self.current_scan_info["scan_number"],
            "data_policy": "no_policy",
            "session": "bec",
        }

        # read info about monitored devices ;
        # create blissdata scan_info structure
        devname_channels, scan_info = bec_scan_info_to_blissdata_scan_info(
            self.current_scan_info, self.connector
        )

        new_blissdata_scan = self._data_store.create_scan(bliss_scan_id, info=scan_info)

        data_streams = {}
        for dev_name, channel_info_list in devname_channels.items():
            data_streams[dev_name] = [
                new_blissdata_scan.create_stream(
                    Stream.recipe(
                        channel_info.name, channel_info.dtype, channel_info.shape, info={}
                    )
                )
                for channel_info in channel_info_list
            ]
        self._blissdata_scan[self.scan_id] = (new_blissdata_scan, data_streams)

        # after 'prepare', clients can connect to streams
        new_blissdata_scan.prepare()

        # now we can tell devices about their data streams
        for dev_name, streams in data_streams.items():
            if dev_name.startswith("axis:"):
                dev_name = dev_name[5:]
            msg = messages.DeviceStreamInfoMessage(
                scan_key=new_blissdata_scan.key,
                device_name=dev_name,
                stream_names=[stream.name for stream in streams],
            )
            self.connector.send(MessageEndpoints.device_blissdata_stream(dev_name), msg)

        # only append the scan_progress if the scan is not using device_progress
        if active_rb.scan.use_scan_progress_report:
            if not self.scan_report_instructions or not self.scan_report_instructions[-1].get(
                "device_progress"
            ):
                self.scan_report_instructions.append({"scan_progress": num_points})
        self.current_instruction_queue_item.parent.queue_manager.send_queue_status()

        new_blissdata_scan.start()
        self._send_scan_status("open")

    def close_scan(self, instr: messages.DeviceInstructionMessage, max_point_id: int) -> None:
        """
        Close a scan and emit a scan status message.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler
            max_point_id (int): Maximum point ID of the scan
        """
        scan_id = instr.metadata.get("scan_id")

        if self.scan_id != scan_id:
            return

        try:
            bliss_scan, data_streams = self._blissdata_scan[self.scan_id]
        except KeyError:
            # are we in tests ?
            pass
        else:
            # close blissdata streams
            for stream in itertools.chain(*data_streams.values()):
                stream.seal()
            bliss_scan.stop()
            bliss_scan.info["end_reason"] = "SUCCESS"
            bliss_scan.info["end_time"] = datetime.datetime.now().astimezone().isoformat()
            bliss_scan.close()
            del self._blissdata_scan[self.scan_id]

        # reset the scan ID now that the scan will be closed
        self.scan_id = None

        scan_info = self.current_scan_info
        if scan_info.get("scan_type") == "fly":
            # flyers do not increase the point_id but instead set the num_points directly
            num_points = self.current_instruction_queue_item.active_request_block.scan.num_pos
            self.current_scan_info["num_points"] = num_points
        else:
            # point_id starts at 0
            scan_info["num_points"] = max_point_id + 1

        self._send_scan_status("closed")

    def wait_for_devices(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Wait for devices to become ready. This is a blocking call.
        Depending on the wait_type, the devices will be checked for different statuses ("idle", "read", "trigger").

        Args:
            instr (DeviceInstructionMessage): DeviceInstructionMessage
        """
        wait_type = instr.content["parameter"].get("type")

        if wait_type == "move":
            self._wait_for_idle(instr)
        elif wait_type == "read":
            self._wait_for_read(instr)
        elif wait_type == "trigger":
            self._wait_for_trigger(instr)
        else:
            logger.error("Unknown wait command")
            raise DeviceMessageError("Unknown wait command")

    def trigger_devices(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Trigger devices by sending a trigger instruction to the device server.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler

        """
        devices = [
            dev.root.name for dev in self.device_manager.devices.get_software_triggered_devices()
        ]
        self._last_trigger = instr
        self.device_manager.connector.send(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=devices,
                action="trigger",
                parameter=instr.content["parameter"],
                metadata=instr.metadata,
            ),
        )
        logger.debug(f"Triggered devices: {devices}")

    def set_devices(self, instr: messages.DeviceInstructionMessage) -> None:
        """Send device instruction to set a device to a specific value

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler
        """

        # send instruction
        self.device_manager.connector.send(MessageEndpoints.device_instructions(), instr)

    def read_devices(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Read from devices by sending a read instruction to the device server.
        This call is not blocking. Instead, a separate call to wait_for_devices is needed to wait for the devices to become ready.
        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler

        """
        if instr.metadata.get("cached"):
            self._publish_readback(instr)
            return

        connector = self.device_manager.connector

        devices = instr.content.get("device")
        if devices is None:
            devices = [
                dev.root.name
                for dev in self.device_manager.devices.monitored_devices(
                    readout_priority=self.readout_priority
                )
            ]
        connector.send(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=devices,
                action="read",
                parameter=instr.content["parameter"],
                metadata=instr.metadata,
            ),
        )
        return

    def kickoff_devices(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Kickoff devices by sending a kickoff instruction to the device server.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler

        """
        # logger.info("kickoff")
        self.device_manager.connector.send(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=instr.content.get("device"),
                action="kickoff",
                parameter=instr.content["parameter"],
                metadata=instr.metadata,
            ),
        )

    def complete_devices(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Complete devices by sending a complete instruction to the device server.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler

        """
        if instr.content.get("device") is None:
            devices = [dev.root.name for dev in self.device_manager.devices.enabled_devices]
        else:
            devices = instr.content.get("device")
        if not isinstance(devices, list):
            devices = [devices]
        self.device_manager.connector.send(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=devices,
                action="complete",
                parameter=instr.content["parameter"],
                metadata=instr.metadata,
            ),
        )
        self._wait_for_status(devices, instr.metadata)

    def baseline_reading(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Perform a baseline reading by sending a read instruction to the device server.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler

        """
        baseline_devices = [
            dev.root.name
            for dev in self.device_manager.devices.baseline_devices(
                readout_priority=self.readout_priority
            )
        ]
        params = instr.content["parameter"]
        self.device_manager.connector.send(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=baseline_devices, action="read", parameter=params, metadata=instr.metadata
            ),
        )

    def pre_scan(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Perform pre-scan actions. This is a blocking call as it waits for devices to become ready again.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler
        """
        devices = [dev.root.name for dev in self.device_manager.devices.enabled_devices]
        self.device_manager.connector.send(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=devices,
                action="pre_scan",
                parameter=instr.content["parameter"],
                metadata=instr.metadata,
            ),
        )
        self._wait_for_status(devices, instr.metadata)

    def publish_data_as_read(self, instr: messages.DeviceInstructionMessage):
        """
        Publish data as read by sending a DeviceMessage to the device_read endpoint.
        This instruction replicates the behaviour of the device server when it receives a read instruction.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler
        """
        connector = self.device_manager.connector
        data = instr.content["parameter"]["data"]
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]
        if not isinstance(data, list):
            data = [data]
        for device, dev_data in zip(devices, data):
            msg = messages.DeviceMessage(signals=dev_data, metadata=instr.metadata)
            connector.set_and_publish(MessageEndpoints.device_read(device), msg)

    def send_rpc(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Send a RPC instruction to the device server.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler

        """
        self.device_manager.connector.send(MessageEndpoints.device_instructions(), instr)

    def process_scan_report_instruction(self, instr):
        """
        Process a scan report instruction by appending it to the scan_report_instructions list.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler

        """
        self.scan_report_instructions.append(instr.content["parameter"])
        self.current_instruction_queue_item.parent.queue_manager.send_queue_status()

    def stage_devices(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Stage devices by sending a stage instruction to the device server.
        This is a blocking call as it waits for devices to return again.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler

        """
        async_devices = self.device_manager.devices.async_devices()
        async_device_names = [dev.root.name for dev in async_devices]
        excluded_devices = async_devices
        excluded_devices.extend(self.device_manager.devices.on_request_devices())
        excluded_devices.extend(self.device_manager.devices.continuous_devices())
        stage_device_names_without_async = [
            dev.root.name
            for dev in self.device_manager.devices.enabled_devices
            if dev not in excluded_devices
        ]
        for det in async_devices:
            self.device_manager.connector.send(
                MessageEndpoints.device_instructions(),
                messages.DeviceInstructionMessage(
                    device=det.name,
                    action="stage",
                    parameter=instr.content["parameter"],
                    metadata=instr.metadata,
                ),
            )
        self._staged_devices.update(async_device_names)

        self.device_manager.connector.send(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=stage_device_names_without_async,
                action="stage",
                parameter=instr.content["parameter"],
                metadata=instr.metadata,
            ),
        )
        self._staged_devices.update(stage_device_names_without_async)
        self._wait_for_stage(staged=True, devices=async_device_names, metadata=instr.metadata)
        self._wait_for_stage(
            staged=True, devices=stage_device_names_without_async, metadata=instr.metadata
        )

    def unstage_devices(
        self, instr: messages.DeviceInstructionMessage = None, devices: list = None, cleanup=False
    ) -> None:
        """
        Unstage devices by sending a unstage instruction to the device server.
        This is a blocking call as it waits for devices to return again.

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler
            devices (list): List of devices to unstage
            cleanup (bool): If True, do not wait for devices to return

        """
        if not devices:
            devices = [dev.root.name for dev in self.device_manager.devices.enabled_devices]
        parameter = {} if not instr else instr.content["parameter"]
        metadata = {} if not instr else instr.metadata
        self._staged_devices.difference_update(devices)
        self.device_manager.connector.send(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=devices, action="unstage", parameter=parameter, metadata=metadata
            ),
        )
        if not cleanup:
            self._wait_for_stage(staged=False, devices=devices, metadata=metadata)

    @property
    def scan_report_instructions(self):
        """
        List of scan report instructions
        """
        req_block = self.current_instruction_queue_item.active_request_block
        return req_block.scan_report_instructions

    def _get_devices_from_instruction(
        self, instr: messages.DeviceInstructionMessage
    ) -> list[DeviceBase]:
        """Extract devices from instruction message

        Args:
            instr (DeviceInstructionMessage): DeviceInstructionMessage

        Returns:
            list[Device]: List of devices
        """
        devices = []
        if not instr.content.get("device"):
            group = instr.content["parameter"].get("group")
            if group == "primary":
                devices = self.device_manager.devices.monitored_devices(
                    readout_priority=self.readout_priority
                )
            elif group == "scan_motor":
                devices = self.scan_motors
        else:
            instr_devices = instr.content.get("device")
            if not isinstance(instr_devices, list):
                instr_devices = [instr_devices]
            devices = [self.device_manager.devices[dev] for dev in instr_devices]
        return devices

    def _add_wait_group(self, instr: messages.DeviceInstructionMessage) -> None:
        """If needed, add a wait_group. This wait_group can later be used to
        wait for instructions to complete before continuing.

        Example:
            DeviceInstructionMessage(({'device': ['samx', 'samy'], 'action': 'read', 'parameter': {'group': 'scan_motor', 'wait_group': 'scan_motor'}}, {DIID': 0,...}))

            This instruction would create a new wait_group entry for the devices samx and samy, to finish DIID 0.

        Args:
            instr (DeviceInstructionMessage): DeviceInstructionMessage

        """
        wait_group = instr.content["parameter"].get("wait_group")
        action = instr.content["action"]
        if not wait_group or action == "wait":
            return

        devices = self._get_devices_from_instruction(instr)
        DIID = instr.metadata.get("DIID")
        if DIID is None:
            raise DeviceMessageError("Device message metadata does not contain a DIID entry.")

        if wait_group in self._groups:
            self._groups[wait_group].update({dev.dotted_name: DIID for dev in devices})
        else:
            self._groups[wait_group] = {dev.dotted_name: DIID for dev in devices}

    def _check_for_failed_movements(
        self, device_status: list, devices: list, instr: messages.DeviceInstructionMessage
    ):
        if all(dev.content["success"] for dev in device_status):
            return
        ind = [dev.content["success"] for dev in device_status].index(False)
        failed_device = devices[ind]

        # make sure that this is not an old message
        matching_DIID = device_status[ind].metadata.get("DIID") >= devices[ind][1]
        matching_RID = device_status[ind].metadata.get("RID") == instr.metadata["RID"]
        if matching_DIID and matching_RID:
            last_pos_msg = self.device_manager.connector.get(
                MessageEndpoints.device_readback(failed_device[0])
            )
            last_pos = last_pos_msg.content["signals"][failed_device[0]]["value"]
            self.connector.raise_alarm(
                severity=Alarms.MAJOR,
                source=instr.content,
                msg=(
                    f"Movement of device {failed_device[0]} failed whilst trying to reach the"
                    f" target position. Last recorded position: {last_pos}"
                ),
                alarm_type="MovementFailed",
                metadata=instr.metadata,
            )
            raise ScanAbortion

    def _wait_for_idle(self, instr: messages.DeviceInstructionMessage) -> None:
        """Wait for devices to become IDLE

        Args:
            instr (DeviceInstructionMessage): Device instruction received from the scan assembler
        """
        start = datetime.datetime.now()

        wait_group = instr.content["parameter"].get("wait_group")

        if not wait_group or wait_group not in self._groups:
            return

        group_devices = [dev.dotted_name for dev in self._get_devices_from_instruction(instr)]
        wait_group_devices = [
            (dev_name, DIID)
            for dev_name, DIID in self._groups[wait_group].items()
            if dev_name in group_devices
        ]

        logger.debug(f"Waiting for devices: {wait_group}")

        while not self.validate.devices_are_ready(
            [dev for dev, _ in wait_group_devices],
            MessageEndpoints.device_req_status,
            messages.DeviceReqStatusMessage,
            instr.metadata,
            [self.validate.devices_returned_successfully, self.validate.matching_requestID],
            wait_group_devices=wait_group_devices,
            instruction=instr,
        ):
            continue

        self._groups[wait_group] = {
            dev: DIID for dev, DIID in self._groups[wait_group].items() if dev not in group_devices
        }
        logger.debug("Finished waiting")
        logger.debug(datetime.datetime.now() - start)

    def _wait_for_read(self, instr: messages.DeviceInstructionMessage) -> None:
        start = datetime.datetime.now()

        wait_group = instr.content["parameter"].get("wait_group")

        if not wait_group or wait_group not in self._groups:
            return

        group_devices = [dev.root.name for dev in self._get_devices_from_instruction(instr)]
        wait_group_devices = [
            (dev_name, DIID)
            for dev_name, DIID in self._groups[wait_group].items()
            if dev_name in group_devices
        ]

        logger.debug(f"Waiting for devices: {wait_group}")

        while not self.validate.devices_are_ready(
            [dev for dev, _ in wait_group_devices],
            MessageEndpoints.device_status,
            messages.DeviceStatusMessage,
            instr.metadata,
            [self.validate.devices_are_idle],
            wait_group_devices=wait_group_devices,
        ):
            continue

        self._groups[wait_group] = {
            dev: DIID for dev, DIID in self._groups[wait_group].items() if dev not in group_devices
        }
        logger.debug("Finished waiting")
        logger.debug(datetime.datetime.now() - start)

    def _wait_for_stage(self, staged: bool, devices: list, metadata: dict) -> None:
        """
        Wait for devices to become staged/unstaged

        Args:
            staged (bool): True if devices should be staged, False if they should be unstaged
            devices (list): List of devices to wait for
            metadata (dict): Metadata of the instruction
        """

        stage_validator = (
            self.validate.devices_are_staged if staged else self.validate.devices_are_unstaged
        )

        while not self.validate.devices_are_ready(
            devices,
            MessageEndpoints.device_staged,
            messages.DeviceStatusMessage,
            metadata,
            [stage_validator, self.validate.matching_requestID],
        ):
            continue

    def _wait_for_device_server(self) -> None:
        self.parent.wait_for_service("DeviceServer")

    def _wait_for_trigger(self, instr: messages.DeviceInstructionMessage) -> None:
        trigger_time = float(
            instr.content["parameter"].get("time", 0)
        ) * self.current_scan_info.get("frames_per_trigger", 1)
        time.sleep(trigger_time)
        devices = [
            dev.dotted_name for dev in self.device_manager.devices.get_software_triggered_devices()
        ]
        metadata = self._last_trigger.metadata
        self._wait_for_status(devices, metadata)

    def _wait_for_status(self, devices, metadata):
        logger_update_delay = 5
        start = time.time()
        print_status = False

        while not self.validate.devices_are_ready(
            devices,
            MessageEndpoints.device_req_status,
            messages.DeviceReqStatusMessage,
            metadata,
            [self.validate.devices_returned_successfully],
            print_status=print_status,
        ):
            if time.time() - start > logger_update_delay:
                # report the status of the devices that are not ready yet
                print_status = True
                time.sleep(1)

    def _publish_readback(
        self, instr: messages.DeviceInstructionMessage, devices: list = None
    ) -> None:
        connector = self.device_manager.connector
        if not devices:
            devices = instr.content.get("device")

        # cached readout
        readouts = self._get_readback(devices)
        pipe = connector.pipeline()
        for readout, device in zip(readouts, devices):
            msg = messages.DeviceMessage(signals=readout, metadata=instr.metadata)
            connector.set_and_publish(MessageEndpoints.device_read(device), msg, pipe)
        return pipe.execute()

    def _get_readback(self, devices: list) -> list:
        connector = self.device_manager.connector
        # cached readout
        pipe = connector.pipeline()
        for dev in devices:
            connector.get(MessageEndpoints.device_readback(dev), pipe=pipe)
        return connector.execute_pipeline(pipe)

    def _check_for_interruption(self) -> None:
        if self.status == InstructionQueueStatus.PAUSED:
            self._send_scan_status("paused")
        while self.status == InstructionQueueStatus.PAUSED:
            time.sleep(0.1)
        if self.status == InstructionQueueStatus.STOPPED:
            raise ScanAbortion

    def _initialize_scan_info(
        self, active_rb: RequestBlock, instr: messages.DeviceInstructionMessage, num_points: int
    ):
        metadata = active_rb.metadata
        self.current_scan_info = {**instr.metadata, **instr.content["parameter"]}
        self.current_scan_info.update(metadata)
        self.current_scan_info.update(
            {
                "scan_number": self.parent.scan_number,
                "dataset_number": self.parent.dataset_number,
                "exp_time": self._exposure_time,
                "frames_per_trigger": active_rb.scan.frames_per_trigger,
                "settling_time": active_rb.scan.settling_time,
                "readout_time": active_rb.scan.readout_time,
                "acquisition_config": active_rb.scan.acquisition_config,
                "scan_report_devices": active_rb.scan.scan_report_devices,
                "monitor_sync": active_rb.scan.monitor_sync,
                "num_points": num_points,
            }
        )
        scan_args = active_rb.scan.parameter["args"]
        scan_kwargs = active_rb.scan.parameter["kwargs"]
        system_config = scan_kwargs.get("system_config", {"file_suffix": "", "file_directory": ""})
        user_suffix = system_config["file_suffix"]
        scan_dir = system_config["file_directory"]
        service_config = self.device_manager._service._service_config
        file_writer_service_config = service_config.service_config.get("file_writer")
        self.current_scan_info["scan_file_path"] = FileWriter(
            service_config=file_writer_service_config
        ).compile_full_filename(
            create_dir=False,
            scan_number=self.parent.scan_number,
            user_suffix=user_suffix,
            scan_dir=scan_dir,
        )

        self.current_scan_info["scan_msgs"] = [
            str(scan_msg) for scan_msg in self.current_instruction_queue_item.scan_msgs
        ]
        self.current_scan_info["args"] = scan_args
        self.current_scan_info["kwargs"] = scan_kwargs
        self.current_scan_info["readout_priority"] = {
            "monitored": [
                dev.full_name
                for dev in self.device_manager.devices.monitored_devices(
                    readout_priority=self.readout_priority
                )
            ],
            "baseline": [
                dev.full_name
                for dev in self.device_manager.devices.baseline_devices(
                    readout_priority=self.readout_priority
                )
            ],
            "async": [
                dev.full_name
                for dev in self.device_manager.devices.async_devices(
                    readout_priority=self.readout_priority
                )
            ],
            "continuous": [
                dev.full_name
                for dev in self.device_manager.devices.continuous_devices(
                    readout_priority=self.readout_priority
                )
            ],
            "on_request": [
                dev.full_name
                for dev in self.device_manager.devices.on_request_devices(
                    readout_priority=self.readout_priority
                )
            ],
        }

    def _send_scan_status(self, status: str):
        current_scan_info_print = self.current_scan_info.copy()
        if current_scan_info_print.get("positions", []):
            current_scan_info_print["positions"] = "..."
        logger.info(
            f"New scan status: {self.current_scan_id} / {status} / {current_scan_info_print}"
        )
        msg = messages.ScanStatusMessage(
            scan_id=self.current_scan_id, status=status, info=self.current_scan_info
        )
        expire = None if status in ["open", "paused"] else 1800
        pipe = self.device_manager.connector.pipeline()
        self.device_manager.connector.set(
            MessageEndpoints.public_scan_info(self.current_scan_id), msg, pipe=pipe, expire=expire
        )
        self.device_manager.connector.set_and_publish(
            MessageEndpoints.scan_status(), msg, pipe=pipe
        )
        pipe.execute()

    def _process_instructions(self, queue: InstructionQueueItem) -> None:
        """
        Process scan instructions and send DeviceInstructions to OPAAS.
        For now this is an in-memory communication. In the future however,
        we might want to pass it through a dedicated Kafka topic.
        Args:
            queue: instruction queue

        Returns:

        """
        if not queue:
            return None
        self.current_instruction_queue_item = queue

        start = time.time()
        self.max_point_id = 0

        # make sure the device server is ready to receive data
        self._wait_for_device_server()

        queue.is_active = True
        try:
            for instr in queue:
                self._check_for_interruption()
                if instr is None:
                    continue
                self._exposure_time = getattr(queue.active_request_block.scan, "exp_time", None)
                self._instruction_step(instr)
        except ScanAbortion as exc:
            self._groups = {}
            if queue.stopped or not (queue.return_to_start and queue.active_request_block):
                raise ScanAbortion from exc
            queue.stopped = True
            try:
                cleanup = queue.active_request_block.scan.return_to_start()
                self.status = InstructionQueueStatus.RUNNING
                for instr in cleanup:
                    self._check_for_interruption()
                    instr.metadata["scan_id"] = queue.queue.active_rb.scan_id
                    instr.metadata["queue_id"] = queue.queue_id
                    self._instruction_step(instr)
            except Exception as exc_return_to_start:
                # if the return_to_start fails, raise the original exception
                content = traceback.format_exc()
                logger.error(content)
                self.connector.raise_alarm(
                    severity=Alarms.MAJOR,
                    source={"ScanWorker": "_process_instructions"},
                    msg=content,
                    alarm_type=exc_return_to_start.__class__.__name__,
                    metadata={},
                )
                raise ScanAbortion from exc
            raise ScanAbortion from exc
        except Exception as exc:
            content = traceback.format_exc()
            logger.error(content)
            self.connector.raise_alarm(
                severity=Alarms.MAJOR,
                source={"ScanWorker": "_process_instructions"},
                msg=content,
                alarm_type=exc.__class__.__name__,
                metadata={},
            )
            raise ScanAbortion from exc
        queue.is_active = False
        queue.status = InstructionQueueStatus.COMPLETED
        self.current_instruction_queue_item = None

        logger.info(f"QUEUE ITEM finished after {time.time()-start:.2f} seconds")
        self.reset()

    def _instruction_step(self, instr: messages.DeviceInstructionMessage):
        logger.debug(instr)
        action = instr.content.get("action")
        scan_def_id = instr.metadata.get("scan_def_id")
        if self.current_scan_id != instr.metadata.get("scan_id"):
            self.current_scan_id = instr.metadata.get("scan_id")

        if "point_id" in instr.metadata:
            self.max_point_id = instr.metadata["point_id"]

        self._add_wait_group(instr)

        logger.debug(f"Device instruction: {instr}")
        self._check_for_interruption()

        if action == "open_scan":
            self.open_scan(instr)
        elif action == "close_scan" and scan_def_id is None:
            self.close_scan(instr, self.max_point_id)
        elif action == "close_scan" and scan_def_id is not None:
            pass
        elif action == "open_scan_def":
            pass
        elif action == "close_scan_def":
            self.close_scan(instr, self.max_point_id)
        elif action == "wait":
            self.wait_for_devices(instr)
        elif action == "trigger":
            self.trigger_devices(instr)
        elif action == "set":
            self.set_devices(instr)
        elif action == "read":
            self.read_devices(instr)
        elif action == "kickoff":
            self.kickoff_devices(instr)
        elif action == "complete":
            self.complete_devices(instr)
        elif action == "baseline_reading":
            self.baseline_reading(instr)
        elif action == "rpc":
            self.send_rpc(instr)
        elif action == "stage":
            self.stage_devices(instr)
        elif action == "unstage":
            self.unstage_devices(instr)
        elif action == "pre_scan":
            self.pre_scan(instr)
        elif action == "publish_data_as_read":
            self.publish_data_as_read(instr)
        elif action == "scan_report_instruction":
            self.process_scan_report_instruction(instr)

        else:
            logger.warning(f"Unknown device instruction: {instr}")

    def reset(self):
        """reset the scan worker and its member variables"""
        self._groups = {}
        self.current_scan_id = ""
        self.current_scan_info = {}
        self.scan_id = None
        self.interception_msg = None
        self.scan_motors = []

    def cleanup(self):
        """perform cleanup instructions"""
        self.unstage_devices(devices=list(self._staged_devices), cleanup=True)

    def run(self):
        try:
            while not self.signal_event.is_set():
                try:
                    for queue in self.parent.queue_manager.queues[self.queue_name]:
                        self._process_instructions(queue)
                        if self.signal_event.is_set():
                            break
                        if not queue.stopped:
                            queue.append_to_queue_history()

                except ScanAbortion:
                    content = traceback.format_exc()
                    logger.error(content)
                    queue.queue.increase_scan_number()
                    if queue.return_to_start:
                        self._send_scan_status("aborted")
                    else:
                        self._send_scan_status("halted")
                    logger.info(f"Scan aborted: {queue.queue_id}")
                    queue.status = InstructionQueueStatus.STOPPED
                    queue.append_to_queue_history()
                    self.cleanup()
                    self.parent.queue_manager.queues[self.queue_name].abort()
                    self.reset()
                    self.status = InstructionQueueStatus.RUNNING

        # pylint: disable=broad-except
        except Exception as exc:
            content = traceback.format_exc()
            logger.error(content)
            self.connector.raise_alarm(
                severity=Alarms.MAJOR,
                source={"ScanWorker": "run"},
                msg=content,
                alarm_type=exc.__class__.__name__,
                metadata={},
            )
            if self.queue_name in self.parent.queue_manager.queues:
                self.parent.queue_manager.queues[self.queue_name].abort()
            self.reset()
            logger.error(f"Scan worker stopped: {exc}. Unrecoverable error.")

    def shutdown(self):
        """shutdown the scan worker"""
        self.signal_event.set()
        if self._started.is_set():
            self.join()
