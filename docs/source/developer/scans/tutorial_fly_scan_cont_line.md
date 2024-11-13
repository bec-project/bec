# Tutorial - Continuous Line / Fly Scan
In this tutorial, we will show you how to write a continuous line fly scan using a BEC server plugin. This tutorial assumes that you have already set up the BEC server and that you have a basic understanding of the scan structure in the BEC server. If not, please refer to the [scan documentation](#developer.scans).

## Desired Outcome
We want to write a fly scan that moves a motor from one position to another at a constant speed. Throughout the scan, we want to send triggers as fast as possible (respecting the requested exposure time). Once the motor reaches the end position, we want to stop the scan.

## Step 1: Create a New Scan
Let's start by creating a new scan file in the `scans` directory of our plugin repository and name it tutorial_fly_scan_cont_line.py. We will start by importing the necessary modules and defining the scan class. Since we are writing a fly scan, we want to inherit from a FlyScan base class. In our case, we will inherit from the `AsyncFlyScanBase` class as our flyer will not be in charge of synchronizing the data collection.

```python
import numpy as np

from bec_lib.device import DeviceBase
from bec_server.scan_server.scans import AsyncFlyScanBase

class TutorialFlyScanContLine(AsyncFlyScanBase):
    scan_name = "tutorial_fly_scan_cont_line"
```

To make the scan available to the BEC server, we need to add it the `__init__.py` file in the scans directory. To this end, add the following line to the `__init__.py` file:

```python
from .tutorial_fly_scan_cont_line import TutorialFlyScanContLine
```

## Step 2: Define the Scan Parameters
Next, we need to define the scan parameters. In our case, we want to pass in the following parameters:
- `motor`: The motor to move during the scan. This should be a `DeviceBase` object, i.e. any device that inherits from the `DeviceBase` class.
- `start`: The starting position of the motor. This should be a float.
- `end`: The ending position of the motor. This should be a float.
- `exp_time`: The exposure time for each trigger. This should be a float.
- `relative`: A boolean flag indicating whether the end position is relative to the start position. If `True`, the end position will be added to the start position. If `False`, the end position will be used as an absolute position. This should be a boolean.

With this in mind, we can define the `__init__` method of our scan class as follows:

```python
    def __init__(
        self,
        motor: DeviceBase,
        start: float,
        stop: float,
        exp_time: float = 0,
        relative: bool = False,
        **kwargs,
    ):
        super().__init__(exp_time=exp_time, relative=relative, **kwargs)
        self.motor = motor
        self.start = start
        self.stop = stop
        self.scan_motors = [self.motor]

```

Here, the `**kwargs` parameter allows us to pass additional keyword arguments to the base class. This is important as the base class may require additional parameters that we do not need to define in our scan class. After initializing the base class (FlyScanBase) using `super().__init__(exp_time=exp_time, relative=relative, **kwargs)`, we store the motor, start, stop, exp_time, and relative parameters as attributes of the scan class. Moreover, we define the `scan_motors` attribute as a list containing the motor to be scanned. The `scan_motors` attribute is used by the BEC server to determine which motors are involved in the scan and need to be read out for relative scans. 

Let's also add a proper doc string for the users of our scan:

```python 
    def __init__(
        self,
        motor: DeviceBase,
        start: float,
        stop: float,
        exp_time: float = 0,
        relative: bool = False,
        **kwargs,
    ):
        """
        A continuous line fly scan. Use this scan if you want to move a motor continuously from start to stop position whilst acquiring data as fast as possible (respecting the exposure time). The scan will stop automatically when the motor reaches the end position.

        Args:
            motor (DeviceBase): motor to move continuously from start to stop position
            start (float): start position
            stop (float): stop position
            exp_time (float): exposure time in seconds. Default is 0.
            relative (bool): if True, the motor will be moved relative to its current position. Default is False.

        Returns:
            ScanReport

        Examples:
            >>> scans.tutorial_cont_line_fly_scan(dev.sam_rot, 0, 180, exp_time=0.1)

        """
        super().__init__(exp_time=exp_time, relative=relative, **kwargs)
        self.motor = motor
        self.start = start
        self.stop = stop
        self.scan_motors = [self.motor]
```

## Step 3: Prepare the positions
Our scan should move the motor from the start position to the stop position at a constant speed. To achieve this, we need to override the `prepare_positions` method:

```python
    def prepare_positions(self):
        self.positions = np.array([[self.start], [self.stop]])
        self.num_pos = None
        yield from self._set_position_offset()
```

By using `self._set_position_offset()`, we ensure that the motor is moved to the correct position before starting the scan, respecting the relative flag.

## Step 4: Define the scan logic
Next, we need to define the scan logic. In our case, the following steps are required and can be built upon the [scan stubs](#developer.scans.scan_stubs) provided by the BEC server:
- Move the motor to the start position. This can be achieved by using the [`set`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.set) method. 
- Send the flyer on its way to the defined stop position. This can be achieved by using the [`set`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.set_with_response) method. Since we want to perform additional operations while the flyer is moving, we set the `wait` parameter to `False` and store the returned status object.
- While the flyer is moving:
    - Send a trigger to all devices that are set to `softwareTrigger = True`. This can be achieved by using the [`trigger`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.trigger) method and wait at least the exposure time.
    - Read out all devices on readout priority "monitored" and assign them to a new `point_id`. This can be achieved by using the [`read`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.read) method, using the group "primary".
    - Increase the `point_id` by one.


Let's build the method accordingly:

```python
    def scan_core(self):
        # move the motor to the start position
        yield from self.stubs.set(device=self.motor, value=self.start)

        # start the flyer
        status_flyer = yield from self.stubs.set(device=self.motor, value=self.stop, wait=False)

        while not status_flyer.done:
            # send a trigger and wait at least the exposure time
            yield from self.stubs.trigger(min_wait=self.exp_time)
            
            # read out all monitored devices
            yield from self.stubs.read(group="primary", point_id=self.point_id)

            # increase the point id
            self.point_id += 1

        self.num_pos = self.point_id
```

Your scan class is now complete and should look like this:

```python
import numpy as np

from bec_lib.device import DeviceBase
from bec_server.scan_server.scans import AsyncFlyScanBase


class TutorialFlyScanContLine(AsyncFlyScanBase):
    scan_name = "tutorial_cont_line_fly_scan"

    def __init__(
        self,
        motor: DeviceBase,
        start: float,
        stop: float,
        exp_time: float = 0,
        relative: bool = False,
        **kwargs,
    ):
        """
        A continuous line fly scan. Use this scan if you want to move a motor continuously from start to stop position whilst
        acquiring data as fast as possible (respecting the exposure time). The scan will stop automatically when the motor
        reaches the end position.

        Args:
            motor (DeviceBase): motor to move continuously from start to stop position
            start (float): start position
            stop (float): stop position
            exp_time (float): exposure time in seconds. Default is 0.
            relative (bool): if True, the motor will be moved relative to its current position. Default is False.

        Returns:
            ScanReport

        Examples:
            >>> scans.tutorial_cont_line_fly_scan(dev.sam_rot, 0, 180, exp_time=0.1)

        """
        super().__init__(exp_time=exp_time, relative=relative, **kwargs)
        self.motor = motor
        self.start = start
        self.stop = stop
        self.scan_motors = [self.motor]

    def prepare_positions(self):
        self.positions = np.array([[self.start], [self.stop]])
        self.num_pos = None
        yield from self._set_position_offset()

    def scan_core(self):
        # move the motor to the start position
        yield from self.stubs.set(device=self.motor, value=self.start)

        # start the flyer
        status_flyer = yield from self.stubs.set(device=self.motor, value=self.stop, wait=False)

        while not status_flyer.done:
            # send a trigger and wait at least the exposure time
            yield from self.stubs.trigger(min_wait=self.exp_time)
            
            # read out all monitored devices
            yield from self.stubs.read(group="primary", point_id=self.point_id)

            # increase the point id
            self.point_id += 1
        
        self.num_pos = self.point_id
        
```

Once you have saved the file, restart the BEC server and the client. You should now be able to see your new scan showing up as `tutorial_fly_scan_cont_line` within `scans.<tab>`.

## Step 6: (Optional) Test the scan
Testing the scan is crucial to ensure that the scan works as expected, even if the components of BEC change. The architecture of scans in BEC allows for easy testing as the scan logic is separated from the hardware control. As a result, we only need to ensure that the scan logic is correct. This can be achieved by ensuring that the correct instructions are sent to the scan worker. 

Let's create a new test file in the `tests/tests_scans` directory of our plugin repository and name it `test_tutorial_fly_scan_cont_line.py`. 

```{important}
In BEC, we are relying on the `pytest` package for testing. Therefore, all test files must be prefixed with `test_` to be picked up by the test runner.
Similarly, any file that should not be picked up by the test runner must not be prefixed with `test_`.
```

We will start by importing the necessary modules and defining the test class. We will then write a test that checks if the scan worker receives the correct instructions.

```python
from unittest import mock

from bec_lib.messages import DeviceInstructionMessage
from bec_server.scan_server.tests.fixtures import *

from <beamline_repo>.scans import TutorialFlyScanContLine
```

Of course, you need to replace `<beamline_repo>` with the name of your beamline repository.

```{note}
The `bec_server.scan_server.tests.fixtures` module provides mock objects that can be used to test scans. The wildcard import `*` imports all objects from the module, an approach that in normal code should be avoided. However, in test code, it is acceptable as it makes the test code more readable, in particular when using multiple fixtures.
```

Next, we will define the test for the scan. 

```python
def test_TutorialFlyScanContLine(scan_assembler, ScanStubStatusMock):

    request = scan_assembler(TutorialFlyScanContLine, motor="samx", start=0, stop=5, relative=False)

```
We use the `scan_assembler` fixture to create a new instance of a scan, passing in the class name and the desired arguments. 

So far, our test only initialized the scan. Since our scan relies on a status object's `done` attribute to return `True`, we need to mock the `set` method of the scan stubs to return a status object that will return `True` when the second time `done` attribute is accessed (cf. `fake_done`). To this end, our `fake_set` method will yield a fake message "fake_set" and return the `ScanStubStatusMock` object. The latter will return the `fake_done` generator when the `done_func` attribute is accessed, simulating the behavior of a real status object.

```python
    def fake_done():
        yield False
        yield True

    def fake_set(*args, **kwargs):
        yield "fake_set"
        return ScanStubStatusMock(done_func=fake_done)

    with (mock.patch.object(request.stubs, "set", side_effect=fake_set), 
          mock.patch.object(request.stubs, "_get_result_from_status", return_value={"samx": {"value": 0}})):
            ref_list = list(request.run())

```

This test configuration will run two rounds within the while loop: On the first round, the `status_flyer.done` will return `False`, and on the second round, it will return `True`. All device instructions will be stored in the `ref_list` list.

Finally, we will check if the scan worker receives the correct instructions. To ignore differences in unique identifiers, we will simply overwrite them in our `ref_list`. 

```python
    for item in ref_list:
        if not hasattr(item, "metadata"):
            continue
        item.metadata.pop("device_instr_id", None)
        if "RID" in item.metadata:
            item.metadata["RID"] = "rid"
        if "rpc_id" in item.parameter:
            item.parameter["rpc_id"] = "rpc_id"
        if "readback" in item.parameter:
            item.parameter["readback"]["RID"] = "rid"
```

Finally, we will compare the `ref_list` with the expected list of instructions.

```python

     assert ref_list == [
        None,
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device="samx",
            action="rpc",
            parameter={
                "device": "samx",
                "func": "read",
                "rpc_id": "rpc_id",
                "args": (),
                "kwargs": {},
            },
        ),
        ... # add the rest of the instructions here
    ]
```

````{dropdown} Full Test Code
```python
from unittest import mock

from bec_lib.messages import DeviceInstructionMessage

from bec_server.scan_server.tests.fixtures import *

from tomcat_bec.scans import TutorialFlyScanContLine



def test_TutorialFlyScanContLine(scan_assembler, ScanStubStatusMock):

    request = scan_assembler(TutorialFlyScanContLine, motor="samx", start=0, stop=5, relative=False)

    def fake_done():
        yield False
        yield True

    def fake_set(*args, **kwargs):
        yield "fake_set"
        return ScanStubStatusMock(done_func=fake_done)

    with (
        mock.patch.object(request.stubs, "set", side_effect=fake_set),
        mock.patch.object(
            request.stubs, "_get_result_from_status", return_value={"samx": {"value": 0}}
        ),
    ):
        ref_list = list(request.run())

    for item in ref_list:
        if not hasattr(item, "metadata"):
            continue
        item.metadata.pop("device_instr_id", None)
        if "RID" in item.metadata:
            item.metadata["RID"] = "rid"
        if "rpc_id" in item.parameter:
            item.parameter["rpc_id"] = "rpc_id"
        if "readback" in item.parameter:
            item.parameter["readback"]["RID"] = "rid"

    assert ref_list == [
        None,
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device="samx",
            action="rpc",
            parameter={
                "device": "samx",
                "func": "read",
                "rpc_id": "rpc_id",
                "args": (),
                "kwargs": {},
            },
        ),
        None,
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=None,
            action="open_scan",
            parameter={
                "scan_motors": ["samx"],
                "readout_priority": {
                    "monitored": [],
                    "baseline": [],
                    "on_request": [],
                    "async": [],
                },
                "num_points": None,
                "positions": [[0], [5]],
                "scan_name": "tutorial_cont_line_fly_scan",
                "scan_type": "fly",
            },
        ),
        messages.DeviceInstructionMessage(
            metadata={},
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="stage",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "baseline"},
            device=["rtx", "samx", "samy", "samz"],
            action="read",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="pre_scan",
            parameter={},
        ),
        "fake_set",
        "fake_set",
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=["eiger"],
            action="trigger",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored", "point_id": 0},
            device=["bpm4i", "eiger"],
            action="read",
            parameter={"group": "primary"},
        ),
        "fake_set",
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="complete",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={},
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="unstage",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=None,
            action="close_scan",
            parameter={},
        ),
    ]


```
````

## Step 7: (Optional) Setting up more devices 
In general, it is good practice to keep the scan logic as simple as possible and to move as much device-specific logic as possible to the device classes. However, there are cases where it is necessary to set up devices after they have been staged for the scan. While we have already seen how to move a device to a specific position, the scan server also grants you access to any ophyd method available on the device. Let's take a delay generator (DDG) as an example: Before the scan, we want to configure the DDG and effectly run the following method on the ophyd object `ddg_detectors`:

```python
ddg_detectors.burst_enable(count=1, delay=0.01, period=exp_time+readout_time,config="first")
```

To run the same command from within the scan server, we can use the `send_rpc_and_wait` method:
    
```python
yield from self.stubs.send_rpc_and_wait(
    "ddg_detectors",
    "burst_enable",
    count=1,
    delay=0.01,
    period=self.exp_time,
    config="first",
)
```

Even nested methods can be called using the `send_rpc_and_wait` method. For example, to run the following command:

```python
status_ddg_detectors_source = yield from self.stubs.send_rpc_and_wait(
    "ddg_detectors", "source.set", 5
)
```

## Step 8: (Optional) Changing the scan report instruction
By default, the scan report instruction is set to `scan_progress` and usually results in a display of the scan progress by using a progress bar and a table report with monitored devices. However, especially for fly scans, it might be more meaningful to display the status of a specific device, e.g. the flyer. Here, two options are available:
- `readback` to display the readback value of a device. This is useful if you want to display the current position of the motor. It requires a constantly updating readback value. 
- `device_progress` to display the progress of a device. This is useful if you want to display the progress of the flyer. It requires a dedicated progress report on the device using the Ophyd `SUB_PROGRESS` event type. 

To demonstrate how to change the scan report instruction, we will use the `readback` option. 

To uniquely identify the readback progress, we need to retrieve the request ID of the flyer. This can be achieved by creating a unique request ID during the initialization of the flyer:


```python
import uuid

... 

super().__init__(exp_time=exp_time, relative=relative, **kwargs)
self.motor = motor
self.start = start
self.stop = stop
self.scan_motors = [self.motor]
self.device_move_request_id = str(uuid.uuid4())

```

Next, we need to update the `scan_report_instruction` method to instruct the client to display the readback value of the motor. 

```python
def scan_report_instructions(self):
    yield from self.stubs.scan_report_instruction(
        {
            "readback": {
                "RID": self.device_move_request_id,
                "devices": [self.motor],
                "start": [self.start],
                "end": [self.stop],
            }
        }
    )
```

With these changes, the scan report will now display the readback value of the motor instead of the scan progress.