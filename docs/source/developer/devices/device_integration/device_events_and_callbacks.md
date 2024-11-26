(developer.devices.device_integration.device_events_and_callbacks)=
# Device Events and Callbacks

*Ophyd* provides mechanisms to execute callbacks in response to specific device events. These events, known as `SUB_EVENTS`, can be defined as class attributes on any device or signal. BEC's device server leverages these events to subscribe callbacks, enabling reactions to changes in devices or signals. Below is an example that demonstrates the available events. Note that these events must be defined as class attributes and cannot be created dynamically at runtime. Syntaxwise, the class attributes need to be defined starting with `SUB_`, but the actual name provided as a string is the one which is relevant for the event.

```python
from ophyd import Device

class DummyDevice(Device):

    SUB_READBACK = 'readback'
    SUB_VALUE = 'value'
    SUB_DONE_MOVING = 'done_moving'
    SUB_MOTOR_IS_MOVING = 'motor_is_moving'
    SUB_PROGRESS = 'progress'
    SUB_FILE_EVENT = 'file_event'
    SUB_DEVICE_MONITOR_1D = 'device_monitor_1d'
    SUB_DEVICE_MONITOR_2D = 'device_monitor_2d'
```

*Ophyd* uses the `SUB_EVENTS` internally to trigger its own callbacks. For example, the `EpicsSignal` class utilises the `SUB_VALUE` event in its `put` method to signal the control layer (e.g., *pyepics*) to update the hardware. Similarly, BEC subscribes to these events to trigger its own custom callbacks.

## Callbacks

xecuting a callback in *Ophyd* is straightforward. The following example demonstrates how to trigger a callback on the `SUB_VALUE` event on the *DummyDevice* defined above.

```python
self._run_subs(sub_type=self.SUB_VALUE)
```
The `self._run_subs` method, inherited from the `Device` class, triggers the all callbacks for the event 'value'. Please note that the execution of callbacks will silently fail in case they can not be executed. Therefore, it is crucial to ensure that the callbacks are provided with the correct arguments. For instance, the 'value' event will invoke the following callback within BEC's device server.

```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
:language: python
:pyobject: DeviceManagerDS._obj_callback_readback
:dedent: 4
```

(developer.devices.device_integration.device_events_and_callbacks.file_event)=
## Use Case for Detector Integration
The tutorial section about [device integration](#developer.devices.device_integration.tutorial) provides a detailed guide on how to integrate a detector. Here, we would like to extend this integration effort using the callback mechanism introduced in this section.

**File Event**
We can extend the `on_stage` and `on_unstage` method to inform BEC about file events. At the bottom of the `on_stage` method, we can add the following code snippet to inform BEC about the file event. The file event informs BEC about the file path, type, and additional information about the file. In particularly, the `hinted_location` can be used to provide additional information about the file structure, e.g. in case of h5 files the location of data that should be linked to the main file.
```python
self._run_subs(
    sub_type=self.SUB_FILE_EVENT,
    file_path=self.parent.filepath.get(),
    file_type="h5",
    hinted_location={'data' : '/entry/data/data'},
    done=False,
    success=False
)
```

Secondly, we can add a similar code snippet to the `on_unstage` method to inform BEC that the file writing is done and the file is closed.
```python
self._run_subs(
    sub_type=self.SUB_FILE_EVENT,
    file_path=self.parent.filepath.get(),
    file_type="h5",
    hinted_location={'data' : '/entry/data/data'},
    done=True,
    success=True
)
```

The full code for the custom detector integration is shown below again.

````{dropdown} View code: Full Custom Detector Class
:icon: code-square
:animate: fade-in-slide-down

```python
import os

from ophyd import Component as Cpt
from ophyd import EpicsSignal

from ophyd_devices.devices.areadetector.cam import PilatusDetectorCam
from ophyd_devices.devices.areadetector.plugins import HDF5Plugin_V35
from ophyd_devices.interfaces.base_classes.psi_detector_base import (
    CustomDetectorMixin,
    PSIDetectorBase,
)


class DetectorError(Exception):
    """Custom exception class for detector errors"""


class BeamlinePilatusCustomPrepare(CustomDetectorMixin):
    """Custom Prepare class for Beamline X"""

    def __init__(self, *_args, parent=None, **_kwargs):
        super().__init__(*_args, parent=parent, **_kwargs)
        self._software_trigger_active = True

    def on_trigger(self):
        """Custom on_trigger method for Beamline X"""
        if self._software_trigger_active is True:
            # Trigger the detector for each frame,
            self.parent.trigger_software.set(1).wait()

    def on_stage(self):
        """Custom on_stage method for Beamline X. Executed in preparation for the scan."""

        ########################################
        # Prepare the detector for the scan
        ########################################
        self.parent.num_images.set(self.parent.scaninfo.num_point).wait()
        self.parent.cam.acquire_time.set(self.parent.scaninfo.exp_time).wait()

        # Set the trigger mode
        if self.parent.scaninfo.scan_type == "fly":
            # Set trigger mode to internal:
            # Assume 0 is software trigger and 1 is gated triggering -> TTL
            self.parent.trigger_mode.set(1).wait()
            self._software_trigger_active = False
        else:
            self.parent.trigger_mode.set(0).wait()
            self._software_trigger_active = True

        ########################################
        # Prepare the file writer plugin
        ########################################

        self.parent.filepath.set(
            self.parent.filewriter.compile_full_filename(f"{self.parent.name}.h5")
        ).wait()
        file_path, file_name = os.path.split(self.parent.filepath.get())
        self.parent.hdf5.file_path.set(file_path).wait()
        self.parent.hdf5.file_name.set(file_name).wait()
        self.parent.hdf5.file_template.set("%s%s").wait()
        self.parent.hdf5.num_capture.set(self.parent.scaninfo.num_points).wait()
        self.parent.hdf5.file_write_mode.set(2).wait()
        # Reset spectrum counter in filewriter, used for indexing & identifying missing triggers
        self.parent.hdf5.array_counter.set(0).wait()

        # Start file writing
        # Don't use `set` method on capture signal, since this will be blocking otherwise
        self.parent.hdf5.capture.put(1)

        self._run_subs(
            sub_type=self.SUB_FILE_EVENT,
            file_path=self.parent.filepath.get(),
            file_type="h5",
            hinted_location={'data' : '/entry/data/data'},
            done=False,
            success=False
        )

    def on_pre_scan(self):
        """Custom on_pre_scan method for Beamline X, executed just before the scan starts."""
        self.parent.acquire.put(0)

        ########################################
        # Wait for detector to be ready
        ########################################
        # wait for detector to be ready, 1 is ready, timeout is 2 seconds
        timeout = 2
        if not self.wait_for_signals(
            signal_conditions=[(self.parent.detector_state.get, 1)],
            timeout=timeout,
            check_stopped=True,
        ):
            raise DetectorError(
                f"Detector is getting ready during pre-scan with timeout of {timeout} seconds"
            )

    def on_complete(self):
        """Custom on_complete method for Beamline X, executed after the scan is complete."""
        # Wait for the detector to finish writing the acquisition and writing the file
        # 0 is idle, 1 is acquiring
        # We wait for both, detector_state and write_status to be 0
        timeout = 5
        status = self.wait_with_status(
            signal_conditions=[
                (self.parent.detector_state.get, 0),
                (self.parent.hdf5.write_status.get, 0),
            ],
            timeout=timeout,
            check_stopped=True,
        )
        return status

    def on_unstage(self):
        """Custom on_unstage method for Beamline X, executed after the scan is complete."""
        if self.parent.stopped is False:
            self._run_subs(
                sub_type=self.SUB_FILE_EVENT,
                file_path=self.parent.filepath.get(),
                file_type="h5",
                hinted_location={'data' : '/entry/data/data'},
                done=True,
                success=True
            )

    def on_stop(self):
        """Custom on_stop method for Beamline X, executed when the scan is stopped."""
        self.parent.acquire.put(0)
        self.parent.hdf5.capture.put(0)
        timeout = 5
        if not self.wait_for_signals(
            signal_conditions=[
                (self.parent.detector_state.get, 0),
                (self.parent.hdf5.write_status.get, 0),
            ],
            timeout=timeout,
            check_stopped=False,
        ):
            raise DetectorError(
                f"Detector did not stop during on_stop with timeout of {timeout} seconds"
            )


class MyPilatusDetector(PSIDetectorBase, PilatusDetectorCam):
    """Custom Detector class for Pilatus detector with HDF5 filewriter plugin"""

    trigger_software = Cpt(EpicsSignal, "TriggerSoftware")
    hdf5 = Cpt(HDF5Plugin_V35, "HDF1:")
    custom_prepare_cls = BeamlinePilatusCustomPrepare

    SUB_FILE_EVENT = "file_event"

```
````

````{note}
Implementing the specific logic for when a file is successfully written is crucial and may vary for each detector. The example above is a simplified implementation, assuming the file was written successfully when the `on_unstage` method is called. While the concept is demonstrated here, the timing of the callback must be tailored to each detector's behavior.
````
## Callback examples

**Readback**
Triggers a `.read()` for all signals classified as *kind.normal* or *kind.hinted*.
```python
self._run_subs(sub_type=self.SUB_READBACK)
```

**Value**
Triggers the same callback in BEC as the **readback** callback. It is typically the default SUB event for a signal.
```python
self._run_subs(sub_type=self.SUB_VALUE)
```

**Done Moving**
Triggers a .read() by executing the **readback** callback on the device.
```python
self._run_subs(sub_type=self.SUB_DONE_MOVING)
```

**Motor is moving**
Reports whether a motor is currently moving. 
* *value*: Binary or boolean value of whether the motor is moving.
```python
self._run_subs(sub_type=self.SUB_MOTOR_IS_MOVING, value=True)
```

**Progress**
Updates BEC on the device's progress, such as motor motion during a scan or detector frame acquisition.
* *progress*: Current progress value.
* *max_value*: Maximum progress value.
* *done*: Binary or boolean value indicating whether the device is done.
```python
self._run_subs(sub_type=self.SUB_PROGRESS, value=5, max_value=100, done=False)
```

**File Event**
Notifies BEC about file-related events, particularly useful for devices with external writing processes (e.g., large 2D detectors). Typically the device sends two messages: one when the device is prepared for a scan and a second when the scan is finished. 
* *file_path*: Path to the file.
* *file_type*: Type of the file.
* *hinted_location*: Additional information about the file structure, the location of data that should be linked to the main file.
* *done*: Binary or boolean value indicating whether the file event is complete.
* *success*: Binary or boolean value indicating whether the file event was successful.

```python
self._run_subs(
    sub_type=self.SUB_FILE_EVENT, 
    file_path='/raw/data/S00000-S00999/S00020/S00020_pilatus.h5', 
    file_type="h5", 
    hinted_location={'data' : '/entry/data/data'}, 
    done=False, 
    success=False)
```

````{note}
We recommend using BEC's file utilities to compile file paths for external writing processes. This ensures paths are correctly formatted and stored in designated directories.
````

**Device Monitor 1D**
Sends preview data in the form of a 1D array to BEC. This data is not stored but is useful for visualization purposes.
* *value*: 1D numpy array of data.
```python
self._run_subs(sub_type=self.SUB_DEVICE_MONITOR_1D, value=np.random.rand(100))
```

**Device Monitor 2D**
Sends preview data in the form of a 2D array or an RGB image (a NumPy array of shape (X, X, 3)) to BEC.
* *value*: 2D numpy array or RGB image.
```python
self._run_subs(sub_type=self.SUB_DEVICE_MONITOR_2D, value=np.random.rand(100, 100))
```

(developer.devices.device_integration.external_data_sources)=
## External Data Sources
For large external data sources not managed by a device, you can send events directly to BEC. This section builds on the file event example and explains how to notify BEC about file events from an external source.
There are two steps involved: (1) Create a [FileMessage](#bec_lib.messages.FileMessage) and (2) emit it to the [public_file endpoint](#bec_lib.endpoints.MessageEndpoints.public_file) and [file_event endpoint](#bec_lib.endpoints.MessageEndpoints.file_event).

```python
from bec_lib.endpoints import MessageEndpoints
from bec_lib import messages
from bec_lib.redis_connector import RedisConnector

scan_id = "scan id of the current scan"

# get a new producer for redis messages, localhost:6379 is the default address
producer = RedisConnector(["localhost:6379"]).producer()

# prepare the message
msg = messages.FileMessage(file_path="/path/to/file.h5", done=False, success=False, file_type="h5", hinted_location={'data' : '/entry/data/data'})

# send the message using the scan_id and a user-friendly but unique name to describe the source (e.g. "eiger")
producer.set_and_publish(
    MessageEndpoints.public_file(scan_id, "eiger"),
    msg,
)
producer.set_and_publish(
    MessageEndpoints.file_event(scan_id, "eiger"),
    msg,
)
```