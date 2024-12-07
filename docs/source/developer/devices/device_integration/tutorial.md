(developer.devices.device_integration.tutorial)=
# Tutorial - Detector Integration
This tutorial guides you through integrating a custom detector class with beamline-specific requirements. Let's say the requirements are: the detector should operate with a gated TTL signal during fly scans and use software triggering for step scans. Additionally, we need to configure the usual parameters such as exposure time and number of images based on scan parameters, and prepare the detector's file writer to write data to a specified file location. Before diving into the tutorial, we highly recommend reading the [generic approach](#developer.devices.device_integration.beamline_specific_integration.generic_approach) section to understand the underlying concepts and classes used in this tutorial.

For this example, we will utilize an EPICS area detector IOC as the backend, focusing on a Pilatus detector with an HDF5 filewriter plugin.

````{note}
When integrating a new detector, generic implementation must go to the Ophyd Devices repository. In this example, though, since the problematic of a specific usage of a device on a specific beamline is detailed, the code should go to the **beamline plugin repository**.
The [plugin repository](#developer.bec_plugins) section provides more details on the plugin structure.
For device integration, each plugin contains a sub-module, *beamline_XX_bec/devices*, where beamline-specific device integrations are located. These integrations may include fully customised devices or thin classes that add beamline-specific logic to existing integrations from *ophyd_devices*. 
````

````{note}
Auto-generated device lists for each beamline repository are available at *beamline_XX_bec/beamline_XX_bec/devices/device_list.md*, i.e. the [device list for cSAXS](https://gitlab.psi.ch/bec/csaxs_bec/-/blob/main/csaxs_bec/devices/device_list.md?ref_type=heads).
````

## Detector Communication 

The [*ophyd_devices.devices.areadetector*](https://gitlab.psi.ch/bec/ophyd_devices/-/tree/main/ophyd_devices/devices/areadetector?ref_type=heads) module provides several communication interfaces for various EPICS area detector IOCs. In this example, we use `PilatusDetectorCam` and `HDF5Plugin_V35` to combine detector control with file writing capabilities.

````{dropdown} View code: Area Detector Cams
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/devices/areadetector/cam.py
:language: python
```

````

````{dropdown} View code: Area Detector Plugins
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/devices/areadetector/plugins.py
:language: python
```
````

````{note}
The `PilatusDetectorCam` and `HDF5Plugin_V35` classes serve as examples. Ensure that you select interfaces matching your beamline's detector backend. Consult your beamline's controls team for guidance.
````

## Custom Detector Class

We combine the `PilatusDetectorCam` and `HDF5Plugin_V35` classes to create a custom detector integration.
```python 
from ophyd import Component as Cpt
from ophyd import EpicsSignal

from ophyd_devices.devices.areadetector.cam import PilatusDetectorCam
from ophyd_devices.devices.areadetector.plugins import HDF5Plugin_V35

class MyPilatusDetector(PilatusDetectorCam):
    """ Custom Detector class for Pilatus detector with HDF5 filewriter plugin """
   
    trigger_software = Cpt(EpicsSignal, "TriggerSoftware")
    hdf5 = Cpt(HDF5Plugin_V35, "HDF1:")

```
In `MyPilatusDetector`, we inherit from `PilatusDetectorCam` and add the `HDF5Plugin_V35` plugin as a sub-device. The HDF5 plugin is added as a component (*sub-device*) *hdf5*, with an extension for the prefix *HDF1:* which is typically the namespace for PVs of the IOC's HDF5Plugin. Please crosscheck this prefix with your beamline's control group to ensure it is correct. Since the *PilatusDetectorCam* lacks a software trigger by default, we include a *trigger_software* signal assuming this to be included within the area detector IOC. Please note that this is just an example and you should adjust the class to your beamline's requirements. 

### Combine with PSIDetectorBase

Next, we combine the custom detector class with *PSIDetectorBase* class and a beamline-specific *custom_prepare_cls* to implement beamline-specific logic via hooks provided by *PSIDetectorBase*. 

```python
from ophyd import Component as Cpt
from ophyd import EpicsSignal

from ophyd_devices.devices.areadetector.cam import PilatusDetectorCam
from ophyd_devices.devices.areadetector.plugins import HDF5Plugin_V35

from ophyd_devices.interfaces.base_classes.psi_detector_base import PSIDetectorBase, CustomDetectorMixin

class BeamlinePilatusCustomPrepare(CustomDetectorMixin):
    """Custom Prepare class for Beamline X"""

class MyPilatusDetector(PSIDetectorBase, PilatusDetectorCam):
    """ Custom Detector class for Pilatus detector with HDF5 filewriter plugin """

    trigger_software = Cpt(EpicsSignal, "TriggerSoftware")
    hdf5 = Cpt(HDF5Plugin_V35, "HDF1:")
    custom_prepare_cls = BeamlinePilatusCustomPrepare
```

We have succeeded in creating a basic implementation of our detector. Next is to add the beamline-specific logic to the *BeamlinePilatusCustomPrepare* class. But before this, we will briefly recapture some information that was discussed in the [generic approach](#developer.devices.device_integration.beamline_specific_integration.generic_approach) section.

````{admonition} Recap: Generic Approach
- **PSIDetectorBase**: Extends Ophyd's standardized methods for BEC's scan engine, providing hooks for scan stages (e.g., `on_stage`, `on_unstage`).
- **CustomDetectorMixin**: Implements hooks as empty methods, allowing specific overrides in beamline-specific classes.
- **`scaninfo` object**: Contains scan metadata, such as the number of points and exposure time.
````

### Implement Beamline-Specific Logic
To meet our requirements, we implement the following logic in *BeamlinePilatusCustomPrepare*:
* configure the detector with the correct exposure time and number of images based on the scan parameters
* prepare the file writer plugin to write data
* trigger the detector with a software trigger if the beamline runs *step* scans


```python

class BeamlinePilatusCustomPrepare(CustomDetectorMixin):
    """Custom Prepare class for Beamline X"""

    def on_trigger(self):
        """Custom on_trigger method for Beamline X"""
        if self.parent.scaninfo.scan_type == "step":
            self.parent.trigger_software.put(1)

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
        ).wait() # Set the file path and name
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

    def on_pre_scan(self):
        """Custom on_pre_scan method for Beamline X, executed just before the scan starts."""
        self.parent.acquire.put(1)
        #TODO Implement logic to wait if the detector is not ready yet

    def on_complete(self):
        """Custom on_complete method for Beamline X, executed after the scan is complete."""
        detector_state = self.parent.detector_state.get()
        writer_state = self.parent.hdf5.capture.get()
        if detector_state != 0 or writer_state != 0:
            raise Exception("Detector did no finish successfully")
        #TODO Implement proper logic to check if the acquisition is finished

    def on_stop(self):
        """Custom on_stop method for Beamline X, executed when the scan is stopped."""
        self.parent.acquire.put(0)
        self.parent.hdf5.capture.put(0)
        #TODO Implement logic to check if the detector is stopped
```

**Key Points**

* *on_trigger()*: Configures the detector's trigger method based on scan type.
* *on_stage()*: Prepares the detector and filewriter for data acquisition. Please note that we use [`filewriter.compile_full_filename`](/api_reference/_autosummary/bec_lib.file_utils.FileWriter.rst#bec_lib.file_utils.FileWriter.compile_full_filename) to compile the full file path with a suffix including the device name.
* *on_pre_scan()*: Initiates the acquisition before the scan starts, useful for time-critical operations before the scan starts.
* *on_complete()*: Validates acquisition and filewriting completion. We still need to implement the logic to check if the acquisition is finished.
* *on_stop()*: Actions to be executed when the device is stopped. We still need to implement the logic to check if the detector is stopped.

### Waiting for Signals

To ensure proper synchronization with EPICS PVs, the `.set(value).wait()` method is recommended for blocking calls. However, for certain PVs like *acquire*, tcustom wait logic may be necessary. Depending on the requirements, two approaches can be used:

* Blocking: In this approach the code will be blocking until the conditions are met.
* Non-blocking: The code will not blocking but return an object that can be used to check if the conditions are met. In *Ophyd*, the objects returned for this purpose are typically `Status` objects.

**Blocking Example**
Below is an example of a blocking implementation in the `on_complete` method. We will use the `wait_for_signals` method to wait for the detector to finish writing the acquisition and writing the file.

````{dropdown} View code: wait_for_signals
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/psi_detector_base.py
:language: python
:pyobject: CustomDetectorMixin.wait_for_signals
:dedent: 4
```
````

```python
def on_complete(self):
        """Custom on_complete method for Beamline X, executed after the scan is complete."""
        # Wait for the detector to finish writing the acquisition and writing the file
        # 0 is idle, 1 is acquiring
        # We wait for both, detector_state and write_status to be 0
        timeout = 5
        if not self.wait_for_signals(
            signal_conditions=[
                (self.parent.detector_state.get, 0),
                (self.parent.hdf5.write_status.get, 0),
            ],
            timeout=timeout,
            check_stopped=True,
        ):
            raise DetectorError(
                f"Detector did not finish during on_complete with timeout of {timeout} seconds"
            )

```

**Non-Blocking Example**
The alternative is to create a status object and return this to BEC. BEC will later on regularly check if the status object are done. Currently, this mechanism is only available for methods that can optionally return a status object. This is the case for `trigger`, `kickoff` and `complete`. However, we are looking into extending this option to other methods of *Ophyd* as well.
We can use the `wait_with_status` method to create this status object.

````{dropdown} View code: wait_with_status
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/psi_detector_base.py
:language: python
:pyobject: CustomDetectorMixin.wait_with_status
:dedent: 4
```
````
```python
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
```

````{note}
The non-blocking implementation pawns a background thread that wraps around the *wait_for_signals* method. This enables asynchronous execution, where the status object (`status.done`) indicates whether conditions are met or if exceptions where raised (status.exception).
````

### Using events

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

The full code for the custom detector integration is shown below: 

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
