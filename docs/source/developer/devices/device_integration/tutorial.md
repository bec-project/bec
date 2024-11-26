(developer.devices.device_integration.tutorial)=
# Tutorial

This example demonstrates extending `EpicsMotor` with custom beamline-specific logic.

## Code Example Motor

```python
from ophyd_devices.interfaces.base_classes.psi_detector_base import PSIDetectorBase, CustomDetectorMixin
from ophyd.epics_motor import EpicsMotor

class BeamlineCustomPrepare(CustomDetectorMixin):
    """Custom Prepare class for Beamline X"""

class BeamlineMotor(PSIDetectorBase, EpicsMotor):
    """Custom Motor class for Beamline X"""

    custom_prepare_cls = BeamlineCustomPrepare
```
**Key Points**
1. *Inheritance Structure*
    * The BeamlineMotor class inherits from both PSIDetectorBase and EpicsMotor.
    * Order of Inheritance: The first class listed (PSIDetectorBase) takes precedence when resolving methods, followed by EpicsMotor. This order is crucial for ensuring the correct functionality of overridden methods. Avoid deviating from this order or introducing unnecessary complexity in inheritance.

2. *PSIDetectorBase & CustomPrepare class*
    * The PSIDetectorBase class acts as a wrapper around the ophyd methods relevant for scanning, i.e. *stage*, *unstage*, *trigger*, *complete*, *kickoff*, *stop* and *wait_for_connection*. 
    This allows us to encapsulate beamline-specific logic in the custom_prepare_cls class and simplifies the process of extending these methods with beamline-specific logic.
    * The CustomPrepare class hosts beamline-specific logic. It provides a structured approach to extending device functionality. The class itself purely serves as an empty container for methods that can be overriden in the BeamlineCustomPrepare class.


**Adding Beamline-Specific Logic**
To ensure a motor operates at a specific velocity and acceleration during scans, implement logic in `BeamlineCustomPrepare`.

```python
from ophyd_devices.interfaces.base_classes.psi_detector_base import PSIDetectorBase, CustomDetectorMixin
from ophyd.epics_motor import EpicsMotor

class BeamlineCustomPrepare(CustomDetectorMixin):
    """Custom Prepare class for Beamline X"""

    def on_stage(self):
        """ Custom stage logic for Beamline X """
        self.parent.velocity.set(2).wait()
        self.parent.acceleration.set(0.5).wait()

class BeamlineMotor(PSIDetectorBase, EpicsMotor):
    """Custom Motor class for Beamline X"""

    custom_prepare_cls = BeamlineCustomPrepare
```

**parent**
* References the `BeamlineMotor` instance, enabling access to its attributes and methods.
* `on_stage()` velocity and acceleration. The `wait()` method ensures these operations complete before proceeding.


## Code Example Detector

The second example integrates a custom detector using the `GenICam `interface, available in `ophyd_devices`.

```python
from ophyd_devices.interfaces.base_classes.psi_detector_base import PSIDetectorBase, CustomDetectorMixin
from ophyd_devices.devices.areadetector.cam import GenICam

class BeamlineCustomPrepare(CustomDetectorMixin):
    """Custom Prepare class for Beamline X"""


class BeamlineGenICam(PSIDetectorBase, GenICam):
    """Custom GenICam class for Beamline X"""

    custom_prepare_cls = BeamlineCustomPrepare
```
**Customizing Detector Logic**
Here’s an example of adding logic for a GenICam-based camera:

```python
from ophyd_devices.interfaces.base_classes.psi_detector_base import PSIDetectorBase, CustomDetectorMixin
from ophyd_devices.devices.areadetector.cam import GenICam

class BeamlineCustomPrepare(CustomDetectorMixin):
    """Custom Prepare class for Beamline X"""

    def on_wait_for_connection(self):
        """ Custom wait_for_connection logic for Beamline X. 
        This method is called when BEC tries to connect to the device.
        """
        self.parent.trigger_source.set(1).wait() # Set the trigger source to 1, this could mean for example software trigger
        self.parent.exposure_mode.set(2).wait() # Set the exposure mode to 1, this could for instance be auto
        self.parent.gain_auto.set(1).wait() # Set the gain to 1, this could mean True

    def on_stage(self):
        """ Custom stage logic for Beamline X """
        # Get the number of expected images from the scaninfo object
        expected_images = self.parent.scaninfo.num_points
        self.parent.num_images.set(expected_images).wait()

    def on_pre_scan(self):
        """ Custom pre_scan logic for Beamline X """
        # Start acquiring mode
        self.parent.acquire.put(1) 

    def on_trigger(self):
        """ Custom trigger logic for Beamline X """
        # Trigger the camera
        self.parent.trigger_software.put(1) 

    def on_complete(self):
        """ Custom complete logic for Beamline X """
        #Check here if the acquisition is finished
        conditions = [(self.parent.acquire.get == 0)]
        status = self.wait_with_status(conditions, timeout=10)
        return status

    def on_unstage(self):
        """ Custom unstage logic for Beamline X """
        pass

    def on_stop(self):
        """ Custom stop logic for Beamline X """
        # Stop acquiring mode
        self.parent.acquire.put(0)


class BeamlineGenICam(PSIDetectorBase, GenICam):
    """Custom GenICam class for Beamline X"""

    custom_prepare_cls = BeamlineCustomPrepare
```
1. **scaninfo object**
    * The `scaninfo` object is a utility provided by `PSIDetectorBase` that contains information about the scan.
    * Once *stage* is called, the `scaninfo` object is populated with the latest information about the scans.
    * Some of these parameters are mapped, such as `num_points`, `exp_time`, etc. 

2. **on_methods()**
    * The `on_wait_for_connection` method is called when BEC tries to connect to the device. We would use this to set parameters that would be set once, and typically do not change. Here, for instance trigger source, exposure mode and gain.
    * The `on_stage` method is called when the device is staged. We use this to set the number of images to be acquired.
    * The `on_pre_scan` method is called just before the scan starts. We use this to start the acquiring mode.
    * The `on_trigger` method is called when the device is triggered. We use this to trigger the camera (software trigger).
    * The `on_stop` method is called when the scan is stopped. We use this to stop the acquiring mode.
    * The `on_complete` method is called when the device is completed. We use this to check if the acquisition is finished. Note this method returns a status.
3. **Status object**
    * Note that the *on_complete* method returns a status object, more specifically an `ophyd.DeviceStatus` object. The status is used to check if the acquisition is finished. 
    * We provide the `wait_with_status` method to wait for a condition to be met. This method receives a list of conditions and potentially a few additional parameters, such as a timeout. It will start a background thread that checks the conditions and resolves the status object when the conditions are met, a timeout occurs, or the scan is stopped.

````{dropdown} View code: wait_with_status
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/psi_detector_base.py
:language: python
:pyobject: CustomDetectorMixin.wait_with_status

```
````