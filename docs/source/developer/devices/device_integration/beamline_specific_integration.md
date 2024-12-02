(developer.devices.device_integration.beamline_specific_integration)=
# Beamline Specific Integration 
It is certainly challenging to integrate single devices at multiple beamlines, all with different rather beamline-specific requirements. At the same time, we would like to ensure that their core functionality remains the same. This is true for direct control of the device, but also for the communication with BEC. 

## Example - Customize EpicsMotor 
```` {note}
If the motor does not need to customize its behavior to the beamline, you can directly use it as an [EpicsMotor](https://nsls-ii.github.io/ophyd/builtin-devices.html#epicsmotor). Please check the [device configuration section](#developer.ophyd_device_config) for more details on how to load and use the device. 
````
Now let's assume that we have a beamline-specific requirement for the motor. We would like to ensure that the motor is always operated with a specific velocity. To accomplish this, we can create a new class that inherits from `EpicsMotor` and adds beamline-specific logic.

``` python
from ophyd import EpicsMotor

class MyBeamlineMotor(EpicsMotor):
   
    def stage(self):
      # Set velocity to 2 during staging
      self.velocity.set(2).wait()
      return super().stage()
```
In the example above we extend the *stage* method of the *EpicsMotor* which is called in preparation for a new scan. We set the velocity to 2, and successfully modified the behavior of the motor for our beamline. Nevertheless, this approach has some limitations. For example, we could imagine that the motor should only adjust its velocity for *fly scans*, and in addition restore the velocity to its original value after a scan. We're now required to receive information about the upcoming scan from BEC, set and restore the motor's velocity.

``` python
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from ophyd import EpicsMotor

class MyBeamlineMotor(EpicsMotor):

   def __init__(self, prefix="", *, name, kind=None, parent=None, device_manager=None, **kwargs):
      super().__init__(prefix, name=name, kind=kind, parent=parent, **kwargs)
      self.device_manager = device_manager
      self._stored_velocity = None

   def stage(self):
      """Stage the motor"""
      scan_msg = self.get_scaninfo()
      # No scan message available, proceed without any actions
      if scan_msg is None:
         return super().stage()
      # Get the scan type
      scan_type = scan_msg.content["info"].get("scan_type")
      if scan_type == "fly":
         # Store velocity
         self._stored_velocity = self.velocity.get()
         # Set velocity to 2 during staging
         self.user_velocity.set(2).wait()
      return super().stage()

   def get_scaninfo(self) -> messages.ScanStatusMessage:
      """Get current scan message"""
      msg = self.device_manager.connector.get(MessageEndpoints.scan_status())
      if not isinstance(msg, messages.ScanStatusMessage):
         return None
      return msg

   def unstage(self):
      """Unstage the motor"""
      # restore the old velocity if it was stored
      if self._stored_velocity is not None:
         self.user_velocity.set(self._stored_velocity).wait()
      return super().unstage()
```

The code is now implementing all our requirements. A couple of additional comments to the code above:
- The *device_manager* is injected as a dependency upon initialising the device object on the device server. This is done automatically for each device that has *device_manager* in its signature. The device manager provides access to the connector class, which allows to retrieve information from BEC, such as the current scan message.
- The scan message is used to determine the scan type, and if it is a *fly scan*, the velocity of the motor is set to 2. The original velocity is stored and restored after the scan.

Given that similar integration requirements might be needed for other devices as well, we recommend a more generic approach that allows for seamless integration of devices across beamlines, while accommodating beamline-specific requirements. 

````{note}
Ophyd provides a set of abstract methods that BEC leverages from for its scans. More details about the methods are summarized in the [ophyd section](#developer.ophyd.device). In addition, we recommend to also check details about the [scan structure](#developer.scans.scan_structure) of BEC to get a better understanding of the hierarchy and methods that are relevant during a scan.
````

## Custom Prepare Actions 
The example above illustrates the challenges of integrating beamline-specific requirements into devices. We would like to avoid that the same code has to be repeated for each device, and that the implementation of beamline-specific logic becomes complex and error-prone. For this reason, we recommend a more generic approach that allows us to encapsulate the beamline-specific logic in a single place, and reuse other parts of the code as much as possible. 

We introducing a base class that wraps around the known interface from *Ophyd*. This class is named `PSIDetectorBase` and provides utility methods to interact with BEC. In addition, we compose the beamline-specific logic into a separate class which we named `CustomDetectorMixin`. The example code can now be rewritten as follows:

``` python
from ophyd import EpicsMotor
from ophyd_devices.interfaces.base_classes.psi_detector_base import (
    CustomDetectorMixin,
    PSIDetectorBase,
)

class BeamlineCustomPrepare(CustomDetectorMixin)
    
   def __init__(self, *_args, parent = None, **_kwargs):
      """ Beamline specific actions during initialization """
      super().__init__(*_args, parent=parent, **_kwargs)
      self._stored_velocity = None

   def on_stage(self):
      """ Beamline specific actions during staging """
      if self.parent.scaninfo.scan_type == "fly":
         self._stored_velocity = self.velocity.get()
         self.velocity.set(2).wait()

   def on_unstage(self):
      """ Beamline specific actions during unstaging """
      if self._stored_velocity is not None:
         self.velocity.set(self._stored_velocity).wait()
   

class MyBeamlineMotor(PSIDetectorBase, EpicsMotor):

    custom_prepare_cls = BeamlineCustomPrepare
```
<!-- #TODO now we need to explain here in more details what happens! -->


This approach allows us to provide beamlines with a structured and organized way to manage the beamline-specific logic. The `PSIDetectorBase` class provides the communication with BEC, while the `CustomDetectorMixin` class implements the beamline-specific logic. The `MyBeamlineMotor` class inherits from both, `EpicsMotor` and `PSIDetectorBase`, and uses the `custom_prepare_cls` attribute to specify the beamline-specific logic.

## Summary
In our suggested approach, we introduce a base class that wraps around the known interface from *Ophyd*. This class provides utility methods to interact with BEC. In addition, we compose the beamline-specific logic into a separate class, which we named `CustomDetectorMixin`. The device class then inherits from both, the device class and the `PSIDetectorBase` class, and uses the `custom_prepare_cls` attribute to specify the beamline-specific logic.
It is important to understand that this approach separates logic required to communicate and control a device, logic required to interact with BEC, and logic required to implement beamline-specific actions. This separation allows us to easily extend the `CustomPrepareBeamline` class with additional methods, such as `on_unstage`, `on_complete`, etc. Finally, we can now easily implement the beamline-specific logic in the beamline's plugin repository, which is a more structured and organized way to manage the beamline-specific logic.

````{dropdown} View code: PSIDetectorBase
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/psi_detector_base.py
:language: python
:pyobject: PSIDetectorBase

```
````

````{dropdown} View code: CustomDetectorMixin
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/psi_detector_base.py
:language: python
:pyobject: CustomDetectorMixin

```
````

<!-- 
On the first glimpse, thise approach might seem more complex, but it has several advantages. First, we have separated the beamline-specific logic from the device class, which makes the code more readable and maintainable. Second, we can now easily reuse the `CustomPrepareBeamline` class for other devices that require the same logic. Third, we can now easily extend the `CustomPrepareBeamline` class with additional methods, such as `on_unstage`, `on_complete`, etc. Finally, we can now easily implement the beamline-specific logic in the beamline's plugin repository, which is a more structured and organized way to manage the beamline-specific logic.



## 

If we now consider again the example of the motor above, we can simply inherit from both, `EpicsMotor` and `PSIDeviceBase` to implement the beamline-specific logic. 

``` python

```

We introduce the `PSIDeviceBase` class, which wraps the comm



How do we include this very specific beamline requirement into the device wihtout repeating or constantly rewriting device classes... Even more so if we consider that the same device might be used in different beamlines, but requires different settings to operate correctly for each of the beamlines. 


## Beamline Specific Integration
We recommend a solution that allows for seamless integration of devices across beamlines, while accommodating beamline-specific requirements.

 without rewriting the entire 

This is a particular requirement for a single beamline,

 for integrating an EpicsMotor into BEC. The control interface is full provided by the [`ophyd.EpicsMotor`](https://nsls-ii.github.io/ophyd/builtin-devices.html#epicsmotor), however, we want to add some beamline-specific actions to the motor.
Let us assume that we would like to ensure that for any scan with BEC, we ensure that the motor is operated with a specific velocity. 
Another beamline could have a different requirement, but the core functionality of the motor remains the same.
This raises 


This is a beamline-specific requirement, an we want
Given the diverse yet similar nature of devices across beamlines, seamless integration provides consistent functionality while accommodating beamline-specific requirements. 

 1. User story from EpicsMotor tutorial.

 2. Beamline specific actions for the epics motor.

 3. Don't rewrite or repeat this for each motor/device.PSIDeviceBase class.
    * Use the PSIDeviceBase class to implement the communication with BEC. 
    * Use the CustomDeviceMixin class to implement the beamline-specific logic. 
    * Implement the beamline-specific logic in the beamline's plugin repository.

 3. 

PSIDeviceBase
````{dropdown} View code: PSIDetectorBase
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/psi_detector_base.py
:language: python
:pyobject: PSIDetectorBase

```
````

````{dropdown} View code: CustomDetectorMixin
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/psi_detector_base.py
:language: python
:pyobject: CustomDetectorMixin

```
```` -->

## Scaninfo
Some extra information about the scaninfo, and how it can be used in through the `PSIDetectorBase` class.
Fetching latest scaninfo message, and parsing it to a user-friendly format.

## Device manager
Some additional information abou the device manager. Here we may keep things rather simple, maybe just mention that this allows to for instance check if another device is connected.

## File Utils 
Additional information about the file utils. This is simply a convenient utility class to handle filenames and paths to ensure that secondary services attached to devices (i.e. detector file writers) are given the correct file paths.