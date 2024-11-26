(developer.devices.device_integration.beamline_specific_integration)=
# Beamline Specific Integration 
Given the diverse yet similar nature of devices across beamlines, seamless integration provides consistent functionality while accommodating beamline-specific requirements. 

 1. User story from EpicsMotor tutorial.

 2. Beamline specific actions for the epics motor.

 3. Don't rewrite or repeat this for each motor/device. --> PSIDeviceBase class.
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
````

## Scaninfo
Some extra information about the scaninfo, and how it can be used in through the `PSIDetectorBase` class.
Fetching latest scaninfo message, and parsing it to a user-friendly format.

## Device manager
Some additional information abou the device manager. Here we may keep things rather simple, maybe just mention that this allows to for instance check if another device is connected.

## File Utils 
Additional information about the file utils. This is simply a convenient utility class to handle filenames and paths to ensure that secondary services attached to devices (i.e. detector file writers) are given the correct file paths.