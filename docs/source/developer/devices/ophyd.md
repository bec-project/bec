(developer.ophyd)=
# Ophyd Library
[Ophyd](https://nsls-ii.github.io/ophyd/) is the hardware abstraction layer developed by NSLS-II and used by BEC to communicate with hardware. It is a Python library that provides a consistent interface between the underlying control communication protocol and high-level software such as BEC. While Ophyd can be used for any device, it comes with EPICS support out of the box. This means that many devices that are controlled by EPICS can be integrated directly into BEC without the need of writing custom Ophyd classes. The most common devices that are integrated into BEC are based on `EpicsMotor` and `EpicsSignal` (or `EpicsSignalRO`). Examples of device configurations can be found in the [Ophyd devices repository](https://gitlab.psi.ch/bec/ophyd_devices/-/tree/main/ophyd_devices/configs?ref_type=heads).

The following paragraph briefly introduces core concepts of Ophyd. A more detailed description of devices within BEC can be found in the [Devices in BEC](#developer.devices.devices_in_bec) section. Besides that, we provide a section about [Device Integration](#developer.devices.device_integration.overview) with information and tutorials how we recommend to pursue custom device integration.

## Introduction
For a high-level control layer such as BEC, it is essential to have a unified interface to control devices. This is where Ophyd comes into play. BEC is agnostic towards the underlying communication protocol of devices, but instead simply expects a set of standardised methods and properties with certain functionality. A good example is that any motor integrated into Ophyd looks the same to BEC, and its move method will move the motor to the target position. 
In general, there are two different types of devices in Ophyd, `Signal` and `Device`. They represent the fundamental building blocks of Ophyd and will be further explained in the following. 

### Signal
A signal represents an atomic process variable. This can be, for instance, a read-only value based on the *readback* of a beam monitor or a settable variable for any type of device, i.e. *velocity* of a motor. Signals can also have strings or arrays as return values — basically anything that the underlying hardware provides. However, as mentioned before, signals are atomic and cannot be further decomposed. Another aspect of signals is that they can be categorised by their [kind](https://nsls-ii.github.io/ophyd/signals.html#kind) attribute, which will become important when working with a device which may have multiple signals.

(developer.ophyd.device)=
### Device
Devices are advanced building blocks in Ophyd. They represent a hierarchy of signals and devices, meaning a device can comprise multiple signals and potentially sub-devices. For example, the `EpicsMotor` device includes signals such as `user_setpoint`, `user_readback`, `motor_is_moving`, and `motor_done_move`. These signals can be accessed through the motor object representing the device. Devices can also consist of multiple sub-devices. Some area detector integrations are built using multiple sub-devices, where different plugins from the area-detector backend, such as *cam* or *hdf*, are implemented as individual sub-devices. More generally, both signals and sub-devices are implemented as [*components*](https://nsls-ii.github.io/ophyd/generated/ophyd.device.Component.html) of the device, which are accessible through the device object. Further details can be found in the [Ophyd documentation](https://nsls-ii.github.io/ophyd/device-overview.html). 

As mentioned, devices implement an abstract hardware interface consisting of a set of methods and properties. Below is a brief introduction to some of these methods in the context of devices and signals.

Every Signal and Device implements:
- `name`: Name of the signal/device. 
- `read()`: Reads the value of the signal; for a device, reads all signals of type *kind.hinted* and *kind.normal*.
- `describe()`: Returns a description of the signal; for a device it includes all signals of type *kind.hinted* and *kind.normal*.
- `read_configuration()`: Returns a description of the signal; for a device it includes all signals of type *kind.configuration*.
- `describe_configuration()`: Returns a description of the signal; for a device it includes all signals of type *kind.configuration*.
- `trigger()`: Method for a software triggered action.

In addition, a Device implements:
- `stage()` and `unstage()`: These methods can be understood as preparation and cleanup steps for a scan. 
- `complete()`: Called at the end of a scan, this method can be used to check if the acquisition on the device completed successfully.  
- `stop()`: This method stops any ongoing actions on the device.

Some additional methods that may be implemented by more specialised device classes are:
- `kickoff()`: Relevant for Ophyd's fly interface.
- `move(value:float)`: Method provided by a *positioner* class; moves the device to a target position.
 

