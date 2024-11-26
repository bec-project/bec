(developer.ophyd)=
# Ophyd Library

[Ophyd](https://nsls-ii.github.io/ophyd/) is the hardware abstraction layer developed by NSLS-II and used by BEC to communicate with hardware. It is a Python library that provides a consistent interface between the underlying control communication protocol and the high-level software BEC. While Ophyd can be used for any device, it comes with EPICS support out of the box. This means that many devices that are controlled by EPICS can be integrated directly into BEC without the need of writing custom Ophyd classes. The most common devices that are integrated into BEC are based on `EpicsMotor` and `EpicsSignal` (or `EpicsSignalRO`). Examples of device configurations can be found in the [Ophyd devices repository](https://gitlab.psi.ch/bec/ophyd_devices/-/tree/main/ophyd_devices/configs?ref_type=heads).

The following paragraph briefly introduces core concepts of Ophyd. A more detailed description of devices within BEC can be found in the [devices in BEC](#developer.devices.devices_in_bec) section. Besides that, we offer a more detailed section about [device integration](#developer.devices.device_integration.overview) with information and tutorials how we recommend to pursue custom device integration.

## Introduction

Ophyd bundles sets of underlying process variables into hierarchical devices and exposes a semantic API in terms of control system primitives. This statement is taken from Ophyd's documentation. In detail, this means that Ophyd allows high-level software, i.e. BEC, to be ignorant of the details of how the communication protocol to a device is implemented. It knows that it can expect certain functionality, methods, and properties. A good example is that any motor integrated into Ophyd looks the same to BEC, and its move method will move the motor to the target position. Two key terms that will reappear are `Signal` and `Device`, which are fundamental building blocks of Ophyd.

### Signal

A signal represents an atomic process variable. This can be, for instance, a read-only value based on the *readback* of a beam monitor or a settable variable for any type of device, i.e. *velocity* of a motor. Signals can also have strings or arrays as return values—basically anything that the underlying hardware provides. However, as mentioned before, signals are atomic and cannot be further decomposed. Another important aspect is the [`kind`](https://nsls-ii.github.io/ophyd/signals.html#kind) attribute. It allows the developer to classify signals into different categories, which becomes relevant for handling callbacks, for instance `read()` or `read_configuration()` for devices.

### Device
A device represents a hierarchy of signals and devices, meaning that devices are composed of signals and potentially sub-devices. These are implemented as a [Component](https://nsls-ii.github.io/ophyd/generated/ophyd.device.Component.html). Devices can have multiple signals integrated as *Components* or even sub-devices, which are integrated in the same fashion. Further details can be found in the [Ophyd documentation](https://nsls-ii.github.io/ophyd/device-overview.html).  

For example, the `EpicsMotor` device includes signals such as `user_setpoint`, `user_readback`, `motor_is_moving`, and `motor_done_move`. These signals are all part of the motor device and can be accessed through the motor object. More complex devices, such as detectors, may be composed of various components used to configure and prepare the detector for an upcoming acquisition.  

In addition to components, devices also implement various methods and properties. Two important methods that any device implements are `read()` and `read_configuration()`. These methods read the values of signals categorized as `kind.hinted` & `kind.normal` or `kind.config`, respectively.  

Several other methods are particularly relevant for working with devices in Ophyd:  
- `stage()` and `unstage()`: These methods can be understood as preparation and cleanup steps for a scan.  
- `trigger()`: This method is used when a device requires one or more software triggers during a scan.  
- `complete()`: Called at the end of a scan, this method can be used to check if the acquisition on the device completed successfully.  
- `stop()`: This method stops any ongoing actions on the device.  

Additional methods include:  
- `kickoff()`: Relevant for Ophyd's fly interface.  
- `describe()` and `read_configuration()`: Used to describe the device and read its configuration, respectively.  
- `move()`: Provided by `Positioner`, this method moves the device to a target position.  

Further details can be found in the [Ophyd documentation](https://nsls-ii.github.io/ophyd/device-overview.html).  

