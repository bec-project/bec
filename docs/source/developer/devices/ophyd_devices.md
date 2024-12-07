(developer.ophyd_devices)=
# Ophyd Devices

## Introduction to Ophyd

Device integration typically involves two steps: establishing communication with the device and adapting its behavior to the beamline requirements.
[Ophyd](https://nsls-ii.github.io/ophyd/) is the Python library to interface with hardware developed at NSLS-II and used by BEC to communicate with hardware. It provides a consistent interface between the underlying control communication protocol and high-level software such as BEC. While Ophyd can theoritically be used for any device, it comes with EPICS support out of the box. This means that many devices that are controlled by EPICS can be integrated directly into BEC without the need of writing custom code. The most common devices that are integrated into BEC are based on `EpicsMotor` and `EpicsSignal` (or `EpicsSignalRO`). Examples of device configurations can be found in the [Ophyd devices repository](https://gitlab.psi.ch/bec/ophyd_devices/-/tree/main/ophyd_devices/configs?ref_type=heads).

## Ophyd concepts

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

# Device Events and Callbacks

Ophyd provides mechanisms to execute callbacks in response to specific device events. These events, known as `SUB_EVENTS`, can be defined as class attributes on any device or signal. Below is an example that demonstrates the available events. Note that these events must be defined as class attributes and cannot be created dynamically at runtime. Syntaxwise, the class attributes need to be defined starting with `SUB_`, but the actual name provided as a string is the one which is relevant for the event.

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

Ophyd uses the `SUB_EVENTS` internally to trigger its own callbacks. For example, the `EpicsSignal` class utilises the `SUB_VALUE` event in its `put` method to signal the control layer (e.g., *pyepics*) to update the hardware. Similarly, BEC subscribes to these events to trigger its own custom callbacks.

## Callbacks

Executing a callback in Ophyd is straightforward. The following example demonstrates how to trigger a callback on the `SUB_VALUE` event on the *DummyDevice* defined above.

```python
self._run_subs(sub_type=self.SUB_VALUE)
```

The `self._run_subs` method, inherited from the `Device` class, triggers the callbacks for the event 'value'. Please note that the execution of callbacks will silently fail in case they can not be executed. Therefore, it is crucial to ensure that the callbacks are provided with the correct arguments. For instance, the 'value' event will invoke the following callback within BEC's device server.

```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
:language: python
:pyobject: DeviceManagerDS._obj_callback_readback
:dedent: 4
```
 
## Extending Ophyd: BEC Ophyd Devices project

Not all EPICS backends used by our beamlines are supported out of the box by Ophyd. Moreover, some devices do not have EPICS drivers, and need lower-level integration. To address this, BEC extends Ophyd with the **ophyd_devices** project.

### Ophyd Devices repository

Devices not supported by *Ophyd* built-in functionality are integrated into the [ophyd_devices](https://gitlab.psi.ch/bec/ophyd_devices) repository. Integrations in this repository should be generic to allow reuse across multiple beamlines. This repository also includes a [simulation framework](https://gitlab.psi.ch/bec/ophyd_devices/-/tree/main/ophyd_devices/sim?ref_type=heads) and utility classes to simplify integration, such as the *socket_controller class* for devices with socket communication. A comprehensive list of already integrated devices is available in the [device list](https://gitlab.psi.ch/bec/ophyd_devices/-/blob/main/ophyd_devices/devices/device_list.md?ref_type=heads). Please note that this list does not include simulation devices, or any utility classes for device integration.

