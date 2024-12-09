(developer.ophyd_devices)=
# Ophyd Devices
Not all EPICS backends used by our beamlines are supported out of the box by *Ophyd*. Moreover, some devices do not have EPICS drivers, and need lower-level integration. To address this, BEC extends Ophyd with the **ophyd_devices** project.
Devices not supported by *Ophyd* built-in functionality are integrated into the [ophyd_devices](https://gitlab.psi.ch/bec/ophyd_devices) repository. Integrations in this repository should be generic to allow reuse across multiple beamlines. This repository also includes a [simulation framework](https://gitlab.psi.ch/bec/ophyd_devices/-/tree/main/ophyd_devices/sim?ref_type=heads) and utility classes to simplify integration, such as the *socket_controller class* for devices with socket communication. A comprehensive list of already integrated devices is available in the [device list](https://gitlab.psi.ch/bec/ophyd_devices/-/blob/main/ophyd_devices/devices/device_list.md?ref_type=heads). Please note that this list does not include simulation devices, or any utility classes for device integration.

If you are interested in custom device integration, please refer to the [Device Integration](developer.device_integration.overview) section. This section will focus on introducing the key concepts and methods of Ophyd to provide a solid knowledge base for custom device integration.

## Concepts of Ophyd

[Ophyd](https://nsls-ii.github.io/ophyd/) is a Python library developed at NSLS-II for interfacing with hardware. It provides a consistent interface between the underlying communication protocols and high-level software like BEC. While Ophyd can theoretically support any device, it includes built-in support for EPICS. This allows many EPICS-controlled devices to be integrated into BEC without requiring custom code.

The most commonly integrated devices in BEC are based on `EpicsMotor` and `EpicsSignal`. Ophyd defines two primary types of objects: `Signal` and `Device`. These form the core building blocks of Ophyd and will be explained in detail in the following sections. 

### Signal
A signal represents an individual hardware parameter, for example a single EPICS PV. Examples for signals include read-only values like the *readback* of a beam monitor or settable parameters like the *velocity* of a motor. Signals can return various data types like strings or arrays depending on what the underlying hardware provides. However, signals are atomic and cannot be further decomposed.

Another key aspect of signals is their [kind](https://nsls-ii.github.io/ophyd/signals.html#kind) attribute, which categorizes signals. This becomes particularly important when working with devices that include multiple signals.

(developer.ophyd.device)=
### Device
Devices are advanced building blocks in Ophyd. They represent a hierarchy of signals and devices, meaning a device can comprise multiple signals and potentially sub-devices. For example, the `EpicsMotor` device includes signals such as *user_setpoint*, *user_readback*, *motor_is_moving*, and *motor_done_move*. These signals can be accessed through the motor object representing the device. EPICS's area detector are typically built using multiple sub-devices. The sub-devices are used to integrate different area-detector backend plugins, such as *cam* or *hdf*. More generally, both signals and sub-devices are implemented as [*components*](https://nsls-ii.github.io/ophyd/generated/ophyd.device.Component.html) of a device, which are accessible through the device object. Further details can be found in the [Ophyd documentation](https://nsls-ii.github.io/ophyd/device-overview.html). 

**Methods and Properties**

Devices and Signals implement an abstract hardware interface consisting of a set of methods and properties. All devices and signals have the following methods/properties:

* `name`: Name of the signal/device. 
* `read()`: Reads the value of the signal; for a device, reads all signals of type *kind.hinted* and *kind.normal*.
* `describe()`: Returns a description of the signal; for a device it includes all signals of type *kind.hinted* and *kind.normal*.
* `read_configuration()`: Returns a description of the signal; for a device it includes all signals of type *kind.configuration*.
* `describe_configuration()`: Returns a description of the signal; for a device it includes all signals of type *kind.configuration*.
* `trigger()`: Method for a software triggered action.

Additional methods for devices include:
* `stage()` and `unstage()`: These methods can be understood as preparation and cleanup steps for a scan. 
* `complete()`: Called at the end of a scan, this method can be used to check if the acquisition on the device completed successfully.  
* `stop()`: This method stops any ongoing actions on the device.

More specialised devices as flyers or positioners implement:
* `kickoff()`: Relevant for Ophyd's fly interface.
* `move(value:float)`: Method provided by a *positioner* class; moves the device to a target position.

## Device Events and Callbacks

Ophyd provides mechanisms for executing callbacks in response to specific device events. These events, called `SUB_EVENTS`, are defined as class attributes on devices or signals. The example below demonstrates the available events. 

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
````{note}
`SUB_EVENTS` have to be class attributes and defined with the naming convention of `SUB_{EVENT}`. Later on, the actual string, i.e. "readback", is what is relevant for subscriptions to the event type.
````

## Callbacks

Executing a callback in Ophyd is straightforward. The following example demonstrates how to trigger a callback on the `SUB_VALUE` event of the *DummyDevice*.

```python
self._run_subs(sub_type=self.SUB_VALUE)
```

The `self._run_subs` method, inherited from the `Device` class, triggers the callbacks for the event 'value'. Please note that the execution of callbacks will silently fail in case they can not be executed. Therefore, it is crucial to ensure that the callbacks are provided with the correct arguments. For instance, the 'value' event will invoke the following callback within BEC's device server.

```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
:language: python
:pyobject: DeviceManagerDS._obj_callback_readback
:dedent: 4
```