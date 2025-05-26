(developer.ophyd)=
# Ophyd 

[Ophyd](https://nsls-ii.github.io/ophyd/) is the hardware abstraction layer developed by NSLS-II and used by BEC to communicate with hardware. It is a Python library that provides a consistent interface between the underlying control communication protocol and the high-level software BEC. While Ophyd can be used for any device, it comes with EPICS support out of the box. This means that many devices that are controlled by EPICS can be integrated directly into BEC without the need of writing custom Ophyd classes. The most common devices that are integrated into BEC are based on `EpicsMotor` and `EpicsSignal` (or `EpicsSignalRO`). Examples of device configurations can be found in the [Ophyd devices repository](https://github.com/bec-project/ophyd_devices/tree/main/ophyd_devices/configs).

The following sections will provide an overview of the core concepts of Ophyd and how they are used in BEC. We will also provide a detailed description of the different types of devices that are used in BEC and the core functionality that is required for a device to be integrated into BEC.

## Introduction

Ophyd bundles sets of underlying process variables into hierarchical devices and exposes a semantic API in terms of control system primitives. This statement is taken from Ophyd's documentation. In detail, this means that Ophyd allows high-level software, i.e. BEC, to be ignorant of the details of how the communication protocol to a device is implemented. It knows that it can expect certain functionality, methods, and properties. A good example is that any motor integrated into Ophyd looks the same to BEC, and its move method will move the motor to the target position. Two key terms that will reappear are `Signal` and `Device`, which are fundamental building blocks of Ophyd.

### Signal

A signal represents an atomic process variable. This can be, for instance, a read-only value based on the *readback* of a beam monitor or a settable variable for any type of device, i.e. *velocity* of a motor. Signals can also have strings or arrays as return values—basically anything that the underlying hardware provides. However, as mentioned before, signals are atomic and cannot be further decomposed. Another important aspect is the [`kind`](https://nsls-ii.github.io/ophyd/signals.html#kind) attribute. It allows the developer to classify signals into different categories, which becomes relevant for handling callbacks, for instance `read()` or `read_configuration()` for devices.

### Device
 
A device represents a hierarchy of signals and devices, meaning that devices are composed of signals and potentially sub-devices. These are implemented as components in the device (further details in the Ophyd [documentation](https://nsls-ii.github.io/ophyd/device-overview.html)) and can be inspected individually by BEC. For a motor, we would, for example, expect *readback*, *setpoint*, and some sort of status, e.g. *motor_is_moving* components. More complex devices, such as detectors, may be composed of various components used to configure and prepare the detector for an upcoming acquisition. Besides components, devices also implement different methods and properties. Two important methods any device implements are `read()` and `read_configuration()`. They read the values for all signals of type `kind.hinted` & `kind.normal` or `kind.config`, respectively. We will now provide more information on different type of devices from the perspective of BEC.

(developer.ophyd.ophyd_device)=
## Ophyd Devices

In BEC, we create representative objects for different types of devices or signals dynamically on the device server. All of them provide a set of core methods with customizations on top. For BEC, a motor becomes a positioner and expects the class to implement, for instance, a `move` method, `limits`, and a few more properties/methods. In total, BEC differentiates between `device`, `signal`, `positioner`, and `flyer`. The hierarchy between these is shown in the diagram below.


```{figure} ../../assets/bec_device_structure.png
Inheritance scheme for devices and signals in BEC.
```
We note that this hierarchy is inspired by different base class from Ophyd:
`Device`, `Signal`, `PositionerBase` and `FlyerInterface`, while also enhancing certain aspects of these classes for ease of use.

### Core functionality
In the following section, core properties and methods are listed that are required for the device server to load the class into BEC. 
We note that by inheriting from Ophyd `Device` or `Signal`, all methods and properties below will already be implemented. 

* **name -> str**\
Property with name of the device; it will also be used for naming convention of signals from a device.

* **kind -> int or ophyd.Kind**\
Property for the kind ([ophyd.Kind](https://nsls-ii.github.io/ophyd/signals.html#kind)) of the device or signal.

* **parent -> object**\
Property that points to the parent device.

* **root -> object**\
Property that points to the root device.

* **hints -> dict**\
Property that returns a dictionary with hints for callback operations on the device. 
The return dictionary lists all signals of type `kind.hinted` with the structure `{'fields' : ['signal_name', ...]}`.

* **connected -> bool**\
Property to check if a device or signal is connected (settable).

* **_destroyed -> bool**\
Instance attribute that indicates whether a device has been destroyed, should be False by default.
The destroy method should set this to True.

* **destroy() -> None**\
Method to destroy the device. A destroyed device can not be reconnected, in addition, the instance attribute `_destroyed` must be set to `True`.

* **read() -> dict**\
Read method of the device which returns a nested dictionary with all signals of type `kind.normal` and `kind.hinted`. Example: `{'signal_name': {'value': ..., "timestamp": ...}, ...}`

* **read_configuration() -> dict**\
Read configuration method of the device which returns a nested dictionary with all signals of type `kind.config`. Example: `{'signal_name': {'value': ..., "timestamp": ...}, ...}`

* **describe() -> dict**\
Describe method of the device which returns a dictionary with signal descriptions of `kind.normal` and `kind.hinted`.
The dictionary is composed of entries for each signal with additional information about *source*, *dtype* and *shape* of the signal's return value.
Example: `{'signal_name': {'source': ..., "dtype": ..., "shape" : ...}, ...}`

* **describe_configuration() -> dict**\
Similar like describe, but returns a dictionary with information about signals of type `kind.config`.
The same pattern as for describe applies.

* **trigger() -> ophyd.DeviceStatus**\
Trigger the device and return an [ophyd.DeviceStatus](https://nsls-ii.github.io/ophyd/status.html?highlight=devicestatus#status-api-details) object, which is used to track the status of the trigger call.
The status should resolve once the device has been triggered successfully, which means the `.set_finished()` method has been called on the status object.

### Signal

Signals are among the simplest objects that can be loaded by BEC. For instance, we can utilize the readback value of an *EPICS* PV to create a basic signal. Alternatively, we may generate a custom signal from a temperature controller to track during the measurement process.

Implementing a signal involves extending the core functionality with a few additional methods. It's important to note that by inheriting from `ophyd.signal`, these methods are already provided. However, their functionality needs to be ensured at the device level, particularly for devices with custom integration, and potentially override methods from `ophyd.signal`.

* **limits -> tuple[float, float]**\
Property that represents the limits of the signal and returns a tuple with high, low limits. 
We note, high_limit == low_limit is equivalent to no limits.

* **high_limit -> float**\
High limit property for the signal.

* **low_limit -> float**\
Low limit property for the signal.

* **write_access -> bool**\
Property which defines whether the signal can be written to.

* **check_value(value : float) -> None**\
Method that checks whether a value is within the given limits of the device. 
Returns None, but should raise `LimitError` if the value is not within limits.

* **put(value : any, force : bool=False, timeout : float=None) -> None**\
Calls of put should be non-blocking, and if not force, it should check the request value using `check_value` method (i.e. for position values). A timeout can be specified for the put call.

* **set(value : any, force : bool=False, timeout : float=None) -> ophyd.Status**\
Set method of signal. This typically calls `put` in a thread and returns an `ophyd.status` object that will resolve once the call return successfully. It can be made a blocking call by running `.wait()` on the status object.

(developer.ophyd_devices.device)=
### Device

Devices provide additional methods that become relevant for the scan interface within BEC.
Scans usually follow a pattern of bootstrapping devices using *stage* and *unstage* for the cleanup procedure.
The relevant methods are implemented through the interface provided here, i.e. again in analogy to the interface implemented for `ophyd.device`.

* **_staged -> ophyd.Staged**\
Instance attribute that indicates whether a device has been staged, and should return an `ophyd.Staged` state, i.e. *Staged.yes*, *Staged.no* or *Staged.partially*.

* **stage -> list[object]**\
The stage method prepares the device for an upcoming acquisition.
The method is idempotent, meaning it should raise if called twice without an `unstage` called in between. 
If possible, we suggest the implementation of a bootstrap approach throughout this function where devices prepare themselves for an upcoming scan based on the metadata provided by the scan itself, i.e. *exposure time*, *number of images/frames* or *scantype==fly/step*.
Stage will return a list of objects that were staged, i.e. itself -> `[self]`.
We recommend to return `super().stage()` from the parent class if the parent is inherited from the Ophyd repository to ensure that all stage signals are properly set.

* **unstage -> list[object]**\
The unstage method should be used to cleanup the device after an acquisition.
It should be possible to call unstage multiple times in a row, and it should resolve fast in case no acquisition took place.
After an actual acquisition, unstage can be used to check whether the acquisition finished properly and potentially raise if not (i.e. logic to be discussed during device integration with BEC developer). 
Again, we recommend to return `super().unstage()` from the parent class if inheritance from Ophyd is present.

* **stop(success : bool) -> None**\
Stop method of the device. The success flag should be used to indicate whether the device 
has been successfully stopped.
We recommend calling `super().stop(success=success)` if a class inherits from the Ophyd repository. Note, the stop call should also resolve whether a device is staged or not, i.e. call unstage of the device. Here, we see that additional instance attributes such as *_stopped* can be useful to handle internal logic of stage/unstage/stop.

* **configure(d:dict) -> dict**\
The configure method accepts a dictionary with signal_name, value pairs to set signals on the device.
In the upstream Ophyd repository, e.g. for `ophyd.device`, the method implements a way to set signals of *kind.config*. 

* **summary() -> dict**\
The summary method provides a string representation of the device. 
This includes the different type of signals of the device.

### Positioner
A simple example for a positioner is the implementation of a motor.
Positioners extend the functionality of the [device](#developer.ophyd_devices.device), and mix in similar properties as seen for the signal. In addition, they need to implement a move method that executes the motion and provides feedback to BEC. 
Below is the functionality that is required to extend the methods of the device to comply with the interface of a positioner.

* **limits -> tuple[float, float]**\
Property that represents the limits of the signal and returns a tuple with high, low limits. 
We note, high_limit == low_limit is equivalent to NO limits.

* **high_limit -> float**\
High limit property for the signal. Should also implement a property.setter method.

* **low_limit -> float**\
Low limit property for the signal. Should also implement a property.setter method.
<!-- Is this device dependent whether limits can be set or not? - just curious, I guess if yes than this could be mentioned here. -->

* **check_value(value : float) -> None**\
Method that checks whether a value is within the given limits of the device. 
Returns None, but should raise `LimitError` if the value is not within limits.

* **move(value : float) -> ophyd.DeviceStatus**\
Method to initiate a motion of the device. It should return a status object of type *DeviceStatus* that needs to resolve once the motion is finished. It should become a blocking call by calling `.wait()` on the status.

* **set(value : float, timeout : float=None, move_cb : callable=None, wait : bool=False) -> ophyd.DeviceStatus**\
With direct inheritance from `PositionerBase` from the upstream repository, the set method of a positioner is calling the move method. The behaviour can be overridden by children but a motion should be executed upon using set.

### Flyer
Flyers extend the existing interface for [device](#developer.ophyd_devices.device) with two more methods.
During a fly scan, the scan logic implemented on the scan server is more linked to the functionality of a device, i.e. continuous line scans.

* **kickoff() -> ophyd.DeviceStatus**\
Upon calling kickoff, the flyer should start and return a status object that resolves once the flyer flies, i.e. is ready to or already acquiring data.

* **complete() -> ophyd.DeviceStatus**\
The complete method of the flyer returns a status object. 
This status should resolve once the flyer finishes, thus, the method can be used to identify when a flyer is finished.
