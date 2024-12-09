(developer.devices.device_integration.best_practices)=
# Best Practices

Integrating devices across beamlines while accommodating beamline-specific requirements is a critical aspect of reliable device management. Ideally, a device can be reused across multiple beamlines without compromising its core functionality. At the same time, beamline-specific requirements, such as customised behavior during scans, should be easily implemented. This section explains how to achieve this balance without rewriting the entire device class for each beamline. 

````{admonition} Motivation
A simple example for such a requirement is the need to set a motor's velocity to a specific value during a fly scan. This requirement is beamline-specific and should not be hardcoded into the motor class. If we were to implement these requirement, we would need to subclass the EpicsMotor class and override the *stage* and *unstage* methods. In addition, we would need to fetch the latest scan information from BEC to determine the scan type and adjust the motor's velocity accordingly. This approach is not only cumbersome but also leads to code duplication, is error-prone, and difficult to maintain. We recommend a more generic approach.
````

(developer.devices.device_integration.beamline_specific_integration.generic_approach)=
## Generic Integration Approach

We recommend adopting a more generic approach to encapsulate beamline-specific logic in a single place. The idea is to wrap the standard Ophyd interface witha base class that provides hooks for beamline-specific logic. In this way, the device communication logic is separated from the beamline-specific logic, making it easier to manage and extend. We will explore this approach in the following.

````{note}
Please check the [ophyd section](#developer.ophyd.device) for more details on which methods Ophyd provides through its interface, and the [scan structure](#developer.scans.scan_structure) section to understand how BEC puts them to use within scans.
````

### Introducing BECDeviceBase

*BECDeviceBase* is the class written for this purpose. The actual logic to separate beamline specific logic is implemented through a *custom_prepare_cls* attribute. This attribute is an exchangable class that should be used to implement beamline specific logic through pre-defined hooks. The class itself is instantiated in the *__init__* method of *BECDeviceBase*, and receives the class itself as the parent. This gives the *custom_prepare_cls* access to all attributes, methods and properties of the device class. 

There are several hooks that can be implemented in the *custom_prepare_cls* class, such as `on_stage`, `on_unstage`, `on_complete`, etc. These hooks are called at specific stages of the device's lifecycle, such as staging, unstaging, or completion of a scan. *BECDeviceBase* ensures that upon calling these hooks, all the necessary information is available and loaded from BEC.

````{dropdown} View code: PSIDeviceBase
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/bec_device_base.py
:language: python
:pyobject: BECDeviceBase
```
````

````{dropdown} View code: CustomPrepare
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/bec_device_base.py
:language: python
:pyobject: CustomPrepare
```
````

While this approach introduces several layers, it provides several key benefits:
- **Modularity:** Separates beamline-specific logic from device classes, improving readability and maintainability.
- **Reusability:** Beamline-specific logic can be reused across multiple devices.
- **Extensibility:** Additional hooks, such as `on_unstage` or `on_complete`, can be added to the custom class as needed.
- **Structured Implementation:** Beamline-specific logic is implemented in a plugin repository, ensuring a more organized codebase.

Some extra information about utilities available in the two classes are listed below:

**PSIDeviceBase**

Base class that wraps around the standard Ophyd interface and provides utility methods for BEC interaction. It includes the following attributes:
* `self.scaninfo`: An object that represents the latest scan information fetched from BEC.
* `self.custom_prepare_cls`: The class that implements the beamline specific logic via hooks, i.e. on_stage, on_unstage, etc.
* `self.filewriter`: A utility class that provides methods to compile file paths for data backend services.
* `self.stopped`: Property to indicate if the device was stopped.

**CustomPrepare**

Class that provides hooks for the beamline specific logic. 
* `self.parent`: The parent attribute gives access to all attributes, properties and methods of the parent class which will be a subclas of PSIDeviceBase.
* `self.wait_for_signal`: A utility method to simplify waiting for signals to reach certain conditions. This method is blocking.
* `self.wait_with_status`: A utility method to simplify waiting for signals to reach certain conditions. This method is non-blocking and returns a status object. status.done and status.success can be used to check if the wait was successful.

````{note}
We invite you to explore the docstrings of all hooks and utility methods within the *CustomPrepare* class to understand their purpose and usage.
````

### Example: Customised EpicsMotor

Revisiting the motor class example, we can now rewrite it with our generic approach. Our new class now inherits from both `EpicsMotor` and `PSIDeviceBase`. Beamline-specific logic is implemented in the `BeamlineCustomPrepare` class, which inherits from `CustomDetectorMixin` and overrides the necessary hooks. In this example, hooks like `on_stage`, `on_unstage` and `on_stop` ensure that motor velocity is restored to its original value even if the motor is stopped during an acquisition.


``` python
from ophyd import EpicsMotor
from ophyd_devices.interfaces.base_classes.bec_device_base import (
    CustomPrepare,
    BECDeviceBase,
)

class BeamlineCustomPrepare(CustomPrepare['MyBeamlineMotor']):

   def on_init(self):
      """ Beamline specific actions during initialization """
      self._stored_velocity = None

   def on_stage(self):
      """ Beamline specific actions during staging """
      if self.parent.scaninfo.scan_type == "fly":
         self._stored_velocity = self.parent.velocity.get()
         self.parent.velocity.set(2).wait()

   def on_unstage(self):
      """ Beamline specific actions during unstaging """
      self.on_stop()

   def on_stop(self):
      """ Beamline specific actions during stopping """
      if self._stored_velocity is not None:
         self.parent.velocity.set(self._stored_velocity).wait()
         self._stored_velocity = None
   

class MyBeamlineMotor(BECDeviceBase, EpicsMotor):

    custom_prepare_cls = BeamlineCustomPrepare
```

Check from here on the rest!!! 

A few final remarks on the implementation:
- The *parent* attribute allows beamline-specific logic to interact directly with the device class.
- The *velocity* component from *EpicsMotor* is accessible via `self.parent.velocity`.
- Since the *CustomDetectorMixin*'s `__init__` is overridden, ensure that `super().__init__(_args=_args, parent=parent, _kwargs=_kwargs)` is called correctly, especially passing the *parent* attribute.

````{note}
Multiple inheritance in Python can be complex. Please ensure that the order of inheritance is correct, with the base class (e.g., `PSIDetectorBase`) listed first.
````

## Summary
This approach introduces a  *PSIDetectorBase* class that provides utility methods for BEC interaction and separates beamline-specific logic into a *CustomDetectorMixin* class. Device classes inherit from both the core device class (e.g. *EpicsMotor*) and *PSIDetectorBase*, using the *custom_prepare* class variable to integrate beamline-specific logic via hooks as `on_stage`, `on_unstage`, etc.

By decoupling device communication, BEC interaction, and beamline-specific logic, this design improves code maintainability, extensibility, and modularity. Beamline-specific logic can now be implemented and extended within a dedicated plugin repository, ensuring a clean and organized implementation.
