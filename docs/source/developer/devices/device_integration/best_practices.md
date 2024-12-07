(developer.devices.device_integration.best_practices)=
# Beamline Specific Integration best practices

Integrating devices across beamlines while accommodating beamline-specific requirements is a critical aspect of reliable device management. Ideally, a device can be reused across multiple beamlines without compromising its core functionality. At the same time, beamline-specific requirements, such as customised behavior during scans, should be easily implemented. This section explains how to achieve this balance without rewriting the entire device class for each beamline.

(developer.devices.device_integration.beamline_specific_integration.generic_approach)=
## Generic Integration Approach

Customising devices is often necessary, but repeating similar logic across multiple beamlines can lead to code duplication and maintenance challenges. Instead, we recommend adopting a more generic approach to encapsulate beamline-specific logic in a single place. This approach separates the device communication logic from the beamline-specific logic, making it easier to manage and extend. We will explore this approach in the following.

````{note}
The *Ophyd* library provides several abstract methods that integrate seamlessly with BEC's scanning framework. Refer to the [ophyd section](#developer.ophyd.device) for details. Additionally, we recommend to explore the [scan structure](#developer.scans.scan_structure) section to understand the hierarchy and methods relevant during scans within BEC.
````

**Base Class for Device Integration**

We recommend encapsulating beamline-specific logic in a single location. This approach separates the logic for device communication and BEC communication as much as possible.
We introduce a base class that wraps around the standard Ophyd interface. This base class provides hooks for beamline-specific logic, implemented through a separate class handling custom prepare actions.

In the code snippet below, the *stage* method of the *PSIDetectorBase* class includes a call to `self.custom_prepare.on_stage()`, which serves as a hook for beamline-specific logic during the staging process. Before this hook is executed, the class fetches and parses the latest scan information from BEC via `self.scaninfo.load_scan_metadata()`.

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/psi_detector_base.py
:language: python
:pyobject: PSIDetectorBase.stage
:dedent: 4
```

This pattern allows flexibility by enabling the customisation of specific stages(e.g., `on_stage`, `on_unstage`, `on_complete`, etc.) without altering the core device or BEC logic. 

**Custom Prepare Actions**
The actual beamline-specific implementation resides in a separate class, passed to the *PSIDetectorBase* through the *custom_prepare_cls* attribute. This class is instantiated in the  *__init__* method of *PSIDetectorBase*.

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/interfaces/base_classes/psi_detector_base.py
:language: python
:pyobject: PSIDetectorBase.__init__
:dedent: 4
```

The *custom_prepare_cls* is initialised as `self.custom_prepare_cls(parent=self, **kwargs)`,  allowing the beamline-specific logic to access all attributes and methods of the device class. This structured approach ensures modularity and allows seamless interaction between beamline logic and device functionality.

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

While this approach introduces several layers, it provides several key benefits:
- **Modularity:** Separates beamline-specific logic from device classes, improving readability and maintainability.
- **Reusability:** Beamline-specific logic can be reused across multiple devices.
- **Extensibility:** Additional hooks, such as `on_unstage` or `on_complete`, can be added to the custom class as needed.
- **Structured Implementation:** Beamline-specific logic is implemented in a plugin repository, ensuring a more organized codebase.


**Final Implementation**
The motor class can now adopt this approach by inheriting from both `EpicsMotor` and `PSIDeviceBase`. Beamline-specific logic is implemented in the `BeamlineCustomPrepare` class, which inherits from `CustomDetectorMixin` and overrides the necessary hooks. In this example, hooks like `on_stage`, `on_unstage` and `on_stop` ensure that motor velocity is restored to its original value even if the motor is stopped during an acquisition.

``` python
from ophyd import EpicsMotor
from ophyd_devices.interfaces.base_classes.psi_detector_base import (
    CustomDetectorMixin,
    PSIDetectorBase,
)

class BeamlineCustomPrepare(CustomDetectorMixin):
    
   def __init__(self, *_args, parent = None, **_kwargs):
      """ Beamline specific actions during initialization """
      super().__init__(_args=_args, parent=parent, _kwargs=_kwargs)
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
   

class MyBeamlineMotor(PSIDetectorBase, EpicsMotor):

    custom_prepare_cls = BeamlineCustomPrepare
```

A few final remarks on the implementation:
- The *parent* attribute allows beamline-specific logic to interact directly with the device class.
- The *velocity* component from *EpicsMotor* is accessible via `self.parent.velocity`.
- Since the *CustomDetectorMixin*'s `__init__` is overridden, ensure that `super().__init__(_args=_args, parent=parent, _kwargs=_kwargs)` is called correctly, especially passing the *parent* attribute.

## Summary
This approach introduces a  *PSIDetectorBase* class that provides utility methods for BEC interaction and separates beamline-specific logic into a *CustomDetectorMixin* class. Device classes inherit from both the core device class (e.g. *EpicsMotor*) and *PSIDetectorBase*, using the *custom_prepare* class variable to integrate beamline-specific logic via hooks as `on_stage`, `on_unstage`, etc.

By decoupling device communication, BEC interaction, and beamline-specific logic, this design improves code maintainability, extensibility, and modularity. Beamline-specific logic can now be implemented and extended within a dedicated plugin repository, ensuring a clean and organized implementation.
