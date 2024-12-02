(developer.devices.device_integration.overview)=
# Device Integration
Device integration is essential to ensure the reliable operation of BEC. We face the challenge of integrating multiple devices from different beamlines, each with its own specific requirements. The same device might be used in different beamlines, but requires different settings to operate correctly for each of the beamlines. We will further explain and introduce our recommended solution to this in the [beamline specific integration](#developer.devices.device_integration.beamline_specific_integration) section, and in addition provide a [tutorial](#developer.devices.device_integration.tutorial) to guide you through the process of integrating a new device, i.e. a detector, into BEC. Furthermore, we also discuss how to forward data to BEC such as *file events*, *preview data* from 2D detectors, or *asynchronous data* in the [device events and callbacks](#developer.devices.device_integration.device_events_and_callbacks) section. This also includes examples for sending data from [external data sources](#developer.devices.device_integration.external_data_sources), which can be useful to stream data from external sources, i.e. a DAQ backend, directly into BEC. Last but not least, we would like to draw the attention to testing as we believe this to be one of the most important aspects of having a reliable and maintainable device integration. We provide some examples how to test EPICS based devices in an automated fashion (CI/CD) in the [automated testing](#developer.devices.device_integration.automated_testing) secion.

# Ophyd, Ophyd_devices and Beamline plugins
Device integration is typically split into two steps: Building the tools to communicate with the device and tailoring the device's behavior to the beamline's needs.
**Ophyd**
*Ophyd* is the hardware abstraction layer for BEC and comes with built-in support for some EPICS devices, i.e. `EpicsMotor` or `EpicsSignalRO`. In a sense, *Ophyd* brings support to communicate with a number of devices at multiple beamline, which is of great advantage already.

**Ophyd_devices**
Our beamlines do have hardware components that are non-EPICS based which require custom integration efforts. In addition, not all EPICS based devices come with built-in support from *Ophyd*. We host our own library, *ophyd_devices*, to do any custom integration of the communication with a device. This brings us also to the scope of the repository. Any code here should be non beamline specific, thus, integrated devices are rather tools to communicate with the hardware but not host beamline-specific logic. We further provide a set of utility methods and classes to simplify the integraction process and automated testing of devices. To get a quick overview of all integrated devices, we auto-generate a list of devices which are integrated in this repository ([device list](https://gitlab.psi.ch/bec/ophyd_devices/-/blob/main/ophyd_devices/devices/device_list.md?ref_type=heads)).

**Beamline plugin repositories**
Some beamlines have special customised devices that only work at their own beamline. For these devices, it may be more complicated to split the integration effort into two parts, thus, the device might be directly integrati
Beamlines may require devices to perform certain beamline-specific actions. This kind of integration should go in the devices module of the [plugin repository](#developer.bec_plugins).
Certain devices might be similar or even the exact same across multiple beamlines, however, some beamlines may have special customised devices that are only working at *beamline_XX*. The beamline may choose to integrate these devices in their own beamlines repositories, and directly mix communication and beamline specific customisations within one class. Whenever possible, we recommend to keep both the two seperated but this is not always possible. We also auto-generate  a list of integrated devices for every beamline repository, which can be found at *beamline_XX_bec/beamline_XX_bec/devices/device_list.md*, i.e. for cSAXS [here](https://gitlab.psi.ch/bec/csaxs_bec/-/blob/main/csaxs_bec/devices/device_list.md?ref_type=heads).
````{note}
The device lists in *ophyd_devices* and the beamline plugin repositories are auto-generated and documentated through the docstring provided by the class. Please keep this in mind and provide a brief description when integrating a new device. 
````


At the end, we would like to briefly 
Different beamlines may have different requirements during operations. 



These classes are tools to communicate with the hardware.
Restructure...:
- (A) Ophyd library; built-in support for certain devices
- (B) Ophyd_devices; wrapper for PSI device integrations. EPICS and non-EPICS + tools/utilities to simplify integration process
- (C) Beamline repositories; beamline specific integrations

--> Common communication and control may be implemented in ophyd_devices, custom beamline logic in beamline repository. Beamlines may integrate their custom devices. --> Controller class hosted in ophyd devices (tool), nPoint piezo in cSAXS beamline repository. DelayGenerator as second example in ophyd_devices? 




 The first step is always to build the tools to communicate with the device. As mentioned in the [Ophyd](#developer.ophyd) section, we use the *Ophyd* library to communicate with the hardware. While *Ophyd* provides built-in support some device, i.e. `EpicsSignal` and `EpicsMotor`, the reality at the beamline is that there are often a few important devices at the beamline that do not have built-in support by *Ophyd* but require custom integration. This is where our own library *ophyd_devices* comes into play. This library provides custom device integrations, utility classes, and functions to simplify the integration process. Here, we integrate all devices that are relevant for multiple beamlines. We would like to integrat the communication protocol (A) to any device that may be of interest for multiple beamlines in the *ophyd_devices* repository. 

```` {note}
Devices integrated into our *ophyd_devices* library should not have beamline specific logic as they may be in use at multiple beamlines
````

Certain devices may be only available at single beam
To accomodate beamline-specific logic, we


All devices integrated within *ophyd_devices* are listed in the [device list](https://gitlab.psi.ch/bec/ophyd_devices/-/blob/main/ophyd_devices/devices/device_list.md?ref_type=heads). Please also check some of our tool  

We combine the *Ophyd* library with our own library *ophyd_devices* to host a goo


Given that *Ophyd* is hosted and maintaind by NSLS-II, we additionally provide our own library *ophyd_devices*. This library provides custom device integrations, utility classes, and functions to simplify the integration process. Here, we integrate all devices that are relevant for multiple beamlines. We also host plugin repositories for each beamline with their 


that provides custom device integrations, utility classes, and functions to simplify the integration process. Here, we integrate all devices that are relevant for multiple beamlines. We also host plugin repositories for each beamline with their

```{note}
The integration of the *nPoint piezo stage* is hosted within one of our beamline repositories. While *ophyd_devices* is a general library hosting integrations of hardware that are used across multiple beamlines, there might also be integrations of devices within the beamline repositories. Please check the [plugin repository](#developer.bec_plugins) section for more details on beamline repositories. 
```

````{dropdown} View code: Socket Controller
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/utils/controller.py
:language: python
:pyobject: Controller

```
````

## 

or our custom device integrations, we have written a library called *ophyd_devices*. This library provides custom.

Besides *Ophyd*, we host our own repository with custom device integrations [ophyd_devices](https://gitlab.psi.ch/bec/ophyd_devices). These integrations range from simple devices with EPICS as a control layer to the implementation of custom communication protocols. We automatically compile a markdown file with a list of devices integrated in *ophyd_device* and the beamline repositories. All devices integrated within *ophyd_devices* are listed in the [device list](https://gitlab.psi.ch/bec/ophyd_devices/-/blob/main/ophyd_devices/devices/device_list.md?ref_type=heads), similarly we compile lists for each beamline repository automatically in *beamline_XX_bec/beamline_XX_bec/devices/device_list.md*, i.e. for cSAXS [here](https://gitlab.psi.ch/bec/csaxs_bec/-/blob/main/csaxs_bec/devices/device_list.md?ref_type=heads).

Besides the device integrations, the library also provides utility classes and functions to simplify the integration process. One common example is to have a hardware controller that can handle multiple axes, but only allows a single socket connection. The *socket_controller* class simplifies the integration of a device with socket communication; please also check its usage for the [nPoint piezo stage](https://gitlab.psi.ch/bec/csaxs_bec/-/blob/main/csaxs_bec/devices/npoint/npoint.py?ref_type=heads).  

```{note}
`

```{toctree}
---
maxdepth: 1
hidden: true
---
beamline_specific_integration/
tutorial/
device_events_and_callbacks/
external_data_sources/
```