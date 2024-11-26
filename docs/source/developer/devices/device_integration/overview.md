(developer.devices.device_integration.overview)=
# Device Integration

Device integration is essential to ensure the reliable operation of BEC. 
As described in the [`ophyd`](#developer.devices.ophyd) section, we use the *ophyd* library to standardize device interactions. 
In addition to `ophyd`, we have our own library [`ophyd_devices`](https://gitlab.psi.ch/bec/ophyd_devices) in which we implement custom device classes.
You can find a list of devices integrated through `ophyd_devices` in the [device list](https://gitlab.psi.ch/bec/ophyd_devices/-/blob/main/ophyd_devices/devices/device_list.md?ref_type=heads). The library also hosts utility classes and functions to facilitate the integration of custom devices. This includes for example a socket controller to handle communication with a device over a socket, or utilities to simplify writing tests for devices ([see code](https://gitlab.psi.ch/bec/ophyd_devices/-/blob/main/ophyd_devices/tests/utils.py?ref_type=heads)). 

````{dropdown} View code: Controller class
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../../ophyd_devices/ophyd_devices/utils/controller.py
:language: python
:pyobject: Controller

```
````

Device integration is a two-step process. First, we need to establish communication with the device, which is agnostic to the beamline. Most likely this class is already provided through `ophyd`, or available in the `ophyd_devices` library. Second, we need to implement the beamline-specific logic. Whenever possible, we embrace integration in which devices are capable of setting themselves up after receiving relevant information from the scan. This does not mean that scan-specific logic is to be implemented on the device, but rather that the device is capable of doing the necessary steps to prepare itself for a scan given the information about the scan is received, i.e. 'step' or 'fly' scan, exposure time, number of images, etc. The final class with the beamline-specific logic should be implemented in the beamline's [plugin repository](developer.bec_plugins) as it will only be able to run as intented at this beamline.

**Overview of this section**
To learn more about the integration of beamline-specific devices, please check the [beamline specific integration](#developer.devices.device_integration.beamline_specific_integration) section and the [tutorial](#developer.devices.device_integration.tutorial). We also provide a more in-depth explanation of the events and callbacks system of ophyd, which BEC takes full leverage of, in the [device events and callbacks](#developer.devices.device_integration.device_events_and_callbacks) section. Finally, the section [external data sources](#developer.devices.device_integration.external_data_sources) provides insights on how to integrate data from external data sources into BEC, i.e. directly from a 2D detector backend.

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