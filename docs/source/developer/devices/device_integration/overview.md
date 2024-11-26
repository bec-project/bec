(developer.devices.device_integration.overview)=
# Device Integration
Device integration is essential to ensuring the reliable operation of BEC. A significant challenge is integrating devices from different beamlines, each with unique requirements. The same device may be used across beamlines but often requires specific configurations to operate correctly for each beamline. We discuss our recommended solution in the [beamline specific integration](#developer.devices.device_integration.beamline_specific_integration) section and provide a [tutorial](#developer.devices.device_integration.tutorial) for integrating a new device, such as a detector, into BEC. Additionally, we cover how to forward data to BEC, including *file events*, *preview data* from 2D detectors, and *asynchronous data* in the [device events and callbacks](#developer.devices.device_integration.device_events_and_callbacks) section. This also includes examples for streaming data from [external data sources](#developer.devices.device_integration.external_data_sources), such as a DAQ backend, directly into BEC. Finally, we emphasize testing as a critical component of reliable and maintainable device integration, with examples of automated testing for EPICS-based devices in the [automated testing](#developer.devices.device_integration.automated_testing) secion. 

Before diving in, we introduce the repositories relevant to device integration, namely *Ophyd*, *Ophyd_devices*, and the *beamline plugin repositories*.

## Relevant repositories
Device integration typically involves two steps: establishing communication with the device and tailoring the device's behavior to the beamline's requirements. *Ophyd* provides built-in support for some EPICS-based devices, but not all EPICS backends used by our beamlines are supported out of the box. To address this, we maintain the *ophyd_devices* repository for integrating devices not covered by *Ophyd*, including non-EPICS devices. We recommend keeping integrations in *ophyd_devices* as generic as possible to ensure reusability across beamlines. Below, we briefly introduce the relevant repositories and their scope to clarify where to integrate device communication and beamline-specific logic.

### Ophyd
*Ophyd* includes built-in support for several EPICS devices, such as `EpicsMotor` or `EpicsSignalRO`. These built-in devices facilitate communication with various hardware across multiple beamlines without requiring customization. When no beamline-specific customization is needed, these devices can be used as-is. Please check the [device configuration](#developer.ophyd_device_config) section for details on loading devices into BEC.

### Ophyd_devices
Devices not supported by *Ophyd* built-in functionality are integrated into the *ophyd_devices* repository. Integrations in this repository should be generic to allow reuse across multiple beamlines. This repository also includes a [simulation framework](https://gitlab.psi.ch/bec/ophyd_devices/-/tree/main/ophyd_devices/sim?ref_type=heads) and utility classes to simplify integration, such as the *socket_controller class* for devices with socket communication. A comprehensive list of devices integrated into *ophyd_devices* is available in the [device list](https://gitlab.psi.ch/bec/ophyd_devices/-/blob/main/ophyd_devices/devices/device_list.md?ref_type=heads). Please note that this list does not include simulation devices, or any utility classes for device integration.

### Beamline plugin repositories
Each beamline has a plugin repository (e.g.,*beamline_XX_bec*) to standardise how BEC is customised for specific beamline requirements. The [plugin repository](#developer.bec_plugins) section provides more details on the plugin structure. For device integration, each plugin contains a sub-module, *beamline_XX_bec/devices*, where beamline-specific device integrations are located. These integrations may include fully customised devices or thin classes that add beamline-specific logic to existing integrations from  *ophyd_devices* or *ophyd*. Details on structuring beamline-specific integrations are in the [beamline specific integration](#developer.devices.device_integration.beamline_specific_integration) section. Auto-generated device lists for each beamline repository are available at *beamline_XX_bec/beamline_XX_bec/devices/device_list.md*, i.e. the [device list for cSAXS](https://gitlab.psi.ch/bec/csaxs_bec/-/blob/main/csaxs_bec/devices/device_list.md?ref_type=heads).

```{toctree}
---
maxdepth: 1
hidden: true
---
beamline_specific_integration/
tutorial/
device_events_and_callbacks/
automated_testing/
```