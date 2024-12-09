(developer.devices)=
# Devices
This section provides an overview of devices, the core components of beamline operation. It covers the following topics:

* [Ophyd Devices](developer.ophyd_devices) A hardware abstraction layer for controlling devices.
* [BEC Device Server](developer.devices.bec_device_server) The central hub for managing all devices within BEC.
* [Defining Devices](developer.ophyd_device_config) Information on how devices are defined through BEC's device config.
* [Simulation Framework](developer.bec_sim) A tool for simulating beamline devices in a controlled environment, enabling feature development without direct beamline access. It also supports automated test pipelines to ensure long-term maintainability.
* [Device Integration](developer.device_integration.overview) Guidance for integrating custom devices into BEC.

```{toctree}
---
maxdepth: 2
hidden: true
---
ophyd_devices/
bec_device_server/
device_configuration/
bec_sim/
device_integration/overview
```
