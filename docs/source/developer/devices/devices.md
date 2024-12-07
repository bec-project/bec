(developer.devices)=
# Devices

This section explains how BEC handles **devices**, i.e. fundamental building blocks of beamline operation:

- [Ophyd Devices](developer.ophyd_devices) provides a hardware abstraction layer for devices
- [BEC Device Server](developer.devices.bec_device_server) is the central hub where BEC manages all devices
- [Defining Devices](developer.ophyd_device_config) details how to configure devices in BEC and load new configurations
    - beamlines will inevitably need to modify device configurations to add, remove, or update devices
- BEC also includes a [simulation framework](developer.bec_sim) for simulating a wide range of beamline devices
    - this is invaluable for developers, allowing them to build and test new features and tools without direct access to the beamline
    - it also enables beamlines to develop and refine scans using simulated devices, reducing reliance on beamline infrastructure
    - it supports automated testing in realistic environments, which is crucial for maintaining BEC's reliability.

Finally [device integration](developer.device_integration.overview) offers guidance to integrate custom devices into BEC.

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
