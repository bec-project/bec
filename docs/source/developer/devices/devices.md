(developer.devices)=
# Devices
Devices are the fundamental building blocks of beamline operations, representing the hardware components controlled by BEC.
BEC leverages the [Ophyd library](https://blueskyproject.io/ophyd/) to provide a hardware abstraction layer for devices.
This enables integration with a wide variety of EPICS-based devices, such as [`EpicsMotor`](https://nsls-ii.github.io/ophyd/builtin-devices.html#epicsmotor) and signals like [`EpicsSignalWithRBV`](https://nsls-ii.github.io/ophyd/generated/ophyd.areadetector.base.EpicsSignalWithRBV.html). 

In the following, we offer an overview of [Ophyd](#developer.ophyd), introducing the library in our own words.
Next, we explain how to [configure devices in BEC](#developer.ophyd_device_config) and load new configurations.
Beamlines will inevitably need to modify device configurations to add, remove, or update devices.
The [device server](developer.architecture) is the central hub where BEC manages all devices.
The section [Devices in BEC](#developer.devices.devices_in_bec) outlines how devices are managed by BEC.

BEC includes a [simulation framework](#developer.bec_sim) for simulating a wide range of beamline devices.
This framework is invaluable for developers, allowing them to build and test new features and tools without direct access to the beamline.
It also enables beamlines to develop and refine scans using simulated devices, reducing reliance on beamline infrastructure.
Additionally, it supports automated testing in realistic environments, which is crucial for maintaining BEC's reliability.

The final section focuses on [device integration](#developer.devices.device_integration.overview), offering guidance to integrate custom devices into BEC.

```{toctree}
---
maxdepth: 2
hidden: true
---
ophyd/
device_configuration/
devices_in_bec/
bec_sim/
device_integration/overview
```