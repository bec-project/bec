(developer.devices)=
# Devices
Whether new devices need to be added to BEC or existing devices need to be modified, the daily operation of beamlines at large-scale facilities depends on the ability change the behavior and configuration of devices. This section provides information on how to configure and use devices in BEC.

After an introduction to the [Ophyd library](#developer.ophyd), the section [device configuration](#developer.ophyd_device_config) explains how to configure devices in BEC and how to load new configs. 

A dedicated section on the [BEC simulation framework](#developer.bec_sim) explains how to simulate devices in BEC, either for testing or for development purposes. Finally, a section on [external sources](#developer.external_sources) explains how to deal with external data sources in BEC. 

```{note}
Before you start, make sure you have familiarized yourself with the [user interfaces for managing devices in BEC](#user.devices).
```

```{toctree}
---
maxdepth: 2
hidden: true
---
ophyd/
device_configuration/
bec_sim/
external_sources/
```