(developer.devices.device_integration.overview)=
# Beamline Specific Device Integration
Device integration is essential to ensuring the reliable operation of BEC. A significant challenge is integrating devices from different beamlines, each with unique requirements. The same device may be used across beamlines but often requires specific configurations to operate correctly for each beamline. 

- Read about [best practices](#developer.devices.device_integration.best_practices) for beamline specific device integration.
- Follow the **step-by-step** [tutorial](#developer.devices.device_integration.tutorial) to integrate a new device, such as a detector, into BEC. This will guide you through the process including examples for publishing events, such as file events or preview images, to BEC.
- Learn about **testing** as a critical component of reliable and maintainable device integration, with examples of automated testing for EPICS-based devices in the [automated testing](#developer.devices.device_integration.automated_testing) section. 


```{toctree}
---
maxdepth: 1
hidden: true
---
best_practices/
tutorial/
automated_testing/
```
