(developer.devices.device_integration.overview)=
# Device Integration Tutorial
Device integration is essential to ensuring the reliable operation of BEC. A significant challenge is integrating devices from different beamlines, each with unique requirements. The same device may be used across beamlines but often requires specific configurations to operate correctly for each beamline. 

- Read about BEC device integration [best practices](#developer.devices.device_integration.best_practices) 
- Follow the **step-by-step** [tutorial](#developer.devices.device_integration.tutorial) to integrate a new device, such as a detector, into BEC
    - this will guide you through the process
    - it also includes examples for streaming data from [external data sources](#developer.devices.device_integration.external_data_sources), such as a DAQ backend
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
