(developer)=

# Developer

Welcome to the developer/expert section of the BEC documentation! This section serves as a comprehensive resource for developers looking to delve into the underlying architecture of BEC (Beamline Experiment Control). Whether you are customizing BEC for specific beamlines or setups, exploring BEC plugins, integrating devices at the beamline, or actively participating in the development of core services, this documentation is tailored to meet your needs.

```{toctree}
---
maxdepth: 2
hidden: true
---

getting_started/getting_started.md
plugin_manager/plugin_manager.md
devices/devices.md
user_interfaces/user_interfaces.md
data_access/data_access.md
scans/scans.md
file_writer/file_writer.md

glossary/
```

---

````{grid} 2
:gutter: 5

```{grid-item-card}
:link: developer.getting_started
:link-type: ref
:img-top: /assets/rocket_launch_48dp.svg
:text-align: center

## Getting Started

Learn about BEC's architecture, contribute to the project, set up your developer environment, and develop new features for BEC using plugins.
```

```{grid-item-card}
:link: developer.devices
:link-type: ref
:img-top: /assets/precision_manufacturing_48dp.svg
:text-align: center

## Devices

No matter if you need to add new devices to BEC or modify existing ones, this section provides information on how to configure and use devices in BEC.

```

```{grid-item-card}
:link: developer.user_interfaces
:link-type: ref
:img-top: /assets/portrait_48dp.svg
:text-align: center

## User Interfaces

Discover and learn how to contribute to the command-line tool and graphical user interface for interacting with BEC.

```

```{grid-item-card}
:link: developer.data_access
:link-type: ref
:img-top: /assets/rocket_launch_48dp.svg
:text-align: center

## Data Access

Discover how to get access to data in BEC. This ranges from an introduction into BEC's messaging system and event data subscriptions to the structure of a *ScanItems*.
```

```{grid-item-card}
:link: developer.scans
:link-type: ref
:img-top: /assets/timeline_48dp.svg
:text-align: center

## Scans

Understand the basic structure of a scan in BEC and learn how to create a scan plugin to extend BEC's functionality and furthe tailor it to your needs.

```

```{grid-item-card}
:link: developer.file_writer
:link-type: ref
:img-top: /assets/source_48dp.svg
:text-align: center

## File Writer

Explore and understand BEC's file writer and how it can be configured.
```

```{grid-item-card}
:link: developer.plugin_manager
:link-type: ref
:img-top: /assets/source_48dp.svg
:text-align: center

## Plugin manager

Explore and understand BEC's plugin management tool (`bec-plugin-manager`) which can help you to create plugin scans, devices, and widgets in a plugin repository.
```

```{grid-item-card}
:link: developer.glossary
:link-type: ref
:img-top: /assets/toc_48dp.svg
:text-align: center

## Glossary

Refer to a glossary of terms used throughout the BEC documentation.
```
````
