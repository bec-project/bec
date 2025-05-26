(developer.ophyd_device_config)=
## Ophyd device configuration
BEC creates representative devices and signals dynamically on the devices server, following the specifications given in the device configuration. 
The device configuration can be loaded from and stored to a yaml file and contains all necessary information about the devices. 

An example of an ophyd device based on EPICS is a single PV, e.g. the synchrotron's ring current: 

```yaml
curr:
  readoutPriority: baseline
  description: SLS ring current
  deviceClass: ophyd.EpicsSignalRO
  deviceConfig:
    auto_monitor: true
    read_pv: ARIDI-PCT:CURRENT
  deviceTags:
    - cSAXS
  onFailure: buffer
  enabled: true
  readOnly: true
  softwareTrigger: false
```

More examples of device configurations can be found in the [Ophyd devices repository](https://github.com/bec-project/ophyd_devices/tree/main/ophyd_devices/configs).

The following sections explain the different parts of the device configuration in more detail.

* **deviceClass** \
The device class specifies the type of the device. In the example above, the device class is `EpicsSignalRO`, which is a read-only signal based on EPICS. Another example is `EpicsMotor` for motors based on EPICS. For a full list of available device classes, please refer to the [Ophyd documentation](https://nsls-ii.github.io/ophyd/architecture.html#device-classes) and the [Ophyd devices repository](https://github.com/bec-project/ophyd_devices/).

* **deviceConfig** \
The device config contains the configuration of the device. In the example above, the device config contains the read PV (`read_pv`). The read PV is the PV that is read out by the device. In this case, the read PV is `ARIDI-PCT:CURRENT`. The device config can contain any configuration parameter that is supported by the device class. 
The device is constructed by passing the device config to the device class. In the example above, the device is constructed by calling `EpicsSignalRO(name='curr', read_pv='ARIDI-PCT:CURRENT', auto_monitor=True)`.

* **readoutPriority** \
The readout priority specifies the priority with which the device is read out. For BEC controlled readouts, set the readout priority either to `on_request`, `baseline` or `monitored`. The "on_request" priority is used for devices that should not be read out during the scan, yet are configured to be read out manually. The baseline priority is used for devices that are read out at the beginning of the scan and whose value does not change during the scan. The monitored priority is used for devices that are read out during the scan and whose value may change during the scan. If the readout of the device is asynchronous to the monitored devices, set the readout priority to `async`. For devices that are read out continuously, set the readout priority to `continuous`. 

* **enabled** \
The enabled status specifies whether the device is enabled. 

* **readOnly** \
The read only indicates if the device is read-only. When set to true, writing to the device is disabled. It's optional in the device configuration and defaults to false.

* **softwareTrigger** \
The software trigger determines if BEC should explicitly invoke the device's trigger method during a scan. It's an optional parameter in the device configuration, defaulting to false

* **deviceTags** \
The device tags contain the tags of the device. The tags are used to group devices and to filter devices.

* **onFailure** \
The on failure parameter specifies the behavior of the device in case of a failure. It can be either `buffer`, `retry` or `raise`. If an error occurs and the on failure parameter is set to `buffer`, the device readout will fall back to the last value in Redis. If the on failure parameter is set to `retry`, the device readout will retry to read the device and raises an error if it fails again. If the on failure parameter is set to `raise`, the device readout will raise an error immediately.

* **description** \
The description contains the description of the device. It is used to provide additional information about the device.

## Combining config files
The device configuration can be split into multiple files. This can be useful to group devices by their functionality or to split the configuration into smaller files for better maintainability. To combine multiple device configuration files, use the `!include` tag in the device configuration.  The paths can be either relative or absolute. Please note that the `!include` tag cannot be placed at the root level of the device configuration and must be within a dictionary, e.g.:

```yaml
base_config:
  - !include ./path/to/base_config.yaml

endstation:
  - !include ./path/to/endstation_config.yaml

curr:
  readoutPriority: baseline
  description: SLS ring current
  deviceClass: ophyd.EpicsSignalRO
  deviceConfig:
    auto_monitor: true
    read_pv: ARIDI-PCT:CURRENT
  deviceTags:
    - cSAXS
  onFailure: buffer
  enabled: true
  readOnly: true
  softwareTrigger: false
```

In the example above, the `base_config.yaml` and `endstation_config.yaml` files are included in the device configuration. The `curr` device is defined directly in the device configuration. Alternatively, the `base_config.yaml` and `endstation_config.yaml` files can bec combined into a single tag:
  
```yaml
external_config:
  - !include ./path/to/base_config.yaml
  - !include ./path/to/endstation_config.yaml
```

For a single file, the `!include` tag can also be merged into a single line:

```yaml
base_config: !include ./path/to/base_config.yaml
```


(developer.ophyd.config_validation)=
## Validation of the device config
To avoid errors during loading of the device config, the device config should be validated before loading it. This can be done by installing the `ophyd_devices` package and running the following command:

```bash
ophyd_test --config ./path/to/my/config/file.yaml
```

This will perform a static validation of the device config and will print any errors that are found. For checking if the devices can be created and connect successfully, an additional flag can be passed:

```bash
ophyd_test --config ./path/to/my/config/file.yaml --connect
``` 

