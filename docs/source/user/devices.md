(user.devices)=
# Devices
BEC becomes truly useful only when devices are configured.
To inform BEC about your devices, you need a **device configuration file** (YAML).

If you already have a list of devices and corresponding configuration, you may skip ahead to loading or updating an existing config.

```{note}
Device configuration files use YAML. If you are unfamiliar with YAML, please refer to official
[yaml documentation](https://yaml.org/).
```

To get started quickly, BEC provides a **demo configuration** containing simulated devices.

## Load the Demo Device Configuration

Load the default simulated device configuration in the CLI:

```python
bec.config.load_demo_config()
```

The configuration is stored on the running BEC server and remains available even after restarting the client or server.


## Export the Current Device Configuration

To save the current session to disk, use

```{code-block} python
bec.config.save_current_session("./config_saved.yaml")
```
This creates a file `config_saved.yaml` in your working directory.

You may now edit this file (e.g., to add a new device).
Here is an example entry:

``` {code-block} yaml
---
name: user.devices.add_epics_signal
---

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

See also:

* **[Ophyd](#developer.ophyd)**
* **[Ophyd Device Configuration](#developer.ophyd_device_config)**
* **[Simulation Framework](#developer.bec_sim)**

## Upload an Updated Device Configuration

Once you have modified your YAML file, upload it using:

```python
bec.config.update_session_with_file("<my-config.yaml>")
```

For example:

```python
bec.config.update_session_with_file("config_saved.yaml")
```

You have now exported, edited, and re-imported the device configuration.

## Viewing Loaded Devices

BEC provides a **device container** named `dev` that gives convenient, tab-completable access to all loaded devices in the current session. The device container is automatically populated when loading a device configuration and is always kept in sync with the current session.

### Discover devices with tab completion

You can explore available devices simply by typing:

```python
dev.<TAB>
```

This will show all device names currently known to the session, making it easy to discover motors, detectors, sensors, or other components without needing to check configuration files.

### Show All Loaded Devices

To view a complete overview of every loaded device, including their device configuration, use:

```python
dev.show_all()
```

This prints a clean, formatted table listing all devices registered in the session.


## Updating Device Settings

You can update individual device settings directly from the CLI. 

```{important}
Runtime changes are not persistent on disk and may be overwritten when reloading the device configuration. To make permanent changes, export the current session to a YAML file, edit it, and re-import it as shown above.
```

### Enable or Disable a Device

```python
dev.samx.enabled = False
```

This disables the device across all clients and on the BEC server.

### Change the Readout Priority

```python
dev.samx.readout_priority = "monitored"
```

Available modes:

* `monitored` — read at every scan point
* `baseline` — read once before a scan
* `on_request` — read only when requested
* `async` — read asynchronously in the background

More details on the readout priority and the different modes can be found in the [developer guide](#developer.ophyd_device_config).

(user.devices.update_device_config)=
### Update the device config

To update the device config, you can set the device's signals, for example to change the velocity of an EPICS motor:

```{code-block}  python
dev.samx.velocity.put(5.0)
```

### Set or update the user parameters

To set the device's user parameters (such as in/out positions), use

```{code-block}  python
dev.samx.set_user_parameter({"in": 2.6, "out": 0.2})
```

If instead you only want to update the user parameters, use

```{code-block} python
dev.samx.update_user_parameter({"in":2.8})
```

```{hint}
The user parameters can be seen as a python dictionary. Therefore, the above commands are equivalent to updating a python dictionary using

```python
user_parameter = {"in": 2.6, "out": 0.2}    # equivalent to set_user_parameter
print(f"Set user parameter: {user_parameter}")


user_parameter.update({"in": 2.8})          # equivalent to update_user_parameter
print(f"Updated user parameter: {user_parameter}")
```

This will result in the output:

``` 
Set user parameter: {'in': 2.6, 'out': 0.2}
Updated user parameter: {'in': 2.8, 'out': 0.2}
```