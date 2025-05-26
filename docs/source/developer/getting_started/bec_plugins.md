(developer.bec_plugins)=
# BEC Plugins
BEC plugins are a way to extend the functionality of BEC. They are written in Python and can be used to add new features to BEC or to modify existing ones. This enables beamlines to customize BEC to their needs without having to modify the core code. Plugins can be used for various purposes but the most common ones are:
* Adding new scan types
* Adding new device types
* Adding additional services, e.g. for data analysis
* Customizing the BEC CLI startup procedure (e.g. to load additional modules)
* Customizing the file structure

Plugins are commonly provided to BEC by installing them as a Python package. Clients and BEC services can then load the specific plugins they need.

## Plugin Structure

The following sections describe the structure of a BEC plugin. As plugins typically live on Github/Gitlab/Gitea, we will use the following example structure of a "beamline_XX_bec" repository to explain the different parts of BEC plugins. Instead of creating the structure manually, you can also use the script located in BEC library to create the structure for you.
```bash
python ./<path_to_bec>/bec/bec_lib/util_scripts/create_plugin_structure.py <path_to_new_plugin>
```

```
beamline_XX_bec/
├── beamline_XX_bec/
│   ├── bec_ipython_client/
│   │   ├── high_level_interface/
│   │   │   ├── __init__.py
│   │   │   └── custom_hli.py
│   │   ├── plugins/
│   │   │   ├── BeamlineXX/
│   │   │   │   ├── __init__.py
│   │   │   │   └── custom_XX_class.py
│   │   │   └── __init__.py
│   │   ├── startup/
│   │   │   ├── __init__.py
│   │   │   ├── post_startup.py
│   │   │   └── pre_startup.py
│   │   └── __init__.py
│   ├── bec_widgets/
│   │   └── __init__.py
│   ├── dap_services/
│   │   ├── __init__.py
│   │   └── custom_dap.py
│   ├── deployments/
│   │   ├── __init__.py
│   │   └── device_server/
│   │       ├── __init__.py
│   │       └── startup.py
│   ├── device_configs/
│   │   ├── __init__.py
│   │   └── tomography_config.yaml
│   ├── devices/
│   │   ├── __init__.py
│   │   └── custom_XX_device.py
│   ├── file_writer/
│   │   └── __init__.py
│   └── scans/
│       ├── custom_scan.py
│       └── __init__.py
├── bin/
│   └── helper_script.sh
├── tests
└── pyproject.toml
```
<!-- done with https://tree.nathanfriend.io  -->
<!--
beamline_XX_bec
  beamline_XX_bec
    bec_ipython_client
      high_level_interface
        __init__.py
        custom_hli.py
      plugins
        BeamlineXX
          __init__.py
          custom_XX_class.py
        __init__.py
      startup
        __init__.py
        post_startup.py
        pre_startup.py
      __init__.py
    bec_widgets
      __init__.py
    dap_services
      __init__.py
      custom_dap.py
    deployments
      __init__.py
      device_server
        __init__.py
        startup.py
    device_configs
      __init__.py
      tomography_config.yaml
    devices
      __init__.py
      custom_XX_device.py
    file_writer
      __init__.py
    scans
      custom_scan.py
      __init__.py
  bin
    helper_script.sh
  tests
  pyproject.toml
   -->

