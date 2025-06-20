(developer.plugin_manager)=

# Plugin management tool

BEC includes a tool (`bec-plugin-manager`) to manage the plugins in the installed plugin repository. To use it, a plugin
repository must be installed in the local python environment, and no more than one plugin repository can be installed at
any one time.

```{typer} bec_lib.utils.plugin_manager.main:_app
    :prog: bec-plugin-manager
    :width: 80
    :preferred: svg
    :theme: dimmed_monokai
```

## Creating new plugins

This tool can be used to create new plugins. Currently, you can use it to create new widgets; scans and devices are
coming soon!

```{typer} bec_lib.utils.plugin_manager.main:_app:create
    :prog: bec-plugin-manager create
    :width: 80
    :preferred: svg
    :theme: dimmed_monokai
```

`````{tab-set}
````{tab-item} Widgets

You can use `bec-plugin-manager create widget` to create a widget:

```{typer}  bec_lib.utils.plugin_manager.create._app:widget
    :prog: bec-plugin-manager create widget
    :width: 80
    :preferred: svg
    :theme: dimmed_monokai
```

This widget will be added to the plugin repository with template files, after which it can be managed using the same interface.

````
````{tab-item} Scans
```{typer} bec_lib.utils.plugin_manager.create._app:scan
    :prog: bec-plugin-manager create scan
    :width: 80
    :preferred: svg
    :theme: dimmed_monokai
```
````
````{tab-item} Devices
```{typer} bec_lib.utils.plugin_manager.create._app:device
    :prog: bec-plugin-manager create device
    :width: 80
    :preferred: svg
    :theme: dimmed_monokai
```
````
`````
