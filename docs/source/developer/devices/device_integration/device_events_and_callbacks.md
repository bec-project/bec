(developer.devices.device_integration.device_events_and_callbacks)=
# Device Events and Callbacks

Explain here in more detail `_run_subs`.




## Device Server Callbacks
The device server provides a set of callbacks which can be attached to the SUB_EVENTS of the devices and signals. Below is a list of the available callbacks and their use cases. For more information on the syntax of the callbacks, please check the referenced code snippets.

* **readback**:
Callback to trigger a `.read()` of all signals of kind `kind.normal` and `kind.hinted`.
    ````{dropdown} View code: callback readback
    :icon: code-square
    :animate: fade-in-slide-down

    ```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
    :language: python
    :pyobject: DeviceManagerDS._obj_callback_readback
    ```
    ````

* **value**:
This callback is the same as the `readback` callback, it is typically the default SUB event for a signal.
    ````{dropdown} View code: callback value
    :icon: code-square
    :animate: fade-in-slide-down

    ```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
    :language: python
    :pyobject: DeviceManagerDS._obj_callback_readback
    ```
    ````

* **done_moving**:
Callback to indicate that a motor has finished moving. 
    ````{dropdown} View code: callback done moving
    :icon: code-square
    :animate: fade-in-slide-down

    ```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
    :language: python
    :pyobject: DeviceManagerDS._obj_callback_done_moving
    ```
    ````

* **motor_is_moving**:
Callback to indicate that a motor is moving. 
    ````{dropdown} View code: callback is moving
    :icon: code-square
    :animate: fade-in-slide-down

    ```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
    :language: python
    :pyobject: DeviceManagerDS._obj_callback_is_moving
    ```
    ````

* **progress**:
Callback to indicate the progress of a device. During a fly scan, this can be a flyer that is kicked off and informs BEC about the progress of a scan.
    ````{dropdown} View code: callback motor moving
    :icon: code-square
    :animate: fade-in-slide-down

    ```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
    :language: python
    :pyobject: DeviceManagerDS._obj_progress_callback
    ```
    ````

* **file_event**:
Callback to publish file events to the BEC server. This can be used for devices who have secondary writing processes attached, i.e. large 2D detectors.
The file writer is subscribed to these events which allows it to create an external link to the file. 
    ````{dropdown} View code: callback file event
    :icon: code-square
    :animate: fade-in-slide-down

    ```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
    :language: python
    :pyobject: DeviceManagerDS._obj_callback_file_event
    ```
    ````

* **device_monitor_1d**:
Callback to handle preview data from a 1D detector. Data send through this callback will not be stored, it is only used for preview purposes.
    ````{dropdown} View code: callback 2d data
    :icon: code-square
    :animate: fade-in-slide-down

    ```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
    :language: python
    :pyobject: DeviceManagerDS._obj_callback_device_monitor_1d
    ```
    ````

* **device_monitor_2d**:
Callback to handle preview data from a 2D detector. Data send through this callback will not be stored, it is only used for preview purposes.
    ````{dropdown} View code: callback 1d data
    :icon: code-square
    :animate: fade-in-slide-down

    ```{literalinclude} ../../../../../bec_server/bec_server/device_server/devices/devicemanager.py
    :language: python
    :pyobject: DeviceManagerDS._obj_callback_device_monitor_2d
    ```
    ````