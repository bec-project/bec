(developer.scans.scan_structure)=
# Scan Structure
A scan in BEC is a Python class that inherits from the [`ScanBase`](/api_reference/_autosummary/bec_server.scan_server.scans.ScanBase.rst#bec_server.scan_server.scans.ScanBase) class and implements methods that should be executed in a specific order.

The order of execution is defined by the [`run`](/api_reference/_autosummary/bec_server.scan_server.scans.ScanBase.rst#bec_server.scan_server.scans.ScanBase.run) method, which is called by the scan server. By default, the `run` method calls the following methods in order:

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: ScanBase.run
:dedent: 4
```

The `run` method is a generator function that, like most other scan methods, yields control to the scan server after each method call. This allows the scan server to handle asynchronous operations, such as moving motors or waiting for certain events. The scan server will call the next method in the scan after the current method has completed. All methods that set or retrieve data from devices must be implemented as generator functions.

```{seealso}
If you want to learn more about generator functions, we recommend to go through the tutorial on [generators](https://realpython.com/introduction-to-python-generators/) on Real Python. A brief introduction can also be found [here](https://wiki.python.org/moin/Generators). For a more detailed explanation, you can read the [official Python documentation](https://docs.python.org/3/reference/expressions.html#generator-iterator-methods). 
```

````{dropdown} View code: ScanBase class
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: ScanBase
```
````

## Basic scan structure
The following structure is targeted at step scans, i.e. scans where the scan server is in control of the scan and the overall progress is determined by the number of steps within the scan. Modifications of the structure as needed for fly scans are described later on.

### Preparation for the scan
After reading out the current scan motor positions with `read_scan_motors`, the scan server will call the `prepare_positions` method to prepare the scan positions. This method should calculate two values: the number of points in the scan (`self.num_pos`) and the positions of the scan motors (`self.positions`). The `num_pos` attribute must be of type `int` while the `positions` attribute must be of type `np.ndarray` with the shape `n x m`, where `n` is the number of points in the scan and `m` is the number of scan motors. The method should also ensure that the calculated positions are within soft limits of the devices. This can be achieved, for example, by calling the `_check_limits` method. 

The default implementation of the `prepare_positions` method in the `ScanBase` class is as follows:

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: ScanBase.prepare_positions
:dedent: 4
``` 

In addition to simply calculating the positions, the default implementation also respects the user's request to perform the scan relative to the current position or absolute (`relative=True` or `relative=False`) by adding the position offset using `_set_position_offset`.


`````{note}
New scans that only require a new way of calculating the positions can simply override the `_calculate_positions` method as it is done e.g. in the `FermatSpiralScan` class.
````{dropdown} View code: FermatSpiralScan class
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: FermatSpiralScan
```
````
`````

The `scan_report_instructions` method is then called to update the instructions for user interfaces. In particular, there are three options for the developer to choose from: 
1. scan progress: Useful for step scans where the scan server is in control of the scan and the overall progress is determined by the number of steps within the scan. As this is the default option, BEC will automatically update scan report instruction if the method is doesn't yield anything.
2. device progress: Useful for fly scans, in particular async fly scans, where the scan server does not trigger every step and thus cannot determine the overall progress. Instead, the progress is determined by a specified device, e.g. a detector's total number of acquired frames or the number of triggers sent out by a controller.
3. readback: Useful for basic move commands.  


### Starting the scan
The scan server will then call the `open_scan` method to open the scan, followed by the `stage` method to stage all devices (see also: [ophyd devices](#developer.ophyd_devices.device)). 
Once all devices are staged and thus ready for the upcoming scan, a baseline reading is triggered. This will read out all devices that are on `readout_priority="baseline"` and are currently enabled (see also: [ophyd device configuration](#developer.ophyd_device_config)).

It is sometimes necessary to perform additional operations before the core of the scan is executed. In BEC, these operations can be implemented in the `pre_scan` method:

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: ScanBase.pre_scan
:dedent: 4
```

If the class attribute `pre_move` is set to `True`, the default `pre_scan` method will move the scan motors to the first position before the scan core is executed and afterwards call the `pre_scan` method of all devices that have a `pre_scan` method implemented.

Now that all motors are ready and in position, we are finally ready to start the core of the scan. This is done by calling the `scan_core` method. Its default implementation is quite simple:

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: ScanBase.scan_core
:dedent: 4
```

For each position in the scan, the method `_at_each_point` is called, providing the current index and a list of positions. The default implementation of `_at_each_point` performs the following actions:

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: ScanBase._at_each_point
:dedent: 4
```

1. Move the scan motors to the target position and wait for the motors to finish moving.
1. Let the device settle for a specified time (default: `settling_time=0`).
1. Send out a trigger and wait at least for the specified time (`min_wait`).
1. Read out all devices that are on `readout_priority="monitored"` and are currently enabled and assign the readout to the `point_id`.
1. Increase the `poind_id` by one.

```{important}
The `point_id` is an identifier for the current point in the scan. It is used later on to bundle and correlate device readings. It is crucial that the `point_id` is increased by one for each point in the scan as as seen in the default implementation of the `_at_each_point` method.
``` 


### Finalizing the scan and cleaning up
Once the core of the scan is finished, `finalize`, `unstage` and `cleanup` are called in this order. The `finalize` method is used to perform additional operations such as 
1. returning the scan motors to the start position,
1. waiting for the last readout of the devices to finish and 
1. waiting for all async devices to finish their operations by calling their `complete` method.

The `unstage` method is used to unstage all devices. Afterwards, no further operations should be performed on the devices. 

Finally, the `cleanup` method closes the scan and can be further extended to perform additional operations after the scan is finished. 


## Scan class configuration
The scan class can be further configured by setting class attributes. The following class attributes are available:

- `scan_name` (required): The name of the scan. This name is used to identify the scan in the user interface.
- `scan_type` (required): The type of the scan. This can be either `step` or `fly`.
- `arg_input` (optional): Sometimes, scans accept `*args` to support different types of scans. For example, a line scan can accept any number of motors to scan but each motor must define a start and stop position. The `arg_input` attribute is a dictionary that defines the type of the arguments that the scan accepts. The keys are the argument names and the values are the types of the arguments. The types are defined in the `ScanArgType` class.
- `arg_bundle_size` (optional): A dictionary that defines the bundle size of the input arguments passed in through `*args`. The dictionary must contain the key `bundle` with the number of arguments that are bundled together and the key `min` with the minimum number of arguments that are required. Set the value of `max` to `None` if there is no maximum number of arguments.
- `required_kwargs` (optional): A list of required keyword arguments that must be passed to the scan.
- `return_to_start_after_abort` (optional): If set to `True`, the scan server will return the scan motors to the start position if the scan is aborted. Default is `True`.
- `pre_move` (optional): If set to `True`, the scan server will move the scan motors to the first position before the scan core is executed. Default is `True`.

The following example shows the configuration of the line scan:

````{dropdown} View code: LineScan class
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: LineScan
```
````

## Scan Structure for a Fly Scan
Fly scans are scans where the scan server does not trigger every step and thus cannot determine the overall progress. Instead, the progress is determined by a specified device, e.g. a detector's total number of acquired frames or the number of triggers sent out by a controller.
BEC distinguishes between two types of fly scans: synchronous and asynchronous fly scans. In a synchronous fly scan, the readout of the flyer is synchronized with the readout of the monitored devices. In an asynchronous fly scan, the readout of the flyer is not synchronized with the readout of the monitored devices.

Both types of fly scans provide dedicated base classes that can and should be used as a starting point for new fly scans. The base classes are `SyncFlyScanBase` and `AsyncFlyScanBase`. 
`SyncFlyScanBase` unsets the `_calculate_positions`, `read_scan_motors`, `prepare_positions` and `scan_core` methods as they must be implemented differently for fly scans and are highly dependent on the specific requirements of the scan. In addition, the `SyncFlyScanBase` adds the `monitor_sync` property that must be implemented by the developer. This property should return the name of the device that is used to synchronize the scan.

````{dropdown} View code: SyncFlyScanBase class
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: SyncFlyScanBase
```
````

`AsyncFlyScansBase` inherits from `SyncFlyScansBase` and set the `monitor_sync` property to "bec" as the monitored devices should be synced by BEC itself and not by an external device.

````{dropdown} View code: AsyncFlyScanBase class
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../bec_server/bec_server/scan_server/scans.py
:language: python
:pyobject: AsyncFlyScanBase
```
````