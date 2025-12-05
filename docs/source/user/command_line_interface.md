(user.command_line_interface)=
## Command-Line Interface (CLI)

In the previous sections, you have succesfully started BEC and also already interacted with the CLI to update the BEC device configuration. 
This section aims to explore the CLI capabilities further.

### Start-up
The CLI can be started from a terminal after activating the [previously installed bec_venv](#user.installation) using the shell command within the directory where ``bec_venv`` is installed.
```{code-block} bash
source ./bec_venv/bin/activate
```
```{code-block} bash
bec
```

### Client interface
The CLI is based on the [IPython](https://ipython.org/) interactive shell. 
As seen in the screenshot below, the prompt is prefixed with, e.g. ``demo [4/522] >>``. 
The prefix contains the name of the current session (*demo*), the current cell number (*4*) and the next scan number (*522*).

### Device access
Devices are grouped in ``dev``. 
This allows users to use tab-completion for finding devices.

```{image} ../assets/tab-complete-devices.png
:align: center
:alt: tab completion for finding devices
:width: 300
```

```{hint}
``dev`` is imported as a builtin. As a result, you can access ``dev`` from everywhere. ``dev`` itself is just an alias for ``bec.device_manager.devices``.
```

To get a quick glance at all available devices, you can type
    
```ipython
demo [1/3] ❯❯ dev.show_all()
```

If you want to the current position, setpoint and limits of some device, you can use the ``wm`` command, e.g.
    
```{image} ../assets/wm-devices.png
:align: center
:alt: tab completion for finding devices
:width: 400
```

The ``wm`` command can receive multiple devices as strings, e.g. ``dev.wm(["samx, "samy"])`` or  a single device, e.g. ``dev.wm("samx")``. It also supports regular expressions, e.g. ``dev.wm("sam*")``. Instead of strings, you can also pass the device objects directly, e.g. ``dev.wm([dev.samx, dev.samy])``.


### Inspect a device

To inspect the device samx, you can simply type ``dev.samx`` and you'll get a printout of the relevant information about this device.
``` ipython
demo [1/3] ❯❯ dev.samx
Out[1]:
Positioner(name=samx, enabled=True):
--------------------
Details:
	Description: samx
	Status: enabled
	Read only: False
	Last recorded value: {'samx': {'value': -0.0011717217935431634, 'timestamp': 1702306192.450343}, 'samx_setpoint': {'value': 0, 'timestamp': 1702306192.382011}, 'samx_motor_is_moving': {'value': 0, 'timestamp': 1702306192.450175}}
	Device class: SimPositioner
	readoutPriority: baseline
	Device tags: ['user motors']
	User parameter: None
--------------------
Config:
	delay: 1
	limits: [-50, 50]
	speed: 100
	tolerance: 0.01
	update_frequency: 400
```

If you just want to see the current position, setpoint and limits of a device, you can simply type
    
```ipython
demo [1/3] ❯❯ dev.samx.wm
```

#### Read interface

While the device inspection as seen above is an easy way to quickly glance at the current state of a device, it cannot be used programmatically, i.e. within a script. 
For reading from a device, we provide two interfaces: `.read` and `.get`.
Devices are composed of signals, each of their own `kind` with possible values `hinted/normal/config/omitted`. 
It is the `kind` attribute that determines what signals are read out by using e.g. `dev.samx.read()`.
For more details on `device`, `signal` and `kind`, refer to [ophyd](#developer.ophyd).

To read out `hinted` and `normal` signals from a device, use

```ipython
demo [1/50] ❯❯ dev.samx.read()
Out[1]:
{'samx': {'value': 0, 'timestamp': 1701942802.6418009},
'samx_setpoint': {'value': 0, 'timestamp': 1701942802.641365},
'samx_motor_is_moving': {'value': 0, 'timestamp': 1701942802.641365}}
```

and

```ipython
demo [4/50] ❯❯ dev.samx.read_configuration()
Out[4]:
{'samx_velocity': {'value': 1, 'timestamp': 1701942802.641421},
'samx_acceleration': {'value': 1, 'timestamp': 1701942802.641428}}
```

to read the `config` signals.
In both cases, a nested dictionary is returned with value/timestamp pairs for each signal. 
The current position of `samx` is accessed `dev.samx.read()['samx']['value']`.

```{note}
The default behaviour of `.read` and `.read_configuration` is to read the last recorded value directly from the device, i.e. `cached=False`.
However, depending on the device and the underlying hardware, frequent polling may be detrimental or even put the device in an unresponsive state. For this reason, cached can be set to `True` to read the last recorded value from redis. Especially for devices that have EPICS auto-monitor enabled, this is much faster and more efficient without missing any updates.
Signals of type `omitted` are currently not stored in redis, nor are they read from the device using e.g.`dev.samx.read()` and therefore must
be read out directly using e.g. `dev.samx.my_omitted_signal.read()`.
```

In addition, we can read for instance the readback or setpoint value from samx by

```ipython
demo [14/3] ❯❯ dev.samx.readback.read()
Out[14]: {'samx': {'value': -0.0011717217935431634, 'timestamp': 1702306192.450343}}
demo [17/3] ❯❯ dev.samx.setpoint.read()
Out[17]: {'samx_setpoint': {'value': 0, 'timestamp': 1702306192.382011}}
demo [18/3] ❯❯ dev.samx.velocity.read()
Out[18]: {'samx_velocity': {'value': 1, 'timestamp': 1702306158.257976}}
```

which again returns a nested dictionary, however, this time only for the requested signal. 

```{note}
The keys in the returned dictionary are composed of `<devicename>_<signalname>`. 
However, for positioners the signal name <readback> is typically ommited, i,e. see `dev.samx.readback.read()`.
```

#### Get interface

We also provide a more convenient access pattern to values of the devices. 
Similar to `.read()` you may call

``` ipython
demo [20/3] ❯❯ dev.samx.readback.get()
Out[20]: -0.0011717217935431634
```

which will return the value of the readback directly.
You can also retrieve all signals from `samx` via `get`. 

```ipython
demo [13/50] ❯❯ signals = dev.samx.get()
demo [14/50] ❯❯ signals
Out[14]: samx(readback=0, setpoint=0, motor_is_moving=0, velocity=1, acceleration=1, high_limit_travel=50, low_limit_travel=-50, unused=1)
```

which includes all different `kind` of signals from the device.
The return object of `dev.samx.get()` is a [namedtuple](https://docs.python.org/3/library/collections.html) with an access pattern similar to class attributes/properties: `signals.readback`.

```{warning}
We recommend not using `dev.samx.get()` due to the fact that it forces a readback from all signals. 
```

### Move a motor

A very common operation in the beginning is to be able to move a device. 
For this, there are two variants of device movements: `updated move` and `move`.

#### Updated move (umv)

A umv command blocks the command-line until the motor arrives at the target position (or an error occurs).

```python
scans.umv(dev.samx, 5, relative=False)
```

#### Move (mv)

A mv command is non-blocking, i.e. it does not wait until the motor reaches the target position.

```python
scans.mv(dev.samx, 5, relative=False)
```

```{note}
Be aware of benefits and risks of executing a non-blocking command. A ``CTRL-C`` will not stop its motion, but it needs to be explicitly called via ``dev.samx.stop()`` ``%abort`` or ``%halt``.
```
However, it can be made a blocking call by

```python
scans.mv(dev.samx, 5, relative=False).wait()
```

The same mv command can also be executed by calling the device method `move`

```python
dev.samx.move(5, relative=False)
```

````{note}
mv and umv can receive multiple devices, e.g.
```python
scans.umv(dev.samx, 5, dev.samy, 10, relative=False)
```
````

#### Update motor limits

In order to move motors in a safe manner, you can add software limits to a motor. 
The following command, For example, changes the limits of `samx` to `-50 (low)` and `50 (high)`

``` ipython
dev.samx.limits = [-50, 50]
```

You may also directly access the low and high limits via `dev.samx.low_limit = -50` and `dev.samx.high_limit=50`.
Both access patterns are identical.
Software limits are updated in the device_config, however, when done via command-line this only updates the current device_config session in redis.
To make sure that limits are stored after reloading the device BEC config, you need to update the deviceConfig on disk, please check [bec.config.save_current_session()](#user.devices.export_device_config).

As per default, software limits for motors are set to the values specified in the [BEC device config](#developer.ophyd), subfield device_config. 

````{note}
If no software limits are specified, the motor will be initialized without software limits. 
This is equivalent to having identical values for high and low limits, e.g. 
```python
dev.samx.limits = [0, 0]
```
````

### Run a scan

All currently available scans are accessible through `scans.`, e.g.

```python
scans.line_scan(dev.samx, -5, 5, steps=50, exp_time=0.1, relative=False)
```
You may in addition, scan multiple axis simultaneously, e.g.
```python
scans.line_scan(dev.samx, -5, 5, dev.samy, -5, 5, steps=50, exp_time=0.1, relative=False)
```
which would be a diagonal trajectory in the xy plane, assuming that samx and samy are in an rectangular coordinate system.
There are also multiple ways plot and investigate the data, for this please explore [data access and plotting](#user.data_access_and_plotting). 
This also includes live plotting of data.

BEC has various different type of scans, for instance `scans.grid_scan`, `scans.list_scan`, which you can explore in the simulation. 

Default scans will provide a live-table view of the monitored signals in the command-line interface as well as a progress bar. To disable the live-table view, you can pass an additional argument `show_live_table=False` to the scan command.
To fully disable all live updates, use `hide_report=True`. 

#### Explore docstring documentation 
What can be very convenient while exploring built-in scans, is using the [Ipython syntax](https://ipython.readthedocs.io/en/stable/interactive/tutorial.html) `?` to print out all sort of useful information about an object, e.g. for `scans.list_scan` 

```ipython
demo [3/31] ❯❯ scans.list_scan?
Signature: scans.list_scan(*args, parameter: dict = None, **kwargs)
Docstring:
A scan following the positions specified in a list.
Please note that all lists must be of equal length.

Args:
    *args: pairs of motors and position lists
    relative: Start from an absolute or relative position
    burst: number of acquisition per point

Returns:
    ScanReport

Examples:
    >>> scans.list_scan(dev.motor1, [0,1,2,3,4], dev.motor2, [4,3,2,1,0], exp_time=0.1, relative=True)
File:      ~/work_psi_awi/bec_workspace/bec/bec_lib/bec_lib/scans.py
Type:      function
```
The shell printout provides information about the scan signature, parameters, as well as a syntax example at the bottom.

### How to write a macro
-----------------------

Macros are user defined functions that can be executed from the BEC console (CLI). 
They are stored in the ``macros`` folder and can be edited with any text editor. 
The macros are loaded automatically on startup of the BEC console but can also be reloaded by typing ``bec.macros.load_all_user_macros()`` in the command-line.
This command will load macros from three locations: 

1. from `~/bec/macros/` in your home directory,
1. from the path specified in the client config for your beamline, e.g. `bec/macros` in your raw data directory, and 
1. from the beamline plugin directory, e.g. `/csaxs_bec/csaxs_bec/macros/`


An example of a user macro could be a function to move a specific motor to a predefined position:

```python 
    def samx_in():
        umv(dev.samx, 0)
```

or 

```python 

    def close_shutter():
        print("Closing the shutter")
        umv(dev.shutter, 0)
```

A slightly more complex example could be a sequence of scans that are executed in a specific order:

```python

    def overnight_scan():
        open_shutter()
        samx_in()
        for i in range(10):
            scans.line_scan(dev.samy, 0, 10, steps=100, exp_time=1, relative=False)
        samx_out()
        close_shutter()
```

This macro can be executed by typing ``overnight_scan()`` in the BEC console and would execute the following sequence of commands:

1. Open the shutter
2. Move the sample in
3. Perform 10 line scans on the sample
4. Move the sample out
5. Close the shutter

Macros can be very useful to automate repetitive tasks or to create complex sequences of commands that can be executed with a single command.

```{important}
As macros are loaded automatically, they should not contain any command outside of a function definition to avoid executing code unintentionally.
```

### Create a custom scan

As seen above, scans can be access through `scans.`. 
However, sometimes it is necessary to run a sequence of functions as if it were a scan. 
For example, we might want to run a grid scan (2D scan) with our sample motor stages but move the sample position in z after each 2D scan. 
Normally, this would create multiple output files that one would need to merge together later. 

This is where the scan definition comes in. 
It allows us to run a sequence of functions as if it were a scan, resulting in a single `scan_number`, a single `scan_id` and a single output file. 

```python

    @scans.scan_def
    def custom_grid_scan():
        open_shutter()
        umv(dev.samz, 0) # move to samz to start position (absolut)
        for i in range(10):
            scans.grid_scan(dev.samx, 0, 10, 10, dev.samy, 0, 10, 10, exp_time=0.1, relative=False)
            umvr(dev.samz, 0.1) # move samz + 0.1mm after each grid scan
        close_shutter()
```

By adding the decorator ``@scans.scan_def`` to the function definition, we mark this function as a scan definition. 

### Computed Signal
Here, we introduce the `ComputedSignal`, which enables users to effortlessly generate custom signals based on signals from other devices.

To utilize this feature, add a new signal, such as `pseudo_signal`, to the device configuration

``` yaml
pseudo_signal:
  deviceClass: ophyd_devices.ComputedSignal
  deviceConfig:
    compute_method: "def compute_signals(signal1, signal2):\n    return signal1.get()*signal2.get()\n"
    input_signals: 
      - "bpm4i_readback"
      - "bpm5i_readback"
  enabled: true
  readOnly: false
  readoutPriority: baseline
```
The `pseudo_signal` is a `ComputedSignal` where the *readback* is calculated based on the configured *input_signals* and *compute_method*. In the provided example, the *readback* of `bpm4i` and `bpm5i` is multiplied to produce the `readback` of the `pseudo_signal`.

Additionally, we offer users a straightforward interface through the client (CLI) to adjust the *compute_method* and *input_signals*. The process involves two steps:

1. Define the *input_signals* using `dev.<device_name>.set_input_signal`.
2. Upload a method for the *compute_method* via `dev.<device_name>.set_compute_method`.

It's worth noting that users have the option to leverage additional packages such as `numpy as np` and `scipy as sp` for accelerated computations.

Below is an example demonstrating the use of the `pseudo_signal` to compute the sum over a 2D detector (`dev.eiger`), excluding hot pixel values:

``` ipython
def calculate_readback(signal):
    data = signal.get()
    std = np.std(data)
    mean = np.mean(data)
    return np.sum(data[data<mean+3*std])
dev.pseudo_signal.set_compute_method(calculate_readback)
dev.pseudo_signal.set_input_signals(dev.eiger.image)
```
This setup enhances flexibility and efficiency in signal processing, empowering users to tailor computations to their specific needs.
