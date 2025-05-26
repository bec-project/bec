(developer.bec_sim)=
# Simulations
The BEC simulation framework comprises a set of simulated devices. These devices offer a platform for developers to explore, test, or add new features to the core libraries of BEC and BEC-Widgets. Additionally, the end-to-end test framework enables the simulation of user behavior at the beamline, facilitating the testing of newly developed features against common user commands. These end-to-end tests encompass various scans, motions, and scenarios, such as requesting a motor to move beyond its limits or simulating system recovery from user interruptions (CTRL-C). Moreover, the framework enables users to develop scripts, GUIs, or data processing tools without requiring access to the physical devices, facilitating faster development cycles.

## Architecture
```{figure} ../../assets/simulation_context_diagram.png
Architecture Diagram:  
The BEC simulation currently hosts three main device types: `SimMonitor`, `SimPositioner`, and `SimCamera`. Their state, as well as readback value, can be configured within the simulation_state of the device. Additionally, we provide two examples of `DeviceProxies` demonstrating how multiple devices can be linked within the simulation to imitate specific scenarios.
```

This architecture diagram illustrates the relevant classes for the simulation and their interconnections. To comprehend individual components fully, we recommend reading the following sections with the architecture in mind.

## Simulated Devices
The core of the simulation provides a range of devices that allow the simulation of monitors (`SimMonitor`), motors (`SimPositioner`), or cameras (`SimCamera`). Although these devices are simulated, we inherit from [Ophyd](https://nsls-ii.github.io/ophyd/) , ensuring compliance with the structure prescribed for devices within Ophyd. Each device can host different signals, for which we have created two custom [Signals](https://nsls-ii.github.io/ophyd/signals.html): a `SettableSignal` for storing configuration signals such as velocity or num_images, with read and write access, and a `ReadOnlySignal` for read-only signals. The computation logic for the readback value, as well as the storage of all signal states, is housed in sim_data.

For example, the readback of the `SimMonitor` device can be configured to return values calculated based on a range of models from [LMFIT](https://lmfit.github.io/lmfit-py/builtin_models.html), together with a reference position defined by a motor. For `SimCamera`, we have implemented two custom readback functions allowing simulation of a multivariate Gaussian or constant readback. Noise can be added to all signals, and we also allow hot, dead, or fluctuating pixels to be added to the readback of `SimCamera`.

## Simulation Frameworks
To mimic the behavior of a beamline more realistically, we provide an additional device type called `DeviceProxy` with two examples for `SimCamera`. The first, H5ImageReplayProxy, allows you to replay data from an existing h5 file. This proxy is configured through the deviceConfig, specifying the h5 file location and the entry with the images composed as an array with dimensions (slice, x, y). The simulation will wrap if your scan exceeds the number of images in the file and play them from the start. Notably, this proxy implements the replay capability within the scope of BEC scans. The second simulation, `SlitProxy`, allows the linking of single or multiple motors' positions to be taken into account when computing the readback for `SimCamera`, e.g., slit widths.

```{Note}
Proxies override the `_compute` method from the signal of the device they are configured to act upon. They can be enabled or disabled from the command-line interface for simple usage.
```
## Usage

The simulation framework for each simulated device can be accessed through `dev.<devname>.sim`. We have implemented three methods and a property to facilitate easy access configuration and configuration of the simulation parameters. These are `dev.<devname>.sim.get_models()`, `dev.<devname>.sim.select_model()`, `dev.<devname>.sim.show_all()`, and `dev.<devname>.sim.params`. With these functions, you can configure the simulation of your device as needed.

### Configuration of simulated devices
As mentioned earlier, `SimMonitor` and `SimCamera` offer different simulation types. To receive a list of strings with all available simulation types for `SimMonitor`, you can use the code below:

```ipython
• demo [11/171] ❯❯ dev.bpm4i.sim.get_models()
Out[11]:
['BreitWignerModel',
 'ConstantModel',
 'DampedHarmonicOscillatorModel',
 'DampedOscillatorModel',
 'DoniachModel',
 'ExponentialGaussianModel',
 'ExponentialModel',
 'GaussianModel',
 'LinearModel',
 'LognormalModel',
 'LorentzianModel',
 'MoffatModel',
 'ParabolicModel',
 'Pearson4Model',
 'Pearson7Model',
 'PolynomialModel',
 'PowerLawModel',
 'PseudoVoigtModel',
 'QuadraticModel',
 'RectangleModel',
 'SineModel',
 'SkewedGaussianModel',
 'SkewedVoigtModel',
 'SplitLorentzianModel',
 'StepModel',
 'StudentsTModel',
 'ThermalDistributionModel',
 'VoigtModel']
```
By default, monitors are initialized with the *ConstantModel* and a uniform noise pattern. To change this for the device `dev.bpm4i` to first simulate a *Gaussian* function and then use *Poisson* noise, follow the steps below:
```ipython
• demo [12/171] ❯❯ dev.bpm4i.sim.select_model("GaussianModel")
+------------------------------------------------------------------------------------------------------------------------------------------+
|                                          Currently active model: <lmfit.Model: Model(gaussian)>                                          |
+----------------------------------------------------------+-------------------+-----------------------------------------------------------+
|                        Parameter                         |       Value       |                            Type                           |
+----------------------------------------------------------+-------------------+-----------------------------------------------------------+
|                        amplitude                         |        100        |                       <class 'int'>                       |
|                          center                          |         0         |                       <class 'int'>                       |
|                          sigma                           |         1         |                       <class 'int'>                       |
|                           fwhm                           |      2.35482      |                      <class 'float'>                      |
|                          height                          |      39.89423     |                      <class 'float'>                      |
|                          noise                           |      uniform      |                     <enum 'NoiseType'>                    |
|                     noise_multiplier                     |         10        |                       <class 'int'>                       |
|                        ref_motor                         |        samx       |                       <class 'str'>                       |
+----------------------------------------------------------+-------------------+-----------------------------------------------------------+
• demo [13/171] ❯❯ dev.bpm4i.sim.params = {"noise" : "poisson"}
• demo [14/171] ❯❯ dev.bpm4i.sim.params
Out[14]:
{'amplitude': 100,
 'center': 0,
 'sigma': 1,
 'fwhm': 2.35482,
 'height': 39.89423,
 'noise': 'poisson',
 'noise_multiplier': 10,
 'ref_motor': 'samx'}
```
```{note}
`dev.<devname>.sim.params` is a property and by assigning a new dictionary to it, you will not override all parameters but update the current set of parameters stored in the property. It will raise if you are trying to set a key to an irregular value, e.g. non existing model, or in case of a key does not exist in params.
```
Finally, you can use `dev.bpm4i.sim.show_all()` to obtain a comprehensive printout of the currently active model, all available methods, and the available models for this device. Similarly, you may configure the `SimCamera`, which implements only a limited scope of simulation models, as shown below:
```ipython
• demo [15/171] ❯❯ dev.eiger.sim.get_models()
Out[15]: ['constant', 'gaussian']
```

````{note}
The simulated device can also be configured through the config file to start a simulation with a specific model and noise type. 
This can be accomplished by passing sim_init through the deviceConfig to the init of the device. 
Below is an example of configuring `bpm4i` to simulate a Gaussian model:
``` yaml
bpm4i:
  readoutPriority: monitored
  deviceClass: ophyd_devices.sim.sim_monitor.SimMonitor
  deviceConfig:
    sim_init:
      model: GaussianModel
      params:
        amplitude: 500
        center: 0
        sigma: 1
  deviceTags:
    - beamline
  enabled: true
  readOnly: false
  softwareTrigger: true
```
````
### Simulation configurations 
We provide a couple of examples how to configure the various different devices for the simulation. 
A range of examples can be found in the ophyd_devices repositroy for the [`ophyd_devices_simulation.yaml`](https://github.com/bec-project/ophyd_devices/blob/main/ophyd_devices/configs/ophyd_devices_simulation.yaml).
Here, we've also added two examples for `DeviceProxy` classes that simulated certain scenarios. `hdf5_proxy` replays data from an h5 file, while `slit_proxy` allows us to simulate scanning the center position of two slits for a 2D camera. Please be aware that both simulations target a camera device, but there is currently a one-to-one mapping between the camera device and proxy. `BEC` will not allow you to add multiple proxies to the same camera device.

