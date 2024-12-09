(developer.devices.bec_device_server)=
# BEC Device Server

The BEC Device Server is the service in charge of connecting with the beamline hardware
and providing the corresponding entry points for all other services (see [BEC architecture](developer.architecture)).

Thanks to BEC message passing interface, the BEC Device Server receives human-readable
YAML files containing **device definitions** and instantiates _Device_ and _Signal_ objects.

Those objects represent control system components, usually EPICS PVs, grouped into
a class (as defined by the [Ophyd library](developer.ophyd_devices)). This allows to have an
uniform hardware abstraction layer for BEC.

```{figure} /assets/bec_device_server_diagram.png
Diagram representing the reading of device definitions to create Devices connected to the control system
```

## BEC Devices and Signals API

The BEC Device Server receives device definitions, and instantiates the corresponding objects from the hardware
abstraction layer. Those objects are **wrapped to fit into 4 main classes**:

- [_Device_](/api_reference/_autosummary/bec_lib.device.Device.rst#bec_lib.device.Device)
    - which corresponds to the [Device type in Ophyd](https://nsls-ii.github.io/ophyd/device-overview.html#device)
- [_Signal_](/api_reference/_autosummary/bec_lib.device.Signal.rst#bec_lib.device.Signal)
    - which corresponds to the [Signal type in Ophyd](https://nsls-ii.github.io/ophyd/signals.html)
- [_Positioner_](/api_reference/_autosummary/bec_lib.device.Signal.rst#bec_lib.device.Signal)
- [_ComputedSignal_]((/api_reference/_autosummary/bec_lib.device.Signal.rst#bec_lib.device.Signal))

BEC provides a set of protocols to define the interfaces of Device Server objects, described below:

```{figure} /assets/bec_device_protocols.png
Class diagram of BEC Device Server protocols
```

## Device Interfaces

We use a set of protocols to define and test against an expected interface for devices loaded into the BEC Device Server. This ensures that all devices conform to a common set of methods and properties. You can find the protocol definitions in the *ophyd devices* repository [here](https://gitlab.psi.ch/bec/ophyd_devices/-/blob/main/ophyd_devices/interfaces/protocols/bec_protocols.py?ref_type=heads).
* Any object of type device or signal within BEC must comply with the *BECBaseProtocol*. The equivalent in Ophyd is the [OphydObject](https://blueskyproject.io/ophyd/user/generated/ophyd.ophydobj.OphydObject.html).
* A device must comply with the *BECDeviceProtocol*. The equivalent in Ophyd is the [Device](https://blueskyproject.io/ophyd/device-overview.html#device).
* A signal must comply with the *BECSignalProtocol*. The equivalent in Ophyd is the [Signal](https://blueskyproject.io/ophyd/user/reference/signals.html).
* A positioner must comply with the *BECPositionerProtocol*. The equivalent in Ophyd is the [Positioner](https://blueskyproject.io/ophyd/user/reference/positioners.html).

````{note}
Please note that `ComputedSignal` is a special case, to provide an easy way to calculate a new signal based on the input of a single or multiple other signals. For the moment, there is no protocol for this class. 
````

## Accessing objects from clients

_Device_, _Signal_, _Positioner_ and _ComputedSignal_ objects live in the BEC Device Server, each client (like BEC Scan Server or BEC command line)
receive a **proxy object**. The proxy object is a local representative of the object on the server side, and can be
used transparently thanks to BEC Remote Procedure Call (RPC).

```{figure} /assets/bec_device_server_proxy.png
Diagram representing proxy object in client, and RPC communication from Device Server
```

````{note}
Any device action requested during scans or by a client is handled by the device server. 
````

### Extending proxy objects with custom methods - User Access

While the protocols provide a core set of methods accessible to users, there may be cases where additional methods for specific devices should be made available to users on the client side.
BEC provides a simple mechanism to enable this, by providing a list of strings containing method names in the `USER_ACCESS` class attribute of any custom BEC Device:

``` python
class MyCustomDevice(Device):
    USER_ACCESS = ['my_custom_method']

    def my_custom_method(self):
        """This is a custom method that should be accessible to the user."""
        ...
```

