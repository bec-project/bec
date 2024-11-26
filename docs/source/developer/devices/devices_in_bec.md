(developer.devices.devices_in_bec)=
# Devices in BEC

In this section, we will briefly discuss how devices are managed in BEC. BEC is a distributed system with a server-client architecture. One of the processes on the server side is the device server which is the central hub for managing all devices. Any device actions requested during scans or by a client will be handled by the device server. While Ophyd already provides a unified interface for devices, BEC adds an additional layer of abstraction, required due to the distributed nature of the system. This may sound complex at first, but the most relevant part for the user is to understand that any service including clients of BEC host device objects representing the actual devices managed by the device server.  

In general, BEC distinguishes between [`Device`](/api_reference/_autosummary/bec_lib.device.Device.rst#bec_lib.device.Device) and [`Signal`](/api_reference/_autosummary/bec_lib.device.Signal.rst#bec_lib.device.Signal) objects. These two types correspond to the atomic types in Ophyd: [*Device*](https://nsls-ii.github.io/ophyd/device-overview.html#device) and [*Signal*](https://nsls-ii.github.io/ophyd/signals.html). Additionally, BEC introduces two subclasses: one for positioners: [`Positioner`](/api_reference/_autosummary/bec_lib.device.Positioner.rst#bec_lib.device.Positioner), and one for computed signals:([`ComputedSignal`](/api_reference/_autosummary/bec_lib.device.ComputedSignal.rst#bec_lib.device.ComputedSignal)). Each device within BEC is categorised into one of these classes automatically based on the interface it provides.

````{note}
Please note that this hierarchy is inspired by the different base classes from Ophyd: `Device`, `Signal`, and `PositionerBase`. BEC enhances certain aspects of these classes for ease of use. Additionally, `ComputedSignal` is included to handle computed signals.
````

## Device Server
In addition to handling device actions, the device server can subscribe to changes in Ophyd devices and propagate these updates to Redis. Updated information in Redis is automatically forwarded to any service, including user interfaces, the IPython client, and GUIs. The device server leverages the [Ophyd callback](https://nsls-ii.github.io/ophyd/architecture.html#callbacks) mechanism, which allows attaching callbacks to device events. If auto_monitor is enabled on a signal, the device server will automatically stay up to date with the signal's value by subscribing to updates. For more details about the callback mechanism and how to use it in custom device integration for BEC, refer to our [device events and callbacks](#developer.devices.device_integration.device_events_and_callbacks) section.

## Device Interface
As mentioned earlier, the device server automatically categorises devices into one of the following classes: `Device`, `Signal`, `Positioner` and `ComputedSignal`.
To understand the differences between these classes, BEC provides a set of protocols that define their relevant interfaces.

````{note}
Please note that `ComputedSignal` is a special case, where we provide an easy way to calculate a new signal base on the input of a single or multiple other signals. For the moment, we do not provide a protocol for this class. 
````


### Base Interface
All devices conform to a core interface defined in the *BECBaseProtocol* protocol. This protocol specifies the core functionality any device in BEC must provide, and this functionality is exposed to any client. The aquivalent in Ophyd is the [OphydObject](https://blueskyproject.io/ophyd/user/generated/ophyd.ophydobj.OphydObject.html).

````{dropdown} View code: BECBaseProtocol
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../ophyd_devices/ophyd_devices/interfaces/protocols/bec_protocols.py
:language: python
:pyobject: BECBaseProtocol
```
````

### Device Interface
The device protocol extends *BECBaseProtocol* by adding additional methods relevant the device interface. Any device inheriting from `ophyd.device.Device` automatically complies with that interface.

````{dropdown} View code: BECDeviceProtocol
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../ophyd_devices/ophyd_devices/interfaces/protocols/bec_protocols.py
:language: python
:pyobject: BECDeviceProtocol
```
````

### Signal Interface
Similarly to the *Device* protocol, the *Signal* protocol extends *BECBaseProtocol* and introduces additional methods relevant to signals. Any device inheriting from `ophyd.signal.Signal` automatically complies with that interface.

````{dropdown} View code: BECSignalProtocol
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../ophyd_devices/ophyd_devices/interfaces/protocols/bec_protocols.py
:language: python
:pyobject: BECSignalProtocol
```
````

### Positioner Interface
Positioners are a special case of devices, providing additional methods to set or move the device to a target position. The most common example from ophyd is the `EpicsMotor` class. Other examples include temperature or pressure controllers, as they can also be set to a target value.

````{dropdown} View code: BECPositionerProtocol
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../../ophyd_devices/ophyd_devices/interfaces/protocols/bec_protocols.py
:language: python
:pyobject: BECPositionerProtocol
```
````

### Custom Methods - User Access
While the protocols provide a core set of methods accessible to users, there may be cases where additional methods for specific devices should be made available to users on the client side.
BEC provides a simple mechanism to enable this. You can specify a list of strings containing method names in the `USER_ACCESS` class attribute in your custom device integration. The device server will then automatically expose these methods to the users in the IPython client.

``` python
class MyCustomDevice(Device):
    USER_ACCESS = ['my_custom_method']

    def my_custom_method(self):
        """This is a custom method that should be accessible to the user."""
        ...
```