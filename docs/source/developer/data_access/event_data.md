(developer.event_data)=

# Introduction to event data

In BEC, the event data is used to inform other services about changes in the system. They are published as messages on specific endpoints in REDIS. The messages are used to inform other services about changes in the system, such as the status of a scan, the status of a device, or the data of a scan segment.

The event data is used to trigger custom actions in the system. For example, when a scan is started, the _ScanServer_ will publish a message on the _scan_status_ endpoint, which informs all other services about the scan. The _DeviceServer_ can provide this information to all loaded devices, which can prepare themselves for the upcoming scan.
In the following, we will show how to access commonly used event types in BEC and how to subscribe to them.

## Commonly used event types

As mentioned in the [previous section](developer.bec_messaging), the event data is published in form of various BECMessage types on specific endpoints in REDIS.
We will show here how to access some commonly used endpoints, and how to subscribe to events from within the _BECIPythonClient_.

**Scan Status**
The [`scan_status`](/api_reference/_autosummary/bec_lib.endpoints.MessageEndpoints.rst#bec_lib.endpoints.MessageEndpoints.scan_status) endpoint allows you to access information about the current scan in BEC. The message includes all information about the scan, such as the scan_ID, the scan_type, the scan_args, and the scan_kwargs.
From the _EndpointInfo_,

```ipython
[52/371] ❯❯ MessageEndpoints.scan_status().message_type
Out[52]:EndpointInfo(endpoint='scans/scan_status', message_type=<class 'bec_messages.messages.ScanStatusMessage'>, message_op=<MessageOp.SET_PUBLISH: ['register', 'set_and_publish', 'delete', 'get', 'keys']>)
```

we see that the message type is a [`ScanStatusMessage`](/api_reference/_autosummary/bec_messages.messages.ScanStatusMessage) and the allowed operations are `register`, `set_and_publish`, `delete`, `get` and `keys`.

To get the latest ScanStatusMessage, we can use the following code:

```python
from bec_lib.endpoints import MessageEndpoints
msg = bec.connector.get(MessageEndpoints.scan_status())
msg.content # Content of the ScanStatusMessage
msg.metadata # Metadata of the ScanStatusMessage
```

**Scan Number**
The [`scan_number`](/api_reference/_autosummary/bec_lib.endpoints.MessageEndpoints.rst#bec_lib.endpoints.MessageEndpoints.scan_number) endpoint allows you to access the current scan number. The message type is a [`VariableMessage`](/api_reference/_autosummary/bec_messages.messages.VariableMessage) and the allowed operations are `set`, `get`, `delete` and `keys`.
To access the current scan number, we can use the following code:

```python
from bec_lib.endpoints import MessageEndpoints
msg = bec.connector.get(MessageEndpoints.scan_number())
current_scan_number = msg.content["value"]
```

**Device readbacks**
Two more endpoints are [`device_readback`](/api_reference/_autosummary/bec_lib.endpoints.MessageEndpoints.rst#bec_lib.endpoints.MessageEndpoints.device_readback) and [`device_read_configuration`](/api_reference/_autosummary/bec_lib.endpoints.MessageEndpoints.rst#bec_lib.endpoints.MessageEndpoints.device_read_configuration).
An active _auto_monitor_ on the EPICS PV will update the corresponding endpoint in REDIS automatically.
The _device_readback_ endpoint will always be up-to-date with the latest reading from signals of type `ophyd.Kind.normal/hinted`, while the _device_read_configuration_ endpoint corresponds to signals of type `ophyd.Kind.config` and only updates on forced readings or when an _auto_monitor_ is set.
In both cases, the _message_type_ is a [`DeviceMessage`](/api_reference/_autosummary/bec_messages.messages.DeviceMessage) and the allowed operations are `register`, `set_and_publish`, `delete`, `get` and `keys`.
The following code shows how to access the latest device_read message from the device with name 'samx':

```python
from bec_lib.endpoints import MessageEndpoints
msg = bec.connector.get(MessageEndpoints.device_readback('samx'))
msg.content # Content of the ScanStatusMessage
msg.metadata # Metadata of the ScanStatusMessage
```

```{note}
To force an update for a device in the *BECIPythonClient*, we may use the `.read(cached=False)` or `.read_configuration(cached=False)` methods of the devices.
```

**Scan Segment**
The [`scan_segment`](/api_reference/_autosummary/bec_lib.endpoints.MessageEndpoints.rst#bec_lib.endpoints.MessageEndpoints.scan_segment) endpoint gives access to the data of a scan segment, which corresponds to readings of devices with `readoutPriority = monitored` during a scan. Please check the _device_configuration_ section for more information about the [readout_priority](developer.ophyd_device_config).
In a step scan, the scan segment is updated for each step, while in a fly scan, the scan segment is updated based on the procedure defined within the scan_core. The message_type is [`ScanMessage`](/api_reference/_autosummary/bec_messages.messages.ScanMessage) and the allowed operations are `register`, `send`. Here, we can only subscribe to the endpoint to receive updates on new readings.

(developer.event_data.subscription)=

## Subscribing to events

Subscriptions to events allow you to react to a new message published on a specific endpoint. We can register our own callback funtion to an endpoint. Incoming messages are enqueued at reception, and callbacks are executed one after the other to preserve events order. Callbacks must not block, otherwise events processing is put on hold. By default a thread consumes the queue, so the callbacks are executed in this consumer thread.

```python
def my_cb(msg, **kwargs):
    if msg.value.content["point_id"] ==20:
        print("Point 20 is done")
        # My custom api call
```

The callback needs to follow the argument signature of this example. The message object passed as the first argument is a [`bec_lib.connector.MessageObject`](/api_reference/_autosummary/bec_lib.connector.MessageObject), with two fields; `msg.topic` for the Endpointinfo and `msg.value` for the respective BECMessage. In the current example, we receive a _ScanMessage_, and print a line once the _point_id=20_ is published.
After defining the callback function, we can subscribe to the endpoint with the following code:

```python
bec.connector.register(MessageEndpoints.scan_segment(), cb=my_cb)
```

Each new message published on the _scan_segment_ endpoint will trigger the callback. If necessary, additional arguments can be passed to the callback function by specifying them in as keyword arguments in the `register` method.

```python
bec.connector.register(MessageEndpoints.scan_segment(), cb=my_cb, my_arg1=arg1, my_arg2=arg2)
```

```{note}
It is very important to keep the execution time of the callback function as short as possible. If the callback function takes too long to execute, it will block the thread and may compromise the performance of the system. For secondary operations, we recommend the callback function to trigger actions through an API call to a separate service.
```

## Accessing event data outside of the BECIPythonClient

If you like to use the event data oustide of the _BECIPythonClient_, you can use the [`RedisConnector`](/api_reference/_autosummary/bec_lib.redis_connector.RedisConnector) to access the REDIS server. You have to provide the correct `"host:port"` to the connector which allows you to connect to REDIS from the system you are running the code. If REDIS runs locally, this would be `"localhost:6379"`.
Note, at the beamline this would be the hostname of the bec_server. The port `"6379"` is the default port for REDIS.

```python
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
bootstrap = "localhost:6379"
connector = RedisConnector(bootstrap)
connector.get(MessageEndpoints.scan_status())
```
