(developer.bec_messaging)=

# Introduction to the BEC messaging system

The BEC messaging system is the backbone of the BEC system and is responsible for the communication between the different components of the BEC system. The following sections provide an overview of its components: the Redis server, the Python-based Redis connector, the BECMessage classes and the EndpointInfo class.

Redis is an open-source, in-memory data structure store that can be used as a database, cache, and message broker. Every entry is associated with a unique channel name, e.g. `internal/devices/readback/samx`. Depending on the stored data, it supports various operations such as get, set, delete, publish/subscribe as well as stream operations and many more. To learn more about the core functionality of Redis, visit the [Redis website](https://redis.io/).

For storing and retrieving data in BEC, there are three building blocks to describe any communication:

- [`RedisConnector`](/api_reference/_autosummary/bec_lib.redis_connector.RedisConnector)
  A wrapper around the `redis-py` package to simplify the interaction with the Redis server.
- [`BECMessage`](/api_reference/_autosummary/bec_messages.messages.BECMessage)
  A set of classes that provide a uniform way to send and receive data irrespective of the data type.
- [`EndpointInfo`](/api_reference/_autosummary/bec_lib.endpoints.EndpointInfo)
  A class that provides a mapping between the Redis channel, the BECMessage class and the supported operations for the channel.

Sending a message to Redis typically involves using an already existing instance of the `RedisConnector` class, creating a new instance of a `BECMessage` class and sending it to the desired endpoint using corresponding method of the EndpointInfo class and the `RedisConnector` class.
The mapping between the Redis channel, the BECMessage class and the supported operations for the channel is provided by the `EndpointInfo` class:

```{figure} ../../assets/messaging_system.png
:name: messaging_system
:align: center

The EndpointInfo class provides a mapping between the endpoint in Redis, the supported operations of the redis channel and the message type, i.e. what kind of BECMessage class is used to generate the data.
```

````{dropdown} View tutorial: Example of a communication with Redis
:icon: code-square
:animate: fade-in-slide-down

```{note}
Please note that the following tutorial also includes the sending of messages to Redis. Under normal circumstances, the messages are sent by the BEC system itself and not by the user. This tutorial is intended to show how the BEC messaging system works and therefore includes the sending of messages.
```

Let's assume we want to send a new file event message to Redis to inform other services about a new file being created for the device "samx". To this end, we will use the `file_event` endpoint:

```python
from bec_lib.endpoints import MessageEndpoints

MessageEndpoints.file_event("samx")
```

Executing the code above will return the following output:

```
• demo [11/10] ❯❯ MessageEndpoints.file_event("samx")
Out[11]: EndpointInfo(endpoint='public/file_event/samx', message_type=<class 'bec_messages.messages.FileMessage'>, message_op=<MessageOp.SET_PUBLISH: ['register', 'set_and_publish', 'delete', 'get', 'keys']>)
```

As we can see, the `file_event` endpoint is associated with the Redis channel `public/file_event/samx` and the message type `FileMessage`. The message operations supported by this endpoint are `register`, `set_and_publish`, `delete`, `get`, and `keys`.

In order to send a message to this endpoint, we need to create a new instance of the `FileMessage` class and send it to the Redis server using the `set_and_publish` operation:

```python
from bec_lib.endpoints import MessageEndpoints
from bec_messages import messages

# create a new instance of a BECMessage class, e.g. a FileMessage
msg = messages.FileMessage(file_path='path/to/file', done=False, successful=True, metadata={'scan_id': "1234"})

bec.connector.set_and_publish(MessageEndpoints.file_event("samx"), msg)

```

Another service listening to the `public/file_event/samx` channel will receive the message and can act accordingly.
If we simply want to retrieve the last message from the `public/file_event/samx` channel, we can use the `get` operation:

```python
from bec_lib.endpoints import MessageEndpoints

# get the last message from the 'public/file_event/samx' channel
out = bec.connector.get(MessageEndpoints.file_event("samx"))
out
```

````

## BECMessage classes

The BECMessage classes are used to create messages that can be sent to Redis. Upon initialization, a BECMessage class validates the input against the predefined schema and raises an error in case of a mismatch. This ensures that the data sent to Redis is always in the correct format. The BECMessage classes are defined in the `bec_messages.messages`:

````{dropdown} View code: BECMessage classes
```{literalinclude} ../../../../bec_lib/bec_lib/messages.py
```
````

Every instance of a BECMessage provides the attributes `content` and `metadata`, both returning dictionaries. The `content` attribute contains the actual data that is sent to Redis, while the `metadata` attribute contains additional information about the message.
