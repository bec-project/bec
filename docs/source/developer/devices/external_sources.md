(developer.external_sources)=

# External data sources

Large data sources typically have their own data pipeline, optimized for their specific use case and data throughput. Yet, it is often desirable to use the BEC to be informed and potentially link to these external data sources.
As of now, only external data sources that are based on HDF5 files are supported. BEC can be informed about new HDF5 files by emitting a [FileMessage](#bec_messages.messages.FileMessage) to the [public_file endpoint](#bec_lib.endpoints.MessageEndpoints.public_file), e.g.

```python
from bec_lib.endpoints import MessageEndpoints
from bec_messages import messages
from bec_lib.redis_connector import RedisConnector

scan_id = "scan id of the current scan"

# get a new producer for redis messages
producer = RedisConnector(["localhost:6379"]).producer()

# prepare the message
msg = messages.FileMessage(file_path="/path/to/file.h5", done=False)

# send the message using the scan_id and a user-friendly but unique name to describe the source (e.g. "eiger")
producer.set_and_publish(
    MessageEndpoints.public_file(scan_id, "eiger"),
    msg,
)
```

The file writer will check all external links and hand them over the file writer plugin. Users can then access the external files and potentially link to them from the BEC master file using external links, e.g.

```python
eiger = instrument.create_ext_link("eiger", "/path/to/file.h5", "entry/instrument/detector")
```
