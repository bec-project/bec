
(developer.scans.scan_stubs)=
# Scan stubs - the building blocks of a scan
In order to simplify the creation of new scans, BEC provides a set of scan stubs that can be used as building blocks for new scans. The scan stubs are located in `bec_server/bec_server/scan_server/scan_stubs.py`. The following scan stubs are available:

*Device operations*

- [`set`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.set) Set a device or a list of devices to the given value.
- [`read`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.read) Read a device and wait for it to finish.
- [`stage`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.stage) Stage all devices.
- [`unstage`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.unstage) Unstage all devices.
- [`kickoff`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.kickoff) Kickoff a device. Usually only needed for fly scans.
- [`complete`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.complete) Wait for a device to finish a long-running operation. Typically used after `kickoff`.
- [`pre_scan`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.pre_scan) Trigger the pre_scan method of a device.
- [`baseline_reading`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.baseline_reading) Trigger the baseline readings. 
- [`trigger`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.trigger) Send a trigger to all devices that have `softwareTrigger` set to `True`.
- [`send_rpc`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.send_rpc) Send an RPC command to a device.
- [`send_rpc_and_wait`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.send_rpc_and_wait) Send an RPC command to a device and wait for it to finish. The return value is the response of the RPC command.


*Scan operations*
- [`open_scan`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.open_scan) Open a scan.
- [`close_scan`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.close_scan) Close a scan. 
- [`publish_data_as_read`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.publish_data_as_read) Publish data as read.
- [`open_scan_def`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.open_scan_def) Open a scan definition. 
- [`close_scan_def`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.close_scan_def) Close a scan definition. 
- [`scan_report_instruction`](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs.scan_report_instruction) Update the scan report instruction.

More information on the scan stubs can be found in the [API reference](/api_reference/_autosummary/bec_server.scan_server.scan_stubs.ScanStubs.rst#bec_server.scan_server.scan_stubs.ScanStubs).


## Synchronous and asynchronous usage
The device scan stubs can be used in a synchronous or asynchronous way. The synchronous way is the default and is used when the `wait` parameter is set to `True`. In this case, the scan stub will wait for the device to finish the operation before continuing. The asynchronous way is used when the `wait` parameter is set to `False`. In this case, the scan stub will start the operation and continue immediately without waiting for the device to finish. The asynchronous way is useful when you want to interact with multiple devices at the same time.

To use the scan stubs in an asynchronous way, you can use the returned `ScanStubStatus` object to later wait for the operation to finish. 

The following example demonstrates how a synchronous and asynchronous operation can be used:

```python
# Synchronous operation: Set the device to the value 10 and wait for it to finish
def my_func(self):
    yield from self.set(device=self.my_motor, value=10)

# Asynchronous operation: Set the device to the value 10 and continue immediately
def my_func_async(self):
    status = yield from self.set(device=self.my_motor, value=10, wait=False)
    # Do something else
    status.wait()
```


Especially for fly scans, the asynchronous way is useful as it allows you to start your flyer and in the meantime do other operations.

```python
def fly_scan_core(self):
    # kickoff the device
    yield from self.kickoff(device=self.my_flyer)

    # run the complete method of the device
    status = yield from self.complete(device=self.my_flyer, wait=False)

    while not status.done:
        # Do other operations, e.g. read all 'monitored' devices at 1 Hz
        yield from self.read(group="monitored", point_id=self.point_id)
        self.point_id += 1

```