(user.data_access_and_plotting)= 
# Data Acess and Plotting

Let's recapture how to do a scan, and explore the data contained within it. 

```ipython
s = scans.line_scan(dev.samx, -5, 5, steps=50, exp_time=0.1, relative=False)
```

```{note}
Scan data is automatically stored in an [HDF5 file structure](https://portal.hdfgroup.org/hdf5/develop/index.html) (h5 file). 
The internal layout of the h5 file is customizable by the beamline.
Please contact your beamline contact for more information about this.
```

BEC provides a convenient way to access the scan data, either directly from the scan object or from the history.

## Scan data access

The return value of a scan is a python object of type `ScanReport`. All data is stored in the `scan.data` attribute. This attribute is a convenient way to access the data of the scan and gives you access to the entire data structure that is stored in the HDF5 file.
However, as the data object relies on the HDF5 file, it is not accessible during the scan. For live data access, we provide you access to monitored devices through `scan.live_data`. For reading devices with a different readout priority, you can directly subscribe to the device data stream. 

### Live data access
```python
print(s.scan.data) 
```
For live data, typically only specific motors are of interest. To facilitate the access to the data, we provide a convenient access pattern `s.scan.data.<device_name>.<hinted_signal>.val`.
For example to access the data of `samx` and the above added device `gauss_bpm`, you may do the following:
```python
samx_data = s.scan.live_data.samx.samx.val 
# or samx_data = s.scan.live_data['samx']['samx'].val

gauss_bpm_data = s.scan.live_data.gauss_bpm.gauss_bpm.val 
# or s.scan.live_data['gauss_bpm']['gauss_bpm'].val
```
If our gui framework is running, and the default figure `fig` has not been close, you may now directly plot the data using:
``` python
fig.plot(gauss_bpm_data, samx_data)
```
Please check the section about our [graphical user interface](#user.graphical_user_interface) for more details on all available widgets not only to plot but also to interact with BEC and its services. A quick start guide can be found [here](https://bec.readthedocs.io/projects/bec-widgets/en/latest/user/getting_started/quick_start.html). You can also manipulate the data directly in the IPython shell.
Keep in mind though, these manipulations only happen locally for yourself in the IPython shell. 
They will not be forwarded to the BEC data in Redis, thus, your modification won't be stored in the raw data file (HDF5 file).

#### Export to pandas
Below, we demonstrate how you may easily convert the data into the commonly used `pandas` dataframe. 
If `pandas` is not installed as a dependency, you can install it via `pip install pandas`.
```python
df = s.scan.to_pandas()
```
You can interact with the dataframe as you see fit, for instance by additionally installing libraries like `matplotlib` to use the `pandas` plotting capabilities linked to `matplotlib`.
However, we still recommend you check out our [graphical user interface](#user.graphical_user_interface) for more advanced plotting capabilities.

### Accessing scan data from the history
BEC maintains the history of the last 10 000 scans. You can easily retrieve scan data from `bec.history`, as demonstrated in the example below, where we fetch data from the latest scan. 
```ipython
scan_data_container = bec.history[-1]
```

Alternatively, you can access the scan data from a specific scan by providing the scan id:
```ipython
scan_data_container = bec.history.get_by_scan_id(scan_id)
```

The history returns a `ScanDataContainer` object, which is a container for the scan data and maps your HDF5 file's "collection" structure to a python object. Upon accessing the data, the object will automatically load the necessary data from the HDF5 file and based on the size of the data, cache it in memory for faster access on subsequent calls.

```{note}
The return value of the history is a `ScanDataContainer` object, the same as the return value of the scan reports `s.scan.data`.
```

The following code snippet demonstrates how to access the data from the `samx` device and its `samx` signal. The return type is a dictionary containing the data.

```ipython
samx_signal_data = scan_data_container.devices.samx.samx.read()
```

If you want to access the data of the `samx` device and all its signals, you can use the following code snippet:

```ipython
samx_data = scan_data_container.devices.samx.read()
```

## Fit the scan data
You can use the builtin models to fit the data. All models are available in the `bec.dap` namespace. As an example, we can fit the data with a Gaussian model and select the `samx` and `bpm4i` devices with their respective (readback) signals `samx` and `bpm4i`:
```python
s = scans.line_scan(dev.samx, -5, 5, steps=50, exp_time=0.1, relative=False)
res = bec.dap.GaussianModel.fit(s.scan, "samx", "samx", "bpm4i", "bpm4i")
```
The result of the fit is stored in the `res` object which contains the fit parameters and the fit result.
You can further optimize the fit by limiting the fit range, e.g. 
```python
res = bec.dap.GaussianModel.fit(s.scan, "samx", "samx", "bpm4i", "bpm4i", x_min=-2, x_max=2)
```

To display the fit, you can use the `plot` method of the `res` object:
```python
res.plot()
```

Often, a fit is simply a means to find the optimal position of a motor. Therefore, the fit result can be used to move the motor to the optimal position, e.g. to the center position of the Gaussian:

```python
umv(dev.samx, res.center)
```


## Export scan data from client
BEC consistently saves data in h5 format, following the NX standard. 
It is recommended to access data through h5 files, as they also contain links to large detector data from secondary data services. 
Additionally, we provide a straightforward method to export scan data to `csv` using the client interface:

```ipython
with scans.scan_export("<path_to_output_file.csv>"):
    scans.line_scan(dev.samx, -5, 5, steps=50, exp_time=0.1, relative=False)
    scans.grid_scan(dev.samx, -5, 5, 50, dev.samy, -1, 1, 10, exp_time=0.1, relative=False)
```

Running this code will generate the scan data output in `<path_to_output_file.csv>`. 
Additionally, you can directly import the export function `scan_to_csv`, enabling you to export scan data from previously conducted scans:

``` ipython
from bec_lib.utils import scan_to_csv

scan_data = bec.history[-1].scans[0]
scan_to_csv(scan_data, "<path_to_output_file.csv>")
```

```{note}
Large data from 2D detectors are usually processing by backend services and are, therefore, not available for client-based export.
```