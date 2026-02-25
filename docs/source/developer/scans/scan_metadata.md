(developer.scans.scan_metadata)=
# Scan Metadata

During an experiment, it can be quite useful for users to store additional metadata about their scan. We believe that this should be 
possible for users, potentially dynamically and as easy as possible. The so-defined user metadata will go to all recorded metadata and 
therefore also stored on disk. 

To add new metadata for a single scan, you can simply add a metadata dictionary to your scan command.

```python
scans.line_scan(dev.samx,-5,5,steps=100,relative=True, metadata={ 'my_user_metadata' : 'second alignment scan of sample'})
```

This command adds a new key-value pair to the metadata dictionary of the scan, which will also be stored in the HDF5 file. If you want to
add metadata to all scans, you can also do this by simply adding your metadata to:

```python
bec.metadata = { 'my_user_metadata' : 'second alignment scan of sample'}
```

## Metadata schema

In the plugin repository, you can add schema against which experiment metadata must be validated, and associate them with specific scans. 
These will automatically generate corresponding forms in the BEC GUI scan control widgets for easy data entry. Scans types which have an
associated schema will not be able to be submitted unless the metadata is added, meaning the data entered conforms to the types and
constraints specified in the schema.

### Defining schema

Metadata schema are based on [Pydantic models](https://docs.pydantic.dev/latest/concepts/models/), which are simple ways of declaring
required and optional fields using Python datatypes. If the entries contain a default value, then including them in the request is optional.
The entry can also include `None` as an acceptable value by including `| None` in the type annotation (such as in `treatment_temperature_k`
below), indicating the value is optional. In many cases, this might be the default value, indicating nothing was entered by the user, such
as for `treatment_temperature_k` below.

If nothing is entered into the metadata for an optional value when running a scan, then the default value will be used. For
example, to ensure that for each scan of a certain type we record a treatment description (a text entry), a treatment temperature (an 
integer number in Kelvins), a treatment time (a real number in hours), and an indication of whether the sample was washed (a true or false 
value), and you wish for all of these to be optional, you could declare the following:

```python
from bec_lib.metadata_schema import BasicScanMetadata

class ExampleSchema(BasicScanMetadata):
   treatment_description: str = ""
   treatment_temperature_k: int | None = None
   treatment_time_h: float = 10.0
   washed: bool = True
```

```{admonition} Required and non-required values
If you wish to make it compulsory to enter a value for a given field, remove the default values and `| None` from the type indicator, so
that the lines in the example above would look something like:  
`treatment_description: str` or `treatment_temperature_k: int`  
This means that unless something is explicitly entered for that field, the scan request will not pass validation and the scan will not
execute.
If only the default value is removed, but `None` is still allowed, it must be explicitly entered to be used.
```

The appropriate information could then be entered when running the scan as follows:

```python
scans.treated_sample_scan(
    dev.samx, -5, 5, steps=100, relative=True,
    metadata={
        "treatment_description": "vacuum oven",
        "treatment_temperature_k": 475,
        "treatment_time_h": 3.50,
        "washed": True,
    },
)
```

If the information entered does not match the specified data types, a `ValidationError` will be printed to the console, describing which
field(s) were wrongly entered and what kind of information they expect.

Currently, the schema themselves and the validation support any of the standard [Python types](https://docs.pydantic.dev/latest/api/standard_library_types/)
but the GUI only supports `str`, `int`, `float`, `Decimal`, and `bool`, and unions of those types with `None`. Any additional datatypes for 
required fields will have to be entered in the GUI by the user as a string in a text field.

### Adding constraints and extra information for users

Say that, for the above experiment, we also wanted the user to enter a lab book sample code (a string of five or six characters, made up of 
two or three letters and three digits), we want to put some constraints on the information entered to check that it meets some requirements,
we would like to provide default values for some entries, and/or we would like to provide some additional information to the users to
correctly interpret what a field means. To achieve these things we can extend the schema above, using
[Pydantic Field specifiers](https://docs.pydantic.dev/latest/concepts/fields/) like so:

```python
from decimal import Decimal
from pydantic import Field
from bec_lib.metadata_schema import BasicScanMetadata

class TreatedSampleSchema(BasicScanMetadata):
   sample_code: str = Field(title="Sample code", description="the entry in the lab book corresponding to the preparation of this sample", pattern=r"^[A-Z]{2,3}\d{3}$")
   treatment_description: str = Field(title="Treatment description", description="which oven was used for heating, and any other special features", min_length=10)
   treatment_temperature_k: int  = Field(title="Treatment temperature / K", description="treatment temperature, in Kelvins", gt=373, le=623)
   treatment_time_h: Decimal = Field(title="Treatment temperature / h", description="", gt=0, le=24, decimal_places=2)
   washed: bool = Field(title="Washed", description="sample was cleaned using supercritical CO2 after treatment: yes/no", default=True)
```

The `title` and `description` arguments are optional but allow you to add information which is presented to the user in the GUI, on printing
the schema in the console using `%schema` (see below), or on validation errors. 

To place limits on numerical values `lt`, `le`, `ge`, and `gt` (meaning 'less than', 'less than or equal to', 'greater than or equal to',
and 'greater than', respectively, can be used). For the `Decimal` type, additionally, `decimal_places` and `max_digits` can be specified.
For string types, `min_length` and `max_length` can be specified, and the `pattern` argument allows you to specify a regular expression
which the string must match. See [Regex101](https://regex101.com/r/aGP3Ya/1) for an explanation of the specific example used here and how it
implements the requirement described above.

For any field, a default value can be entered. In this case, manually supplying a value is no longer required, and the default value
will automatically be propagated to the GUI as well.

These limits described here will be used in the GUI to simplify the experience by making it impossible to enter nonconforming data, but 
other constraints described in the Pydantic fields documentation can still be used.

### Associating schemas with scans

In the plugin repository, in the file `scans/metadata_schema/metadata_schema_registry.py`, the association between a particular scan and
a schema is defined in the python dictionary `METADATA_SCHEMA_REGISTRY`. For example, to associate the `TreatedSampleSchema` above with
a scan named `treated_sample_scan`, an entry would be added like so:

```python
from .sample_schemata import TreatedSampleSchema

METADATA_SCHEMA_REGISTRY = {
    "treated_sample_scan": TreatedSampleSchema
}
```

Note the scan name is in quotes and the schema class is not, and that the schema class must be imported into the registry file. This example
assumes that the schema class is defined in a file called `sample_schemata.py` in the same directory as `metadata_schema_registry.py`.

You can also define a default, fallback schema by assigning it to the variable `DEFAULT_SCHEMA` in the same file. This means it will be used
for all scans which don't have an associated schema.

```{admonition} Warning
If you assign `DEFAULT_SCHEMA` to something which has a required field it will be compulsory to enter a value for that field for every
scan! You can do this if, for example, you really want to make sure a sample code is entered for every scan, but beware!

Schemas entered into the `METADATA_SCHEMA_REGISTRY` will override this value, and only have the specific requirements in those schema.
```

### Viewing schema contents in the console

Users can print the schema for a given scan in the BEC console using the iPython Magic `%schema`, for example:

```ipython
• demo [1/1] ❯❯ %schema grid_scan
{
  "additionalProperties": true,
  "description": "Basic scan metadata class. Only requires a sample name. Accepts any additional\nmetadata that the user wishes to provide. Can be extended to add required fields\nfor specific scans.",
  "properties": {
    "sample_name": {
      "description": "A human-friendly identifier for the sample",
      "title": "Sample name",
      "type": "string"
    }
  },
  "required": [
    "sample_name"
  ],
  "title": "BasicScanMetadata",
  "type": "object"
}
```

