(developer.tests)=
# Writing tests
BEC relies on [pytest](https://pytest.org/) in order to provide a comprehensive test suite of BEC core and all its components.
When contributing code to BEC it is mandatory to provide test code, in order to demonstrate the good functioning of the new
code, and to ensure long-term maintenance.

There are 2 kind of tests in BEC:
- unit tests
- end-2-end tests

Unit tests are meant to guarantee the fundamental behaviour of the code being added to the project, limiting its scope to the
new classes or functions. Other parts of the project are _mocked_, since the goal is to focus on the new functionality only.
Those tests should execute quickly and should not require network connections or simulation hardware.

End-2-end tests are ensuring the whole system remains stable, and guarantee the new code to be properly integrated. Those
tests really run BEC servers, Redis and operate on simulated hardware.

BEC provides pytest fixtures to make it easy for developers to test BEC code in a reproducible way, using common mock objects
or runtime conditions.

## BEC fixtures installation

BEC fixtures come with `bec-lib` as 2 pytest plugins:
- `bec_lib_fixtures`
    - source module: `bec_lib.tests.fixtures`
- `bec_lib_end2end_fixtures`
    - source module: `bec_lib.tests.end2end_fixtures`

So, it is enough to install `bec_lib` to automatically append the pytest plugins within a Python environment. In order to
install all tests dependencies, including `pytest` and all required packages, the easiest is to execute `pip install .[dev]`
from `bec_lib` source directory. In case of modifying BEC code itself, add `-e` argument in order to be able to modify
source code in-place and have changes immediately reflected at execution time (otherwise, each test would require an
installation for changes to be propagated to the environment). 

## General purpose fixtures

General purpose fixtures are useful for all tests, or unit tests.

### Threads check

The `threads_check` fixture ensures a test does not leak threads. Indeed, it is important for tests reproducibility and for
BEC integrity to make sure that threads do not remain alive across tests execution, and that BEC provides the mechanism to
shut down threads gracefully to guarantee proper freeing of resources.

Just add `threads_check` fixture to a test to enable this check:

```
def my_test(threads_check, ...): #threads_check + other fixtures probably
    # write your test code
    ...
```

In case of thread leak, verify if the newly added code is creating threads in the wild, and fix it by providing a `.shutdown()`
method (for example) to stop threads. The test code has to call `.shutdown()` methods (or equivalent) of BEC objects. Some
threads may come from third-party libraries, like `loguru` which is used by BEC for logging. In case of `loguru`, call
`bec_logger.logger.remove()` to clean it.

It could be useful to automatically enable the threads check and `loguru` cleaning for all tests ; in this case, add the
following fixture to a `conftest.py` file with your new test files:

```
from bec_lib.logger import bec_logger
def auto_check_threads(threads_check):
    yield
    bec_logger.logger.remove()
```

### dm and dm_with_devices

The `dm` fixture provides a mock `DeviceManager` object, to be used in BEC unit tests.

The `dm_with_devices` fixture provides a mock `DeviceManager` object, with fake test devices already loaded.

### bec_client_mock

The `bec_client_mock` fixture provides a mock BEC client, initialized with a mock `DeviceManager` loaded with fake test devices.

It does not make any network connection and do not provide simulated hardware - it is meant to be used in unit tests.

## End-2-end fixtures

End-2-end fixtures provide convenient fixtures to automatically start Redis and BEC servers, in order to make integration
tests.

### Starting of BEC servers

`bec_servers` is a fixture, ensuring Redis and all BEC servers are started and ready before the test starts.
By default, this fixture just makes the Redis server check - it does not automatically start servers.

However, `--start-servers` command line argument can be specified on pytest command line in order to start servers automatically:

- `--start-servers`: start all servers automatically
    - the fixture will always check that Redis server is running, regardless if servers have been started automatically or not
- `--files-path` can be used to specify the directory where to put output files (config.yaml, log files, file writer .h5 files)
    - only works if `--start-servers` is enabled, since it depends how BEC servers have been started
- `--bec-redis-host`: indicates which host is running Redis
    - by default, tests will try to connect to `localhost:6379`
    - this is mainly useful when Redis is running within Docker on a different network for example, for the CI
- `--flush-redis`: specifies if Redis should be flushed at the end of the test
    - by default Redis stays filled with keys added during the test
    - the scope of the `bec_servers` fixture changes dynamically from "session" to "function" if `--flush-redis` option is given
    - note: if Redis flushing is enabled, BEC servers are restarted at each test (need `--start-servers`, see above)
- `--bec-redis-cmd` can be used to specify the command line for Redis execution
    - could be `docker run ...` for example, otherwise it defaults to `redis-server`

### BEC client fixtures
Depending how the new code interfaces with BEC, the following fixtures provide a `BECClient` object:

- `bec_client_with_demo_config` is a fixture returning a `BECIPythonClient` object, which also initializes BEC with the demo config
(simulation hardware).
- _`bec_client_lib_with_demo_config` returns a simple `BECClient` object instead of IPython client)._
- `bec_client_fixture` provides a BEC IPython object similar to `bec_client_with_demo_config`, with a cleaned scan queue.
- _`bec_client_lib` is the same as `bec_client_fixture`, but with a `BECClient` object (no IPython)._

Typically, a GUI application on top of BEC testing access to simulation hardware and scans may want to use `bec_client_lib`,
i.e. a full-fledged BEC client without IPython. A test for a new scan procedure to be executed from the BEC shell may want
to use `bec_client_fixture`.



