(developer.bec_cli)=
# Command-line interface (CLI)

BEC provides a command-line interface (CLI) as an interface for users to interact with the BEC server. The CLI is based on the [*IPython*](https://ipython.readthedocs.io/en/stable/) interactive shell, which provides a powerful and flexible environment for users to interact with the BEC server. To get an idea of the capabilities of the CLI, have a look at the [user section](user.command_line_interface).

## Customizing the CLI
There are several ways to customize the CLI to suit your needs. BEC allows users to define pre- and post-startup scripts that are executed before and after the CLI is started. These extensions are implemented through [*BEC plugin*](developer.bec_plugins), more specifically within the *bec_ipython_client/startup* module.

**Pre-startup script**: The pre-startup script can be used to extend the command line arguments for starting the CLI. For example, you can add a custom session name to the CLI that allows to identify the session in the post-startup script, i.e. 

```bash
bec --session my_session
```
````{dropdown} View code: Pre-startup script
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../bec_lib/util_scripts/plugin_setup_files/pre_startup.py
:language: python
```
````

**Post-startup script**: The post-startup script will be executed after the CLI is started. This script allows you to customize the CLI environment, e.g. by loading custom Python modules, setting reusable variables, or creating fully customized objects that represent experimental setups.

````{dropdown} View code: Post-startup script
:icon: code-square
:animate: fade-in-slide-down

```{literalinclude} ../../../../bec_lib/util_scripts/plugin_setup_files/post_startup.py
:language: python
```
````

### Customizing CLI print behaviour
We also allow you to dynamically change the printed output of the CLI by adjusting the variables of the live updates config object. This includes the live table printout and the client-info messages. You can adjust the following variables in the *bec.live_updates_config* object:

```python
bec.live_updates_config.print_live_table = False #True
bec.live_updates_config.print_client_info = False #True
```  

**Client-Info messages**: The *RedisConnector* class provides a method to print client-info messages to the CLI. These messages can be send from any service or device, and will be printed in the CLI either immediately (show_asap=True) or at the end of a scan. Please keep in mind that all services have access to the *RedisConnector* class, and can send client-info messages to the CLI.

``` python
from bec_lib.redis_connector import RedisConnector
bootstrap = "localhost:6379" 
connector = RedisConnector(bootstrap)
connector.send_client_info(message="This is a client info message", show_asap=True)
```



