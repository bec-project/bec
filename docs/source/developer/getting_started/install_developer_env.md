(developer.install_developer_env)=
# Install developer environment
If your goal is to install BEC in an environment for code development purposes, this section will guide you through the steps.
In contrast to a deployed production system of BEC, this installation will allow you to edit the code base of BEC actively while you are operating the system.
In that sense, installing BEC in `[dev]` mode, is the right choice in case you like to:

- Integrate a new ophyd device at the beamline
- Add a new feature to the code base of BEC
- Allow more flexibility within code base, in particular useful during beamline commissioning
- Explore and adjust the BEC simulation framework to your needs


## Requirements

---
- [python](https://www.python.org) (>=3.10)
- [redis](https://redis.io)
- [tmux](https://github.com/tmux/tmux/wiki) (=3.2)
---


## Step-by-step guide
### Clone BEC

Clone the BEC repository and the ophyd devices repository. If you also want to work on BEC Widgets, you can clone the repository as well.

```bash
git clone https://gitlab.psi.ch/bec/ophyd_devices.git
git clone https://gitlab.psi.ch/bec/bec.git

git clone https://gitlab.psi.ch/bec/bec_widgets.git
```

Additionally, if you have a BEC plugin repository, pull the repository as well.

```bash
git clone https://gitlab.psi.ch/bec/<PLUGIN_REPO_NAME>.git
```

### Create a new Python environment
There are several ways to create a new Python environment. Here are a few examples:

`````{tab-set}

````{tab-item} pyenv
If you have [pyenv](https://github.com/pyenv/pyenv) and [pyenv virtualenv](https://github.com/pyenv/pyenv-virtualenv) installed, you can create a new Python environment via

```{code-block} bash
pyenv install 3.11 bec
pyenv local bec 
```

Note that pyenv "local" will create a `.python-version` file in the current directory, which will activate the Python environment when you enter the directory. Alternatively, you can use `pyenv shell bec` to activate the environment only in the current shell.
````

````{tab-item} Python virtual environment
If you have Python 3.11 installed, you can create a new Python environment via

```{code-block} bash
python -m venv ./bec_venv
source ./bec_venv/bin/activate
```

Note that the environment is activated _only_ in the current shell. You can deactivate the environment via `deactivate`.
````

````{tab-item} Conda
If you have conda installed, you can create a new Python environment via

```{code-block} bash
conda create -n bec python=3.11
conda activate bec
```
````
````` 

### Install BEC
Make sure that the Python environment is activated as described above before you install BEC.

```bash
pip install -e './bec/bec_lib[dev]'
pip install -e './bec/bec_ipython_client[dev]'
pip install -e './bec/bec_server[dev]'
pip install -e './ophyd_devices[dev]'
```

```bash
pip install -e './bec_widgets[pyside6,dev]'
```

```bash
pip install -e './<PLUGIN_REPO_NAME>[dev]'
```

```{note}
The extension [dev] will install additional dependencies, which are useful for code development such as `pytest` and `black`.
```

```{note}
BEC Widgets does not come with a default Qt backend. If you want to use the BEC Widgets, you need to install the Qt backend of your choice, e.g. `PyQt6` or `PySide6` by adding the respective backend to the installation command. In the example above, we install the BEC Widgets with the PyQt6 backend. More information can be found in the [BEC Widgets documentation](https://bec-widgets.readthedocs.io/en/latest/).
```

### Install Redis and tmux
Open a new terminal.

`````{tab-set}

````{tab-item} Conda
If you have conda installed, you can install Redis and tmux via

```{code-block} bash
conda install redis-server tmux
redis-server
```
````

````{tab-item} MacOS
On MacOS, you can install Redis via [Homebrew](https://brew.sh) and start the server via
```bash
brew install redis
brew install tmux
redis-server
```
````
`````

Per default, Redis will start on port `6379`.

```{tip}
Redis will create a `dump.rdb`, where it regularly stores data on disk. Make sure that you have a few GB in the directory where you start Redis, i.e. avoid the home directory of the e-account at the beamline.
```

### Start the BEC server
Now we can start the BEC server.
Make sure that you activate the `bec_venv` created above, and that `tmux/3.2` is available as described above.

Then you can start the BEC server
```bash
bec-server start
```
Check the command-line printout for instructions of tmux.
You may open the tmux session to look at the different BEC services via

```bash
bec-server attach
```

and exit the tmux session again via `CTRL+b d` (first press `CTRL+b`, release and press `d`).
```{note}
You can also connect to the tmux session via `tmux attach -t bec` and detach via `CTRL+b d`.
```
Both commands are also highlighted in your command-line interface.

```{note}
Strictly speaking, you do not need to install tmux. However, if you do not use tmux, you need to start each service manually, e.g. `bec-scan-server start`, `bec-device-server start` etc. each in a separate terminal. Tmux simplifies this process by starting all services in a single terminal.
```

### Start the command-line interface

```bash
bec
```

You are now ready to load your first device configuration.
For a quick start, please follow the instructions given in the [user guide](#user.devices.load_demo_config).

### Start services with different port

It could be the case, that port `6379` is already occupied or that you have to run multiple Redis server on the same system.
If this is the case, you can also spin up the system with a modified configuration, e.g. on port `xxxx`.
The redis-server can be passed on a specific port.

```bash
redis-server --port xxxx
```
In addition, you will have to start the bec-server with a customized config.
Please check the example file ``bec/bec_config_template.yaml`` to create a custom config and specify port `xxxx` and pass it to the bec-server upon start

``` bash
bec-server start --config my_new_bec_config.yaml
```
and finally also to the client

```bash
bec --config my_new_bec_config.yaml
```
