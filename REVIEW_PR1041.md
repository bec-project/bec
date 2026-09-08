# Review notes for PR #1041 (`fix/add_to_config_disabled`)

This branch sits on top of the PR head `f6cf9224` and adds two probe files:

- `bec_server/tests/tests_scihub/test_review_pr1041_scihub.py`
- `bec_server/tests/tests_device_server/test_review_pr1041_device_server.py`

Each probe asserts the **desired** behaviour, so on the PR head it **fails** and
the assertion message describes the defect. The branch is intentionally red and
is not meant to be merged. Once a change lands, the matching probe passes and
can be kept as a regression test or deleted.

```bash
pytest -q bec_server/tests/tests_scihub/test_review_pr1041_scihub.py bec_server/tests/tests_device_server/test_review_pr1041_device_server.py
```

Expected on `f6cf9224`: `8 failed`. Only the repo's existing fixtures are used
(`config_handler`, `dm_with_devices`), so no services need to be running.

The findings below are ordered by severity. Section 9 lists what the PR's
second commit already fixed, for context.

---

## 1. scihub waits 30 s for an enable, the device server honours an unbounded `connectionTimeout`

**Where.** `bec_server/bec_server/scihub/atlas/config_handler.py`,
`ConfigHandler._update_device_config`, `enabled` branch:

```python
self._update_device_server(request_id, {device.name: dev_config})
updated, msg = self._wait_for_device_server_update(request_id)   # default timeout_time=30
```

`_set_config` and `_add_to_config` scale their wait with the number of devices
(`min(300, 30 * len(...))`), but none of the three paths looks at the device's
`connectionTimeout`. On the device server, `DeviceManagerDS.initialize_device`
calls `connect_device(obj, wait_for_all=True, timeout=dev.get("connectionTimeout", 5))`,
and `connectionTimeout` (`bec_lib/bec_lib/atlas_models.py`) has no upper bound.
scihub never sends a `cancel` when it gives up; only `_cancel_config_request`
does, and that is triggered by a client cancel.

**What happens.** With `connectionTimeout` above 30 s, or a device whose
`wait_for_connection(all_signals=True)` is slow, scihub raises `TimeoutError`
after 30 s and replies `accepted=False`. The device server keeps working and
writes its own reply to a key nobody reads (`expire=60`).

- *update / enable*: the device server can finish successfully and enable the
  device, while scihub, redis, the scan server and all clients keep
  `enabled=False`. Because the PR now (correctly) defers scihub's own write
  until the device server confirms, the PR changes which side is stale but
  does not close the gap.
- *add*: the device server registers the device (disabled if the connection
  failed) while scihub writes nothing to redis and broadcasts nothing. A retry
  of the add is rejected by the device server ("already exists"), a remove is
  rejected by scihub ("not found in the device manager"), until a config
  reload.

**Reproduce.**

- Probes: `test_enable_wait_covers_connection_timeout`,
  `test_enable_timeout_cancels_device_server_request`.
- Live, from the IPython client (no IOC needed):

  ```python
  bec.device_manager.config_helper.send_config_request(
      action="add",
      config={"slow_motor": {"deviceClass": "ophyd.EpicsMotor",
                             "deviceConfig": {"prefix": "BEC:DEMO:UNREACHABLE:"},
                             "connectionTimeout": 45, "readoutPriority": "baseline",
                             "enabled": True, "readOnly": False}})
  ```

  After ~30 s the client gets a `DeviceConfigError` carrying a `TimeoutError`
  traceback. Watch the device server log: ~15 s later it finishes and adds the
  device disabled. Sending the same add again is now rejected with "already
  exists"; a remove is rejected with "not found". Recover with a config reload.

**Possible fix.**

- In `_update_device_config` (and in `_add_to_config`/`_set_config` using the
  maximum over the devices in the request), derive the wait from the device
  config: `timeout_time = max(30, connectionTimeout + margin)`.
- On `TimeoutError`, send `action="cancel"` to the device server (reuse the
  block from `_cancel_config_request`) and, if the device server reports it
  already completed, apply the update locally instead of dropping it.
- Alternatively pass a deadline in the request metadata and make the device
  server's enable/add path abort past it.

---

## 2. `_add_to_config` indexes the request with every reported failed device

**Where.** `bec_server/bec_server/scihub/atlas/config_handler.py`,
`ConfigHandler._add_to_config`:

```python
for dev in failed_devices:
    dev_configs[dev]["enabled"] = False
```

**What happens.** `failed_devices` comes from the device server reply, where
`ConfigUpdateHandler.parse_config_request` attaches `dm.failed_devices` **by
reference** and serialises it only in the `finally` block. That dict is a
single attribute shared by the startup loader (`DeviceManagerDS._init_device`,
main thread) and by `_add_config`, which runs on the config executor thread and
starts with `self.device_manager.failed_devices = {}`. `_rollback_added_devices`
resets it as well.

- An add processed while the device server is still loading its startup
  config can carry a startup device name in `failed_devices`. scihub then hits
  a `KeyError`, replies `accepted=False` and writes nothing, although the device
  server already registered the requested device and added it to its session.
  The next add of that device is rejected with "already exists".
- The `= {}` resets wipe startup failures that were recorded before the add,
  so `handle_failed_device_inits` at the end of `_load_session` no longer sees
  them and those devices stay enabled with unconnected objects, which is the
  bug the PR fixes.

The same `KeyError` occurs for any sender whose request key differs from
`dev_config["name"]`; scihub itself forces them equal, so this is only relevant
for direct publishers.

**Reproduce.** Probe `test_foreign_failed_device_does_not_abort_add`. Live it is
a race (send an add from the client while the device server is restarting with
a slow, unreachable startup device), so it is not reliable to demonstrate.

**Possible fix.**

- scihub: `for dev in failed_devices.keys() & dev_configs.keys():` and log any
  extra names.
- device server: collect failures of an add in a local dict and attach a copy
  to the reply, leaving the startup dict alone; take a snapshot
  (`dict(...)`) before attaching it to `msg.metadata`.

---

## 3. Multi-device `update`: partial success is applied but never broadcast

**Where.** `bec_server/bec_server/scihub/atlas/config_handler.py`,
`ConfigHandler._update_config`:

```python
for dev, config in dev_configs.items():
    ...
    updated = self._update_device_config(device, config.copy())
    if updated:
        self.update_config_in_redis(device)
# send updates to services
if updated:
    self.send_config(msg)
    self.send_config_request_reply(accepted=True, ...)
```

**What happens.** For `{"samx": {"enabled": True}, "unreachable": {"enabled": True}}`
the first device is enabled on the device server, written to redis and marked
enabled in scihub memory. The second device raises `DeviceConfigError` out of
`_update_device_config`, `parse_config_request` replies `accepted=False`, and
`send_config` never runs. The scan server and every client keep
`samx.enabled == False`, so scans omit a device the device server is reading.
The client prints `Failed to update the config: ... No devices were updated.`,
which is false. The public `ConfigHelper.send_config_request(action="update",
config={...})` accepts multi-device dicts; the `Device` setters and bec_widgets
send one device at a time.

**Reproduce.**

- Probe: `test_partial_multi_device_update_is_broadcast`.
- Live, with a healthy sim motor and an unreachable EPICS motor, both disabled:

  ```python
  dev.samx.enabled = False
  bec.device_manager.config_helper.send_config_request(
      action="update",
      config={"samx": {"enabled": True}, "unreachable_epics_motor": {"enabled": True}})
  # -> DeviceConfigError "... No devices were updated."
  dev.samx.enabled                       # False in the client
  from bec_lib.endpoints import MessageEndpoints
  [d for d in bec.connector.get(MessageEndpoints.device_config()).content["resource"]
   if d["name"] == "samx"]               # enabled: True in redis
  ```

**Possible fix.** Track the successfully applied subset in `_update_config`;
on an exception broadcast `DeviceConfigMessage(action="update", config=applied)`
and include `metadata["updated_config"] = True` (or an `updated_devices` list)
in the failure reply so `ConfigHelper.handle_update_reply` reports what really
happened. Alternatively make the update all-or-nothing by sending
`{name: {"enabled": False}}` rollbacks to the device server for the applied
devices before re-raising.

---

## 4. Two writers of the redis `device_config` key; a stale scihub copy re-enables a device

**Where.**

- `bec_server/bec_server/device_server/devices/config_update_handler.py`,
  `ConfigUpdateHandler.handle_failed_device_inits` →
  `force_update_config_in_redis`: writes `MessageEndpoints.device_config()`
  directly and sends no `device_config_update` broadcast.
- `bec_server/bec_server/scihub/atlas/config_handler.py`,
  `ConfigHandler.update_config_in_redis`: `config[index] = device._config`
  replaces the whole redis entry with scihub's in-memory copy.
- `bec_lib/bec_lib/devicemanager.py`: services subscribe to
  `device_config_update()` only; `device_config()` is read at startup/reload.

**What happens.** The device server restarts while an IOC is down and scihub
keeps running. The device server disables the device locally and writes redis,
but nobody is told, so scihub's `devices[name]._config["enabled"]` stays `True`
(so does every client's). Any later accepted update on that device that needs
no device-server round trip, e.g. `readoutPriority` or `deviceTags`, goes
through `update_config_in_redis` and writes the stale entry back: redis now
says `enabled: True` while the device server has the device disabled and
unconnected. The broadcast carries only the changed key, so live services do
not flip either; the next reload or a new client re-reads redis and retries the
dead PV, and `save_current_session` would persist `enabled: True`.

The PR extends the split rather than closing it: `add` flips `enabled` on the
scihub side, `update` relies on the device server reply, startup/reload writes
scihub's key from the device server.

**Reproduce.**

- Probe: `test_stale_scihub_copy_does_not_reenable_device_in_redis`.
- Live (involved): with a device that is `enabled: True` in redis but
  unreachable, restart only scihub, then restart only the device server
  (`bec-server attach`, then `bec-scihub` / `bec-device-server` in their
  windows). Now send an unrelated update for the device, e.g.
  `send_config_request(action="update", config={name: {"readoutPriority": "baseline"}})`,
  and read the redis config: the device is enabled again.

**Possible fix.** Single owner of the key. The device server should *report*
failures instead of writing scihub's key: publish an ordinary
`DeviceConfigMessage(action="update", config={name: {"enabled": False}})` on
`device_config_update()` (or send a `device_config_request`) so scihub applies
it, updates redis and broadcasts, and every service including scihub's own
device manager converges. Then `force_update_config_in_redis` can go. Independently,
make `update_config_in_redis` merge only the keys that changed instead of
replacing the entry, so a stale copy can never resurrect a field it did not
touch.

---

## 5. e2e test: unmatched `pytest.raises` and a lookup outside the block

**Where.** `bec_ipython_client/tests/end-2-end/test_scans_e2e.py`,
`test_unreachable_device_stays_disabled_when_enabled_twice`:

```python
with pytest.raises(DeviceConfigError):
    bec.device_manager.config_helper.send_config_request(action="add", config=config)
device = dev[device_name]          # outside the block
...
with pytest.raises(DeviceConfigError):
    device.enabled = True
```

**What happens.** Every failure mode of `send_config_request` raises
`DeviceConfigError` (rejected request, config-reply timeout, service-ack
timeout, and the intended "The following devices were disabled"). Only the
intended one leaves the device in the client container. In every other case
`dev[device_name]` (plain `dict.__getitem__`) raises a bare `KeyError`, and the
test fails on the wrong line with no hint of the real cause. A concrete
rejection path: a device server running with `OPHYD_CONTROL_LAYER=dummy`
(the value the device-server test conftest selects) raises in the `EpicsMotor`
constructor, so the add is rejected instead of added-disabled. The second
`pytest.raises` block equally accepts a 32 s "Timeout reached whilst waiting
for config reply". The `finally` cleanup runs only when the *client* holds the
device, so a device that exists only on the device server is not removed.

**Reproduce.** Probe `test_e2e_lookup_after_rejected_add_gives_a_clear_error`.
Live: start the servers with `OPHYD_CONTROL_LAYER=dummy` and run the e2e test;
it errors with `KeyError: 'unreachable_epics_motor'`.

**Possible fix.**

```python
with pytest.raises(DeviceConfigError, match="following devices were disabled"):
    ...send_config_request(action="add", config=config)
assert device_name in dev
device = dev[device_name]
...
with pytest.raises(DeviceConfigError, match=r"Failed to update device unreachable_epics_motor"):
    device.enabled = True
```

and an unconditional `finally` cleanup that logs a `DeviceConfigError` instead
of masking the primary assertion.

---

## 6. Enable-failure path uses a different recovery strategy than every other path

**Where.** `bec_server/bec_server/device_server/devices/config_update_handler.py`,
`ConfigUpdateHandler._update_config`, `except` block of the enable branch:

```python
except Exception:
    device._config["enabled"] = was_enabled
    failed_device = self.device_manager.devices.get(dev)
    if failed_device is not None and failed_device is not device:
        failed_device._config["enabled"] = was_enabled
        self.device_manager.devices._add_device(dev, device)
        self._cleanup_failed_device_init(failed_device.obj, failed_device)
    elif obj is not None:
        self._cleanup_failed_device_init(obj)
    raise
```

**What happens.** This swaps the *old* `DSDevice` back and destroys the new
one. `_add_config`, `DeviceManagerDS._init_device` and
`handle_failed_device_inits` all keep the *new* `DSDevice`, disabled, with a
destroyed object. The end states are equivalent in `enabled`, `initialized` and
`connected`, but the swap-back leaves an object that was never destroyed in the
container, needs a write to `failed_device._config` on an object that is
discarded on the next line, and a `.get()` guard that cannot be `None`
(`devices[dev]` succeeded at the top of the loop and nothing removes entries in
between). `test_config_handler_failed_enable_cleans_and_restores_old_device`
pins object identity, which locks the divergence in.

**Reproduce.** Probe `test_failed_enable_leaves_a_destroyed_object_like_failed_add`.
Not observable from outside the device server process.

**Possible fix.** Use the keep-new form, matching `_add_config`:

```python
except Exception:
    current = self.device_manager.devices[dev]
    current._config["enabled"] = was_enabled
    if obj is not None:
        self._cleanup_failed_device_init(obj, current)
    raise
```

and rewrite the test to assert `devices[name].enabled is False`,
`.initialized is False`, `.obj._destroyed is True`, as
`test_config_handler_update_config` already does. With this form all existing
PR tests pass except the identity assertions.

---

## 7. By-reference mock assertion in the new scihub test

**Where.** `bec_server/tests/tests_scihub/test_atlas_config_handler.py`,
`test_config_handler_add_to_config_disables_failed_devices`:

```python
add_devices.assert_called_once_with(config)
```

**What happens.** `DeviceConfigMessage(config=config)` shares the inner dicts
with the caller, so the mock's recorded argument *is* the dict the handler
mutates. The assertion passes whether the `enabled=False` flip happened before
or after `add_devices_to_redis` was called. If the flip loop were moved below
the redis write, redis would be written with `enabled=True` and this test would
still pass; only the end-state assertion on `config` covers the flip itself.

**Reproduce.** Probe `test_by_reference_assertion_detects_a_late_flip`.

**Possible fix.** Snapshot at call time:

```python
snapshots = []
with mock.patch.object(config_handler, "add_devices_to_redis",
                       side_effect=lambda c: snapshots.append(copy.deepcopy(c))):
    ...
assert snapshots[0]["failed_device"]["enabled"] is False
```

---

## 8. Unrelated test rewrite bundled into the fix

**Where.** `bec_server/tests/tests_device_server/test_device_manager_ds.py`,
the `file_event` and `progress` signal tests: the mock target changes to
`device_manager._bec_message_handler.connector` and exact-call assertions are
added.

**What happens.** The PR does not change `devicemanager.py` or
`bec_message_handler.py`; `_bec_message_handler` has existed since
`06b8f039` and its connector is the same object as `device_manager.connector`.
The base version of both tests passes unchanged against the PR code. `AGENTS.md`
asks to "Keep diffs focused. Avoid unrelated refactors while fixing a specific
issue."

**Reproduce.**

```bash
git show origin/main:bec_server/tests/tests_device_server/test_device_manager_ds.py > bec_server/tests/tests_device_server/test_zz_base_dm_ds.py
pytest -q bec_server/tests/tests_device_server/test_zz_base_dm_ds.py -k "obj_callback_file_event_signal or obj_callback_progress_signal"
rm bec_server/tests/tests_device_server/test_zz_base_dm_ds.py
```

Expected: `7 passed`.

**Possible fix.** Move those hunks to a separate `test:` commit or PR.

---

## 9. Fixed by the PR's second commit (`f6cf9224`), for context

These were found on the first PR head and are addressed now; each is covered by
the tests that commit adds.

- **Phantom device after a failed add.** A failure before `_add_device` now
  rejects the add, and `_rollback_added_devices` removes the batch from the
  container, deletes its redis data, restores the session list and clears
  `failed_devices`. Previously the reply was `accepted=True` and the device
  ended up in the session, redis and every client but not in `dm.devices`.
- **Disable path wrote `enabled=False` before disconnecting.** The flag is now
  written after `disconnect_device`/`reset_device` succeed, so a destroy
  failure no longer leaves the device server and scihub disagreeing.
- **Startup/reload path leaked the failed object and left the session stale.**
  The `if not obj.connected: return` guard was removed from `disconnect_device`,
  `handle_failed_device_inits` now destroys failed objects and syncs the
  session via `update_session_config`, and the duplicate call from
  `_reload_config` is gone.
- **Partial multi-device add left earlier devices registered but unrecorded.**
  Covered by the same rollback; a retry no longer fails with "already exists".
  The asymmetry (constructor failures reject the whole batch, connection
  failures add the device disabled) is now explicit and tested.

To watch those regression tests fail without the fix, revert only the source
part of the commit, run them, and restore:

```bash
git diff 5d24328e f6cf9224 -- bec_server/bec_server | git apply -R
pytest -q bec_server/tests/tests_device_server/test_config_handler.py bec_server/tests/tests_device_server/test_device_manager_ds.py -k "failed_disable_preserves or disable_destroys_disconnected or rejected_add_rolls_back or survives_redis_cleanup or load_unreachable_device_cleans_up or add_preserves_existing_device_data"
git checkout -- bec_server/bec_server
```
