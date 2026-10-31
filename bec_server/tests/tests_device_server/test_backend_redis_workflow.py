"""Real Redis service-boundary workflows; not a full scan-server integration."""

from __future__ import annotations

import shutil
import socket
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import numpy as np
import pytest
import redis
from async_backend_fixture import AsyncProvider
from ophyd import Component as Cpt
from ophyd import Device, Signal
from ophyd_devices.bec_adapters import OphydProvider
from ophyd_devices.utils.bec_signals import (
    DynamicSignal,
    FileEventSignal,
    PreviewSignal,
    ProgressSignal,
)
from ophyd_devices.utils.psi_device_base_utils import StatusBase

from bec_lib import messages
from bec_lib.device_backend import DeviceEvent, DeviceIdentity, EventKind, MessageChannel
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_server.device_server.device_server import DeviceServer, RequestHandler
from bec_server.device_server.devices.devicemanager import DeviceManagerDS
from bec_server.device_server.rpc_handler import RPCHandler


def until(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        threading.Event().wait(0.01)
    raise AssertionError(f"Workflow condition not reached; last result: {last!r}")


@pytest.fixture(scope="module")
def redis_address(tmp_path_factory):
    executable = shutil.which("redis-server")
    if executable is None:
        pytest.skip("Required workflow environment: redis-server is unavailable")
    directory = tmp_path_factory.mktemp("hal-redis")
    with socket.socket() as address:
        address.bind(("127.0.0.1", 0))
        port = address.getsockname()[1]
    log = (directory / "redis.log").open("w")
    process = subprocess.Popen(
        [
            executable,
            "--bind",
            "127.0.0.1",
            "--port",
            str(port),
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            str(directory),
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    client = redis.Redis(host="127.0.0.1", port=port)
    try:

        def ready():
            try:
                return client.ping()
            except redis.ConnectionError:
                return False

        until(ready)
        yield f"127.0.0.1:{port}", client
    finally:
        client.close()
        process.terminate()
        try:
            process.wait(3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(3)
        log.close()


class MessageTree(Device):
    value = Cpt(Signal, value=0, kind="hinted")
    file = Cpt(FileEventSignal)
    progress = Cpt(ProgressSignal)
    preview = Cpt(PreviewSignal, ndim=1)
    dynamic = Cpt(DynamicSignal, signals=["counts"], max_size=100)

    def __init__(self, name):
        super().__init__(name=name)
        self.value._auto_monitor = True
        self.started = threading.Event()
        self.pending = None

    def trigger(self):
        self.pending = StatusBase(obj=self)
        self.started.set()
        return self.pending

    def stop(self, *, success=False):
        if self.pending is not None and not self.pending.done:
            self.pending.set_exception(RuntimeError("Simulated acquisition stopped"))

    def complete(self):
        operation = StatusBase(obj=self)
        operation.set_finished()
        return operation


class ExistingDeviceProvider(OphydProvider):
    def load_class(self, specification):
        return (
            MessageTree
            if specification == "proof.MessageTree"
            else super().load_class(specification)
        )


@pytest.mark.parametrize("backend", ["existing", "asyncio"])
def test_request_push_interruption_and_shutdown_over_real_redis(redis_address, backend):
    address, redis_client = redis_address
    # This dedicated Redis belongs to the module fixture; no existing service is touched.
    redis_client.flushall()
    provider = ExistingDeviceProvider() if backend == "existing" else AsyncProvider()
    service_connector = RedisConnector(address, name="HALWorkflowService")
    client = RedisConnector(address, name="HALWorkflowClient")
    parent = SimpleNamespace(
        connector=service_connector, device_provider=provider, _service_name="device_server"
    )
    manager = DeviceManagerDS(parent, device_provider=provider)
    manager.config_update_handler = None
    config = {
        "name": "proof",
        "deviceClass": "proof.MessageTree" if backend == "existing" else "proof.AsyncDevice",
        "enabled": True,
        "readOnly": False,
        "readoutPriority": "monitored",
        "deviceConfig": {},
    }
    server = DeviceServer.__new__(DeviceServer)
    server.device_provider, server.device_manager, server.connector = (
        provider,
        manager,
        service_connector,
    )
    server._send_service_status = lambda: None
    server.requests_handler = RequestHandler(server)
    server.rpc_handler = RPCHandler(server)
    server.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="HALWorkflowRequest")
    responses = []
    response_lock = threading.Lock()
    try:
        raw, remaining = manager.construct_device_obj(config, manager)
        manager.initialize_device(config, remaining, raw)
        manager.resolve_device_dependencies([config])
        manager.scan_info.msg = SimpleNamespace(scan_id="proof-scan", status="running")
        handler = manager.devices.proof.handler
        service_connector.register(
            MessageEndpoints.device_instructions(), cb=server.instructions_callback
        )
        service_connector.register(MessageEndpoints.stop_devices(), cb=server.on_stop_devices)

        def received(message):
            with response_lock:
                responses.append(message.value)

        client.register(MessageEndpoints.device_instructions_response(), cb=received)
        topics = [
            MessageEndpoints.device_instructions(),
            MessageEndpoints.stop_devices(),
            MessageEndpoints.device_instructions_response(),
        ]
        until(
            lambda: all(redis_client.pubsub_numsub(topic.endpoint)[0][1] >= 1 for topic in topics)
        )

        def submit(action, parameter=None):
            identifier = str(uuid4())
            instruction = messages.DeviceInstructionMessage(
                device="proof",
                action=action,
                parameter=parameter or {},
                metadata={"device_instr_id": identifier, "RID": identifier},
            )
            client.send(MessageEndpoints.device_instructions(), instruction)
            return identifier

        def terminal(identifier):
            with response_lock:
                return next(
                    (
                        response
                        for response in responses
                        if response.instruction_id == identifier
                        and response.status in ("completed", "error")
                    ),
                    None,
                )

        stage_id = submit("stage")
        assert until(lambda: terminal(stage_id)).status == "completed"
        trigger_id = submit("trigger")
        native = raw if backend == "existing" else handler
        assert native.started.wait(3)
        assert terminal(trigger_id) is None

        # Native push reaches the real Redis cache while acquisition is still pending.
        if backend == "existing":
            raw.value.put(9)
        else:
            handler.transport_update(9)
        expected_key = "proof_value" if backend == "existing" else "proof"
        until(
            lambda: (message := client.get(MessageEndpoints.device_readback("proof"))) is not None
            and message.signals[expected_key]["value"] == 9
        )
        assert terminal(trigger_id) is None

        if backend == "existing":
            raw.file.put(file_path="proof.h5", done=True, successful=True)
            raw.progress.put(value=1, max_value=2, done=False)
            raw.preview.put(np.arange(4))
            raw.dynamic.put(
                {raw.dynamic.name: {"value": np.arange(3), "timestamp": time.time()}},
                async_update={"type": "add", "max_shape": [None]},
            )
        else:
            handler.publish_file("proof.h5")
            provider.run(
                lambda: handler._emit(
                    DeviceEvent(
                        DeviceIdentity("proof", "progress", "proof_progress"),
                        EventKind.MESSAGE,
                        channel=MessageChannel.PROGRESS,
                        message=messages.ProgressMessage(value=1, max_value=2, done=False),
                    )
                )
            )
            provider.run(
                lambda: handler._emit(
                    DeviceEvent(
                        DeviceIdentity("proof", "preview", "proof_preview"),
                        EventKind.MESSAGE,
                        channel=MessageChannel.PREVIEW,
                        message=messages.DevicePreviewMessage(
                            device="proof", signal="preview", data=np.arange(4)
                        ),
                    )
                )
            )
            provider.run(
                lambda: handler._emit(
                    DeviceEvent(
                        DeviceIdentity("proof", "dynamic", "proof_dynamic"),
                        EventKind.MESSAGE,
                        channel=MessageChannel.DYNAMIC,
                        signal_metadata={"max_size": 100},
                        message=messages.DeviceMessage(
                            signals={"counts": {"value": np.arange(3), "timestamp": time.time()}},
                            metadata={"async_update": {"type": "add", "max_shape": [None]}},
                        ),
                    )
                )
            )
        assert (
            until(lambda: client.get(MessageEndpoints.file_event("proof"))).file_path == "proof.h5"
        )
        assert (
            client.get(MessageEndpoints.public_file(scan_id="proof-scan", name="proof")).file_path
            == "proof.h5"
        )
        progress = until(lambda: client.get(MessageEndpoints.device_progress("proof")))
        assert progress.metadata["scan_id"] == "proof-scan"
        preview = until(
            lambda: client.xrange(
                MessageEndpoints.device_preview(device="proof", signal="preview"), "-", "+"
            )
        )
        assert preview[-1]["data"].data.tolist() == [0, 1, 2, 3]
        dynamic_name = "proof_dynamic"
        dynamic = until(
            lambda: client.xrange(
                MessageEndpoints.device_async_signal(
                    scan_id="proof-scan", device="proof", signal=dynamic_name
                ),
                "-",
                "+",
            )
        )
        assert (
            dynamic[-1]["data"].metadata["async_indices"][
                dynamic_name if backend == "existing" else "counts"
            ]
            == 0
        )

        client.send(MessageEndpoints.stop_devices(), messages.VariableMessage(value=["proof"]))
        interrupted = until(lambda: terminal(trigger_id))
        assert interrupted.status == "error"
        assert interrupted.error_info.device == "proof"
        assert (
            len(
                [
                    response
                    for response in responses
                    if response.instruction_id == trigger_id
                    and response.status in ("completed", "error")
                ]
            )
            == 1
        )
        complete_id = submit("complete")
        assert until(lambda: terminal(complete_id)).status == "completed"
        unstage_id = submit("unstage")
        assert until(lambda: terminal(unstage_id)).status == "completed"
    finally:
        manager.shutdown()
        server.executor.shutdown(wait=True, cancel_futures=True)
        client.shutdown()
        service_connector.shutdown()
    if backend == "asyncio":
        assert provider.closed
        assert not provider.thread.is_alive()
