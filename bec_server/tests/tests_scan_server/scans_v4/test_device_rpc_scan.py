from unittest import mock

import pytest


@pytest.mark.parametrize(
    ("hook_name",),
    [
        ("prepare_scan",),
        ("open_scan",),
        ("stage",),
        ("pre_scan",),
        ("post_scan",),
        ("unstage",),
        ("close_scan",),
    ],
)
def test_device_rpc_scan_default_noop_hooks_do_not_raise(v4_scan_assembler, hook_name):
    scan = v4_scan_assembler("_v4_device_rpc", "samx", "read", [], {}, rpc_id="rpc-id-123")

    getattr(scan, hook_name)()


def test_device_rpc_scan_core_sends_fire_and_forget_rpc(v4_scan_assembler):
    scan = v4_scan_assembler(
        "_v4_device_rpc",
        "samx",
        "controller.set_mode",
        [1, "fast"],
        {"armed": True},
        rpc_id="rpc-id-123",
    )
    status = mock.MagicMock(_device_instr_id="device-instr-id")
    scan.actions.rpc_call_no_wait = mock.MagicMock(return_value=status)
    scan.actions._status_registry = {"device-instr-id": status}

    scan.scan_core()

    scan.actions.rpc_call_no_wait.assert_called_once_with(
        scan.device, "controller.set_mode", "rpc-id-123", 1, "fast", armed=True
    )
    assert scan.actions._status_registry == {}


def test_device_rpc_scan_is_registered_as_non_scan(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_device_rpc", "samx", "read", [], {}, rpc_id="rpc-id-123")

    assert scan.scan_name == "_v4_device_rpc"
    assert scan.is_scan is False
    assert scan.scan_info.scan_type is None
