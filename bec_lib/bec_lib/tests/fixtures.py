from __future__ import annotations

import copy
import os
import threading
import uuid
from unittest import mock

import pytest
import yaml

from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ClientMock, ConnectorMock, DMClientMock


@pytest.fixture
def threads_check():
    threads_at_start = set(th for th in threading.enumerate() if th is not threading.main_thread())
    yield
    threads_after = set(th for th in threading.enumerate() if th is not threading.main_thread())
    additional_threads = threads_after - threads_at_start
    assert (
        len(additional_threads) == 0
    ), f"Test creates {len(additional_threads)} threads that are not cleaned: {additional_threads}"


@pytest.fixture(scope="session")
def test_config_yaml_file_path():
    return os.path.join(os.path.dirname(__file__), "test_config.yaml")


@pytest.fixture(scope="session")
def test_config_yaml(test_config_yaml_file_path):
    with open(test_config_yaml_file_path, "r") as config_yaml_file:
        return yaml.safe_load(config_yaml_file)


@pytest.fixture(scope="session")
def session_from_test_config(test_config_yaml):
    device_configs = []
    session_id = str(uuid.uuid4())
    for name, conf in test_config_yaml.items():
        dev_conf = {
            "id": str(uuid.uuid4()),
            "accessGroups": "customer",
            "name": name,
            "sessionId": session_id,
            "enabled": conf["enabled"],
            "read_only": conf["readOnly"],
        }
        dev_conf.update(conf)
        device_configs.append(dev_conf)
    session = {"accessGroups": "customer", "devices": device_configs}
    return session


@pytest.fixture
def device_manager_class():
    return DMClientMock


@pytest.fixture
def device_manager(device_manager_class):
    service_mock = mock.MagicMock()
    service_mock.connector = ConnectorMock("", store_data=False)
    dev_manager = device_manager_class(service_mock)
    dev_manager.config_update_handler = mock.MagicMock()
    yield dev_manager
    dev_manager.shutdown()


@pytest.fixture
def dm_with_devices(session_from_test_config, device_manager):
    device_manager._session = copy.deepcopy(session_from_test_config)
    device_manager._load_session()
    yield device_manager


@pytest.fixture()
def bec_client_mock(dm_with_devices):
    client = ClientMock(
        ServiceConfig(redis={"host": "host", "port": 123}, scibec={"host": "host", "port": 123}),
        ConnectorMock,
        wait_for_server=False,
        forced=True,
    )
    client.start()
    device_manager = dm_with_devices
    for name, dev in device_manager.devices.items():
        dev._info["hints"] = {"fields": [name]}
    client.device_manager = device_manager
    try:
        yield client
    finally:
        client.shutdown()
        client._reset_singleton()
        device_manager.devices.flush()
