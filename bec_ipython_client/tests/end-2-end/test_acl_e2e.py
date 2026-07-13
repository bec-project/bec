import shutil

import pytest
import redis.exceptions

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_lib.service_config import ServiceConfig, ServiceConfigModel
from bec_lib.tests.utils import wait_for_empty_queue
from bec_lib.utils.user_acls_test import BECAccessDemo
from bec_server.bec_server_utils.service_handler import ServiceHandler


@pytest.fixture
def acl_enabled_bec_services(
    request,
    bec_files_path,
    bec_services_config_file_path,
    bec_test_config_file_path,
    bec_redis_host_port,
    test_config_yaml_file_path,
):
    if not request.config.getoption("--start-servers") or not request.config.getoption(
        "--flush-redis"
    ):
        pytest.skip("ACL e2e test mutates Redis ACLs and requires isolated pytest BEC services.")

    redis_host, redis_port = bec_redis_host_port
    redis_url = f"{redis_host}:{redis_port}"
    acl_env_file = bec_services_config_file_path.parent / ".bec_acl.env"
    acl_env_file.write_text("REDIS_USER=admin\nREDIS_PASSWORD=admin\n", encoding="utf-8")
    shutil.copyfile(test_config_yaml_file_path, bec_test_config_file_path)

    service_config = ServiceConfigModel(
        redis={"host": redis_host, "port": redis_port},
        file_writer={"base_path": str(bec_services_config_file_path.parent)},
        acl={"env_file": str(acl_env_file)},
    )
    bec_services_config_file_path.write_text(
        service_config.model_dump_json(indent=4), encoding="utf-8"
    )

    service_handler = ServiceHandler(
        bec_path=bec_files_path, config_path=bec_services_config_file_path, interface="subprocess"
    )
    processes = None
    try:
        acl_connector = RedisConnector(redis_url)
        try:
            access_control = BECAccessDemo(acl_connector)
            access_control.reset()
            access_control.add_user()
            access_control.add_admin()
            access_control.set_default_non_admin()
        finally:
            acl_connector.shutdown()

        processes = service_handler.start()
        yield bec_services_config_file_path
    finally:
        if processes is not None:
            service_handler.stop(processes)
        restore_connector = RedisConnector(redis_url)
        try:
            BECAccessDemo(restore_connector).reset()
        finally:
            restore_connector.shutdown()


@pytest.mark.timeout(120)
def test_acl_admin_server_allows_default_user_scan(acl_enabled_bec_services):
    admin_config = ServiceConfig(acl_enabled_bec_services)
    admin_client = BECClient(admin_config, RedisConnector, forced=True, wait_for_server=True)
    admin_client.start()
    try:
        admin_client.config.load_demo_config(force=True)
    finally:
        admin_client.shutdown()
        admin_client._client._reset_singleton()

    user_config = ServiceConfig(acl_enabled_bec_services, acl={"env_file": "", "user": "user"})
    user_client = BECClient(user_config, RedisConnector, forced=True, wait_for_server=True)
    user_client.start()
    try:
        user_client.queue.request_queue_reset()
        user_client.queue.request_scan_continuation()
        wait_for_empty_queue(user_client)
        assert user_client.username == "user"

        dev = user_client.device_manager.devices
        status = user_client.scans.line_scan(
            dev.samx, -0.1, 0.1, steps=3, exp_time=0.01, relative=False
        )
        status.wait(num_points=True, file_written=True)

        assert status.scan.num_points == 3
    finally:
        user_client.shutdown()
        user_client._client._reset_singleton()


@pytest.mark.timeout(120)
def test_acl_account_endpoint_allows_user_read_but_admin_only_write(acl_enabled_bec_services):
    account_endpoint = MessageEndpoints.account()
    initial_account_msg = messages.VariableMessage(value="initial-admin-account")
    updated_account_msg = messages.VariableMessage(value="updated-admin-account")

    user_config = ServiceConfig(acl_enabled_bec_services, acl={"env_file": "", "user": "user"})
    user_client = BECClient(user_config, RedisConnector, forced=True, wait_for_server=True)
    user_client.start()
    try:
        assert user_client.username == "user"
        with user_client.acl.temporary_user(username="admin", token="admin"):
            user_client._update_username()
            assert user_client.username == "admin"
            user_client.connector.xadd(account_endpoint, {"data": initial_account_msg})
            assert user_client.connector.get_last(account_endpoint, "data") == initial_account_msg

        user_client._update_username()
        assert user_client.username == "user"
        assert user_client.connector.get_last(account_endpoint, "data") == initial_account_msg
        with pytest.raises(redis.exceptions.NoPermissionError):
            user_client.connector.xadd(
                account_endpoint, {"data": messages.VariableMessage(value="user-account")}
            )

        with user_client.acl.temporary_user(username="admin", token="admin"):
            user_client._update_username()
            assert user_client.username == "admin"
            user_client.connector.xadd(account_endpoint, {"data": updated_account_msg})
            assert user_client.connector.get_last(account_endpoint, "data") == updated_account_msg

        user_client._update_username()
        assert user_client.username == "user"
        assert user_client.connector.get_last(account_endpoint, "data") == updated_account_msg
    finally:
        user_client.shutdown()
        user_client._client._reset_singleton()
