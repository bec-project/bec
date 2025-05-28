from unittest.mock import patch

import pytest
from pydantic import ValidationError

from bec_lib import metadata_schema
from bec_lib.messages import ScanQueueMessage
from bec_lib.metadata_schema import BasicScanMetadata
from bec_lib.scans import Scans

TEST_DICT = {"foo": "bar", "baz": 123}


class ChildMetadata(BasicScanMetadata):
    number_field: int


class BeamlineDefaultSchema(BasicScanMetadata):
    sample_name_long: str


TEST_REGISTRY = {
    "fake_scan_with_extra_metadata": ChildMetadata,
    "fake_scan_with_basic_metadata": BasicScanMetadata,
}


@pytest.fixture(scope="function", autouse=True)
def clear_schema_registry_cache():
    metadata_schema.cache_clear()
    yield
    metadata_schema.cache_clear()


def test_required_fields_validate():
    with pytest.raises(ValidationError):
        test_metadata = ChildMetadata.model_validate(TEST_DICT)

    test_metadata = ChildMetadata.model_validate(TEST_DICT | {"number_field": 123})
    assert test_metadata.number_field == 123
    test_metadata.number_field = 234
    assert test_metadata.number_field == 234

    with pytest.raises(ValidationError):
        test_metadata.number_field = "string"


def test_creating_scan_queue_message_validates_metadata():
    with patch.dict(metadata_schema._METADATA_SCHEMA_REGISTRY, TEST_REGISTRY, clear=True):
        with pytest.raises(ValidationError):
            ScanQueueMessage(scan_type="fake_scan_with_extra_metadata")
        with pytest.raises(ValidationError):
            ScanQueueMessage(
                scan_type="fake_scan_with_extra_metadata",
                parameter={},
                metadata={"user_metadata": {"number_field": "string"}},
            )
        ScanQueueMessage(
            scan_type="fake_scan_with_extra_metadata",
            parameter={},
            metadata={"user_metadata": {"number_field": 123}},
        )
        msg_with_extra_keys = ScanQueueMessage(
            scan_type="fake_scan_with_extra_metadata",
            parameter={},
            metadata={"user_metadata": {"number_field": 123, "extra": "data"}},
        )
        assert msg_with_extra_keys.metadata["user_metadata"]["extra"] == "data"


def test_default_schema_is_used_as_fallback():
    with patch.dict(metadata_schema._METADATA_SCHEMA_REGISTRY, TEST_REGISTRY, clear=True):
        metadata_schema.get_metadata_schema_for_scan("")  # create cache before patching default
        with patch.object(metadata_schema, "_DEFAULT_SCHEMA", BeamlineDefaultSchema):

            assert metadata_schema.get_default_schema() is BeamlineDefaultSchema
            assert (
                metadata_schema.get_metadata_schema_for_scan("not associated with anything")
                is BeamlineDefaultSchema
            )

            with pytest.raises(ValidationError):
                _msg_not_matching_default_and_no_specified_schema = ScanQueueMessage(
                    scan_type="not associated with anything",
                    parameter={},
                    metadata={"user_metadata": {"number_field": 123}},
                )
            with pytest.raises(ValidationError):
                _msg_matching_default_but_with_specified_schema = ScanQueueMessage(
                    scan_type="fake_scan_with_extra_metadata",
                    parameter={},
                    metadata={"user_metadata": {"sample_name_long": "long string of text"}},
                )
            _msg_matching_default_and_no_specified_schema = ScanQueueMessage(
                scan_type="not associated with anything",
                parameter={},
                metadata={"user_metadata": {"sample_name_long": "long string of text"}},
            )


def test_prepare_scan_request_produces_conforming_message():
    with patch.dict(metadata_schema._METADATA_SCHEMA_REGISTRY, TEST_REGISTRY, clear=True):
        with pytest.raises(ValidationError):
            Scans.prepare_scan_request(
                scan_name="fake_scan_with_extra_metadata",
                scan_info={"required_kwargs": []},
                system_config={},
            )
        with pytest.raises(ValidationError):
            Scans.prepare_scan_request(
                scan_name="fake_scan_with_extra_metadata",
                scan_info={"required_kwargs": []},
                system_config={},
                user_metadata={"number_field": "string"},
            )
        msg = Scans.prepare_scan_request(
            scan_name="fake_scan_with_extra_metadata",
            scan_info={"required_kwargs": []},
            system_config={},
            user_metadata={"number_field": 123},
        )
        assert msg.metadata["user_metadata"] == {"number_field": 123}
