import json
from copy import copy
from typing import Literal

import pytest
from pydantic import ValidationError

from bec_lib.atlas_models import DeviceHashModel, DictHashInclusion, HashableDevice, HashInclusion
from bec_lib.utils.json import ExtendedEncoder

TEST_DEVICE_DICT = {
    "name": "test_device",
    "deviceClass": "TestDeviceClass",
    "readoutPriority": "baseline",
    "enabled": True,
}


def _test_device_dict(extra={}, **kwargs):
    new = copy(TEST_DEVICE_DICT)
    new.update(extra)
    new.update(kwargs)
    return new


@pytest.mark.parametrize(
    "init_kwargs, valid",
    [
        ({}, True),
        ({"field_inclusion": HashInclusion.EXCLUDE, "inclusion_keys": ["a", "b"]}, False),
        ({"field_inclusion": HashInclusion.INCLUDE, "inclusion_keys": ["a", "b"]}, False),
        (
            {
                "field_inclusion": HashInclusion.INCLUDE,
                "inclusion_keys": ["a", "b"],
                "remainder_inclusion": HashInclusion.EXCLUDE,
            },
            True,
        ),
        (
            {
                "field_inclusion": HashInclusion.INCLUDE,
                "inclusion_keys": [],
                "remainder_inclusion": HashInclusion.EXCLUDE,
            },
            False,
        ),
        (
            {
                "field_inclusion": HashInclusion.INCLUDE,
                "inclusion_keys": ["a", "b"],
                "remainder_inclusion": HashInclusion.INCLUDE,
            },
            False,
        ),
    ],
)
def test_dict_hash_inclusion_init_params(init_kwargs: dict, valid: bool):
    if valid:
        assert DictHashInclusion(**init_kwargs)
    else:
        with pytest.raises(ValidationError):
            DictHashInclusion(**init_kwargs)


def test_roundtrip_normal_device():
    device = HashableDevice(**_test_device_dict())
    normal_device = device.as_normal_device()
    back = device.model_validate(normal_device)
    assert device == back


@pytest.mark.parametrize(
    "hash_model, extra_fields, expected",
    [
        # Default case
        (DeviceHashModel(), {}, {"deviceClass": "TestDeviceClass", "name": "test_device"}),
        # Description is excluded from the hash by default
        (
            DeviceHashModel(),
            {"description": "abcde"},
            {"deviceClass": "TestDeviceClass", "name": "test_device"},
        ),
        # Description should be included in the hash model too, if used
        (
            DeviceHashModel(description=HashInclusion.INCLUDE),
            {"description": "abcde"},
            {
                "description": "abcde",
                "deviceClass": "TestDeviceClass",
                "hash_model": {"description": "INCLUDE"},
                "name": "test_device",
            },
        ),
        # deviceConfig included with inclusion_keys
        (
            DeviceHashModel(
                deviceConfig=DictHashInclusion(
                    field_inclusion=HashInclusion.INCLUDE,
                    inclusion_keys=set(["foo"]),
                    remainder_inclusion=HashInclusion.EXCLUDE,
                )
            ),
            {"deviceConfig": {"foo": 1, "bar": 2}},
            {
                "deviceClass": "TestDeviceClass",
                "deviceConfig": {"foo": 1},
                "hash_model": {
                    "deviceConfig": {
                        "field_inclusion": "INCLUDE",
                        "inclusion_keys": ["foo"],
                        "remainder_inclusion": "EXCLUDE",
                    }
                },
                "name": "test_device",
            },
        ),
        # deviceConfig EXCLUDE, should not appear
        (
            DeviceHashModel(deviceConfig=DictHashInclusion(field_inclusion=HashInclusion.EXCLUDE)),
            {"deviceConfig": {"foo": 1}},
            {
                "deviceClass": "TestDeviceClass",
                # Uses all default values for the DictHashInclusion, which are not default for the HashModel itself
                "hash_model": {"deviceConfig": {}},
                "name": "test_device",
            },
        ),
        # userParameter included
        (
            DeviceHashModel(userParameter=DictHashInclusion(field_inclusion=HashInclusion.INCLUDE)),
            {"userParameter": {"x": 42}},
            {
                "deviceClass": "TestDeviceClass",
                "hash_model": {"userParameter": {"field_inclusion": "INCLUDE"}},
                "name": "test_device",
                "userParameter": {"x": 42},
            },
        ),
        # description VARIANT, should not appear
        (
            DeviceHashModel(description=HashInclusion.VARIANT),
            {"description": "abcde"},
            {
                "deviceClass": "TestDeviceClass",
                "hash_model": {"description": "VARIANT"},
                "name": "test_device",
            },
        ),
        # enabled INCLUDE, should appear
        (
            DeviceHashModel(enabled=HashInclusion.INCLUDE),
            {"enabled": False},
            {
                "deviceClass": "TestDeviceClass",
                "enabled": False,
                "hash_model": {"enabled": "INCLUDE"},
                "name": "test_device",
            },
        ),
    ],
)
def test_hash_input_generation(hash_model: DeviceHashModel, extra_fields: dict, expected: dict):
    device = HashableDevice(**_test_device_dict(extra_fields), hash_model=hash_model)
    hash_input = device._hash_input()
    expected_input = json.dumps(expected, sort_keys=True, cls=ExtendedEncoder)
    assert hash_input == expected_input


@pytest.mark.parametrize(
    "hash_model, equal",
    [
        (DeviceHashModel(), True),
        (DeviceHashModel(readoutPriority=HashInclusion.INCLUDE), False),
        (DeviceHashModel(readoutPriority=HashInclusion.VARIANT), True),
        # deviceConfig is different between them
        (
            DeviceHashModel(deviceConfig=DictHashInclusion(field_inclusion=HashInclusion.INCLUDE)),
            False,
        ),
        # Only care about the "l" key in deviceConfig, which is the same
        (
            DeviceHashModel(
                deviceConfig=DictHashInclusion(
                    field_inclusion=HashInclusion.INCLUDE,
                    inclusion_keys={"l"},
                    remainder_inclusion=HashInclusion.EXCLUDE,
                )
            ),
            True,
        ),
        # Adding a field which is the same keeps them equal
        (DeviceHashModel(softwareTrigger=HashInclusion.INCLUDE), True),
    ],
)
def test_device_equality_according_to_model(hash_model: DeviceHashModel, equal: bool):
    device_1 = HashableDevice(
        name="device",
        enabled=True,
        deviceClass="Class",
        deviceConfig={"a": "b", "c": "d", "l": "m"},
        readoutPriority="baseline",
        description="description a",
        readOnly=False,
        softwareTrigger=False,
        userParameter={"a": "b", "c": "d"},
        hash_model=hash_model,
    )
    device_2 = HashableDevice(
        name="device",
        enabled=True,
        deviceClass="Class",
        deviceConfig={"q": "x", "y": "z", "l": "m"},
        readoutPriority="async",
        description="description a",
        readOnly=True,
        softwareTrigger=False,
        userParameter={"q": "x", "y": "z"},
        hash_model=hash_model,
    )
    assert (device_1 == device_2) is equal


@pytest.mark.parametrize(
    "hash_model, expected",
    [
        (DeviceHashModel(), {"deviceConfig": {"a": "b", "c": "d", "l": "m"}}),
        (DeviceHashModel(deviceConfig=DictHashInclusion()), {}),
        (
            DeviceHashModel(
                deviceConfig=DictHashInclusion(),
                enabled=HashInclusion.VARIANT,
                softwareTrigger=HashInclusion.VARIANT,
            ),
            {"enabled": True, "softwareTrigger": False},
        ),
        (
            DeviceHashModel(
                deviceConfig=DictHashInclusion(),
                userParameter=DictHashInclusion(
                    field_inclusion=HashInclusion.INCLUDE,
                    inclusion_keys={"a"},
                    remainder_inclusion=HashInclusion.VARIANT,
                ),
            ),
            {"userParameter": {"c": "d"}},
        ),
        (
            DeviceHashModel(
                userParameter=DictHashInclusion(
                    field_inclusion=HashInclusion.INCLUDE,
                    inclusion_keys={"a"},
                    remainder_inclusion=HashInclusion.EXCLUDE,
                )
            ),
            {"deviceConfig": {"a": "b", "c": "d", "l": "m"}},
        ),
    ],
)
def test_variant_info(hash_model, expected):
    device = HashableDevice(
        name="device",
        enabled=True,
        deviceClass="Class",
        deviceConfig={"a": "b", "c": "d", "l": "m"},
        readoutPriority="baseline",
        description="description a",
        readOnly=False,
        softwareTrigger=False,
        userParameter={"a": "b", "c": "d"},
        hash_model=hash_model,
    )

    assert device._variant_info() == expected


@pytest.mark.parametrize(
    "hash_model, is_equal, is_variant",
    [
        (DeviceHashModel(), True, True),
        # Not equal, fails
        (
            DeviceHashModel(deviceConfig=DictHashInclusion(field_inclusion=HashInclusion.INCLUDE)),
            False,
            False,
        ),
        (DeviceHashModel(readOnly=HashInclusion.VARIANT), True, True),
        # Exclude deviceConfig, devices are now fully equal, not variants
        (DeviceHashModel(deviceConfig=DictHashInclusion()), True, False),
    ],
)
def test_is_variant(hash_model: DeviceHashModel, is_equal: bool, is_variant: bool):
    device_1 = HashableDevice(
        name="device",
        enabled=True,
        deviceClass="Class",
        deviceConfig={"a": "b", "c": "d", "l": "m"},
        readoutPriority="baseline",
        description="description a",
        readOnly=False,
        softwareTrigger=False,
        userParameter={"a": "b", "c": "d"},
        hash_model=hash_model,
    )
    device_2 = HashableDevice(
        name="device",
        enabled=True,
        deviceClass="Class",
        deviceConfig={"q": "x", "y": "z", "l": "m"},
        readoutPriority="baseline",
        description="description a",
        readOnly=True,
        softwareTrigger=False,
        userParameter={"a": "b", "c": "d"},
        hash_model=hash_model,
    )
    assert (device_1 == device_2) is is_equal
    assert device_1.is_variant(device_2) is is_variant
