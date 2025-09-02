import json
from copy import copy

import pytest
from pydantic import ValidationError

from bec_lib.atlas_models import (
    DeviceHashModel,
    DictHashInclusion,
    HashableDevice,
    HashableDeviceSet,
    HashInclusion,
)
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


def test_hash_caching():

    model = HashableDevice(**_test_device_dict())
    assert model._hash_cache is None

    hash_ = hash(model)
    assert model._hash_cache is not None

    model.deviceClass = "Something Else"
    assert model._hash_cache is None

    modified_hash = hash(model)
    assert model._hash_cache is not None

    assert hash_ != modified_hash


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
    hash_input = device._hash_input(device._hashing_data())
    expected_input = json.dumps(expected, sort_keys=True, cls=ExtendedEncoder).encode()
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


@pytest.mark.parametrize(
    "kwargs_1, kwargs_2, kwargs_3, kwargs_4, n",
    [
        ({}, {}, {}, {}, 1),
        ({}, {}, {}, {"deviceConfig": {"a": 1}}, 1),
        ({}, {}, {}, {"name": "test_device_2"}, 2),
        ({}, {}, {"name": "test_device_2"}, {"deviceClass": "OtherDeviceClass"}, 3),
    ],
)
def test_hashable_device_set_merges_equal(kwargs_1, kwargs_2, kwargs_3, kwargs_4, n):
    item_1 = HashableDevice(**_test_device_dict(**kwargs_1))
    item_2 = HashableDevice(**_test_device_dict(**kwargs_2))
    item_3 = HashableDevice(**_test_device_dict(**kwargs_3))
    item_4 = HashableDevice(**_test_device_dict(**kwargs_4))

    test_set = HashableDeviceSet((item_1, item_2, item_3, item_4))
    assert len(test_set) == n


def test_hashable_device_set_or_adds_sources():
    item_1 = HashableDevice(**_test_device_dict())
    item_1._source_files = {"a", "b"}
    item_2 = HashableDevice(**_test_device_dict())
    item_2._source_files = {"c", "d"}

    set_1 = HashableDeviceSet((item_1,))
    set_2 = HashableDeviceSet((item_2,))

    combined = set_1 | set_2
    assert len(combined) == 1
    assert combined.pop()._source_files == {"a", "b", "c", "d"}


def test_hashable_device_set_or_adds_tags():
    hash_model = DeviceHashModel(
        deviceConfig=DictHashInclusion(field_inclusion=HashInclusion.INCLUDE)
    )
    item_1 = HashableDevice(
        **_test_device_dict(deviceTags={"tag1"}, deviceConfig={"param": "value"}),
        hash_model=hash_model,
    )
    item_1._source_files = {"a", "b"}
    item_2 = HashableDevice(
        **_test_device_dict(deviceTags={"tag2"}, deviceConfig={"param": "value"}),
        hash_model=hash_model,
    )
    item_2._source_files = {"c", "d"}
    item_3 = HashableDevice(
        **_test_device_dict(deviceTags={"tag3"}, deviceConfig={"param": "other_value"}),
        hash_model=hash_model,
    )
    item_3._source_files = {"q"}

    set_1 = HashableDeviceSet((item_1,))
    set_2 = HashableDeviceSet((item_2,))
    set_3 = HashableDeviceSet((item_3,))

    combined = sorted(set_1 | set_2 | set_3, key=lambda hd: hd.deviceConfig["param"])
    assert len(combined) == 2
    assert combined[0]._source_files == {"q"}
    assert combined[0].deviceTags == {"tag3"}
    assert combined[1]._source_files == {"a", "b", "c", "d"}
    assert combined[1].deviceTags == {"tag1", "tag2"}


def test_hashable_device_set_or_adds_variants():
    hash_model = DeviceHashModel()
    item_1 = HashableDevice(
        **_test_device_dict(deviceTags={"tag1"}, deviceConfig={"param": "value"}),
        hash_model=hash_model,
    )
    item_2 = HashableDevice(
        **_test_device_dict(deviceTags={"tag3"}, deviceConfig={"param": "other_value"}),
        hash_model=hash_model,
    )

    assert item_1.is_variant(item_2)

    set_1 = HashableDeviceSet((item_1,))
    set_2 = HashableDeviceSet((item_2,))

    combined = set_1 | set_2
    assert len(combined) == 1
    combined_device: HashableDevice = combined.pop()
    assert combined_device.variants != set()
    assert HashableDevice.model_validate(combined_device.variants.pop())._variant_info() == {
        "deviceConfig": {"param": "other_value"}
    }
