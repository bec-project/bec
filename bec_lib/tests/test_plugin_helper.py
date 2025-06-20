import pytest

import bec_lib
from bec_lib import plugin_helper


@pytest.mark.parametrize(
    "class_spec, out_name",
    [("bec_lib.messages.BECMessage", "BECMessage"), ("bec_lib.messages.BECStatus", "BECStatus")],
)
def test_get_plugin_class(class_spec, out_name):
    cls = plugin_helper.get_plugin_class(class_spec, [bec_lib])
    assert cls.__name__ == out_name


@pytest.mark.parametrize(
    "class_spec", ["bec_lib.nonexistent_module.NonexistentClass", "bec_lib.NonexistentClass"]
)
def test_get_plugin_class_module_not_found(class_spec):
    with pytest.raises((ModuleNotFoundError, AttributeError)):
        plugin_helper.get_plugin_class(class_spec, [bec_lib])


def test_module_dist_info():
    result = plugin_helper.module_dist_info("bec_lib")
    assert result["dir_info"] == {"editable": True}
    assert result["url"] is not None
