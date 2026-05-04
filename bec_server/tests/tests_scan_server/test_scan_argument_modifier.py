from __future__ import annotations

from typing import Annotated, get_args, get_origin
from unittest import mock

from bec_lib.scan_args import ScanArgument
from bec_lib.signature_serializer import serialize_dtype
from bec_server.scan_server.scans.fermat_scan import FermatSpiralScan
from bec_server.scan_server.scans.line_scan import LineScan
from bec_server.scan_server.scans.scan_argument_modifier import (
    _convert_annotation,
    gui_config_with_modifiers,
    scan_doc_with_modifiers,
    scan_signature_with_modifiers,
)
from bec_server.scan_server.scans.scan_base import ScanBase


def test_convert_annotation_wraps_plain_type_with_scan_argument():
    converted = _convert_annotation("float")

    assert get_origin(converted) is Annotated
    base_annotation, metadata = get_args(converted)
    assert base_annotation is float
    assert isinstance(metadata, ScanArgument)


def test_convert_annotation_preserves_existing_scan_argument_metadata():
    annotation = Annotated[float, ScanArgument(display_name="Exposure Time", ge=0)]

    converted = _convert_annotation(serialize_dtype(annotation))

    assert get_origin(converted) is Annotated
    base_annotation, metadata = get_args(converted)
    assert base_annotation is float
    assert isinstance(metadata, ScanArgument)
    assert metadata.display_name == "Exposure Time"
    assert metadata.ge == 0


def test_convert_annotation_returns_none_for_empty_annotation():
    assert _convert_annotation("_empty") is None


class DummyScan(ScanBase):
    scan_name = "dummy_scan"

    def __init__(self, value: float, relative: bool = True, **kwargs):
        """
        Dummy scan used for modifier tests.
        """
        super().__init__(**kwargs)
        self.value = value
        self.relative = relative


class GenericClassDocScan(ScanBase):
    scan_name = "generic_class_doc_scan"
    __doc__ = ScanBase.__init__.__doc__

    def __init__(self, level: int, **kwargs):
        """
        Specific init doc used for modifier tests.

        Args:
            level (int): level value
        """
        super().__init__(**kwargs)


class DummyModifier:
    @staticmethod
    def scan_argument_overrides(scan_name, arguments, defaults):
        arguments.pop("relative", None)
        defaults["value"] = 2.5
        arguments["extra"] = Annotated[
            int, ScanArgument(description="extra modifier parameter", ge=0)
        ]
        defaults["extra"] = 3
        return arguments, defaults

    @staticmethod
    def gui_config_overrides(scan_name, gui_config):
        gui_config["Timing"] = [*gui_config.get("Timing", []), "extra"]
        return gui_config


def test_scan_signature_with_modifiers_uses_init_signature_directly():
    with mock.patch(
        "bec_server.scan_server.scans.scan_argument_modifier.get_scan_modifier",
        return_value=DummyModifier,
    ):
        signature = scan_signature_with_modifiers(DummyScan)

    names = [entry["name"] for entry in signature]
    assert "relative" not in names

    value = next(entry for entry in signature if entry["name"] == "value")
    extra = next(entry for entry in signature if entry["name"] == "extra")

    assert value["default"] == 2.5
    assert "Annotated" in value["annotation"]
    assert extra["kind"] == "KEYWORD_ONLY"
    assert extra["default"] == 3


def test_scan_signature_with_modifiers_preserves_arg_bundles():
    signature = scan_signature_with_modifiers(LineScan)

    assert signature[0]["name"] == "args"
    assert signature[0]["kind"] == "VAR_POSITIONAL"


def test_gui_config_with_modifiers_returns_original_config_without_modifier():
    gui_config = {"Timing": ["value"]}

    with mock.patch(
        "bec_server.scan_server.scans.scan_argument_modifier.get_scan_modifier", return_value=None
    ):
        modified = gui_config_with_modifiers(DummyScan, gui_config)

    assert modified == {"Timing": ["value"]}


def test_gui_config_with_modifiers_applies_modifier_override():
    gui_config = {"Timing": ["value"]}

    with mock.patch(
        "bec_server.scan_server.scans.scan_argument_modifier.get_scan_modifier",
        return_value=DummyModifier,
    ):
        modified = gui_config_with_modifiers(DummyScan, gui_config)

    assert modified == {"Timing": ["value", "extra"]}


def test_scan_doc_with_modifiers_does_not_suffix_non_bundle_devices():
    with mock.patch(
        "bec_server.scan_server.scans.scan_argument_modifier.get_scan_modifier", return_value=None
    ):
        doc = scan_doc_with_modifiers(FermatSpiralScan)

    assert "dev.motor1" in doc
    assert "dev.motor2" in doc


def test_scan_doc_with_modifiers_rebuilds_args_section_from_effective_signature():
    with mock.patch(
        "bec_server.scan_server.scans.scan_argument_modifier.get_scan_modifier",
        return_value=DummyModifier,
    ):
        doc = scan_doc_with_modifiers(DummyScan)

    assert "Dummy scan used for modifier tests." in doc
    assert "Args:" in doc
    assert "value (float): value. Default: 2.5" in doc
    assert "extra (int): extra modifier parameter. Default: 3" in doc
    assert "relative (bool)" not in doc
    assert "Examples:" in doc
    assert "Minimum:" in doc
    assert ">>> scans.dummy_scan()" in doc
    assert "Full:" in doc
    assert ">>> scans.dummy_scan(2.5, extra=3)" in doc
    assert "relative=True" not in doc
    assert "value=99.0" not in doc


def test_scan_doc_with_modifiers_ignores_generic_base_docstring():
    doc = scan_doc_with_modifiers(GenericClassDocScan)

    assert "Specific init doc used for modifier tests." in doc
    assert "Base class for all scans." not in doc
