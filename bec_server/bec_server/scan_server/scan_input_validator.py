from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import Annotated, Any, TypeAlias, get_args, get_origin, get_type_hints

from bec_lib.scan_args import ScanArgument

from .errors import ScanInputValidationError

ScanClass: TypeAlias = type[Any]
AnnotationMap: TypeAlias = dict[str, Any]


class ScanInputValidator:
    """Validate scan inputs against supported ``ScanArgument`` annotation metadata.

    The validator checks numeric bounds declared on ``ScanArgument`` metadata inside
    ``typing.Annotated`` declarations. It supports both v4 scan input styles:
    bundled positional arguments declared through ``scan_cls.arg_input`` and fixed
    constructor inputs declared on ``scan_cls.__init__``.
    """

    def validate(self, scan_cls: ScanClass, args: Sequence[Any], kwargs: Mapping[str, Any]) -> None:
        """Validate resolved scan inputs for a scan class.

        Args:
            scan_cls (ScanClass): Scan class whose input annotations should be used.
            args (Sequence[Any]): Positional scan arguments after device-name resolution.
            kwargs (Mapping[str, Any]): Keyword scan arguments after device-name resolution.

        Raises:
            ScanInputValidationError: If an input violates a supported ``ScanArgument`` bound.
        """
        self._validate_arg_input_bundle(scan_cls, args)
        self._validate_signature_inputs(scan_cls, args, kwargs)

    def _validate_arg_input_bundle(self, scan_cls: ScanClass, args: Sequence[Any]) -> None:
        """Validate bundled positional arguments declared by ``scan_cls.arg_input``.

        Args:
            scan_cls (ScanClass): Scan class that may define ``arg_input`` bundles.
            args (Sequence[Any]): Positional scan arguments after device-name resolution.

        Raises:
            ScanInputValidationError: If a bundled input violates a supported ``ScanArgument`` bound.
        """
        arg_input = getattr(scan_cls, "arg_input", {}) or {}
        bundle_size = getattr(scan_cls, "arg_bundle_size", {}).get("bundle", 0)
        if not arg_input or bundle_size <= 0:
            return

        arg_names = list(arg_input.keys())
        for bundle_start in range(0, len(args), bundle_size):
            for offset, arg_name in enumerate(arg_names):
                arg_index = bundle_start + offset
                if arg_index >= len(args):
                    break
                self._validate_value(arg_name, args[arg_index], arg_input[arg_name])

    def _validate_signature_inputs(
        self, scan_cls: ScanClass, args: Sequence[Any], kwargs: Mapping[str, Any]
    ) -> None:
        """Validate constructor-annotated positional and keyword inputs.

        Args:
            scan_cls (ScanClass): Scan class whose constructor annotations should be used.
            args (Sequence[Any]): Positional scan arguments after device-name resolution.
            kwargs (Mapping[str, Any]): Keyword scan arguments after device-name resolution.

        Raises:
            ScanInputValidationError: If a constructor input violates a supported ``ScanArgument`` bound.
        """
        signature_annotations = self.scan_signature_annotations(scan_cls)
        if not self._uses_arg_input_bundle(scan_cls):
            for arg_index, arg_name in enumerate(signature_annotations):
                if arg_index >= len(args):
                    break
                self._validate_value(arg_name, args[arg_index], signature_annotations[arg_name])

        for arg_name, value in kwargs.items():
            if arg_name in signature_annotations:
                self._validate_value(arg_name, value, signature_annotations[arg_name])

    def _uses_arg_input_bundle(self, scan_cls: ScanClass) -> bool:
        """Return whether positional arguments are described by ``arg_input`` bundles.

        Args:
            scan_cls (ScanClass): Scan class to inspect.

        Returns:
            bool: True if the scan class uses bundled positional inputs.
        """
        return bool(getattr(scan_cls, "arg_input", {}) or {}) and (
            getattr(scan_cls, "arg_bundle_size", {}).get("bundle", 0) > 0
        )

    def scan_signature_annotations(self, scan_cls: ScanClass) -> AnnotationMap:
        """Return constructor input annotations keyed by argument name.

        ``*args`` and ``**kwargs`` are intentionally excluded because they do not
        describe individual user-facing scan inputs.

        Args:
            scan_cls (ScanClass): Scan class whose constructor annotations should be inspected.

        Returns:
            AnnotationMap: Constructor annotations keyed by argument name.
        """
        type_hints = get_type_hints(scan_cls.__init__, include_extras=True)
        return {
            name: type_hints.get(name, parameter.annotation)
            for name, parameter in inspect.signature(scan_cls).parameters.items()
            if name not in {"args", "kwargs"}
            and parameter.annotation is not inspect.Parameter.empty
        }

    def _validate_value(self, arg_name: str, value: Any, annotation: Any) -> None:
        """Validate a single input value against any ``ScanArgument`` bounds.

        Args:
            arg_name (str): Name of the scan input being validated.
            value (Any): Input value after device-name resolution.
            annotation (Any): Type annotation that may contain ``ScanArgument`` metadata.

        Raises:
            ScanInputValidationError: If the input violates a supported ``ScanArgument`` bound.
        """
        scan_argument = self._scan_argument_from_annotation(annotation)
        if scan_argument is None:
            return

        for operator_name, limit in [
            ("gt", scan_argument.gt),
            ("ge", scan_argument.ge),
            ("lt", scan_argument.lt),
            ("le", scan_argument.le),
        ]:
            if limit is None:
                continue
            if not self._satisfies_bound(value, operator_name, limit):
                raise ScanInputValidationError(
                    f"Invalid value for scan argument '{arg_name}': {value!r} must be "
                    f"{self._bound_description(operator_name)} {limit!r}."
                )

    def _scan_argument_from_annotation(self, annotation: Any) -> ScanArgument | None:
        """Extract ``ScanArgument`` metadata from a ``typing.Annotated`` annotation.

        Args:
            annotation (Any): Type annotation to inspect.

        Returns:
            ScanArgument | None: Extracted scan argument metadata, if present.
        """
        if get_origin(annotation) is not Annotated:
            return None
        for metadata in get_args(annotation)[1:]:
            if isinstance(metadata, ScanArgument):
                return metadata
        return None

    def _satisfies_bound(self, value: Any, operator_name: str, limit: float) -> bool:
        """Return whether ``value`` satisfies the named numeric bound.

        Args:
            value (Any): Input value to compare.
            operator_name (str): Bound operator name, one of ``gt``, ``ge``, ``lt``, or ``le``.
            limit (float): Numeric limit from ``ScanArgument`` metadata.

        Returns:
            bool: True if the value satisfies the bound.

        Raises:
            ScanInputValidationError: If the value cannot be compared to the limit.
        """
        try:
            if operator_name == "gt":
                return value > limit
            if operator_name == "ge":
                return value >= limit
            if operator_name == "lt":
                return value < limit
            if operator_name == "le":
                return value <= limit
        except TypeError as exc:
            raise ScanInputValidationError(
                f"Invalid value for scan argument: {value!r} cannot be compared to {limit!r}."
            ) from exc
        return True

    def _bound_description(self, operator_name: str) -> str:
        """Return a user-facing description for a bound operator.

        Args:
            operator_name (str): Bound operator name, one of ``gt``, ``ge``, ``lt``, or ``le``.

        Returns:
            str: Human-readable bound description.
        """
        return {
            "gt": "greater than",
            "ge": "greater than or equal to",
            "lt": "less than",
            "le": "less than or equal to",
        }[operator_name]
