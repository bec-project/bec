from __future__ import annotations

import inspect
import types
from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Literal, Union, get_args, get_origin

from typeguard import CollectionCheckStrategy, TypeCheckError, check_type

from bec_lib.bec_errors import ScanInputValidationError
from bec_lib.device import DeviceBase
from bec_lib.scan_args import ScanArgument
from bec_lib.signature_serializer import deserialize_dtype, dict_to_signature

# Internal scan kwargs that are automatically added to the scan request
INTERNAL_SCAN_KWARGS = {"user_metadata", "system_config", "scan_queue"}


class ScanInputValidator:
    """Validate scan inputs against serialized scan metadata."""

    def __init__(self, device_manager=None) -> None:
        """Initialize the scan input validator.

        Args:
            device_manager (Any | None): Optional device manager used to resolve device-name
                strings to ``DeviceBase`` instances before validation.
        """
        self.device_manager = device_manager

    def validate(
        self, scan_name: str, scan_info: dict, args: Sequence[Any], kwargs: Mapping[str, Any]
    ) -> tuple[list[Any], dict[str, Any]]:
        """Validate scan inputs using serialized scan metadata.

        Args:
            scan_name (str): Scan name used in user-facing validation errors.
            scan_info (dict): Serialized scan metadata published by the scan manager.
            args (Sequence[Any]): Positional scan arguments after any caller-side preprocessing.
            kwargs (Mapping[str, Any]): Keyword scan arguments after any caller-side preprocessing.

        Returns:
            tuple[list[Any], dict[str, Any]]: Validated and normalized positional and keyword
                arguments. Device-name strings are resolved to ``DeviceBase`` objects when
                possible.

        Raises:
            ScanInputValidationError: If required inputs are missing, bundle constraints are
                violated, or any value fails type/bounds validation.
        """
        args, kwargs = self._resolve_device_inputs(scan_info, args, kwargs)
        arg_input = self.arg_input_annotations(scan_info)
        arg_bundle_size = scan_info.get("arg_bundle_size", {})
        bundle_size = arg_bundle_size.get("bundle", 0)
        uses_arg_input_bundle = bool(arg_input) and bundle_size > 0

        validated_signature_args = self._validate_signature_inputs(
            scan_info, args, kwargs, uses_arg_input_bundle
        )

        required_kwargs = scan_info.get("required_kwargs") or []
        required_names = (
            required_kwargs.keys() if isinstance(required_kwargs, dict) else required_kwargs
        )
        if not all(
            req_kwarg in kwargs or req_kwarg in validated_signature_args
            for req_kwarg in required_names
        ):
            self._raise_validation_error(
                f"{scan_info.get('doc')}\n Not all required keyword arguments have been"
                f" specified. The required arguments are: {required_kwargs}"
            )

        if arg_input:
            self._validate_arg_input(scan_name, scan_info, args, arg_input)

        if bundle_size:
            self._validate_bundle_count(scan_name, scan_info, args, arg_bundle_size)

        return args, kwargs

    def arg_input_annotations(self, scan_info: dict) -> dict[str, Any]:
        """Return deserialized ``arg_input`` annotations keyed by input name.

        Args:
            scan_info (dict): Serialized scan metadata.

        Returns:
            A mapping from bundled argument names to deserialized annotations.
        """
        return {
            name: deserialize_dtype(annotation)
            for name, annotation in (scan_info.get("arg_input") or {}).items()
        }

    def scan_signature_annotations(self, scan_info: dict) -> dict[str, Any]:
        """Return deserialized signature annotations keyed by input name.

        ``*args`` and ``**kwargs`` entries are intentionally excluded because they do not
        describe user-facing named inputs.

        Args:
            scan_info (dict): Serialized scan metadata.

        Returns:
            A mapping from signature parameter names to deserialized annotations.
        """
        annotations = {}
        for param in scan_info.get("signature") or []:
            if param["name"] in {"args", "kwargs"}:
                continue
            if param["annotation"] == "_empty":
                continue
            annotations[param["name"]] = deserialize_dtype(param["annotation"])
        return annotations

    def scan_signature_kwargs(self, scan_info: dict) -> set[str]:
        """Return keyword-capable parameter names from the serialized signature.

        Args:
            scan_info (dict): Serialized scan metadata.

        Returns:
            set[str]: Parameter names that may be supplied as keyword arguments.
        """
        return {
            param["name"]
            for param in (scan_info.get("signature") or [])
            if param["kind"] in {"POSITIONAL_OR_KEYWORD", "KEYWORD_ONLY"}
        }

    def is_device_annotation(self, annotation: Any) -> bool:
        """Return whether an annotation describes a device input.

        Args:
            annotation (Any): Deserialized type annotation to inspect.

        Returns:
            ``True`` if the annotation ultimately resolves to ``DeviceBase`` or a subclass.
        """
        origin = get_origin(annotation)
        if origin is Annotated:
            return self.is_device_annotation(get_args(annotation)[0])
        if origin is Literal:
            return False
        if annotation is None:
            return False
        if origin in {Union, types.UnionType}:
            return any(self.is_device_annotation(arg) for arg in get_args(annotation))
        return inspect.isclass(annotation) and issubclass(annotation, DeviceBase)

    def _resolve_device_inputs(
        self, scan_info: dict, args: Sequence[Any], kwargs: Mapping[str, Any]
    ) -> tuple[list[Any], dict[str, Any]]:
        """Resolve device-name strings to ``DeviceBase`` instances where annotations require it.

        Args:
            scan_info (dict): Serialized scan metadata.
            args (Sequence[Any]): Positional input values.
            kwargs (Mapping[str, Any]): Keyword input values.

        Returns:
            tuple[list[Any], dict[str, Any]]: Normalized positional and keyword arguments.
        """
        arg_input = self.arg_input_annotations(scan_info)
        signature_annotations = self.scan_signature_annotations(scan_info)
        kwarg_annotations = {**signature_annotations, **arg_input}
        if not arg_input and not signature_annotations:
            return list(args), dict(kwargs)

        resolved_args = list(args)
        resolved_kwargs = dict(kwargs)

        if arg_input and scan_info.get("arg_bundle_size", {}).get("bundle", 0) > 0:
            bundle_size = scan_info["arg_bundle_size"]["bundle"]
            arg_names = list(arg_input.keys())
            for bundle_start in range(0, len(resolved_args), bundle_size):
                for offset, arg_name in enumerate(arg_names):
                    arg_index = bundle_start + offset
                    if arg_index >= len(resolved_args):
                        break
                    if self.is_device_annotation(arg_input.get(arg_name)):
                        resolved_args[arg_index] = self._resolve_device(
                            resolved_args[arg_index], index=arg_index
                        )
        else:
            arg_names = list(signature_annotations.keys())
            for arg_index, arg_name in enumerate(arg_names):
                if arg_index >= len(resolved_args):
                    break
                if self.is_device_annotation(signature_annotations.get(arg_name)):
                    resolved_args[arg_index] = self._resolve_device(
                        resolved_args[arg_index], index=arg_index
                    )

        for key, value in resolved_kwargs.items():
            if self.is_device_annotation(kwarg_annotations.get(key)):
                resolved_kwargs[key] = self._resolve_device(value)

        return resolved_args, resolved_kwargs

    def _resolve_device(self, value: Any, index: int | None = None) -> Any:
        """Resolve a device-name string to a ``DeviceBase`` instance when possible.

        Args:
            value (Any): Value to resolve.
            index (int | None): Optional index of the argument in the positional arguments list.
                    Used to provide more specific error messages when resolving bundled inputs.
        Returns:
            Any: The resolved device object or the original value if no resolution is needed.

        Raises:
            ScanInputValidationError: If a string is supplied for a device-typed input and the
                device manager is unavailable or the device name is unknown.
        """
        if isinstance(value, DeviceBase):
            return value
        if not isinstance(value, str):
            return value
        if self.device_manager is None:
            self._raise_validation_error(
                f"Cannot resolve device '{value}': no device manager is available."
            )
        if value not in self.device_manager.devices:
            arg_position = f" at position {index}" if index is not None else ""
            self._raise_validation_error(
                f"Device '{value}' specified for scan argument{arg_position} was not found in the device manager. Please check your scan input arguments."
            )
        return self.device_manager.devices[value]

    def _validate_arg_input(
        self, scan_name: str, scan_info: dict, args: Sequence[Any], arg_input: dict[str, Any]
    ) -> None:
        """Validate bundled positional inputs declared in ``scan_info['arg_input']``.

        Args:
            scan_name (str): Scan name used in user-facing validation errors.
            scan_info (dict): Serialized scan metadata.
            args (Sequence[Any]): Positional arguments to validate.
            arg_input (dict[str, Any]): Deserialized bundled input annotations keyed by input
                name.

        Raises:
            ScanInputValidationError: If the bundle arity is wrong, the same device object is
                repeated, or any bundled value fails validation.
        """
        arg_input_items = list(arg_input.items())
        if len(args) % len(arg_input_items) != 0:
            self._raise_validation_error(
                f"{scan_info.get('doc')}\n {scan_name} takes multiples of"
                f" {len(arg_input_items)} arguments ({len(args)} given)."
            )

        seen_devices: set[int] = set()
        for arg in args:
            if isinstance(arg, DeviceBase):
                device_id = id(arg)
                if device_id in seen_devices:
                    self._raise_validation_error(
                        f"{scan_info.get('doc')}\n All specified devices must be different objects."
                    )
                seen_devices.add(device_id)

        for index, arg in enumerate(args):
            arg_name, annotation = arg_input_items[index % len(arg_input_items)]
            self._validate_value(arg_name=arg_name, value=arg, annotation=annotation, index=index)

    def _validate_bundle_count(
        self, scan_name: str, scan_info: dict, args: Sequence[Any], arg_bundle_size: dict
    ) -> None:
        """Validate the number of positional argument bundles.

        Args:
            scan_name (str): Scan name used in user-facing validation errors.
            scan_info (dict): Serialized scan metadata.
            args (Sequence[Any]): Positional arguments whose bundle count should be checked.
            arg_bundle_size (dict): Serialized bundle-size metadata.

        Raises:
            ScanInputValidationError: If the number of supplied bundles is outside the configured
                minimum or maximum.
        """
        bundle_size = arg_bundle_size.get("bundle", 0)
        num_bundles = len(args) // bundle_size
        min_bundles = arg_bundle_size.get("min")
        max_bundles = arg_bundle_size.get("max")
        if min_bundles and num_bundles < min_bundles:
            self._raise_validation_error(
                f"{scan_info.get('doc')}\n {scan_name} requires at least {min_bundles} bundles"
                f" of arguments ({num_bundles} given)."
            )
        if max_bundles and num_bundles > max_bundles:
            self._raise_validation_error(
                f"{scan_info.get('doc')}\n {scan_name} requires at most {max_bundles} bundles"
                f" of arguments ({num_bundles} given)."
            )

    def _validate_signature_inputs(
        self,
        scan_info: dict,
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
        uses_arg_input_bundle: bool,
    ) -> set[str]:
        """Validate inputs described by the serialized function signature.

        Args:
            scan_info (dict): Serialized scan metadata containing the serialized signature.
            args (Sequence[Any]): Positional arguments supplied by the caller.
            kwargs (Mapping[str, Any]): Keyword arguments supplied by the caller.
            uses_arg_input_bundle (bool): Whether positional user inputs are described by
                ``arg_input`` bundles instead of regular signature parameters.

        Returns:
            The set of signature parameter names successfully bound from the supplied inputs.

        Raises:
            ScanInputValidationError: If argument binding fails, a bound value has the wrong type,
                or a bound ``ScanArgument`` violates numeric bounds.
        """
        serialized_signature = scan_info.get("signature")
        if not isinstance(serialized_signature, list) or not serialized_signature:
            return set()

        signature = dict_to_signature(serialized_signature)

        # get all kwargs from the scan signature
        allowed_kwargs = self.scan_signature_kwargs(scan_info)

        # Check if there are any kwargs that are not in the signature nor in the set of
        # internal scan kwargs.
        invalid_kwargs = sorted(
            key for key in kwargs if key not in allowed_kwargs and key not in INTERNAL_SCAN_KWARGS
        )
        if invalid_kwargs and scan_info.get("base_class") == "ScanBaseV4":
            # NOTE: We only apply the strict validation of kwargs for the new scans.
            # Once we have migrated all scans to the new format, we should apply it to all and remove
            # the "scan_info.get("base_class") == "ScanBaseV4" check.
            unknown_kwargs = ", ".join(repr(key) for key in invalid_kwargs)
            self._raise_validation_error(f"Unknown keyword argument(s) for scan: {unknown_kwargs}.")
        known_kwargs = {
            name: value for name, value in kwargs.items() if name in signature.parameters
        }

        try:
            bound = (
                signature.bind(**known_kwargs)
                if uses_arg_input_bundle
                else signature.bind(*args, **known_kwargs)
            )
        except TypeError as exc:
            self._raise_validation_error(f"{scan_info.get('doc')}\n {exc}.")

        self._validate_signature_bound_types(signature, bound)
        self._validate_signature_bounds(signature, bound)
        return set(bound.arguments)

    def _validate_signature_bound_types(
        self, signature: inspect.Signature, bound: inspect.BoundArguments
    ) -> None:
        """Validate bound signature arguments with ``typeguard.check_type()``.

        Args:
            signature (inspect.Signature): Signature reconstructed from serialized scan metadata.
            bound (inspect.BoundArguments): Bound arguments produced by
                ``inspect.Signature.bind``.

        Raises:
            ScanInputValidationError: If any bound argument fails runtime type validation.
        """
        for name, value in bound.arguments.items():
            parameter = signature.parameters[name]
            if parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
                continue
            annotation = parameter.annotation
            if annotation is inspect.Parameter.empty:
                continue
            try:
                check_type(
                    self._normalize_tuple_payloads(value, annotation),
                    annotation,
                    collection_check_strategy=CollectionCheckStrategy.ALL_ITEMS,
                )
            except TypeCheckError as exc:
                self._raise_validation_error(f"Invalid type for scan argument '{name}': {exc}")

    def _validate_signature_bounds(
        self, signature: inspect.Signature, bound: inspect.BoundArguments
    ) -> None:
        """Validate ``ScanArgument`` bounds for bound signature arguments.

        Args:
            signature (inspect.Signature): Signature reconstructed from serialized scan metadata.
            bound (inspect.BoundArguments): Bound arguments produced by
                ``inspect.Signature.bind``.

        Raises:
            ScanInputValidationError: If a bound argument violates numeric bounds or bounds are
                configured on a non-scalar annotation.
        """
        for name, value in bound.arguments.items():
            parameter = signature.parameters[name]
            if parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
                continue
            if parameter.annotation is inspect.Parameter.empty:
                continue
            scan_argument = self._scan_argument_from_annotation(parameter.annotation)
            if scan_argument is None:
                continue
            self._validate_scan_argument_bounds(name, value, parameter.annotation, scan_argument)

    def _validate_value(self, *, arg_name: str, value: Any, annotation: Any, index: int) -> None:
        """Validate a single value against its annotation and any ``ScanArgument`` bounds.

        Args:
            arg_name (str): User-facing input name used in validation errors.
            value (Any): Input value to validate.
            annotation (Any): Deserialized type annotation for the input.
            index (int): Index of the argument in the positional arguments list.
                Used to provide more specific error messages when validating bundled inputs.

        Raises:
            ScanInputValidationError: If the value fails type validation or any configured bound.
        """
        normalized_value = self._normalize_tuple_payloads(value, annotation)
        if not self._arg_matches_type(normalized_value, annotation):
            msg = (
                f"Invalid type for scan argument '{arg_name}' at position {index}: expected "
                f"{self._type_description(annotation)}, got {type(value).__name__} ({value!r})."
            )
            self._raise_validation_error(msg)

        scan_argument = self._scan_argument_from_annotation(annotation)
        if scan_argument is None:
            return

        self._validate_scan_argument_bounds(arg_name, value, annotation, scan_argument)

    def _validate_scan_argument_bounds(
        self, arg_name: str, value: Any, annotation: Any, scan_argument: ScanArgument
    ) -> None:
        """Validate numeric bounds declared in ``ScanArgument`` metadata.

        Args:
            arg_name (str): User-facing input name used in validation errors.
            value (Any): Input value to compare against the configured bounds.
            annotation (Any): Full annotation for the input, potentially including ``Annotated``
                metadata.
            scan_argument (ScanArgument): Extracted ``ScanArgument`` metadata.

        Raises:
            ScanInputValidationError: If bounds are configured on a non-scalar annotation or the
                value violates one of the configured bounds.
        """
        if not self._supports_scalar_bounds(annotation):
            self._raise_validation_error(
                f"Invalid bounds configuration for scan argument '{arg_name}': "
                "ScanArgument bounds are only supported for scalar annotations."
            )

        for operator_name, limit in [
            ("gt", scan_argument.gt),
            ("ge", scan_argument.ge),
            ("lt", scan_argument.lt),
            ("le", scan_argument.le),
        ]:
            if limit is None:
                continue
            if not self._satisfies_bound(value, operator_name, limit):
                self._raise_bound_error(arg_name, value, operator_name, limit)

    def _supports_scalar_bounds(self, annotation: Any) -> bool:
        """Return whether ``ScanArgument`` numeric bounds are valid for an annotation.

        Args:
            annotation (Any): Annotation to inspect.

        Returns:
            bool: ``True`` for scalar-like annotations such as plain types, ``Literal`` values,
                and unions of scalar types. ``False`` for container annotations.
        """
        annotation = self._strip_annotation_metadata(annotation)
        origin = get_origin(annotation)

        if origin is None or origin is Literal:
            return True

        if origin in {Union, types.UnionType}:
            return all(self._supports_scalar_bounds(arg) for arg in get_args(annotation))

        if origin in {list, dict, tuple, Sequence, Mapping, set, frozenset}:
            return False

        return True

    def _raise_bound_error(
        self, arg_name: str, value: Any, operator_name: str, limit: float
    ) -> None:
        """Raise a standardized bounds-validation error."""
        self._raise_validation_error(
            f"Invalid value for scan argument '{arg_name}': {value!r}. Input must be "
            f"{self._bound_description(operator_name)} {limit!r}."
        )

    def _raise_validation_error(self, message: str) -> None:
        """Raise ``ScanInputValidationError`` with attached ``ErrorInfo``.

        Args:
            message (str): User-facing validation error message.

        Raises:
            ScanInputValidationError: Always raised with populated ``error_info``.
        """
        raise ScanInputValidationError.with_error_info(message)

    def _scan_argument_from_annotation(self, annotation: Any) -> ScanArgument | None:
        """Extract ``ScanArgument`` metadata from an ``Annotated`` annotation."""
        if get_origin(annotation) is not Annotated:
            return None
        for metadata in get_args(annotation)[1:]:
            if isinstance(metadata, ScanArgument):
                return metadata
        return None

    def _strip_annotation_metadata(self, annotation: Any) -> Any:
        """Return the base annotation without ``Annotated`` metadata.

        Args:
            annotation (Any): Annotation that may contain ``Annotated`` metadata.

        Returns:
            The underlying base annotation.
        """
        if get_origin(annotation) is Annotated:
            return self._strip_annotation_metadata(get_args(annotation)[0])
        return annotation

    def _normalize_tuple_payloads(self, value: Any, annotation: Any) -> Any:
        """Convert list payloads to tuples where the annotation expects tuples.

        Scan payloads are serialized through message objects where tuple-shaped values may arrive
        as lists. This normalization keeps runtime type checking strict while still accepting the
        transport representation used by scan messages.

        Args:
            value (Any): Input value to normalize.
            annotation (Any): Annotation describing the expected shape of ``value``.

        Returns:
            The normalized value, with tuple-compatible nested lists converted to tuples.
        """
        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin is Annotated:
            return self._normalize_tuple_payloads(value, args[0])

        if origin is Literal or annotation in {Any, inspect.Parameter.empty, object, None}:
            return value

        if origin in {Union, types.UnionType}:
            return value

        if not args:
            return value

        if origin in {list, Sequence} and isinstance(value, list):
            item_type = args[0]
            return [self._normalize_tuple_payloads(item, item_type) for item in value]

        if origin in {dict, Mapping} and isinstance(value, dict):
            key_type, value_type = args if len(args) == 2 else (Any, Any)
            return {
                self._normalize_tuple_payloads(key, key_type): self._normalize_tuple_payloads(
                    item, value_type
                )
                for key, item in value.items()
            }

        if origin is tuple and isinstance(value, (list, tuple)):
            if len(args) == 2 and args[1] is Ellipsis:
                return tuple(self._normalize_tuple_payloads(item, args[0]) for item in value)
            if len(args) == len(value):
                return tuple(
                    self._normalize_tuple_payloads(item, item_type)
                    for item, item_type in zip(value, args, strict=True)
                )

        return value

    def _arg_matches_type(self, arg: Any, dtype: object) -> bool:
        """Return whether a value matches an annotation using ``typeguard``.

        Args:
            arg (Any): Value to validate.
            dtype (object): Annotation to validate against.

        Returns:
            ``True`` if ``arg`` satisfies ``dtype``, otherwise ``False``.
        """
        try:
            check_type(arg, dtype, collection_check_strategy=CollectionCheckStrategy.ALL_ITEMS)
        except TypeCheckError:
            return False
        return True

    def _type_description(self, annotation: Any) -> str:
        """Return a user-facing type description for an annotation.

        Args:
            annotation (Any): Annotation to describe.

        Returns:
            A compact human-readable description of the annotation.
        """
        if get_origin(annotation) is Annotated:
            return self._type_description(get_args(annotation)[0])
        if get_origin(annotation) is Literal:
            return "Literal"
        if (
            annotation.__class__.__name__ == "_UnionGenericAlias"
            or annotation.__class__ == types.UnionType
        ):
            return " or ".join(self._type_description(arg) for arg in get_args(annotation))
        origin = get_origin(annotation)
        if origin is not None:
            return str(annotation).replace("typing.", "")
        if annotation is None or annotation is type(None):
            return "None"
        return getattr(annotation, "__name__", str(annotation))

    def _satisfies_bound(self, value: Any, operator_name: str, limit: float) -> bool:
        """Return whether ``value`` satisfies the named numeric bound.

        Args:
            value (Any): Input value to compare.
            operator_name (str): Bound operator name, one of ``gt``, ``ge``, ``lt``, or ``le``.
            limit (float): Numeric bound to compare against.

        Returns:
            ``True`` if the value satisfies the bound.

        Raises:
            ScanInputValidationError: If the value cannot be compared to the configured limit.
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
        except TypeError:
            self._raise_validation_error(
                f"Invalid value for scan argument: {value!r} cannot be compared to {limit!r}."
            )
        return True

    def _bound_description(self, operator_name: str) -> str:
        """Return a user-facing description for a bound operator.

        Args:
            operator_name (str): Bound operator name, one of ``gt``, ``ge``, ``lt``, or ``le``.

        Returns:
            A human-readable description of the bound.
        """
        return {
            "gt": "greater than",
            "ge": "greater than or equal to",
            "lt": "less than",
            "le": "less than or equal to",
        }[operator_name]
