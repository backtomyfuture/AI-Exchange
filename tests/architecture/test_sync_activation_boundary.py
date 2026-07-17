from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass
from pathlib import Path

import pytest


_TARGET_CLASSES = frozenset({"ColdStartService", "SyncCoordinator"})
_TARGET_METHODS = frozenset({"apply", "approve", "preview", "resume", "run_folder"})
_TARGET_MODULES = frozenset({"src.ingestion.cold_start", "src.ingestion.sync"})
_TARGET_CONTAINER_IMPORTS = frozenset(
    {"ColdStartService", "SyncCoordinator", "cold_start", "ingestion", "sync"}
)
_DYNAMIC_IMPORT_NAMES = frozenset({"__import__", "import_module"})
_DYNAMIC_CODE_NAMES = frozenset({"compile", "eval", "exec"})
_NAMESPACE_REFLECTION_NAMES = frozenset({"globals", "locals", "vars"})
_ATTRIBUTE_REFLECTION_NAMES = frozenset(
    {
        "__getattribute__",
        "attrgetter",
        "delattr",
        "getattr",
        "hasattr",
        "itemgetter",
        "methodcaller",
        "setattr",
    }
)
_REGISTRATION_METHODS = frozenset(
    {
        "__setattr__",
        "__setitem__",
        "bind",
        "provide",
        "register",
        "setdefault",
        "update",
    }
)
_REGISTRY_SEGMENTS = frozenset(
    {
        "container",
        "dependencies",
        "dependency_container",
        "providers",
        "registry",
        "service_container",
        "services",
    }
)
_RUNTIME_EXACT_FILENAMES = frozenset({"exchange_service.py", "main.py", "server.py"})
_RUNTIME_PATH_MARKERS = ("scheduler", "startup", "worker")
_RUNTIME_SCAN_EXEMPT_PATHS = frozenset(
    {
        "src/ingestion/__init__.py",
        "src/ingestion/cold_start.py",
        "src/ingestion/sync.py",
    }
)
_REVIEWED_EXPORT_ONLY_EXEMPT_AST_SHA256 = {
    "src/ingestion/__init__.py": (
        "66d3fdb92975bce8a13df703b1a4e7dcec28463c691224832b950cadaeaf86bd"
    ),
}
_EXPORT_ONLY_IMPORT_PREFIX = "src.ingestion."
_RUNTIME_SERVICE_KEYS = frozenset(
    {
        "cold_start",
        "cold_start_service",
        "coordinator",
        "pipeline",
        "provider",
        "providers",
        "service",
        "services",
        "sync",
        "sync_coordinator",
    }
)

_PROV_TARGET_MODULE = "target_module"
_PROV_TARGET_CLASS = "target_class"
_PROV_TARGET_INSTANCE = "target_instance"
_PROV_TARGET_METHOD = "target_method"
_PROV_UNKNOWN_TARGET = "unknown_target"
_PROV_UNKNOWN_TARGET_MODULE = "unknown_target_module"
_PROV_TARGET_CLASS_ACCESSOR = "target_class_accessor"
_PROV_TARGET_METHOD_CALLER = "target_method_caller"
_PROV_DYNAMIC_IMPORT = "dynamic_import"
_PROV_IMPORT_MODULE = "import_module"
_PROV_DUNDER_IMPORT = "dunder_import"
_PROV_DYNAMIC_CODE = "dynamic_code"
_PROV_NAMESPACE = "namespace"
_PROV_NAMESPACE_GETTER = "namespace_getter"
_PROV_IMPORTLIB_MODULE = "importlib_module"
_PROV_OPERATOR_MODULE = "operator_module"
_PROV_BUILTINS_MODULE = "builtins_module"
_PROV_FUNCTOOLS_MODULE = "functools_module"
_PROV_COPY_MODULE = "copy_module"
_PROV_COLLECTIONS_MODULE = "collections_module"
_PROV_TYPES_MODULE = "types_module"
_PROV_OBJECT_TYPE = "object_type"
_PROV_PARTIAL_FACTORY = "partial_factory"
_PROV_MAPPING_FACTORY = "mapping_factory"
_PROV_SAFE_DYNAMIC_MODULE = "safe_dynamic_module"
_PROV_APP_STATE = "app_state"
_PROV_REGISTRY = "registry"
_PROV_REGISTRY_GETTER = "registry_getter"
_PROV_RUNTIME_SERVICE_ACCESSOR = "runtime_service_accessor"
_REFLECTION_TAG_PREFIX = "reflection:"
_BOUND_MAPPING_METHOD_TAG_PREFIX = "bound_mapping_method:"
_VALUE_TRANSFORM_TAG_PREFIX = "value_transform:"
_UNKNOWN_KEYWORD_EXPANSION = "\0unknown_keyword_expansion"

_TRANSFORM_IDENTITY = "identity"
_TRANSFORM_MAPPING_MERGE = "mapping_merge"

_BOUND_MAPPING_METHODS = frozenset(
    {
        "__getitem__",
        "__setattr__",
        "__setitem__",
        "append",
        "bind",
        "copy",
        "extend",
        "get",
        "insert",
        "pop",
        "provide",
        "register",
        "setdefault",
        "update",
    }
)

_TARGET_PROVENANCE = frozenset(
    {
        _PROV_TARGET_MODULE,
        _PROV_TARGET_CLASS,
        _PROV_TARGET_INSTANCE,
        _PROV_TARGET_METHOD,
        _PROV_UNKNOWN_TARGET,
        _PROV_UNKNOWN_TARGET_MODULE,
    }
)


@dataclass(frozen=True, order=True)
class _Violation:
    filename: str
    lineno: int
    col_offset: int
    code: str
    detail: str


@dataclass(frozen=True)
class _AbstractValue:
    tags: frozenset[str] = frozenset()
    strings: frozenset[str] | None = frozenset()
    items: tuple[_AbstractValue, ...] | None = None
    mapping: tuple[tuple[str | None, _AbstractValue], ...] | None = None
    forwarder: _LambdaForwarder | _FunctionForwarder | None = None
    partial: _PartialValue | None = None
    operation: _BoundOperation | None = None
    bound_owner_name: str | None = None


@dataclass(frozen=True)
class _LambdaForwarder:
    node: ast.Lambda


@dataclass(frozen=True)
class _FunctionForwarder:
    node: ast.FunctionDef | ast.AsyncFunctionDef


@dataclass(frozen=True)
class _PartialValue:
    function: _AbstractValue
    args: tuple[_AbstractValue, ...]
    keywords: tuple[tuple[str, _AbstractValue], ...]


@dataclass(frozen=True)
class _BoundOperation:
    method: str
    arguments: tuple[_AbstractValue, ...] = ()
    keywords: tuple[tuple[str, _AbstractValue], ...] = ()
    invoke: bool = False
    unbound: bool = False


def _merge_abstract_values(values: list[_AbstractValue]) -> _AbstractValue:
    if not values:
        return _AbstractValue()
    tags = frozenset(tag for value in values for tag in value.tags)
    strings = (
        None
        if any(value.strings is None for value in values)
        else frozenset(
            text for value in values for text in (value.strings or frozenset())
        )
    )
    item_lengths = {len(value.items) for value in values if value.items is not None}
    if len(item_lengths) == 1 and all(value.items is not None for value in values):
        item_count = item_lengths.pop()
        items = tuple(
            _merge_abstract_values(
                [value.items[index] for value in values if value.items is not None]
            )
            for index in range(item_count)
        )
    else:
        items = None
    mappings = [value.mapping for value in values]
    if all(mapping is not None for mapping in mappings):
        flattened_mapping = tuple(
            entry for mapping in mappings if mapping is not None for entry in mapping
        )
        mapping = flattened_mapping if len(flattened_mapping) <= 256 else None
    else:
        mapping = None
    forwarder = (
        values[0].forwarder
        if all(value.forwarder is values[0].forwarder for value in values)
        else None
    )
    partial = (
        values[0].partial
        if all(value.partial == values[0].partial for value in values)
        else None
    )
    operation = (
        values[0].operation
        if all(value.operation == values[0].operation for value in values)
        else None
    )
    bound_owner_name = (
        values[0].bound_owner_name
        if all(value.bound_owner_name == values[0].bound_owner_name for value in values)
        else None
    )
    return _AbstractValue(
        tags=tags,
        strings=strings,
        items=items,
        mapping=mapping,
        forwarder=forwarder,
        partial=partial,
        operation=operation,
        bound_owner_name=bound_owner_name,
    )


def _reflection_tag(name: str) -> str:
    return f"{_REFLECTION_TAG_PREFIX}{name}"


def _reflection_names(value: _AbstractValue) -> frozenset[str]:
    return frozenset(
        tag.removeprefix(_REFLECTION_TAG_PREFIX)
        for tag in value.tags
        if tag.startswith(_REFLECTION_TAG_PREFIX)
    )


def _bound_mapping_method_tag(name: str) -> str:
    return f"{_BOUND_MAPPING_METHOD_TAG_PREFIX}{name}"


def _bound_mapping_method_names(value: _AbstractValue) -> frozenset[str]:
    return frozenset(
        tag.removeprefix(_BOUND_MAPPING_METHOD_TAG_PREFIX)
        for tag in value.tags
        if tag.startswith(_BOUND_MAPPING_METHOD_TAG_PREFIX)
    )


def _value_transform_tag(name: str) -> str:
    return f"{_VALUE_TRANSFORM_TAG_PREFIX}{name}"


def _value_transform_names(value: _AbstractValue) -> frozenset[str]:
    return frozenset(
        tag.removeprefix(_VALUE_TRANSFORM_TAG_PREFIX)
        for tag in value.tags
        if tag.startswith(_VALUE_TRANSFORM_TAG_PREFIX)
    )


def _expression_path(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        owner = _expression_path(node.value)
        return f"{owner}.{node.attr}" if owner is not None else node.attr
    return None


def _constant_text(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _constant_text(node.left)
        right = _constant_text(node.right)
        return left + right if left is not None and right is not None else None
    if not isinstance(node, ast.JoinedStr):
        return None
    parts: list[str] = []
    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parts.append(value.value)
        elif isinstance(value, ast.FormattedValue):
            rendered = _constant_text(value.value)
            if rendered is None:
                return None
            parts.append(rendered)
        else:
            return None
    return "".join(parts)


def _normalized_identifier(value: str) -> str:
    return "".join(character for character in value.casefold() if character.isalnum())


def _module_is_target(value: str) -> bool:
    module = value.casefold().lstrip(".")
    suffixes = {target.removeprefix("src.") for target in _TARGET_MODULES}
    return module in (_TARGET_MODULES | suffixes) or any(
        module.endswith(f".{target}") for target in (_TARGET_MODULES | suffixes)
    )


def _module_exposes_target(value: str) -> bool:
    module = value.casefold().lstrip(".")
    return _module_is_target(module) or module in {"ingestion", "src.ingestion"}


def _identifier_is_target(value: str) -> bool:
    normalized = _normalized_identifier(value)
    return (
        normalized in {"coldstartservice", "synccoordinator"}
        or "coldstartservice" in normalized
        or "synccoordinator" in normalized
    )


def _identifier_may_reference_target(value: str) -> bool:
    normalized = _normalized_identifier(value)
    return _identifier_is_target(value) or any(
        marker in normalized
        for marker in ("coldstart", "coordinator", "pipeline", "service", "sync")
    )


def _identifier_may_reference_target_module(value: str) -> bool:
    normalized = _normalized_identifier(value)
    return normalized == "module" or normalized.endswith("module")


def _reflection_name_is_target(value: str) -> bool:
    return (
        _identifier_is_target(value)
        or value in _TARGET_METHODS
        or _module_is_target(value)
    )


def _reflection_name_is_forbidden(value: str) -> bool:
    return (
        _reflection_name_is_target(value)
        or value in _DYNAMIC_IMPORT_NAMES
        or value in _DYNAMIC_CODE_NAMES
        or value in _NAMESPACE_REFLECTION_NAMES
        or value in _ATTRIBUTE_REFLECTION_NAMES
    )


def _path_segments(node: ast.AST) -> tuple[str, ...]:
    path = _expression_path(node)
    if path is None:
        return ()
    return tuple(segment.casefold() for segment in path.split("."))


def _is_app_state_expression(node: ast.AST) -> bool:
    segments = _path_segments(node)
    return any(
        left in {"app", "application"} and right == "state"
        for left, right in zip(segments, segments[1:], strict=False)
    )


def _is_registry_expression(node: ast.AST) -> bool:
    if isinstance(node, ast.Subscript):
        key = _constant_text(node.slice)
        return _is_registry_expression(node.value) or (
            key is not None and key.casefold() in _REGISTRY_SEGMENTS
        )
    return any(segment in _REGISTRY_SEGMENTS for segment in _path_segments(node))


def _node_contains_target_signal(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and _identifier_is_target(child.id):
            return True
        if isinstance(child, ast.Attribute) and (
            _identifier_is_target(child.attr) or child.attr in _TARGET_METHODS
        ):
            return True
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            if _reflection_name_is_target(child.value):
                return True
    return False


def _assignment_registers_runtime_service(target: ast.AST, value: ast.AST) -> bool:
    if _is_app_state_expression(target):
        return True
    if isinstance(target, ast.Subscript) and _is_registry_expression(target.value):
        return True
    if isinstance(target, ast.Attribute) and _is_registry_expression(target.value):
        return True
    if isinstance(target, ast.Name) and _is_registry_expression(target):
        return _node_contains_target_signal(value)
    if isinstance(target, ast.Subscript):
        key = _constant_text(target.slice)
        return key is not None and _reflection_name_is_target(key)
    return False


class _ActivationBoundaryScanner(ast.NodeVisitor):
    def __init__(self, filename: str) -> None:
        self._filename = filename
        self._violations: list[_Violation] = []
        self._seen: set[tuple[int, int, str]] = set()
        self._scopes: list[dict[str, _AbstractValue]] = [{}]
        self._constant_dynamic_imports: set[str] = set()
        self._module_definitions: set[str] = set()
        self._module_definition_values: dict[str, _AbstractValue] = {}
        self._module_execution_depth = 0
        self._active_function_calls: set[tuple[int, int]] = set()

    def predeclare_module_definitions(self, tree: ast.Module) -> None:
        definitions: dict[str, _AbstractValue] = {}
        for statement in tree.body:
            if isinstance(statement, (ast.AsyncFunctionDef, ast.FunctionDef)):
                definitions[statement.name] = _AbstractValue(
                    strings=None,
                    forwarder=_FunctionForwarder(node=statement),
                )
            elif isinstance(statement, ast.ClassDef):
                definitions[statement.name] = _AbstractValue(strings=None)
        self._module_definition_values = definitions
        self._module_definitions = set(definitions)

    @property
    def violations(self) -> list[_Violation]:
        return sorted(self._violations)

    @property
    def constant_dynamic_imports(self) -> frozenset[str]:
        return frozenset(self._constant_dynamic_imports)

    def _report(self, node: ast.AST, code: str, detail: str) -> None:
        lineno = int(getattr(node, "lineno", 1))
        col_offset = int(getattr(node, "col_offset", 0))
        identity = (lineno, col_offset, code)
        if identity in self._seen:
            return
        self._seen.add(identity)
        self._violations.append(
            _Violation(
                filename=self._filename,
                lineno=lineno,
                col_offset=col_offset,
                code=code,
                detail=detail,
            )
        )

    def _lookup(self, name: str) -> _AbstractValue | None:
        for scope in reversed(self._scopes[1:]):
            if name in scope:
                return scope[name]
        if (
            len(self._scopes) > 1
            and self._module_execution_depth == 0
            and name in self._module_definition_values
        ):
            return self._module_definition_values[name]
        return self._scopes[0].get(name)

    def _bind(self, name: str, value: _AbstractValue) -> None:
        self._scopes[-1][name] = value

    def _rebind(self, name: str, value: _AbstractValue) -> None:
        for scope in reversed(self._scopes):
            if name in scope:
                scope[name] = value
                return
        self._bind(name, value)

    def _unknown_name_value(self, name: str) -> _AbstractValue:
        tags: set[str] = set()
        if _identifier_is_target(name):
            tags.add(_PROV_TARGET_CLASS)
        elif _identifier_may_reference_target(name):
            tags.add(_PROV_UNKNOWN_TARGET)
        elif _identifier_may_reference_target_module(name):
            tags.add(_PROV_UNKNOWN_TARGET_MODULE)
        if name == "__import__":
            tags.update({_PROV_DYNAMIC_IMPORT, _PROV_DUNDER_IMPORT})
        elif name == "import_module":
            tags.update({_PROV_DYNAMIC_IMPORT, _PROV_IMPORT_MODULE})
        if name in _DYNAMIC_CODE_NAMES:
            tags.add(_PROV_DYNAMIC_CODE)
        if name in _NAMESPACE_REFLECTION_NAMES:
            tags.add(_PROV_NAMESPACE)
        if name in _ATTRIBUTE_REFLECTION_NAMES:
            tags.add(_reflection_tag(name))
        if name == "importlib":
            tags.add(_PROV_IMPORTLIB_MODULE)
        elif name == "operator":
            tags.add(_PROV_OPERATOR_MODULE)
        elif name == "builtins":
            tags.add(_PROV_BUILTINS_MODULE)
        elif name == "functools":
            tags.add(_PROV_FUNCTOOLS_MODULE)
        elif name == "copy":
            tags.add(_PROV_COPY_MODULE)
        elif name == "collections":
            tags.add(_PROV_COLLECTIONS_MODULE)
        elif name == "types":
            tags.add(_PROV_TYPES_MODULE)
        elif name == "object":
            tags.add(_PROV_OBJECT_TYPE)
        elif name == "dict":
            tags.add(_PROV_MAPPING_FACTORY)
        if name.casefold() in _REGISTRY_SEGMENTS:
            tags.add(_PROV_REGISTRY)
        return _AbstractValue(tags=frozenset(tags), strings=None)

    def _unknown_parameter_value(self, name: str) -> _AbstractValue:
        tags: set[str] = set()
        if _identifier_is_target(name):
            tags.add(_PROV_TARGET_CLASS)
        elif _identifier_may_reference_target(name):
            tags.add(_PROV_UNKNOWN_TARGET)
        elif _identifier_may_reference_target_module(name):
            tags.add(_PROV_UNKNOWN_TARGET_MODULE)
        if name.casefold() in _REGISTRY_SEGMENTS:
            tags.add(_PROV_REGISTRY)
        return _AbstractValue(tags=frozenset(tags), strings=None)

    def _name_value(self, name: str) -> _AbstractValue:
        bound = self._lookup(name)
        return bound if bound is not None else self._unknown_name_value(name)

    def _string_product(
        self,
        left: frozenset[str] | None,
        right: frozenset[str] | None,
    ) -> frozenset[str] | None:
        if left is None or right is None or not left or not right:
            return None
        values = {prefix + suffix for prefix in left for suffix in right}
        return frozenset(values) if len(values) <= 256 else None

    def _value(self, node: ast.AST) -> _AbstractValue:
        if isinstance(node, ast.Name):
            return self._name_value(node.id)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                return _AbstractValue(strings=frozenset({node.value}))
            return _AbstractValue()
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return _merge_abstract_values(
                [self._value(node.left), self._value(node.right)]
            )
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = self._value(node.left)
            right = self._value(node.right)
            return _AbstractValue(
                tags=left.tags | right.tags,
                strings=self._string_product(left.strings, right.strings),
            )
        if isinstance(node, ast.JoinedStr):
            possible = frozenset({""})
            tags: frozenset[str] = frozenset()
            for part in node.values:
                if isinstance(part, ast.FormattedValue):
                    value = self._value(part.value)
                else:
                    value = self._value(part)
                tags |= value.tags
                possible = self._string_product(possible, value.strings)
                if possible is None:
                    break
            return _AbstractValue(tags=tags, strings=possible)
        if isinstance(node, ast.IfExp):
            return _merge_abstract_values(
                [self._value(node.body), self._value(node.orelse)]
            )
        if isinstance(node, ast.BoolOp):
            return _merge_abstract_values([self._value(value) for value in node.values])
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            items = tuple(self._value(element) for element in node.elts)
            merged = _merge_abstract_values(list(items))
            return _AbstractValue(tags=merged.tags, items=items)
        if isinstance(node, ast.Dict):
            entries: list[tuple[str | None, _AbstractValue]] = []
            values: list[_AbstractValue] = []
            for key_node, value_node in zip(node.keys, node.values, strict=True):
                value = self._value(value_node)
                values.append(value)
                if key_node is None:
                    if value.mapping is not None:
                        entries.extend(value.mapping)
                    else:
                        entries.append((None, value))
                    continue
                key_value = self._value(key_node)
                keys = key_value.strings if key_value is not None else None
                if keys:
                    entries.extend((key, value) for key in keys)
                else:
                    entries.append((None, value))
            merged = _merge_abstract_values(values)
            return _AbstractValue(tags=merged.tags, mapping=tuple(entries))
        if isinstance(node, (ast.GeneratorExp, ast.ListComp, ast.SetComp)):
            return self._sequence_comprehension_value(node)
        if isinstance(node, ast.DictComp):
            return self._dict_comprehension_value(node)
        if isinstance(node, ast.Attribute):
            return self._attribute_value(node)
        if isinstance(node, ast.Subscript):
            return self._subscript_value(node)
        if isinstance(node, ast.NamedExpr):
            return self._value(node.value)
        if isinstance(node, ast.Lambda):
            return _AbstractValue(
                strings=None,
                forwarder=_LambdaForwarder(node=node),
            )
        if isinstance(node, ast.Call):
            return self._call_result(node)
        return _AbstractValue(strings=None)

    def _bind_comprehension_generators(
        self,
        generators: list[ast.comprehension],
    ) -> None:
        for generator in generators:
            iterable = self._value(generator.iter)
            loop_value = (
                _merge_abstract_values(list(iterable.items))
                if iterable.items is not None
                else iterable
            )
            self._bind_target(generator.target, loop_value)

    def _sequence_comprehension_value(
        self,
        node: ast.GeneratorExp | ast.ListComp | ast.SetComp,
    ) -> _AbstractValue:
        self._scopes.append({})
        try:
            self._bind_comprehension_generators(node.generators)
            element = self._value(node.elt)
            return _AbstractValue(tags=element.tags, items=(element,))
        finally:
            self._scopes.pop()

    def _dict_comprehension_value(self, node: ast.DictComp) -> _AbstractValue:
        self._scopes.append({})
        try:
            self._bind_comprehension_generators(node.generators)
            key = self._value(node.key)
            value = self._value(node.value)
            keys = key.strings
            mapping = (
                tuple((item, value) for item in keys) if keys else ((None, value),)
            )
            return _AbstractValue(tags=value.tags, mapping=mapping)
        finally:
            self._scopes.pop()

    def _attribute_value(self, node: ast.Attribute) -> _AbstractValue:
        owner = self._value(node.value)
        if _identifier_is_target(node.attr):
            return _AbstractValue(tags=frozenset({_PROV_TARGET_CLASS}), strings=None)
        if node.attr in _TARGET_METHODS and bool(owner.tags & _TARGET_PROVENANCE):
            return _AbstractValue(tags=frozenset({_PROV_TARGET_METHOD}), strings=None)
        if node.attr.casefold() in _RUNTIME_SERVICE_KEYS and (
            _PROV_APP_STATE in owner.tags or _is_registry_expression(node.value)
        ):
            return _AbstractValue(tags=frozenset({_PROV_UNKNOWN_TARGET}), strings=None)
        if node.attr in _DYNAMIC_IMPORT_NAMES and (
            _PROV_IMPORTLIB_MODULE in owner.tags or _PROV_BUILTINS_MODULE in owner.tags
        ):
            kind = (
                _PROV_DUNDER_IMPORT
                if node.attr == "__import__"
                else _PROV_IMPORT_MODULE
            )
            return _AbstractValue(
                tags=frozenset({_PROV_DYNAMIC_IMPORT, kind}),
                strings=None,
            )
        if node.attr in _DYNAMIC_CODE_NAMES and _PROV_BUILTINS_MODULE in owner.tags:
            return _AbstractValue(tags=frozenset({_PROV_DYNAMIC_CODE}), strings=None)
        if (
            node.attr in _NAMESPACE_REFLECTION_NAMES
            and _PROV_BUILTINS_MODULE in owner.tags
        ):
            return _AbstractValue(tags=frozenset({_PROV_NAMESPACE}), strings=None)
        if (
            node.attr in {"attrgetter", "itemgetter", "methodcaller"}
            and _PROV_OPERATOR_MODULE in owner.tags
        ):
            return _AbstractValue(
                tags=frozenset({_reflection_tag(node.attr)}),
                strings=None,
            )
        if node.attr in {"getitem", "setitem"} and _PROV_OPERATOR_MODULE in owner.tags:
            return _AbstractValue(
                strings=None,
                operation=_BoundOperation(
                    method=f"__{node.attr}__",
                    invoke=True,
                    unbound=True,
                ),
            )
        if (
            node.attr in {"__getitem__", "__setitem__", "get", "setdefault", "update"}
            and _PROV_MAPPING_FACTORY in owner.tags
        ):
            return _AbstractValue(
                strings=None,
                operation=_BoundOperation(
                    method=node.attr,
                    invoke=True,
                    unbound=True,
                ),
            )
        if node.attr == "partial" and _PROV_FUNCTOOLS_MODULE in owner.tags:
            return _AbstractValue(
                tags=frozenset({_PROV_PARTIAL_FACTORY}),
                strings=None,
            )
        if node.attr in {"copy", "deepcopy"} and _PROV_COPY_MODULE in owner.tags:
            return _AbstractValue(
                tags=frozenset({_value_transform_tag(_TRANSFORM_IDENTITY)}),
                strings=None,
            )
        if node.attr == "MappingProxyType" and _PROV_TYPES_MODULE in owner.tags:
            return _AbstractValue(
                tags=frozenset({_value_transform_tag(_TRANSFORM_IDENTITY)}),
                strings=None,
            )
        if node.attr == "ChainMap" and _PROV_COLLECTIONS_MODULE in owner.tags:
            return _AbstractValue(
                tags=frozenset({_value_transform_tag(_TRANSFORM_MAPPING_MERGE)}),
                strings=None,
            )
        if node.attr in _ATTRIBUTE_REFLECTION_NAMES and (
            _PROV_BUILTINS_MODULE in owner.tags
            or (node.attr == "__getattribute__" and _PROV_OBJECT_TYPE in owner.tags)
        ):
            return _AbstractValue(
                tags=frozenset({_reflection_tag(node.attr)}),
                strings=None,
            )
        if node.attr == "__getattribute__" and _PROV_OBJECT_TYPE not in owner.tags:
            primitive = _AbstractValue(
                tags=frozenset({_reflection_tag(node.attr)}),
                strings=None,
            )
            return _AbstractValue(
                tags=primitive.tags,
                strings=None,
                partial=_PartialValue(
                    function=primitive,
                    args=(owner,),
                    keywords=(),
                ),
            )
        if node.attr == "__dict__":
            return _AbstractValue(
                tags=owner.tags
                | frozenset({_PROV_NAMESPACE})
                | (
                    frozenset({_PROV_REGISTRY})
                    if _is_registry_expression(node.value)
                    else frozenset()
                ),
                strings=None,
            )
        if node.attr in _BOUND_MAPPING_METHODS and (
            owner.mapping is not None
            or owner.items is not None
            or bool(owner.tags & {_PROV_APP_STATE, _PROV_NAMESPACE, _PROV_REGISTRY})
            or _is_registry_expression(node.value)
        ):
            compatibility_tags: set[str] = set()
            if node.attr == "get" and _PROV_NAMESPACE in owner.tags:
                compatibility_tags.add(_PROV_NAMESPACE_GETTER)
            if node.attr == "get" and bool(
                owner.tags & {_PROV_APP_STATE, _PROV_REGISTRY}
            ):
                compatibility_tags.add(_PROV_REGISTRY_GETTER)
            return self._bound_mapping_method_value(
                owner,
                node.attr,
                compatibility_tags=frozenset(compatibility_tags),
                owner_name=node.value.id if isinstance(node.value, ast.Name) else None,
            )
        if _is_app_state_expression(node):
            return _AbstractValue(tags=frozenset({_PROV_APP_STATE}), strings=None)
        if _is_registry_expression(node):
            return _AbstractValue(tags=frozenset({_PROV_REGISTRY}), strings=None)
        return _AbstractValue(strings=None)

    def _bound_mapping_method_value(
        self,
        owner: _AbstractValue,
        method: str,
        *,
        compatibility_tags: frozenset[str] = frozenset(),
        owner_name: str | None = None,
    ) -> _AbstractValue:
        return _AbstractValue(
            tags=owner.tags
            | compatibility_tags
            | frozenset({_bound_mapping_method_tag(method)}),
            strings=owner.strings,
            items=owner.items,
            mapping=owner.mapping,
            bound_owner_name=owner_name,
        )

    def _subscript_value(self, node: ast.Subscript) -> _AbstractValue:
        container = self._value(node.value)
        key = self._value(node.slice)
        if container.items is not None:
            if isinstance(node.slice, ast.Constant) and isinstance(
                node.slice.value, int
            ):
                index = node.slice.value
                if -len(container.items) <= index < len(container.items):
                    return container.items[index]
            return _merge_abstract_values(list(container.items))
        if container.mapping is not None:
            values = [
                value
                for mapped_key, value in container.mapping
                if key.strings is None
                or mapped_key is None
                or mapped_key in key.strings
            ]
            return _merge_abstract_values(values)
        if key.strings:
            reflected = _merge_abstract_values(
                [self._reflected_name_value(name) for name in key.strings]
            )
            if reflected.tags:
                return reflected
        if _PROV_NAMESPACE in container.tags:
            return _AbstractValue(tags=frozenset({_PROV_UNKNOWN_TARGET}), strings=None)
        return _AbstractValue(strings=None)

    def _reflected_name_value(self, name: str) -> _AbstractValue:
        if _identifier_is_target(name):
            return _AbstractValue(tags=frozenset({_PROV_TARGET_CLASS}), strings=None)
        if name in _TARGET_METHODS:
            return _AbstractValue(tags=frozenset({_PROV_TARGET_METHOD}), strings=None)
        if _module_is_target(name):
            return _AbstractValue(tags=frozenset({_PROV_TARGET_MODULE}), strings=None)
        if name in _DYNAMIC_IMPORT_NAMES:
            return _AbstractValue(tags=frozenset({_PROV_DYNAMIC_IMPORT}), strings=None)
        if name in _DYNAMIC_CODE_NAMES:
            return _AbstractValue(tags=frozenset({_PROV_DYNAMIC_CODE}), strings=None)
        if name in _NAMESPACE_REFLECTION_NAMES:
            return _AbstractValue(tags=frozenset({_PROV_NAMESPACE}), strings=None)
        if name in _ATTRIBUTE_REFLECTION_NAMES:
            return _AbstractValue(tags=frozenset({_reflection_tag(name)}), strings=None)
        return _AbstractValue(strings=None)

    def _reflection_name_value(
        self,
        node: ast.Call,
        primitives: frozenset[str],
        arguments: tuple[_AbstractValue, ...] | None = None,
    ) -> _AbstractValue:
        values_by_position = arguments or tuple(self._value(arg) for arg in node.args)
        indexes: set[int] = set()
        if primitives & {"attrgetter", "itemgetter", "methodcaller"}:
            indexes.add(0)
        if primitives - {"attrgetter", "itemgetter", "methodcaller"}:
            indexes.add(1)
        values = [
            values_by_position[index]
            for index in indexes
            if index < len(values_by_position)
        ]
        return (
            _merge_abstract_values(values) if values else _AbstractValue(strings=None)
        )

    def _reflection_receiver_value(
        self,
        node: ast.Call,
        primitives: frozenset[str],
        arguments: tuple[_AbstractValue, ...] | None = None,
    ) -> _AbstractValue | None:
        values_by_position = arguments or tuple(self._value(arg) for arg in node.args)
        if (
            primitives <= {"attrgetter", "itemgetter", "methodcaller"}
            or not values_by_position
        ):
            return None
        return values_by_position[0]

    def _contains_runtime_service_key(self, value: _AbstractValue) -> bool:
        return bool(
            value.strings
            and any(key.casefold() in _RUNTIME_SERVICE_KEYS for key in value.strings)
        )

    def _reflection_builds_runtime_service_accessor(
        self,
        primitives: frozenset[str],
        arguments: tuple[_AbstractValue, ...],
    ) -> bool:
        if primitives & {"attrgetter", "itemgetter"}:
            return bool(arguments) and self._contains_runtime_service_key(arguments[0])
        if "methodcaller" not in primitives or len(arguments) < 2:
            return False
        methods = arguments[0].strings or frozenset()
        return bool(
            methods & {"__getattribute__", "__getitem__", "get"}
            and self._contains_runtime_service_key(arguments[1])
        )

    def _reflection_reads_runtime_service_slot(
        self,
        node: ast.Call,
        primitives: frozenset[str],
        arguments: tuple[_AbstractValue, ...],
    ) -> bool:
        receiver = self._reflection_receiver_value(node, primitives, arguments)
        if receiver is None or not self._value_is_runtime_container(receiver):
            return False
        name = self._reflection_name_value(node, primitives, arguments)
        return self._contains_runtime_service_key(name)

    def _reflection_result(
        self,
        node: ast.Call,
        primitives: frozenset[str],
        arguments: tuple[_AbstractValue, ...] | None = None,
        keyword_arguments: dict[str, _AbstractValue] | None = None,
    ) -> _AbstractValue:
        values_by_position = arguments or tuple(self._value(arg) for arg in node.args)
        name_value = self._reflection_name_value(node, primitives, arguments)
        names = name_value.strings
        if names and len(names) == 1:
            method = next(iter(names))
            if method in _BOUND_MAPPING_METHODS and "attrgetter" in primitives:
                return _AbstractValue(
                    strings=None,
                    operation=_BoundOperation(method=method),
                )
            if method in _BOUND_MAPPING_METHODS and "methodcaller" in primitives:
                return _AbstractValue(
                    strings=None,
                    operation=_BoundOperation(
                        method=method,
                        arguments=values_by_position[1:],
                        keywords=tuple((keyword_arguments or {}).items()),
                        invoke=True,
                    ),
                )
        if self._reflection_builds_runtime_service_accessor(
            primitives,
            values_by_position,
        ):
            return _AbstractValue(
                tags=frozenset({_PROV_RUNTIME_SERVICE_ACCESSOR}),
                strings=None,
            )
        if names and self._reflection_reads_runtime_service_slot(
            node,
            primitives,
            values_by_position,
        ):
            return _AbstractValue(
                tags=frozenset({_PROV_UNKNOWN_TARGET}),
                strings=None,
            )
        if names:
            receiver = self._reflection_receiver_value(
                node,
                primitives,
                values_by_position,
            )
            if (
                receiver is not None
                and len(names) == 1
                and next(iter(names)) in _BOUND_MAPPING_METHODS
                and (
                    receiver.mapping is not None
                    or self._value_is_runtime_container(receiver)
                    or _PROV_NAMESPACE in receiver.tags
                )
            ):
                method_name = next(iter(names))
                return self._bound_mapping_method_value(receiver, method_name)
            reflected = _merge_abstract_values(
                [self._reflected_name_value(name) for name in names]
            )
            if "attrgetter" in primitives and _PROV_TARGET_CLASS in reflected.tags:
                return _AbstractValue(
                    tags=frozenset({_PROV_TARGET_CLASS_ACCESSOR}),
                    strings=None,
                )
            if "methodcaller" in primitives and _PROV_TARGET_METHOD in reflected.tags:
                return _AbstractValue(
                    tags=frozenset({_PROV_TARGET_METHOD_CALLER}),
                    strings=None,
                )
            return reflected
        receiver = self._reflection_receiver_value(node, primitives, arguments)
        if receiver is not None and bool(receiver.tags & _TARGET_PROVENANCE):
            return _AbstractValue(tags=frozenset({_PROV_UNKNOWN_TARGET}), strings=None)
        return _AbstractValue(strings=None)

    def _call_values(
        self,
        node: ast.Call,
    ) -> tuple[tuple[_AbstractValue, ...], dict[str, _AbstractValue]]:
        arguments = tuple(self._value(argument) for argument in node.args)
        keywords: dict[str, _AbstractValue] = {}
        unknown_expansions: list[_AbstractValue] = []
        for keyword in node.keywords:
            value = self._value(keyword.value)
            if keyword.arg is not None:
                keywords[keyword.arg] = value
                continue
            if value.mapping is None:
                unknown_expansions.append(value)
                continue
            for key, mapped_value in value.mapping:
                if key is None:
                    unknown_expansions.append(mapped_value)
                else:
                    keywords[key] = mapped_value
        if unknown_expansions:
            keywords[_UNKNOWN_KEYWORD_EXPANSION] = _merge_abstract_values(
                unknown_expansions
            )
        return arguments, keywords

    def _effective_call(
        self,
        function: _AbstractValue,
        node: ast.Call,
    ) -> tuple[_AbstractValue, tuple[_AbstractValue, ...], dict[str, _AbstractValue]]:
        arguments, keywords = self._call_values(node)
        while function.partial is not None:
            partial = function.partial
            arguments = (*partial.args, *arguments)
            keywords = {**dict(partial.keywords), **keywords}
            function = partial.function
        return function, arguments, keywords

    def _string_items(self, value: _AbstractValue | None) -> frozenset[str]:
        if value is None:
            return frozenset()
        strings = set(value.strings or ())
        if value.items is not None:
            for item in value.items:
                strings.update(item.strings or ())
        return frozenset(strings)

    def _resolved_dynamic_modules(
        self,
        function: _AbstractValue,
        arguments: tuple[_AbstractValue, ...],
        keywords: dict[str, _AbstractValue],
    ) -> frozenset[str] | None:
        modules = arguments[0].strings if arguments else None
        if not modules:
            return None
        resolved: set[str] = set()
        packages = self._string_items(keywords.get("package"))
        for module in modules:
            if module.startswith(".") and _PROV_IMPORT_MODULE in function.tags:
                dot_count = len(module) - len(module.lstrip("."))
                suffix = module[dot_count:]
                for package in packages:
                    package_parts = package.split(".")
                    keep = len(package_parts) - dot_count + 1
                    if keep <= 0:
                        continue
                    base = ".".join(package_parts[:keep])
                    resolved.add(f"{base}.{suffix}" if suffix else base)
                continue
            resolved.add(module)

        if _PROV_DUNDER_IMPORT in function.tags:
            fromlist = keywords.get("fromlist")
            if fromlist is None and len(arguments) > 3:
                fromlist = arguments[3]
            for module in tuple(resolved):
                for imported in self._string_items(fromlist):
                    if imported != "*" and all(
                        part.isidentifier() for part in imported.split(".")
                    ):
                        resolved.add(f"{module}.{imported}")
        return frozenset(resolved)

    def _dynamic_import_result(
        self,
        function: _AbstractValue,
        arguments: tuple[_AbstractValue, ...],
        keywords: dict[str, _AbstractValue],
    ) -> _AbstractValue:
        modules = self._resolved_dynamic_modules(function, arguments, keywords)
        if modules and any(_module_exposes_target(module) for module in modules):
            return _AbstractValue(tags=frozenset({_PROV_TARGET_MODULE}), strings=None)
        if modules:
            tags: set[str] = set()
            if "builtins" in modules:
                tags.add(_PROV_BUILTINS_MODULE)
            if "importlib" in modules:
                tags.add(_PROV_IMPORTLIB_MODULE)
            if "operator" in modules:
                tags.add(_PROV_OPERATOR_MODULE)
            if "functools" in modules:
                tags.add(_PROV_FUNCTOOLS_MODULE)
            if tags:
                return _AbstractValue(tags=frozenset(tags), strings=None)
            return _AbstractValue(
                tags=frozenset({_PROV_SAFE_DYNAMIC_MODULE}),
                strings=None,
            )
        return _AbstractValue(
            tags=frozenset({_PROV_UNKNOWN_TARGET_MODULE}),
            strings=None,
        )

    def _callable_bindings(
        self,
        arguments_node: ast.arguments,
        node: ast.Call,
    ) -> dict[str, _AbstractValue]:
        positional_names = [
            argument.arg
            for argument in (*arguments_node.posonlyargs, *arguments_node.args)
        ]
        values, keywords = self._call_values(node)
        bindings = {
            name: values[index]
            for index, name in enumerate(positional_names)
            if index < len(values)
        }
        bindings.update(
            (name, keywords[name]) for name in positional_names if name in keywords
        )
        for argument in arguments_node.kwonlyargs:
            if argument.arg in keywords:
                bindings[argument.arg] = keywords[argument.arg]
        for name in positional_names:
            bindings.setdefault(name, self._unknown_parameter_value(name))
        for argument in arguments_node.kwonlyargs:
            bindings.setdefault(
                argument.arg,
                self._unknown_parameter_value(argument.arg),
            )
        if arguments_node.vararg is not None:
            bindings[arguments_node.vararg.arg] = _AbstractValue(
                items=tuple(values[len(positional_names) :]),
                strings=None,
            )
        if arguments_node.kwarg is not None:
            bindings[arguments_node.kwarg.arg] = _AbstractValue(
                mapping=tuple(
                    (
                        None if name == _UNKNOWN_KEYWORD_EXPANSION else name,
                        value,
                    )
                    for name, value in keywords.items()
                ),
                strings=None,
            )
        return bindings

    def _lambda_bindings(
        self,
        forwarder: _LambdaForwarder,
        node: ast.Call,
    ) -> dict[str, _AbstractValue]:
        return self._callable_bindings(forwarder.node.args, node)

    def _function_bindings(
        self,
        forwarder: _FunctionForwarder,
        node: ast.Call,
    ) -> dict[str, _AbstractValue]:
        return self._callable_bindings(forwarder.node.args, node)

    def _invoke_lambda_value(
        self,
        forwarder: _LambdaForwarder,
        node: ast.Call,
    ) -> _AbstractValue:
        self._scopes.append(self._lambda_bindings(forwarder, node))
        try:
            return self._value(forwarder.node.body)
        finally:
            self._scopes.pop()

    def _function_return_expressions(
        self,
        forwarder: _FunctionForwarder,
    ) -> list[ast.expr]:
        expressions: list[ast.expr] = []

        class _ReturnCollector(ast.NodeVisitor):
            def visit_Return(self, node: ast.Return) -> None:
                if node.value is not None:
                    expressions.append(node.value)

            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
                return None

            def visit_ClassDef(self, node: ast.ClassDef) -> None:
                return None

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                return None

            def visit_Lambda(self, node: ast.Lambda) -> None:
                return None

        collector = _ReturnCollector()
        for statement in forwarder.node.body:
            collector.visit(statement)
        return expressions

    def _invoke_function_value(
        self,
        forwarder: _FunctionForwarder,
        node: ast.Call,
    ) -> _AbstractValue:
        frame = (id(forwarder.node), id(node))
        if frame in self._active_function_calls:
            return _AbstractValue(strings=None)
        module_execution = self._module_execution_depth > 0 or len(self._scopes) == 1
        self._active_function_calls.add(frame)
        self._scopes.append(self._function_bindings(forwarder, node))
        if module_execution:
            self._module_execution_depth += 1
        try:
            values = [
                self._value(expression)
                for expression in self._function_return_expressions(forwarder)
            ]
            return _merge_abstract_values(values)
        finally:
            if module_execution:
                self._module_execution_depth -= 1
            self._scopes.pop()
            self._active_function_calls.remove(frame)

    def _call_result(self, node: ast.Call) -> _AbstractValue:
        raw_function = self._value(node.func)
        if isinstance(raw_function.forwarder, _LambdaForwarder):
            return self._invoke_lambda_value(raw_function.forwarder, node)
        if isinstance(raw_function.forwarder, _FunctionForwarder):
            return self._invoke_function_value(raw_function.forwarder, node)
        function, arguments, keywords = self._effective_call(raw_function, node)
        if _PROV_PARTIAL_FACTORY in function.tags:
            if not arguments:
                return _AbstractValue(strings=None)
            underlying = arguments[0]
            return _AbstractValue(
                tags=underlying.tags,
                strings=None,
                partial=_PartialValue(
                    function=underlying,
                    args=arguments[1:],
                    keywords=tuple(keywords.items()),
                ),
            )
        transforms = _value_transform_names(function)
        if _TRANSFORM_IDENTITY in transforms:
            return arguments[0] if arguments else _AbstractValue(strings=None)
        if _TRANSFORM_MAPPING_MERGE in transforms:
            return _merge_abstract_values(list(arguments))
        if _PROV_MAPPING_FACTORY in function.tags:
            entries: list[tuple[str | None, _AbstractValue]] = []
            tags: set[str] = set()
            if arguments:
                source = arguments[0]
                tags.update(source.tags)
                if source.mapping is not None:
                    entries.extend(source.mapping)
                else:
                    entries.append((None, source))
            entries.extend(
                (None if key == _UNKNOWN_KEYWORD_EXPANSION else key, value)
                for key, value in keywords.items()
            )
            return _AbstractValue(
                tags=frozenset(tags),
                strings=None,
                mapping=tuple(entries),
            )
        if function.operation is not None:
            if not arguments:
                return _AbstractValue(strings=None)
            operation = function.operation
            bound = self._bound_mapping_method_value(arguments[0], operation.method)
            if not operation.invoke:
                return bound
            operation_arguments = (
                arguments[1:] if operation.unbound else operation.arguments
            )
            return self._bound_mapping_call_result(bound, operation_arguments)
        if _bound_mapping_method_names(function):
            return self._bound_mapping_call_result(function, arguments)
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and (
                _is_registry_expression(node.func.value)
                or _PROV_APP_STATE in self._value(node.func.value).tags
            )
            and node.args
        ):
            keys = self._value(node.args[0]).strings
            if keys and any(key.casefold() in _RUNTIME_SERVICE_KEYS for key in keys):
                return _AbstractValue(
                    tags=frozenset({_PROV_UNKNOWN_TARGET}),
                    strings=None,
                )
        if _PROV_NAMESPACE_GETTER in function.tags and arguments:
            names = arguments[0].strings
            if names:
                reflected = _merge_abstract_values(
                    [self._reflected_name_value(name) for name in names]
                )
                if reflected.tags:
                    return reflected
                if bool(function.tags & {_PROV_APP_STATE, _PROV_REGISTRY}) and any(
                    name.casefold() in _RUNTIME_SERVICE_KEYS for name in names
                ):
                    return _AbstractValue(
                        tags=frozenset({_PROV_UNKNOWN_TARGET}),
                        strings=None,
                    )
                return reflected
            return _AbstractValue(
                tags=frozenset({_PROV_UNKNOWN_TARGET}),
                strings=None,
            )
        if _PROV_REGISTRY_GETTER in function.tags and arguments:
            keys = arguments[0].strings
            if keys and any(key.casefold() in _RUNTIME_SERVICE_KEYS for key in keys):
                return _AbstractValue(
                    tags=frozenset({_PROV_UNKNOWN_TARGET}),
                    strings=None,
                )
        if (
            _PROV_RUNTIME_SERVICE_ACCESSOR in function.tags
            and arguments
            and self._value_is_runtime_container(arguments[0])
        ):
            return _AbstractValue(
                tags=frozenset({_PROV_UNKNOWN_TARGET}),
                strings=None,
            )
        if _PROV_TARGET_CLASS in function.tags:
            return _AbstractValue(tags=frozenset({_PROV_TARGET_INSTANCE}), strings=None)
        if _PROV_TARGET_CLASS_ACCESSOR in function.tags:
            return _AbstractValue(tags=frozenset({_PROV_TARGET_CLASS}), strings=None)
        if _PROV_DYNAMIC_IMPORT in function.tags:
            return self._dynamic_import_result(function, arguments, keywords)
        primitives = _reflection_names(function)
        if primitives:
            return self._reflection_result(
                node,
                primitives,
                arguments,
                keywords,
            )
        if _PROV_NAMESPACE in function.tags:
            receiver_tags = arguments[0].tags if arguments else frozenset()
            return _AbstractValue(
                tags=receiver_tags | frozenset({_PROV_NAMESPACE}),
                strings=None,
            )
        return _AbstractValue(strings=None)

    def _bound_mapping_call_result(
        self,
        function: _AbstractValue,
        arguments: tuple[_AbstractValue, ...],
    ) -> _AbstractValue:
        bound_methods = _bound_mapping_method_names(function)
        owner = self._bound_mapping_method_owner(function)
        if "copy" in bound_methods:
            return owner
        if bound_methods & {"__getitem__", "get", "pop", "setdefault"}:
            key = arguments[0] if arguments else _AbstractValue(strings=None)
            result = self._mapping_lookup_result(owner, key)
            if not result.tags and result.mapping is None and len(arguments) > 1:
                return arguments[1]
            return result
        return _AbstractValue(strings=None)

    def _bound_mapping_method_owner(
        self,
        function: _AbstractValue,
    ) -> _AbstractValue:
        return _AbstractValue(
            tags=frozenset(
                tag
                for tag in function.tags
                if not tag.startswith(_BOUND_MAPPING_METHOD_TAG_PREFIX)
                and tag not in {_PROV_NAMESPACE_GETTER, _PROV_REGISTRY_GETTER}
            ),
            strings=function.strings,
            items=function.items,
            mapping=function.mapping,
        )

    def _mapping_lookup_result(
        self,
        owner: _AbstractValue,
        key: _AbstractValue,
    ) -> _AbstractValue:
        if owner.mapping is not None:
            values = [
                value
                for mapped_key, value in owner.mapping
                if key.strings is None
                or mapped_key is None
                or mapped_key in key.strings
            ]
            concrete_values = [
                value
                for mapped_key, value in owner.mapping
                if mapped_key is not None
                and key.strings is not None
                and mapped_key in key.strings
            ]
            if concrete_values:
                return _merge_abstract_values(concrete_values)
            if values and key.strings is None:
                return _merge_abstract_values(values)
        if self._value_is_runtime_container(owner):
            if key.strings is None or any(
                name.casefold() in _RUNTIME_SERVICE_KEYS for name in key.strings
            ):
                return _AbstractValue(
                    tags=frozenset({_PROV_UNKNOWN_TARGET}),
                    strings=None,
                )
        if _PROV_NAMESPACE in owner.tags:
            if key.strings:
                reflected = _merge_abstract_values(
                    [self._reflected_name_value(name) for name in key.strings]
                )
                if reflected.tags:
                    return reflected
                return _AbstractValue()
            return _AbstractValue(
                tags=frozenset({_PROV_UNKNOWN_TARGET}),
                strings=None,
            )
        return _AbstractValue()

    def _bind_target(self, target: ast.AST, value: _AbstractValue) -> None:
        if isinstance(target, ast.Name):
            self._bind(target.id, value)
            return
        if isinstance(target, ast.Starred):
            self._bind_target(target.value, value)
            return
        if isinstance(target, (ast.Tuple, ast.List)):
            if value.items is not None and len(value.items) == len(target.elts):
                values = value.items
            else:
                values = tuple(value for _ in target.elts)
            for child, child_value in zip(target.elts, values, strict=True):
                self._bind_target(child, child_value)

    def _with_mapping_updates(
        self,
        owner: _AbstractValue,
        updates: tuple[tuple[str | None, _AbstractValue], ...],
    ) -> _AbstractValue:
        if owner.mapping is None:
            return owner
        overwritten = {key for key, _value in updates if key is not None}
        retained = tuple(
            (key, value)
            for key, value in owner.mapping
            if key is None or key not in overwritten
        )
        mapping = (*retained, *updates)
        return _AbstractValue(
            tags=owner.tags,
            strings=owner.strings,
            items=owner.items,
            mapping=mapping if len(mapping) <= 256 else None,
            forwarder=owner.forwarder,
            partial=owner.partial,
            operation=owner.operation,
            bound_owner_name=owner.bound_owner_name,
        )

    def _with_sequence_items(
        self,
        owner: _AbstractValue,
        items: tuple[_AbstractValue, ...],
    ) -> _AbstractValue:
        return _AbstractValue(
            tags=owner.tags,
            strings=owner.strings,
            items=items if len(items) <= 256 else None,
            mapping=owner.mapping,
            forwarder=owner.forwarder,
            partial=owner.partial,
            operation=owner.operation,
            bound_owner_name=owner.bound_owner_name,
        )

    def _rebind_expression(
        self,
        expression: ast.AST,
        value: _AbstractValue,
    ) -> None:
        if isinstance(expression, ast.Name):
            self._rebind(expression.id, value)

    def _mutate_assignment_target(
        self,
        target: ast.AST,
        value: _AbstractValue,
    ) -> None:
        if not isinstance(target, ast.Subscript):
            return
        owner = self._value(target.value)
        if owner.mapping is None:
            return
        keys = self._value(target.slice).strings
        updates = tuple((key, value) for key in keys) if keys else ((None, value),)
        self._rebind_expression(
            target.value,
            self._with_mapping_updates(owner, updates),
        )

    def _value_may_be_target(self, value: _AbstractValue) -> bool:
        return bool(value.tags & _TARGET_PROVENANCE)

    def _assignment_is_registration(
        self,
        target: ast.AST,
        value: _AbstractValue,
    ) -> bool:
        return self._target_is_runtime_service_slot(target) or (
            isinstance(target, (ast.Attribute, ast.Subscript))
            and self._value_may_be_target(value)
        )

    def _target_is_runtime_service_slot(self, target: ast.AST) -> bool:
        if isinstance(target, ast.Attribute):
            owner = self._value(target.value)
            return target.attr.casefold() in _RUNTIME_SERVICE_KEYS and (
                _PROV_APP_STATE in owner.tags or _is_registry_expression(target.value)
            )
        if not isinstance(target, ast.Subscript):
            return False
        keys = self._value(target.slice).strings
        if not keys or not any(key.casefold() in _RUNTIME_SERVICE_KEYS for key in keys):
            return False
        owner = self._value(target.value)
        return _PROV_APP_STATE in owner.tags or _is_registry_expression(target.value)

    def _report_primitive_alias(self, node: ast.AST, value: _AbstractValue) -> None:
        if _PROV_DYNAMIC_IMPORT in value.tags:
            self._report(
                node,
                "dynamic_import_primitive",
                "runtime aliases a dynamic import primitive",
            )
        if _PROV_DYNAMIC_CODE in value.tags:
            self._report(
                node,
                "dynamic_code_primitive",
                "runtime aliases a dynamic code primitive",
            )
        # Reflection and namespace primitives are only unsafe once their call target,
        # receiver, lookup key, or registration effect is known. Keeping aliases in
        # the abstract value lets those effect sites be classified without rejecting
        # ordinary bounded uses such as ``getattr(client, "close")``.

    def _import_value(self, module: str) -> _AbstractValue:
        if _module_is_target(module) or module == "src.ingestion":
            return _AbstractValue(tags=frozenset({_PROV_TARGET_MODULE}), strings=None)
        if module == "importlib":
            return _AbstractValue(
                tags=frozenset({_PROV_IMPORTLIB_MODULE}), strings=None
            )
        if module == "operator":
            return _AbstractValue(tags=frozenset({_PROV_OPERATOR_MODULE}), strings=None)
        if module == "builtins":
            return _AbstractValue(tags=frozenset({_PROV_BUILTINS_MODULE}), strings=None)
        if module == "functools":
            return _AbstractValue(
                tags=frozenset({_PROV_FUNCTOOLS_MODULE}),
                strings=None,
            )
        if module == "copy":
            return _AbstractValue(tags=frozenset({_PROV_COPY_MODULE}), strings=None)
        if module == "collections":
            return _AbstractValue(
                tags=frozenset({_PROV_COLLECTIONS_MODULE}),
                strings=None,
            )
        if module == "types":
            return _AbstractValue(tags=frozenset({_PROV_TYPES_MODULE}), strings=None)
        return _AbstractValue(strings=None)

    def _from_import_value(self, module: str, name: str) -> _AbstractValue:
        if _identifier_is_target(name):
            return _AbstractValue(tags=frozenset({_PROV_TARGET_CLASS}), strings=None)
        if _module_is_target(module):
            return _AbstractValue(tags=frozenset({_PROV_UNKNOWN_TARGET}), strings=None)
        if module in {"src", "src.ingestion"} and name in _TARGET_CONTAINER_IMPORTS:
            return _AbstractValue(tags=frozenset({_PROV_TARGET_MODULE}), strings=None)
        if module == "operator" and name in {"getitem", "setitem"}:
            return _AbstractValue(
                strings=None,
                operation=_BoundOperation(
                    method=f"__{name}__",
                    invoke=True,
                    unbound=True,
                ),
            )
        if module in {"builtins", "importlib"} and name in _DYNAMIC_IMPORT_NAMES:
            kind = _PROV_DUNDER_IMPORT if name == "__import__" else _PROV_IMPORT_MODULE
            return _AbstractValue(
                tags=frozenset({_PROV_DYNAMIC_IMPORT, kind}),
                strings=None,
            )
        if module in {"operator", "builtins"} and name in _ATTRIBUTE_REFLECTION_NAMES:
            return _AbstractValue(
                tags=frozenset({_reflection_tag(name)}),
                strings=None,
            )
        if module == "builtins" and name in _DYNAMIC_CODE_NAMES:
            return _AbstractValue(tags=frozenset({_PROV_DYNAMIC_CODE}), strings=None)
        if module == "builtins" and name in _NAMESPACE_REFLECTION_NAMES:
            return _AbstractValue(tags=frozenset({_PROV_NAMESPACE}), strings=None)
        if module == "functools" and name == "partial":
            return _AbstractValue(
                tags=frozenset({_PROV_PARTIAL_FACTORY}),
                strings=None,
            )
        if module == "copy" and name in {"copy", "deepcopy"}:
            return _AbstractValue(
                tags=frozenset({_value_transform_tag(_TRANSFORM_IDENTITY)}),
                strings=None,
            )
        if module == "types" and name == "MappingProxyType":
            return _AbstractValue(
                tags=frozenset({_value_transform_tag(_TRANSFORM_IDENTITY)}),
                strings=None,
            )
        if module == "collections" and name == "ChainMap":
            return _AbstractValue(
                tags=frozenset({_value_transform_tag(_TRANSFORM_MAPPING_MERGE)}),
                strings=None,
            )
        return _AbstractValue(strings=None)

    def visit_Import(self, node: ast.Import) -> None:
        for imported in node.names:
            value = self._import_value(imported.name)
            binding = imported.asname or imported.name.split(".", 1)[0]
            self._bind(binding, value)
            if _module_is_target(imported.name) or imported.name == "src.ingestion":
                self._report(
                    node,
                    "forbidden_import",
                    f"runtime imports dormant ingestion module {imported.name}",
                )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        names = {imported.name for imported in node.names}
        target_container = module in {"src", "src.ingestion"} and bool(
            names & _TARGET_CONTAINER_IMPORTS
        )
        if (
            _module_is_target(module)
            or target_container
            or (module == "src.ingestion" and "*" in names)
        ):
            self._report(
                node,
                "forbidden_import",
                f"runtime imports dormant ingestion authority from {module or '.'}",
            )
        for imported in node.names:
            if imported.name == "*":
                continue
            self._bind(
                imported.asname or imported.name,
                self._from_import_value(module, imported.name),
            )

    def visit_Name(self, node: ast.Name) -> None:
        if _identifier_is_target(node.id):
            self._report(
                node,
                "target_symbol_reference",
                f"runtime references dormant symbol {node.id}",
            )

    def visit_Attribute(self, node: ast.Attribute) -> None:
        value = self._attribute_value(node)
        if _identifier_is_target(node.attr):
            self._report(
                node,
                "target_symbol_reference",
                f"runtime references dormant attribute {node.attr}",
            )
        if _PROV_TARGET_METHOD in value.tags:
            self._report(
                node,
                "target_method_reference",
                f"runtime references dormant method {node.attr}",
            )
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        value = self._value(node.value)
        self._report_primitive_alias(node.value, value)
        for target in node.targets:
            if self._assignment_is_registration(target, value):
                self._report(
                    target,
                    "runtime_registration",
                    "runtime registers target provenance through an object or mapping",
                )
            self._bind_target(target, value)
            self._mutate_assignment_target(target, value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            value = self._value(node.value)
            self._report_primitive_alias(node.value, value)
            if self._assignment_is_registration(node.target, value):
                self._report(
                    node.target,
                    "runtime_registration",
                    "runtime registers target provenance through an object or mapping",
                )
            self._bind_target(node.target, value)
            self._mutate_assignment_target(node.target, value)
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        current = self._value(node.target)
        value = self._value(node.value)
        if isinstance(node.op, ast.BitOr):
            if self._value_is_runtime_container(current) and (
                self._mapping_has_target_slot(value) or self._value_may_be_target(value)
            ):
                self._report(
                    node,
                    "runtime_registration",
                    "runtime registers target provenance through mapping union",
                )
            self._bind_target(
                node.target,
                _merge_abstract_values([current, value]),
            )
        self.visit(node.target)
        self.visit(node.value)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        value = self._value(node.value)
        self._report_primitive_alias(node.value, value)
        if self._assignment_is_registration(node.target, value):
            self._report(
                node.target,
                "runtime_registration",
                "runtime registers target provenance through an object or mapping",
            )
        self._bind_target(node.target, value)
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        key = self._value(node.slice)
        if key.strings:
            forbidden = [
                name for name in key.strings if _reflection_name_is_forbidden(name)
            ]
            if forbidden:
                self._report(
                    node,
                    "target_reflection",
                    f"runtime indexes forbidden symbol {sorted(forbidden)[0]}",
                )
            service_keys = {
                name.casefold() for name in key.strings if isinstance(name, str)
            } & _RUNTIME_SERVICE_KEYS
            if (
                service_keys
                and _is_registry_expression(node.value)
                and isinstance(node.ctx, ast.Load)
            ):
                self._report(
                    node,
                    "runtime_registry_access",
                    "runtime reads a target service slot from a dependency container",
                )
        elif _PROV_NAMESPACE in self._value(node.value).tags:
            self._report(
                node,
                "unresolved_reflection",
                "runtime dynamically indexes an attribute namespace",
            )
        self.generic_visit(node)

    def _scan_dynamic_import_call(
        self,
        node: ast.Call,
        function: _AbstractValue,
        arguments: tuple[_AbstractValue, ...],
        keywords: dict[str, _AbstractValue],
    ) -> None:
        modules = self._resolved_dynamic_modules(function, arguments, keywords)
        if not modules:
            self._report(
                node,
                "dynamic_import_primitive",
                "runtime invokes an unbounded dynamic import primitive",
            )
            self._report(
                node,
                "unresolved_dynamic_import",
                "runtime dynamic import target cannot be proven safe",
            )
            return
        self._constant_dynamic_imports.update(modules)
        targets = [module for module in modules if _module_exposes_target(module)]
        if targets:
            self._report(
                node,
                "dynamic_import_primitive",
                "runtime invokes a dynamic import primitive for dormant authority",
            )
            self._report(
                node,
                "dynamic_target_import",
                f"runtime dynamically imports dormant module {sorted(targets)[0]}",
            )

    def _scan_reflection_call(
        self,
        node: ast.Call,
        primitives: frozenset[str],
        arguments: tuple[_AbstractValue, ...],
    ) -> None:
        if self._reflection_reads_runtime_service_slot(
            node,
            primitives,
            arguments,
        ):
            self._report(
                node,
                "runtime_registry_access",
                "runtime reflects a target service slot from a dependency container",
            )
        name_value = self._reflection_name_value(node, primitives, arguments)
        names = name_value.strings
        if names:
            forbidden = [name for name in names if _reflection_name_is_forbidden(name)]
            if forbidden:
                self._report(
                    node,
                    "target_reflection",
                    f"runtime reflects dormant symbol {sorted(forbidden)[0]}",
                )
        else:
            receiver = self._reflection_receiver_value(node, primitives, arguments)
            reflection_requires_proof = bool(
                primitives & {"attrgetter", "itemgetter", "methodcaller"}
            ) or (receiver is not None and bool(receiver.tags & _TARGET_PROVENANCE))
            if reflection_requires_proof:
                self._report(
                    node,
                    "unresolved_reflection",
                    "runtime reflection target cannot be proven safe",
                )

    def _value_is_runtime_container(self, value: _AbstractValue) -> bool:
        return bool(value.tags & {_PROV_APP_STATE, _PROV_REGISTRY})

    def _mapping_has_target_slot(self, value: _AbstractValue) -> bool:
        return value.mapping is not None and any(
            key is not None and key.casefold() in _RUNTIME_SERVICE_KEYS
            for key, _mapped in value.mapping
        )

    def _bound_lookup_reads_runtime_service(
        self,
        function: _AbstractValue,
        arguments: tuple[_AbstractValue, ...],
    ) -> bool:
        if not (
            _bound_mapping_method_names(function)
            & {"__getitem__", "get", "pop", "setdefault"}
        ):
            return False
        owner = self._bound_mapping_method_owner(function)
        if not self._value_is_runtime_container(owner):
            return False
        keys = arguments[0].strings if arguments else None
        return keys is None or any(
            key.casefold() in _RUNTIME_SERVICE_KEYS for key in keys
        )

    def _operation_effect_call(
        self,
        function: _AbstractValue,
        arguments: tuple[_AbstractValue, ...],
        keywords: dict[str, _AbstractValue],
    ) -> tuple[
        _AbstractValue,
        tuple[_AbstractValue, ...],
        dict[str, _AbstractValue],
    ]:
        operation = function.operation
        if operation is None or not operation.invoke or not arguments:
            return function, arguments, keywords
        operation_arguments = (
            arguments[1:] if operation.unbound else operation.arguments
        )
        operation_keywords = keywords if operation.unbound else dict(operation.keywords)
        return (
            self._bound_mapping_method_value(arguments[0], operation.method),
            operation_arguments,
            operation_keywords,
        )

    def _apply_mutating_call(
        self,
        node: ast.Call,
        call_name: str | None,
        original_function: _AbstractValue,
        effect_function: _AbstractValue,
        arguments: tuple[_AbstractValue, ...],
        keywords: dict[str, _AbstractValue],
    ) -> None:
        bound_methods = _bound_mapping_method_names(effect_function)
        if not bound_methods:
            return
        method = call_name if call_name in bound_methods else next(iter(bound_methods))
        owner = self._bound_mapping_method_owner(effect_function)
        owner_name = effect_function.bound_owner_name
        if (
            owner_name is None
            and original_function.operation is not None
            and node.args
            and isinstance(node.args[0], ast.Name)
        ):
            owner_name = node.args[0].id
        if owner_name is None:
            return
        if method == "update" and owner.mapping is not None:
            updates: list[tuple[str | None, _AbstractValue]] = []
            for argument in arguments:
                if argument.mapping is not None:
                    updates.extend(argument.mapping)
                else:
                    updates.append((None, argument))
            updates.extend(
                (None if key == _UNKNOWN_KEYWORD_EXPANSION else key, value)
                for key, value in keywords.items()
            )
            self._rebind(
                owner_name,
                self._with_mapping_updates(owner, tuple(updates)),
            )
            return
        if method in {"__setattr__", "__setitem__"} and owner.mapping is not None:
            if len(arguments) < 2:
                return
            keys = arguments[0].strings
            updates = (
                tuple((key, arguments[1]) for key in keys)
                if keys
                else ((None, arguments[1]),)
            )
            self._rebind(
                owner_name,
                self._with_mapping_updates(owner, updates),
            )
            return
        if method == "setdefault" and owner.mapping is not None and arguments:
            keys = arguments[0].strings
            default = arguments[1] if len(arguments) > 1 else _AbstractValue()
            existing = {key for key, _value in owner.mapping if key is not None}
            updates = tuple(
                (key, default) for key in (keys or ()) if key not in existing
            )
            if updates:
                self._rebind(
                    owner_name,
                    self._with_mapping_updates(owner, updates),
                )
            return
        if owner.items is None or not arguments:
            return
        if method == "append":
            updated_items = (*owner.items, arguments[0])
        elif method == "extend" and arguments[0].items is not None:
            updated_items = (*owner.items, *arguments[0].items)
        elif method == "insert" and len(arguments) >= 2:
            indexes = arguments[0].strings
            if indexes is not None:
                return
            updated_items = (*owner.items, arguments[1])
        else:
            return
        self._rebind(
            owner_name,
            self._with_sequence_items(owner, updated_items),
        )

    def _call_registers_target(
        self,
        node: ast.Call,
        call_name: str | None,
        function: _AbstractValue,
        arguments: tuple[_AbstractValue, ...],
        keywords: dict[str, _AbstractValue],
    ) -> bool:
        primitives = _reflection_names(function)
        if "setattr" in primitives and len(arguments) >= 3:
            keys = arguments[1].strings
            if (
                self._value_is_runtime_container(arguments[0])
                and keys
                and any(key.casefold() in _RUNTIME_SERVICE_KEYS for key in keys)
            ):
                return True
            return self._value_may_be_target(arguments[2])
        bound_methods = _bound_mapping_method_names(function)
        registration_methods = bound_methods & _REGISTRATION_METHODS
        if not registration_methods and call_name in _REGISTRATION_METHODS:
            registration_methods = frozenset({call_name})
        for registration_method in registration_methods:
            owner = (
                self._bound_mapping_method_owner(function)
                if registration_method in bound_methods
                else (
                    self._value(node.func.value)
                    if isinstance(node.func, ast.Attribute)
                    else _AbstractValue(strings=None)
                )
            )
            owner_is_container = self._value_is_runtime_container(owner) or (
                isinstance(node.func, ast.Attribute)
                and _is_registry_expression(node.func.value)
            )
            if owner_is_container:
                if _UNKNOWN_KEYWORD_EXPANSION in keywords:
                    return True
                if registration_method == "update" and (
                    any(self._mapping_has_target_slot(value) for value in arguments)
                    or any(key.casefold() in _RUNTIME_SERVICE_KEYS for key in keywords)
                ):
                    return True
                if arguments:
                    keys = arguments[0].strings
                    if keys and any(
                        key.casefold() in _RUNTIME_SERVICE_KEYS for key in keys
                    ):
                        return True
            values = [*arguments, *keywords.values()]
            if any(self._value_may_be_target(value) for value in values):
                return True
        return False

    def _visit_function_call(
        self,
        forwarder: _FunctionForwarder,
        node: ast.Call,
    ) -> None:
        frame = (id(forwarder.node), id(node))
        if frame in self._active_function_calls:
            return
        module_execution = self._module_execution_depth > 0 or len(self._scopes) == 1
        self._active_function_calls.add(frame)
        self._scopes.append(self._function_bindings(forwarder, node))
        if module_execution:
            self._module_execution_depth += 1
        try:
            for statement in forwarder.node.body:
                self.visit(statement)
        finally:
            if module_execution:
                self._module_execution_depth -= 1
            self._scopes.pop()
            self._active_function_calls.remove(frame)

    def visit_Call(self, node: ast.Call) -> None:
        raw_function = self._value(node.func)
        if isinstance(raw_function.forwarder, _LambdaForwarder):
            self._scopes.append(self._lambda_bindings(raw_function.forwarder, node))
            try:
                self.visit(raw_function.forwarder.node.body)
            finally:
                self._scopes.pop()
        elif isinstance(raw_function.forwarder, _FunctionForwarder):
            self._visit_function_call(raw_function.forwarder, node)
        function, arguments, keywords = self._effective_call(raw_function, node)
        call_path = _expression_path(node.func)
        call_name = call_path.rsplit(".", 1)[-1] if call_path else None
        effect_function, effect_arguments, effect_keywords = (
            self._operation_effect_call(function, arguments, keywords)
        )

        if _PROV_TARGET_CLASS in function.tags:
            self._report(
                node,
                "target_constructor",
                "runtime constructs a dormant target service",
            )
        if (
            _PROV_TARGET_METHOD in function.tags
            or _PROV_TARGET_METHOD_CALLER in function.tags
        ):
            self._report(
                node,
                "target_method_call",
                "runtime invokes a dormant target method",
            )
        if _PROV_DYNAMIC_IMPORT in function.tags:
            self._scan_dynamic_import_call(node, function, arguments, keywords)
        if _PROV_DYNAMIC_CODE in function.tags:
            self._report(
                node,
                "dynamic_code_primitive",
                "runtime invokes a dynamic code primitive",
            )
        if _PROV_NAMESPACE in function.tags:
            receiver = self._value(node.args[0]) if node.args else None
            if receiver is None or bool(receiver.tags & _TARGET_PROVENANCE):
                self._report(
                    node,
                    "namespace_reflection",
                    "runtime opens a namespace that can contain dormant authority",
                )
        primitives = _reflection_names(function)
        if primitives:
            self._scan_reflection_call(node, primitives, arguments)
        if _PROV_NAMESPACE_GETTER in function.tags and arguments:
            names = arguments[0].strings
            if names and any(_reflection_name_is_target(name) for name in names):
                self._report(
                    node,
                    "target_reflection",
                    "runtime reads a dormant symbol through an attribute namespace",
                )
        if (
            _PROV_RUNTIME_SERVICE_ACCESSOR in function.tags
            and arguments
            and self._value_is_runtime_container(arguments[0])
        ):
            self._report(
                node,
                "runtime_registry_access",
                "runtime reflects a target service slot from a dependency container",
            )
        if self._bound_lookup_reads_runtime_service(
            effect_function,
            effect_arguments,
        ):
            self._report(
                node,
                "runtime_registry_access",
                "runtime reads a target service slot through a bound mapping method",
            )
        if self._call_registers_target(
            node,
            call_name,
            effect_function,
            effect_arguments,
            effect_keywords,
        ):
            self._report(
                node,
                "runtime_registration",
                "runtime registers target provenance through a container call",
            )
        self._apply_mutating_call(
            node,
            call_name,
            function,
            effect_function,
            effect_arguments,
            effect_keywords,
        )
        self.generic_visit(node)

    def _semantic_runtime_container_signal(self, node: ast.AST) -> bool:
        return any(
            _is_registry_expression(child) or _is_app_state_expression(child)
            for child in ast.walk(node)
        )

    def _semantic_target_authority_signal(self, node: ast.AST) -> bool:
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and _identifier_is_target(child.id):
                return True
            if isinstance(child, ast.Attribute) and _identifier_is_target(child.attr):
                return True
            if isinstance(child, ast.Constant) and isinstance(child.value, str):
                if _identifier_is_target(child.value) or _module_is_target(child.value):
                    return True
        return False

    def _semantic_service_key(self, node: ast.AST) -> bool:
        value = _constant_text(node)
        return value is not None and value.casefold() in _RUNTIME_SERVICE_KEYS

    def _semantic_backstop(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        target_calls: list[ast.Call] = []
        runtime_retrieval = False
        target_authority = False
        scanner = self

        class _SemanticCollector(ast.NodeVisitor):
            def visit_AsyncFunctionDef(self, child: ast.AsyncFunctionDef) -> None:
                return None

            def visit_ClassDef(self, child: ast.ClassDef) -> None:
                return None

            def visit_FunctionDef(self, child: ast.FunctionDef) -> None:
                return None

            def visit_Call(self, child: ast.Call) -> None:
                nonlocal runtime_retrieval, target_authority
                if (
                    isinstance(child.func, ast.Attribute)
                    and child.func.attr in _TARGET_METHODS
                ):
                    target_calls.append(child)
                if (
                    isinstance(child.func, ast.Attribute)
                    and child.func.attr in {"__getitem__", "get", "pop", "setdefault"}
                    and child.args
                    and scanner._semantic_service_key(child.args[0])
                    and scanner._semantic_runtime_container_signal(child.func.value)
                ):
                    runtime_retrieval = True
                if scanner._semantic_target_authority_signal(child):
                    target_authority = True
                self.generic_visit(child)

            def visit_Subscript(self, child: ast.Subscript) -> None:
                nonlocal runtime_retrieval, target_authority
                if scanner._semantic_service_key(
                    child.slice
                ) and scanner._semantic_runtime_container_signal(child.value):
                    runtime_retrieval = True
                if scanner._semantic_target_authority_signal(child):
                    target_authority = True
                self.generic_visit(child)

        collector = _SemanticCollector()
        for statement in node.body:
            collector.visit(statement)
        if not (runtime_retrieval or target_authority):
            return
        for call in target_calls:
            self._report(
                call,
                "target_method_call",
                "runtime invokes a target method after a service authority lookup",
            )

    def _visit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        self._bind(
            node.name,
            _AbstractValue(
                strings=None,
                forwarder=_FunctionForwarder(node=node),
            ),
        )
        for decorator in node.decorator_list:
            self.visit(decorator)
        positional_arguments = [*node.args.posonlyargs, *node.args.args]
        defaults_by_name = (
            {
                argument.arg: self._value(default)
                for argument, default in zip(
                    positional_arguments[-len(node.args.defaults) :],
                    node.args.defaults,
                    strict=True,
                )
            }
            if node.args.defaults
            else {}
        )
        defaults_by_name.update(
            {
                argument.arg: self._value(default)
                for argument, default in zip(
                    node.args.kwonlyargs,
                    node.args.kw_defaults,
                    strict=True,
                )
                if default is not None
            }
        )
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default is not None:
                self.visit(default)
        if node.returns is not None:
            self.visit(node.returns)
        self._scopes.append({})
        arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        if node.args.vararg is not None:
            arguments.append(node.args.vararg)
        if node.args.kwarg is not None:
            arguments.append(node.args.kwarg)
        for argument in arguments:
            self._bind(
                argument.arg,
                defaults_by_name.get(
                    argument.arg,
                    self._unknown_parameter_value(argument.arg),
                ),
            )
            if argument.annotation is not None:
                self.visit(argument.annotation)
        for statement in node.body:
            self.visit(statement)
        self._semantic_backstop(node)
        self._scopes.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._bind(node.name, _AbstractValue(strings=None))
        for decorator in node.decorator_list:
            self.visit(decorator)
        for base in node.bases:
            self.visit(base)
        for keyword in node.keywords:
            self.visit(keyword.value)
        self._scopes.append({})
        for statement in node.body:
            self.visit(statement)
        self._scopes.pop()

    def _visit_for(self, node: ast.For | ast.AsyncFor) -> None:
        iterable = self._value(node.iter)
        self.visit(node.iter)
        loop_value = (
            _merge_abstract_values(list(iterable.items))
            if iterable.items is not None
            else iterable
        )
        self._bind_target(node.target, loop_value)
        for statement in node.body:
            self.visit(statement)
        for statement in node.orelse:
            self.visit(statement)

    def visit_For(self, node: ast.For) -> None:
        self._visit_for(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self._visit_for(node)

    def _visit_comprehension(
        self,
        node: ast.GeneratorExp | ast.ListComp | ast.SetComp | ast.DictComp,
    ) -> None:
        self._scopes.append({})
        try:
            for generator in node.generators:
                iterable = self._value(generator.iter)
                self.visit(generator.iter)
                loop_value = (
                    _merge_abstract_values(list(iterable.items))
                    if iterable.items is not None
                    else iterable
                )
                self._bind_target(generator.target, loop_value)
                for condition in generator.ifs:
                    self.visit(condition)
            if isinstance(node, ast.DictComp):
                self.visit(node.key)
                self.visit(node.value)
            else:
                self.visit(node.elt)
        finally:
            self._scopes.pop()

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comprehension(node)

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comprehension(node)

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comprehension(node)

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self._visit_comprehension(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self._scopes.append({})
        arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        if node.args.vararg is not None:
            arguments.append(node.args.vararg)
        if node.args.kwarg is not None:
            arguments.append(node.args.kwarg)
        for argument in arguments:
            self._bind(argument.arg, self._unknown_parameter_value(argument.arg))
        self.visit(node.body)
        self._scopes.pop()


def _scan_runtime_source(source: str, *, filename: str) -> list[_Violation]:
    try:
        tree = ast.parse(source, filename=filename)
    except (SyntaxError, ValueError) as error:
        return [
            _Violation(
                filename=filename,
                lineno=int(getattr(error, "lineno", 1) or 1),
                col_offset=int(getattr(error, "offset", 0) or 0),
                code="unparseable_runtime_source",
                detail="runtime source cannot be proven dormant",
            )
        ]
    scanner = _ActivationBoundaryScanner(filename)
    scanner.predeclare_module_definitions(tree)
    scanner.visit(tree)
    return scanner.violations


def _is_runtime_entrypoint_path(relative_path: Path) -> bool:
    parts = relative_path.parts
    if not parts or parts[0].casefold() != "src" or relative_path.suffix != ".py":
        return False
    if relative_path.name.casefold() in _RUNTIME_EXACT_FILENAMES:
        return True
    return any(
        marker in part.casefold()
        for part in parts[1:]
        for marker in _RUNTIME_PATH_MARKERS
    )


def _module_name_for_path(path: Path, *, project_root: Path) -> tuple[str, ...]:
    relative_path = path.relative_to(project_root)
    if relative_path.name == "__init__.py":
        return relative_path.parts[:-1]
    return (*relative_path.parts[:-1], relative_path.stem)


def _local_module_paths(module: str, *, project_root: Path) -> list[Path]:
    parts = tuple(part for part in module.split(".") if part)
    if not parts or parts[0] != "src":
        return []
    paths: list[Path] = []
    for depth in range(1, len(parts)):
        package_init = project_root.joinpath(*parts[:depth], "__init__.py")
        if package_init.is_file():
            paths.append(package_init)
    module_base = project_root.joinpath(*parts)
    module_file = module_base.with_suffix(".py")
    package_init = module_base / "__init__.py"
    if module_file.is_file():
        paths.append(module_file)
    if package_init.is_file():
        paths.append(package_init)
    return list(dict.fromkeys(paths))


def _absolute_import_from_module(
    node: ast.ImportFrom,
    *,
    importer: Path,
    project_root: Path,
) -> str | None:
    if node.level == 0:
        return node.module
    module_parts = _module_name_for_path(importer, project_root=project_root)
    package_parts = (
        module_parts if importer.name == "__init__.py" else module_parts[:-1]
    )
    ascend = node.level - 1
    if ascend > len(package_parts):
        return None
    base_parts = package_parts[: len(package_parts) - ascend]
    if node.module:
        base_parts = (*base_parts, *node.module.split("."))
    return ".".join(base_parts)


def _static_local_import_paths(path: Path, *, project_root: Path) -> list[Path]:
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=path.as_posix())
    except (OSError, UnicodeError, SyntaxError, ValueError):
        return []

    imports: list[Path] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for imported in node.names:
                imports.extend(
                    _local_module_paths(imported.name, project_root=project_root)
                )
        elif isinstance(node, ast.ImportFrom):
            module = _absolute_import_from_module(
                node,
                importer=path,
                project_root=project_root,
            )
            if not module:
                continue
            imports.extend(_local_module_paths(module, project_root=project_root))
            for imported in node.names:
                if imported.name == "*":
                    continue
                imports.extend(
                    _local_module_paths(
                        f"{module}.{imported.name}",
                        project_root=project_root,
                    )
                )
    scanner = _ActivationBoundaryScanner(path.as_posix())
    scanner.predeclare_module_definitions(tree)
    scanner.visit(tree)
    for module in sorted(scanner.constant_dynamic_imports):
        imports.extend(_local_module_paths(module, project_root=project_root))
    return list(dict.fromkeys(imports))


def _normalized_module_ast_sha256(tree: ast.Module) -> str:
    payload = ast.dump(tree, include_attributes=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _export_only_exemption_violations(
    tree: ast.Module,
    *,
    filename: str,
) -> list[_Violation]:
    violations: list[_Violation] = []
    imported_bindings: set[str] = set()
    exported_names: tuple[str, ...] | None = None
    export_node: ast.AST = tree

    def reject(node: ast.AST, detail: str) -> None:
        violations.append(
            _Violation(
                filename=filename,
                lineno=int(getattr(node, "lineno", 1)),
                col_offset=int(getattr(node, "col_offset", 0)),
                code="runtime_exemption_effect",
                detail=detail,
            )
        )

    for index, statement in enumerate(tree.body):
        if (
            index == 0
            and isinstance(statement, ast.Expr)
            and isinstance(statement.value, ast.Constant)
            and isinstance(statement.value.value, str)
        ):
            continue
        if isinstance(statement, ast.ImportFrom):
            module = statement.module or ""
            if (
                statement.level != 0
                or not module.startswith(_EXPORT_ONLY_IMPORT_PREFIX)
                or any(alias.name == "*" for alias in statement.names)
            ):
                reject(
                    statement,
                    "runtime export exemption contains a non-domain or wildcard import",
                )
                continue
            imported_bindings.update(
                alias.asname or alias.name for alias in statement.names
            )
            continue
        if (
            isinstance(statement, ast.Assign)
            and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name)
            and statement.targets[0].id == "__all__"
            and exported_names is None
            and isinstance(statement.value, (ast.List, ast.Tuple))
            and all(
                isinstance(element, ast.Constant) and isinstance(element.value, str)
                for element in statement.value.elts
            )
        ):
            export_node = statement
            exported_names = tuple(
                element.value
                for element in statement.value.elts
                if isinstance(element, ast.Constant) and isinstance(element.value, str)
            )
            continue
        reject(
            statement,
            "runtime export exemption may contain only static re-exports and __all__",
        )

    if exported_names is None:
        reject(tree, "runtime export exemption must declare a constant __all__")
    elif (
        len(exported_names) != len(set(exported_names))
        or set(exported_names) != imported_bindings
    ):
        reject(
            export_node,
            "runtime export exemption __all__ must exactly match imported bindings",
        )
    return violations


def _scan_export_only_runtime_exemption_path(
    path: Path,
    *,
    project_root: Path,
    enforce_reviewed_hash: bool = False,
) -> list[_Violation]:
    filename = path.relative_to(project_root).as_posix()
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=filename)
    except (OSError, UnicodeError, SyntaxError, ValueError) as error:
        return [
            _Violation(
                filename=filename,
                lineno=int(getattr(error, "lineno", 1) or 1),
                col_offset=int(getattr(error, "offset", 0) or 0),
                code="runtime_exemption_effect",
                detail="runtime export exemption cannot be parsed and reviewed",
            )
        ]
    violations = _export_only_exemption_violations(tree, filename=filename)
    expected_hash = _REVIEWED_EXPORT_ONLY_EXEMPT_AST_SHA256.get(filename)
    if enforce_reviewed_hash and (
        expected_hash is None or _normalized_module_ast_sha256(tree) != expected_hash
    ):
        violations.append(
            _Violation(
                filename=filename,
                lineno=1,
                col_offset=0,
                code="runtime_exemption_drift",
                detail="runtime export exemption differs from the reviewed AST",
            )
        )
    return sorted(set(violations))


def _production_runtime_entrypoints(project_root: Path) -> list[Path]:
    src_root = project_root / "src"
    roots = sorted(
        path
        for path in src_root.rglob("*.py")
        if path.is_file()
        and _is_runtime_entrypoint_path(path.relative_to(project_root))
    )
    pending = list(roots)
    visited: set[Path] = set()
    candidates: set[Path] = set()
    while pending:
        path = pending.pop()
        if path in visited:
            continue
        visited.add(path)
        relative_path = path.relative_to(project_root).as_posix()
        if relative_path not in _RUNTIME_SCAN_EXEMPT_PATHS:
            candidates.add(path)
        pending.extend(
            dependency
            for dependency in _static_local_import_paths(
                path,
                project_root=project_root,
            )
            if dependency not in visited
        )
    return sorted(candidates)


def _scan_runtime_path(path: Path, *, project_root: Path) -> list[_Violation]:
    filename = path.relative_to(project_root).as_posix()
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return [
            _Violation(
                filename=filename,
                lineno=1,
                col_offset=0,
                code="unreadable_runtime_source",
                detail="runtime source cannot be proven dormant",
            )
        ]
    return _scan_runtime_source(source, filename=filename)


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            "import src.ingestion.sync\n",
            "forbidden_import",
            id="direct-module-import",
        ),
        pytest.param(
            "from src.ingestion.sync import SyncCoordinator\n",
            "forbidden_import",
            id="from-import",
        ),
        pytest.param(
            "import src.ingestion.cold_start as cold\nservice = cold.ColdStartService()\n",
            "forbidden_import",
            id="alias-import",
        ),
        pytest.param(
            "from src.ingestion.cold_start import *\n",
            "forbidden_import",
            id="star-import",
        ),
        pytest.param(
            "from .ingestion.sync import SyncCoordinator\n",
            "forbidden_import",
            id="relative-from-import",
        ),
        pytest.param(
            "from src import ingestion as ingest\nservice = ingest.cold_start.ColdStartService()\n",
            "forbidden_import",
            id="attribute-container-import",
        ),
        pytest.param(
            "def build(module):\n    return module.SyncCoordinator()\n",
            "target_constructor",
            id="attribute-sync-constructor",
        ),
        pytest.param(
            "def build():\n    return ColdStartService()\n",
            "target_constructor",
            id="direct-cold-start-constructor",
        ),
        pytest.param(
            "def build():\n    factory = ColdStartService\n    return factory()\n",
            "target_symbol_reference",
            id="constructor-assignment-alias",
        ),
        pytest.param(
            "def wire(app, service):\n    app.state.pipeline = service\n",
            "runtime_registration",
            id="app-state-registration",
        ),
        pytest.param(
            "def wire(container, service):\n    container['pipeline'] = service\n",
            "runtime_registration",
            id="container-item-registration",
        ),
        pytest.param(
            "def wire(container, service):\n    container.register('pipeline', service)\n",
            "runtime_registration",
            id="container-call-registration",
        ),
        pytest.param(
            "async def activate(coordinator):\n    await coordinator.run_folder(8, 'INBOX')\n",
            "target_method_call",
            id="run-folder-call",
        ),
        pytest.param(
            "async def activate(service):\n    await service.preview(8, 'INBOX')\n",
            "target_method_call",
            id="preview-call",
        ),
        pytest.param(
            "async def activate(service, plan_id):\n    await service.resume(plan_id)\n",
            "target_method_call",
            id="resume-call",
        ),
        pytest.param(
            "async def activate(service, plan_id):\n    await service.approve(plan_id)\n",
            "target_method_call",
            id="approve-call",
        ),
        pytest.param(
            "async def activate(service, plan_id):\n    await service.apply(plan_id)\n",
            "target_method_call",
            id="apply-call",
        ),
        pytest.param(
            "async def activate(coordinator):\n    runner = coordinator.run_folder\n    await runner()\n",
            "target_method_reference",
            id="method-assignment-alias",
        ),
        pytest.param(
            "def build(module):\n    return getattr(module, 'Sync' + 'Coordinator')()\n",
            "target_reflection",
            id="getattr-constant-folding",
        ),
        pytest.param(
            "def build(module, class_name):\n    return getattr(module, class_name)()\n",
            "unresolved_reflection",
            id="getattr-dynamic-name",
        ),
        pytest.param(
            "def build(module):\n    return object.__getattribute__(module, 'ColdStartService')()\n",
            "target_reflection",
            id="object-getattribute",
        ),
        pytest.param(
            "def build(module):\n    return vars(module)['SyncCoordinator']()\n",
            "namespace_reflection",
            id="vars-namespace",
        ),
        pytest.param(
            "def build(module):\n    return module.__dict__['ColdStartService']()\n",
            "target_reflection",
            id="dunder-dict-reflection",
        ),
        pytest.param(
            "import operator\ndef build(module):\n    return operator.attrgetter('SyncCoordinator')(module)()\n",
            "target_reflection",
            id="operator-attrgetter",
        ),
        pytest.param(
            "import operator\ndef run(service):\n    return operator.methodcaller('apply', object())(service)\n",
            "target_reflection",
            id="operator-methodcaller",
        ),
        pytest.param(
            "import importlib\nmodule = importlib.import_module('src.ingestion.sync')\n",
            "dynamic_target_import",
            id="import-module-target",
        ),
        pytest.param(
            "module = __import__('src.ingestion.' + 'cold_start', fromlist=['ColdStartService'])\n",
            "dynamic_target_import",
            id="dunder-import-constant-folding",
        ),
        pytest.param(
            "from importlib import import_module\ndef load(name):\n    return import_module(name)\n",
            "unresolved_dynamic_import",
            id="dynamic-import-unresolved",
        ),
        pytest.param(
            "import importlib\nloader = importlib.import_module\n",
            "dynamic_import_primitive",
            id="dynamic-import-assignment-alias",
        ),
        pytest.param(
            "import builtins\ndef load():\n    loader = builtins.__import__\n    return loader('src.ingestion.sync')\n",
            "dynamic_import_primitive",
            id="dynamic-import-attribute-alias",
        ),
        pytest.param(
            "import importlib\nloader = getattr(importlib, 'import_module')\n",
            "target_reflection",
            id="dynamic-import-getattr-alias",
        ),
        pytest.param(
            "def load(builtins):\n    return builtins['__import__']('src.ingestion.sync')\n",
            "target_reflection",
            id="dynamic-import-mapping-alias",
        ),
        pytest.param(
            "def load(source):\n    exec(source)\n",
            "dynamic_code_primitive",
            id="exec-bypass",
        ),
        pytest.param(
            "def wire(app, service):\n    setattr(app.state, 'pipeline', service)\n",
            "runtime_registration",
            id="setattr-app-state-registration",
        ),
        pytest.param(
            "def wire(container, service):\n    setattr(container, 'pipeline', service)\n",
            "runtime_registration",
            id="setattr-container-registration",
        ),
        pytest.param(
            "def wire(container, service):\n    container['services']['pipeline'] = service\n",
            "runtime_registration",
            id="nested-container-registration",
        ),
        pytest.param(
            "def load(container):\n    return container['pipeline']\n",
            "runtime_registry_access",
            id="container-indirect-read",
        ),
    ),
)
def test_detector_rejects_forbidden_runtime_mutation(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}


def test_detector_allows_benign_runtime_patterns() -> None:
    source = """
import json

def read_runtime_state(settings, state):
    enabled = getattr(settings, "SYNC_RECONCILIATION_ENABLED", False)
    values = getattr(state, "values", None)
    return json.dumps({"enabled": enabled, "values": values, "preview": "label"})
"""

    assert _scan_runtime_source(source, filename="src/main.py") == []


@pytest.mark.parametrize(
    ("relative_path", "expected"),
    (
        pytest.param("src/main.py", True, id="main"),
        pytest.param("src/api/server.py", True, id="nested-server"),
        pytest.param(
            "src/services/exchange_service.py",
            True,
            id="nested-exchange-service",
        ),
        pytest.param("src/startup.py", True, id="startup-file"),
        pytest.param("src/runtime/app_startup.py", True, id="startup-stem"),
        pytest.param("src/scheduler/jobs.py", True, id="scheduler-directory"),
        pytest.param("src/job_scheduler.py", True, id="scheduler-stem"),
        pytest.param("src/workers/mail.py", True, id="worker-directory"),
        pytest.param("src/mail_worker.py", True, id="worker-stem"),
        pytest.param("src/ingestion/sync.py", False, id="dormant-implementation"),
        pytest.param("src/domain/work.py", False, id="unrelated-production"),
        pytest.param(
            "tests/architecture/test_worker_boundary.py",
            False,
            id="test-module",
        ),
    ),
)
def test_runtime_entrypoint_path_classifier_is_fail_closed(
    relative_path: str,
    expected: bool,
) -> None:
    assert _is_runtime_entrypoint_path(Path(relative_path)) is expected


def test_production_runtime_entrypoints_keep_sync_services_dormant() -> None:
    project_root = Path(__file__).resolve().parents[2]
    candidates = _production_runtime_entrypoints(project_root)
    export_only_exemption = project_root / "src/ingestion/__init__.py"
    relative_candidates = {
        path.relative_to(project_root).as_posix() for path in candidates
    }
    assert {
        "src/main.py",
        "src/server.py",
        "src/exchange_service.py",
        "src/scheduler/__init__.py",
        "src/scheduler/daily_summary.py",
        "src/scheduler/polling.py",
    } <= relative_candidates

    violations = [
        violation
        for path in candidates
        for violation in _scan_runtime_path(path, project_root=project_root)
    ]
    violations.extend(
        _scan_export_only_runtime_exemption_path(
            export_only_exemption,
            project_root=project_root,
            enforce_reviewed_hash=True,
        )
    )

    assert violations == [], (
        "SyncCoordinator and ColdStartService must remain dormant in every runtime "
        f"entrypoint: {violations}"
    )


@pytest.fixture
def synthetic_runtime_import_closure(tmp_path: Path) -> Path:
    sources = {
        "src/__init__.py": "",
        "src/main.py": "from src.init_app import build_app\n\napp = build_app()\n",
        "src/init_app.py": (
            "from src.self_healing import SelfHealer\n\n"
            "def build_app():\n"
            "    return SelfHealer()\n"
        ),
        "src/self_healing.py": (
            "from src.ingestion.cold_start import ColdStartService\n"
            "from src.ingestion.sync import SyncCoordinator\n\n"
            "class SelfHealer:\n"
            "    providers = (ColdStartService, SyncCoordinator)\n"
        ),
        "src/ingestion/__init__.py": "",
        "src/ingestion/cold_start.py": "class ColdStartService:\n    pass\n",
        "src/ingestion/sync.py": "class SyncCoordinator:\n    pass\n",
        "src/unrelated.py": "VALUE = 'not reachable from a runtime root'\n",
    }
    for relative_path, source in sources.items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")
    return tmp_path


def test_runtime_import_closure_discovers_local_dependencies_and_exempts_providers(
    synthetic_runtime_import_closure: Path,
) -> None:
    project_root = synthetic_runtime_import_closure
    relative_candidates = {
        path.relative_to(project_root).as_posix()
        for path in _production_runtime_entrypoints(project_root)
    }
    runtime_closure = {
        "src/main.py",
        "src/init_app.py",
        "src/self_healing.py",
    }
    dormant_provider_exemptions = {
        "src/ingestion/cold_start.py",
        "src/ingestion/sync.py",
    }

    assert runtime_closure <= relative_candidates
    assert relative_candidates.isdisjoint(dormant_provider_exemptions)
    assert "src/unrelated.py" not in relative_candidates


def test_real_runtime_import_closure_includes_required_dependencies() -> None:
    project_root = Path(__file__).resolve().parents[2]
    relative_candidates = {
        path.relative_to(project_root).as_posix()
        for path in _production_runtime_entrypoints(project_root)
    }
    required_runtime_dependencies = {
        "src/init_app.py",
        "src/ingestion/runtime.py",
    }
    dormant_provider_exemptions = {
        "src/ingestion/cold_start.py",
        "src/ingestion/sync.py",
        "src/utils/self_healing.py",
    }

    assert required_runtime_dependencies <= relative_candidates
    assert relative_candidates.isdisjoint(dormant_provider_exemptions)


def test_detector_rejects_aliased_dynamic_target_construction() -> None:
    source = """
from importlib import import_module as load
from operator import attrgetter as pick

def build():
    module = load("src.ingestion." + "sy" + "nc")
    factory = pick("Sync" + "Coordinator")(module)
    return factory()
"""

    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")
    codes = {violation.code for violation in violations}

    assert {
        "dynamic_target_import",
        "target_reflection",
        "target_constructor",
    } <= codes


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
def build(module, enabled):
    reflect = getattr if enabled else getattr
    return reflect(module, "SyncCoordinator")()
""",
            id="if-expression-reflection-alias",
        ),
        pytest.param(
            """
def build(module):
    reflect = (getattr, setattr)[0]
    return reflect(module, "SyncCoordinator")()
""",
            id="tuple-subscript-reflection-alias",
        ),
        pytest.param(
            """
def build(module):
    reflect = {"inspect": getattr}["inspect"]
    return reflect(module, "SyncCoordinator")()
""",
            id="mapping-subscript-reflection-alias",
        ),
        pytest.param(
            """
def build(module):
    reflect = getattr or setattr
    return reflect(module, "SyncCoordinator")()
""",
            id="boolean-expression-reflection-alias",
        ),
        pytest.param(
            """
def build(module):
    return (reflect := getattr)(module, "SyncCoordinator")()
""",
            id="named-expression-reflection-alias",
        ),
        pytest.param(
            """
def build(module):
    reflect, unused = (getattr, setattr)
    return reflect(module, "SyncCoordinator")()
""",
            id="unpacked-reflection-alias",
        ),
    ),
)
def test_detector_rejects_composed_reflection_alias_call(source: str) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert "target_reflection" in {violation.code for violation in violations}


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
async def activate(app):
    await app.state.pipeline.run_folder(8, "INBOX")
""",
            "target_method_call",
            id="app-state-target-slot-call",
        ),
        pytest.param(
            """
async def activate(registry):
    await registry.get("pipeline").run_folder(8, "INBOX")
""",
            "target_method_call",
            id="registry-get-target-slot-call",
        ),
        pytest.param(
            """
def wire(app, value):
    app.state.pipeline = value
""",
            "runtime_registration",
            id="app-state-target-slot-unknown-value",
        ),
        pytest.param(
            """
def wire(container, value):
    container.register("pipeline", value)
""",
            "runtime_registration",
            id="container-register-target-slot-unknown-value",
        ),
        pytest.param(
            """
def wire(container, value):
    container["pipeline"] = value
""",
            "runtime_registration",
            id="container-subscript-target-slot-unknown-value",
        ),
        pytest.param(
            """
from builtins import __import__ as load

def build():
    namespace = load("src.ingestion", fromlist=["*"])
    factory = namespace.__all__[0]
    return factory()
""",
            "dynamic_target_import",
            id="builtins-dunder-import-alias-target-container",
        ),
    ),
)
def test_detector_rejects_target_slots_and_builtins_import_alias(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}, violations


def test_runtime_import_closure_follows_constant_local_dynamic_imports(
    tmp_path: Path,
) -> None:
    sources = {
        "src/__init__.py": "",
        "src/main.py": (
            "import importlib\n\n"
            "hidden = importlib.import_module('src.hidden_activation')\n"
        ),
        "src/server.py": (
            "from builtins import __import__ as load\n\n"
            "hidden = load('src.hidden_builtin', fromlist=['*'])\n"
        ),
        "src/hidden_activation.py": (
            "from src.ingestion.cold_start import ColdStartService\n"
        ),
        "src/hidden_builtin.py": "from src.ingestion.sync import SyncCoordinator\n",
        "src/ingestion/__init__.py": "",
        "src/ingestion/cold_start.py": "class ColdStartService:\n    pass\n",
        "src/ingestion/sync.py": "class SyncCoordinator:\n    pass\n",
    }
    for relative_path, source in sources.items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")

    candidates = _production_runtime_entrypoints(tmp_path)
    relative_candidates = {path.relative_to(tmp_path).as_posix() for path in candidates}
    violations = [
        violation
        for path in candidates
        for violation in _scan_runtime_path(path, project_root=tmp_path)
    ]

    assert {"src/hidden_activation.py", "src/hidden_builtin.py"} <= relative_candidates
    assert {violation.code for violation in violations} >= {"forbidden_import"}


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            "def render(report):\n    return report.preview()\n",
            id="unrelated-preview-method",
        ),
        pytest.param(
            "def install(patch):\n    return patch.apply()\n",
            id="unrelated-apply-method",
        ),
        pytest.param(
            """
def configure_health(app):
    app.state.health_config = {"enabled": True}
    return app.state.health_config
""",
            id="app-state-health-config-read-write",
        ),
        pytest.param(
            """
def cache_report(registry, report):
    registry["latest_report"] = report
    return registry.get("latest_report")
""",
            id="ordinary-registry-read-write",
        ),
    ),
)
def test_detector_allows_unrelated_runtime_operations(source: str) -> None:
    assert _scan_runtime_source(source, filename="src/main.py") == []


@pytest.mark.parametrize(
    ("source", "expected_codes"),
    (
        pytest.param(
            """
from src.ingestion.sync import SyncCoordinator as Build

def wire(catalog):
    catalog["primary"] = Build
""",
            frozenset({"forbidden_import", "runtime_registration"}),
            id="target-import-alias-registration",
        ),
        pytest.param(
            """
def build():
    factory = SyncCoordinator
    renamed_factory = factory
    return renamed_factory()
""",
            frozenset({"target_symbol_reference", "target_constructor"}),
            id="target-class-alias-construction",
        ),
        pytest.param(
            """
def wire(catalog):
    service = ColdStartService()
    renamed_service = service
    catalog["primary"] = renamed_service
""",
            frozenset({"target_constructor", "runtime_registration"}),
            id="target-instance-alias-registration",
        ),
        pytest.param(
            """
async def activate():
    service = SyncCoordinator()
    renamed_service = service
    action = renamed_service.run_folder
    return await action(8, "INBOX")
""",
            frozenset(
                {
                    "target_constructor",
                    "target_method_reference",
                    "target_method_call",
                }
            ),
            id="target-instance-method-alias-call",
        ),
    ),
)
def test_detector_rejects_target_alias_activation(
    source: str,
    expected_codes: frozenset[str],
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_codes <= {violation.code for violation in violations}


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
def build(module, reflect=getattr):
    return reflect(module, "SyncCoordinator")()
""",
            "target_reflection",
            id="getattr-default-parameter-forwarder",
        ),
        pytest.param(
            """
def load(loader=__import__):
    return loader("src.ingestion.sync", fromlist=["*"])
""",
            "dynamic_target_import",
            id="dunder-import-default-parameter-forwarder",
        ),
        pytest.param(
            """
def execute(source, runner=exec):
    return runner(source)
""",
            "dynamic_code_primitive",
            id="exec-default-parameter-forwarder",
        ),
        pytest.param(
            """
def build(module):
    forward = lambda owner, name: getattr(owner, name)
    return forward(module, "SyncCoordinator")()
""",
            "target_reflection",
            id="lambda-reflection-forwarder",
        ),
        pytest.param(
            """
import functools

def load():
    forward = functools.partial(__import__, fromlist=["*"])
    return forward("src.ingestion.sync")
""",
            "dynamic_target_import",
            id="functools-partial-import-forwarder",
        ),
        pytest.param(
            """
def build(module):
    return vars(module).get("SyncCoordinator")()
""",
            "target_reflection",
            id="vars-get-reflection",
        ),
        pytest.param(
            """
def build(module):
    return module.__dict__.get("ColdStartService")()
""",
            "target_reflection",
            id="dunder-dict-get-reflection",
        ),
        pytest.param(
            """
def build():
    for opaque in (SyncCoordinator,):
        return opaque()
""",
            "target_constructor",
            id="loop-bound-target-constructor",
        ),
        pytest.param(
            """
import importlib

def load():
    namespace = importlib.import_module("builtins")
    forward = namespace.__import__
    return forward("src.ingestion.sync", fromlist=["*"])
""",
            "dynamic_target_import",
            id="dynamically-loaded-builtins-alias",
        ),
        pytest.param(
            """
def wire(app, opaque):
    setattr(app.state, "pipeline", opaque)
""",
            "runtime_registration",
            id="setattr-app-state-target-key-unknown-value",
        ),
        pytest.param(
            """
def wire(app, opaque):
    app.state.update({"pipeline": opaque})
""",
            "runtime_registration",
            id="app-state-update-target-key-unknown-value",
        ),
        pytest.param(
            """
def wire(app, opaque):
    app.state.__dict__["pipeline"] = opaque
""",
            "runtime_registration",
            id="app-state-dunder-dict-target-key-unknown-value",
        ),
        pytest.param(
            """
async def activate(app):
    catalog = app.state.registry
    fetch = catalog.get
    opaque = fetch("pipeline")
    return await opaque.run_folder(8, "INBOX")
""",
            "target_method_call",
            id="registry-and-get-method-alias",
        ),
        pytest.param(
            """
async def activate(app):
    namespace = app.state.__dict__
    fetch = namespace.get
    opaque = fetch("pipeline")
    return await opaque.run_folder(8, "INBOX")
""",
            "target_method_call",
            id="app-state-dunder-dict-get-method-alias",
        ),
    ),
)
def test_detector_rejects_forwarded_activation_bypasses(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}, violations


def test_runtime_import_closure_resolves_relative_dynamic_imports_and_fromlists(
    tmp_path: Path,
) -> None:
    sources = {
        "src/__init__.py": "",
        "src/main.py": (
            "import importlib\n\n"
            "hidden = importlib.import_module('.hidden_relative', package='src')\n"
        ),
        "src/server.py": ("hidden = __import__('src', fromlist=['hidden_fromlist'])\n"),
        "src/hidden_relative.py": (
            "from src.ingestion.cold_start import ColdStartService\n"
        ),
        "src/hidden_fromlist.py": "from src.ingestion.sync import SyncCoordinator\n",
        "src/ingestion/__init__.py": "",
        "src/ingestion/cold_start.py": "class ColdStartService:\n    pass\n",
        "src/ingestion/sync.py": "class SyncCoordinator:\n    pass\n",
    }
    for relative_path, source in sources.items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")

    candidates = _production_runtime_entrypoints(tmp_path)
    relative_candidates = {path.relative_to(tmp_path).as_posix() for path in candidates}
    violations = [
        violation
        for path in candidates
        for violation in _scan_runtime_path(path, project_root=tmp_path)
    ]

    assert {"src/hidden_relative.py", "src/hidden_fromlist.py"} <= relative_candidates
    assert {violation.code for violation in violations} >= {"forbidden_import"}


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
def inspect(owner, getattr):
    return getattr(owner, "SyncCoordinator")
""",
            id="shadowed-getattr-parameter",
        ),
        pytest.param(
            """
class importlib:
    @staticmethod
    def import_module(name):
        return name

loader = importlib.import_module
""",
            id="shadowed-importlib-class",
        ),
        pytest.param(
            """
def render(source):
    return exec(source)

def exec(source):
    return source
""",
            id="later-module-exec-definition",
        ),
        pytest.param(
            """
def load(name):
    return import_module(name)

def import_module(name):
    return {"name": name}
""",
            id="later-module-import-function-definition",
        ),
    ),
)
def test_detector_allows_shadowed_primitives_and_later_module_definitions(
    source: str,
) -> None:
    assert _scan_runtime_source(source, filename="src/main.py") == []


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
def fetch(owner, key):
    return owner.get(key)

async def activate(registry):
    opaque = fetch(registry, "pipeline")
    return await opaque.run_folder(8, "INBOX")
""",
            "target_method_call",
            id="ordinary-function-return-forwarder",
        ),
        pytest.param(
            """
def install(owner, key, opaque):
    owner[key] = opaque

def wire(app, opaque):
    install(app.state, "pipeline", opaque)
""",
            "runtime_registration",
            id="ordinary-function-registration-wrapper",
        ),
    ),
)
def test_detector_rejects_ordinary_function_activation_wrappers(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}, violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
async def activate(app):
    opaque = getattr(app.state, "pipeline")
    return await opaque.run_folder(8, "INBOX")
""",
            id="getattr-app-state-service-slot",
        ),
        pytest.param(
            """
import operator

async def activate(app):
    opaque = operator.attrgetter("pipeline")(app.state)
    return await opaque.run_folder(8, "INBOX")
""",
            id="attrgetter-app-state-service-slot",
        ),
        pytest.param(
            """
import operator

async def activate(registry):
    opaque = operator.methodcaller("get", "pipeline")(registry)
    return await opaque.run_folder(8, "INBOX")
""",
            id="methodcaller-registry-service-slot",
        ),
    ),
)
def test_detector_rejects_reflected_runtime_service_slot_access(source: str) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")
    codes = {violation.code for violation in violations}

    assert {"runtime_registry_access", "target_method_call"} <= codes, violations


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
async def activate(registry):
    services = [registry.get(key) for key in ("pipeline",)]
    return await services[0].run_folder(8, "INBOX")
""",
            "target_method_call",
            id="list-comprehension-service-key-provenance",
        ),
        pytest.param(
            """
def wire(app, opaque):
    return [setattr(app.state, key, opaque) for key in ("pipeline",)]
""",
            "runtime_registration",
            id="list-comprehension-registration-key-provenance",
        ),
        pytest.param(
            """
from importlib import import_module

modules = [import_module(name) for name in ("src.ingestion.sync",)]
""",
            "dynamic_target_import",
            id="list-comprehension-dynamic-import-provenance",
        ),
    ),
)
def test_detector_rejects_comprehension_activation_provenance(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}, violations


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
def load():
    return builtins.__import__("src.ingestion.sync", fromlist=["*"])

load()

class builtins:
    __import__ = None
""",
            "dynamic_target_import",
            id="builtins-call-before-later-shadow",
        ),
        pytest.param(
            """
def load():
    return importlib.import_module("src.ingestion.sync")

load()

class importlib:
    import_module = None
""",
            "dynamic_target_import",
            id="importlib-call-before-later-shadow",
        ),
        pytest.param(
            """
def inspect(module):
    return operator.attrgetter("SyncCoordinator")(module)

inspect(object())

class operator:
    attrgetter = None
""",
            "target_reflection",
            id="operator-call-before-later-shadow",
        ),
    ),
)
def test_detector_honors_module_execution_before_later_primitive_shadow(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/main.py")

    assert expected_code in {violation.code for violation in violations}, violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
def load():
    return builtins.__import__("src.ingestion.sync", fromlist=["*"])

class builtins:
    @staticmethod
    def __import__(*args, **kwargs):
        return {"args": args, "kwargs": kwargs}

load()
""",
            id="builtins-call-after-shadow",
        ),
        pytest.param(
            """
def load():
    return importlib.import_module("src.ingestion.sync")

class importlib:
    @staticmethod
    def import_module(name):
        return {"name": name}

load()
""",
            id="importlib-call-after-shadow",
        ),
        pytest.param(
            """
def inspect(module):
    return operator.attrgetter("SyncCoordinator")(module)

class operator:
    @staticmethod
    def attrgetter(name):
        return lambda owner: (name, owner)

inspect(object())
""",
            id="operator-call-after-shadow",
        ),
    ),
)
def test_detector_allows_module_call_after_primitive_shadow(source: str) -> None:
    assert _scan_runtime_source(source, filename="src/main.py") == []


def test_detector_rejects_dotted_dunder_import_fromlist_target() -> None:
    source = "module = __import__('src', fromlist=['ingestion.sync'])\n"

    violations = _scan_runtime_source(source, filename="src/main.py")

    assert "dynamic_target_import" in {violation.code for violation in violations}, (
        violations
    )


def test_runtime_import_closure_follows_dotted_dunder_import_fromlist(
    tmp_path: Path,
) -> None:
    sources = {
        "src/__init__.py": "",
        "src/main.py": ("hidden = __import__('src', fromlist=['hidden.activation'])\n"),
        "src/hidden/__init__.py": "",
        "src/hidden/activation.py": (
            "from src.ingestion.sync import SyncCoordinator\n"
        ),
        "src/ingestion/__init__.py": "",
        "src/ingestion/sync.py": "class SyncCoordinator:\n    pass\n",
    }
    for relative_path, source in sources.items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")

    candidates = _production_runtime_entrypoints(tmp_path)
    relative_candidates = {path.relative_to(tmp_path).as_posix() for path in candidates}
    violations = [
        violation
        for path in candidates
        for violation in _scan_runtime_path(path, project_root=tmp_path)
    ]

    assert "src/hidden/activation.py" in relative_candidates
    assert "forbidden_import" in {violation.code for violation in violations}


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
async def activate(registry):
    service = registry.copy().get("sync")
    return await service.apply()
""",
            "target_method_call",
            id="registry-copy-get-service",
        ),
        pytest.param(
            """
def build(module):
    return module.__getattribute__("SyncCoordinator")()
""",
            "target_reflection",
            id="bound-dunder-getattribute-target-class",
        ),
        pytest.param(
            """
async def activate(registry):
    service = registry.pop("sync")
    return await service.apply()
""",
            "target_method_call",
            id="registry-pop-service",
        ),
        pytest.param(
            """
async def activate(app):
    service = app.state.copy().pop("sync")
    return await service.apply()
""",
            "target_method_call",
            id="app-state-copy-pop-service",
        ),
        pytest.param(
            """
async def activate(registry):
    wrapper = dict(registry)
    service = wrapper.get("sync")
    return await service.apply()
""",
            "target_method_call",
            id="dict-wrapper-preserves-registry-provenance",
        ),
        pytest.param(
            """
import operator

async def activate(registry):
    service = operator.itemgetter("sync")(registry)
    return await service.apply()
""",
            "target_method_call",
            id="operator-itemgetter-service-accessor",
        ),
        pytest.param(
            """
def wire(registry, opaque):
    registry.update({**{"sync": opaque}})
""",
            "runtime_registration",
            id="registry-update-unpacked-mapping",
        ),
        pytest.param(
            """
def wire(registry, opaque):
    put = registry.__setitem__
    put("sync", opaque)
""",
            "runtime_registration",
            id="bound-registry-setitem-registration",
        ),
        pytest.param(
            """
async def activate(app):
    namespace = vars(app.state)
    service = namespace.pop("sync")
    return await service.apply()
""",
            "target_method_call",
            id="app-state-namespace-pop-service",
        ),
        pytest.param(
            """
def wire(registry, opaque):
    put = getattr(registry, "__setitem__")
    put("sync", opaque)
""",
            "runtime_registration",
            id="reflected-registry-setitem-registration",
        ),
    ),
)
def test_detector_rejects_mapping_and_bound_method_activation_bypasses(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}, violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
def close(client):
    reflect = getattr
    return reflect(client, "close")()
""",
            id="getattr-alias-safe-name-and-receiver",
        ),
        pytest.param(
            """
def enabled(settings):
    namespace = vars(settings)
    return namespace.get("enabled")
""",
            id="vars-namespace-safe-key",
        ),
        pytest.param(
            """
from operator import attrgetter

def enabled(settings):
    pick = attrgetter
    return pick("enabled")(settings)
""",
            id="attrgetter-alias-safe-name-and-receiver",
        ),
        pytest.param(
            """
def getattr(owner, name):
    return (owner, name)

def inspect(owner):
    reflect = getattr
    return reflect(owner, "SyncCoordinator")
""",
            id="ordinary-local-getattr-shadow",
        ),
        pytest.param(
            """
def inspect(owner, attrgetter):
    pick = attrgetter
    return pick("SyncCoordinator")(owner)
""",
            id="ordinary-parameter-attrgetter-shadow",
        ),
    ),
)
def test_detector_allows_bounded_safe_reflection_aliases(source: str) -> None:
    assert _scan_runtime_source(source, filename="src/main.py") == []


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
def activate(registry):
    return registry.copy().get("sync")
""",
            "runtime_registry_access",
            id="copied-registry-direct-get-effect",
        ),
        pytest.param(
            """
def activate(app):
    namespace = vars(app.state)
    return namespace.pop("sync")
""",
            "runtime_registry_access",
            id="app-state-namespace-direct-pop-effect",
        ),
        pytest.param(
            """
def build(module):
    pick = module.__getattribute__ or module.__getattribute__
    return pick("SyncCoordinator")()
""",
            "target_reflection",
            id="composed-bound-dunder-getattribute",
        ),
    ),
)
def test_detector_classifies_bound_lookup_effect_sites(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}, violations


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
import copy

async def activate(registry):
    service = copy.copy(registry).get("sync")
    return await service.apply()
""",
            "target_method_call",
            id="copy-module-copy-preserves-registry",
        ),
        pytest.param(
            """
from copy import deepcopy

async def activate(registry):
    service = deepcopy(registry).get("sync")
    return await service.apply()
""",
            "target_method_call",
            id="deepcopy-import-preserves-registry",
        ),
        pytest.param(
            """
from types import MappingProxyType

async def activate(registry):
    service = MappingProxyType(registry).get("sync")
    return await service.apply()
""",
            "target_method_call",
            id="mapping-proxy-preserves-registry",
        ),
        pytest.param(
            """
from collections import ChainMap

async def activate(registry):
    service = ChainMap({}, registry).get("sync")
    return await service.apply()
""",
            "target_method_call",
            id="chain-map-preserves-registry",
        ),
        pytest.param(
            """
async def activate(registry):
    merged = {} | registry
    return await merged.get("sync").apply()
""",
            "target_method_call",
            id="mapping-union-right-registry",
        ),
        pytest.param(
            """
async def activate(registry):
    merged = registry | {}
    return await merged.get("sync").apply()
""",
            "target_method_call",
            id="mapping-union-left-registry",
        ),
        pytest.param(
            """
async def activate(registry):
    merged = {}
    merged |= registry
    return await merged.get("sync").apply()
""",
            "target_method_call",
            id="mapping-in-place-union-registry",
        ),
        pytest.param(
            """
def wire(registry, opaque):
    registry |= {"sync": opaque}
""",
            "runtime_registration",
            id="registry-in-place-union-registration",
        ),
        pytest.param(
            """
def identity(value):
    return value

async def activate(registry):
    fetch = identity(identity(registry.get))
    return await fetch("sync").apply()
""",
            "target_method_call",
            id="nested-identity-call-frames",
        ),
        pytest.param(
            """
import operator

async def activate(registry):
    fetch = operator.attrgetter("get")(registry)
    return await fetch("sync").apply()
""",
            "target_method_call",
            id="attrgetter-bound-get",
        ),
        pytest.param(
            """
import operator

def wire(registry, opaque):
    put = operator.attrgetter("__setitem__")(registry)
    put("sync", opaque)
""",
            "runtime_registration",
            id="attrgetter-bound-setitem",
        ),
        pytest.param(
            """
import operator

async def activate(registry):
    service = operator.methodcaller("pop", "sync")(registry)
    return await service.apply()
""",
            "target_method_call",
            id="methodcaller-pop-effect",
        ),
        pytest.param(
            """
import operator

def wire(registry, opaque):
    operator.methodcaller("update", {"sync": opaque})(registry)
""",
            "runtime_registration",
            id="methodcaller-update-effect",
        ),
        pytest.param(
            """
import operator

def wire(registry, opaque):
    operator.methodcaller("__setitem__", "sync", opaque)(registry)
""",
            "runtime_registration",
            id="methodcaller-setitem-effect",
        ),
        pytest.param(
            """
async def activate(registry):
    box = {}
    box["registry"] = registry
    return await box["registry"].get("sync").apply()
""",
            "target_method_call",
            id="mapping-subscript-mutation",
        ),
        pytest.param(
            """
async def activate(registry):
    box = {}
    box.update({"registry": registry})
    return await box["registry"].get("sync").apply()
""",
            "target_method_call",
            id="mapping-update-mutation",
        ),
        pytest.param(
            """
async def activate(registry):
    box = []
    box.append(registry)
    return await box[0].get("sync").apply()
""",
            "target_method_call",
            id="list-append-mutation",
        ),
        pytest.param(
            """
def activate(registry, transform):
    service = transform(registry).get("sync")
    return service.apply()
""",
            "target_method_call",
            id="semantic-backstop-for-opaque-transform",
        ),
    ),
)
def test_detector_rejects_v4_provenance_and_effect_bypasses(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}, violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
import copy

def enabled(settings):
    return copy.copy(settings).get("enabled")
""",
            id="safe-copy-lookup",
        ),
        pytest.param(
            """
from copy import deepcopy

def close(client):
    return deepcopy(client).close()
""",
            id="safe-deepcopy-close",
        ),
        pytest.param(
            """
from types import MappingProxyType

def enabled(settings):
    return MappingProxyType({"enabled": True}).get("enabled")
""",
            id="safe-mapping-proxy",
        ),
        pytest.param(
            """
from collections import ChainMap

def enabled(settings):
    return ChainMap({"enabled": True}, {}).get("enabled")
""",
            id="safe-chain-map",
        ),
        pytest.param(
            """
def label():
    merged = {"sync": "label"} | {"enabled": True}
    merged |= {"count": 1}
    return merged.get("sync")
""",
            id="safe-mapping-union",
        ),
        pytest.param(
            """
import operator

def enabled(settings):
    fetch = operator.attrgetter("get")({"enabled": True})
    return fetch("enabled")
""",
            id="safe-attrgetter-bound-get",
        ),
        pytest.param(
            """
import operator

def enabled(settings):
    return operator.methodcaller("pop", "enabled")({"enabled": True})
""",
            id="safe-methodcaller-pop",
        ),
        pytest.param(
            """
def install(patch):
    return patch.apply()
""",
            id="unrelated-patch-apply-remains-safe",
        ),
    ),
)
def test_detector_allows_v4_bounded_transforms_and_effects(source: str) -> None:
    assert _scan_runtime_source(source, filename="src/main.py") == []


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
import types

async def activate(registry):
    service = types.MappingProxyType(registry).get("sync")
    return await service.apply()
""",
            "target_method_call",
            id="types-module-mapping-proxy",
        ),
        pytest.param(
            """
import collections

async def activate(registry):
    service = collections.ChainMap({}, registry).get("sync")
    return await service.apply()
""",
            "target_method_call",
            id="collections-module-chain-map",
        ),
        pytest.param(
            """
import operator

def wire(registry, opaque):
    operator.methodcaller("update", sync=opaque)(registry)
""",
            "runtime_registration",
            id="methodcaller-update-keyword-effect",
        ),
    ),
)
def test_detector_rejects_v4_module_and_keyword_transform_bypasses(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}, violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
import types

def enabled(settings):
    return types.MappingProxyType({"enabled": True}).get("enabled")
""",
            id="safe-types-module-mapping-proxy",
        ),
        pytest.param(
            """
import collections

def enabled(settings):
    return collections.ChainMap({"enabled": True}).get("enabled")
""",
            id="safe-collections-module-chain-map",
        ),
    ),
)
def test_detector_allows_v4_module_transform_variants(source: str) -> None:
    assert _scan_runtime_source(source, filename="src/main.py") == []


@pytest.mark.parametrize(
    ("source", "expected_code"),
    (
        pytest.param(
            """
import operator

async def activate(registry):
    service = operator.getitem(registry, "sync")
    return await service.apply()
""",
            "target_method_call",
            id="operator-module-getitem",
        ),
        pytest.param(
            """
from operator import getitem as pick

async def activate(registry):
    service = pick(registry, "sync")
    return await service.apply()
""",
            "target_method_call",
            id="operator-imported-getitem",
        ),
        pytest.param(
            """
import operator

def wire(registry, opaque):
    operator.setitem(registry, "sync", opaque)
""",
            "runtime_registration",
            id="operator-module-setitem",
        ),
        pytest.param(
            """
from operator import setitem as put

def wire(registry, opaque):
    put(registry, "sync", opaque)
""",
            "runtime_registration",
            id="operator-imported-setitem",
        ),
        pytest.param(
            """
async def activate(registry):
    service = dict.get(registry, "sync")
    return await service.apply()
""",
            "target_method_call",
            id="unbound-dict-get",
        ),
        pytest.param(
            """
def wire(registry, opaque):
    dict.__setitem__(registry, "sync", opaque)
""",
            "runtime_registration",
            id="unbound-dict-setitem",
        ),
        pytest.param(
            """
def activate(registry):
    box = {}
    mutate = box.update
    mutate({"registry": registry})
    return box["registry"].get("sync")
""",
            "runtime_registry_access",
            id="bound-mapping-update-alias-writes-fact",
        ),
        pytest.param(
            """
def activate(registry):
    box = []
    add = box.append
    add(registry)
    return box[0].get("sync")
""",
            "runtime_registry_access",
            id="bound-list-append-alias-writes-fact",
        ),
        pytest.param(
            """
def activate(registry):
    box = {}
    box.setdefault("registry", registry)
    return box["registry"].get("sync")
""",
            "runtime_registry_access",
            id="mapping-setdefault-writes-fact",
        ),
        pytest.param(
            """
def wire(registry, opaque):
    registry.update(**{"sync": opaque})
""",
            "runtime_registration",
            id="registry-update-static-starstar-mapping",
        ),
        pytest.param(
            """
def wire(registry, payload):
    registry.update(**payload)
""",
            "runtime_registration",
            id="registry-update-unknown-starstar-fail-closed",
        ),
    ),
)
def test_detector_rejects_v5_unbound_operations_and_mutation_aliases(
    source: str,
    expected_code: str,
) -> None:
    violations = _scan_runtime_source(source, filename="src/runtime_worker.py")

    assert expected_code in {violation.code for violation in violations}, violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
import operator

def enabled(settings):
    return operator.getitem({"enabled": True}, "enabled")
""",
            id="safe-operator-getitem",
        ),
        pytest.param(
            """
from operator import setitem

def cache_label():
    cache = {}
    setitem(cache, "sync", "label")
    return cache["sync"]
""",
            id="safe-operator-setitem",
        ),
        pytest.param(
            """
def enabled(settings):
    return dict.get({"enabled": True}, "enabled")
""",
            id="safe-unbound-dict-get",
        ),
        pytest.param(
            """
def cache_label():
    cache = {}
    dict.__setitem__(cache, "sync", "label")
    return cache["sync"]
""",
            id="safe-unbound-dict-setitem",
        ),
        pytest.param(
            """
def cache_label():
    cache = {}
    mutate = cache.update
    mutate({"sync": "label"})
    return cache.get("sync")
""",
            id="safe-bound-update-alias",
        ),
        pytest.param(
            """
def cache_label():
    cache = []
    add = cache.append
    add("label")
    return cache[0]
""",
            id="safe-bound-append-alias",
        ),
        pytest.param(
            """
def cache_label():
    cache = {}
    cache.setdefault("sync", "label")
    return cache.get("sync")
""",
            id="safe-setdefault",
        ),
        pytest.param(
            """
def configure(registry, opaque):
    registry.update(**{"health": opaque})
""",
            id="safe-static-starstar-key",
        ),
        pytest.param(
            """
def configure(cache, payload):
    cache.update(**payload)
""",
            id="safe-unknown-starstar-non-runtime-mapping",
        ),
    ),
)
def test_detector_allows_v5_safe_unbound_and_mutation_patterns(source: str) -> None:
    assert _scan_runtime_source(source, filename="src/main.py") == []


@pytest.mark.parametrize(
    "runtime_effect",
    (
        pytest.param(
            "service = ColdStartService()\n",
            id="constructor",
        ),
        pytest.param(
            'registry["sync"] = ColdStartService\n',
            id="registry-assignment",
        ),
        pytest.param(
            'service.run_folder(8, "INBOX")\n',
            id="run-folder-call",
        ),
        pytest.param(
            'service.apply("plan-1")\n',
            id="apply-call",
        ),
    ),
)
def test_export_only_runtime_exemption_rejects_appended_effects(
    tmp_path: Path,
    runtime_effect: str,
) -> None:
    path = tmp_path / "src/ingestion/__init__.py"
    path.parent.mkdir(parents=True)
    path.write_text(
        '"""Dormant ingestion exports."""\n\n'
        "from src.ingestion.cold_start import ColdStartService\n"
        "from src.ingestion.sync import SyncCoordinator\n\n"
        '__all__ = ["ColdStartService", "SyncCoordinator"]\n\n' + runtime_effect,
        encoding="utf-8",
    )

    violations = _scan_export_only_runtime_exemption_path(
        path,
        project_root=tmp_path,
    )

    assert "runtime_exemption_effect" in {violation.code for violation in violations}, (
        violations
    )


def test_export_only_runtime_exemption_allows_exact_static_reexports(
    tmp_path: Path,
) -> None:
    path = tmp_path / "src/ingestion/__init__.py"
    path.parent.mkdir(parents=True)
    path.write_text(
        '"""Dormant ingestion exports."""\n\n'
        "from src.ingestion.cold_start import ColdStartService\n"
        "from src.ingestion.sync import SyncCoordinator\n\n"
        '__all__ = ["ColdStartService", "SyncCoordinator"]\n',
        encoding="utf-8",
    )

    assert (
        _scan_export_only_runtime_exemption_path(
            path,
            project_root=tmp_path,
        )
        == []
    )
