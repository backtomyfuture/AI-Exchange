from __future__ import annotations

import ast
import dataclasses
import hashlib
import inspect
import textwrap
from collections import Counter
from pathlib import Path

import pytest

from src.ingestion.models import SyncBatch
from src.utils.exchange_api import ExchangeClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]

_SYNC_CALL_NAME = "sync_emails"
_SYNC_POSITIONAL_PARAMETERS = ("account_id", "folder", "cursor", "limit")
_REFLECTION_TARGET = "reflection_target"
_REFLECTION_UNRESOLVED = "reflection_unresolved"
_REFLECTION_SAFE = "reflection_safe"
_REFLECTION_NAMESPACE = "reflection_namespace"
_REFLECTION_NAMESPACE_GET = "reflection_namespace_get"
_REFLECTION_FORWARDER = "reflection_forwarder"
_REFLECTION_PRIMITIVE_PREFIX = "reflection_primitive:"
_REFLECTION_MODULE_BUILTINS = "reflection_module:builtins"
_REFLECTION_MODULE_FUNCTOOLS = "reflection_module:functools"
_REFLECTION_MODULE_OPERATOR = "reflection_module:operator"
_REFLECTION_MODULE_TYPES = "reflection_module:types"
_AST_DUMP_SUPPORTS_SHOW_EMPTY = "show_empty" in inspect.signature(ast.dump).parameters


@dataclasses.dataclass(frozen=True)
class _ReflectionSequenceFact:
    elements: tuple[_ReflectionFact, ...]


@dataclasses.dataclass(frozen=True)
class _ReflectionMappingFact:
    entries: tuple[tuple[str | None, _ReflectionFact], ...]


@dataclasses.dataclass(frozen=True)
class _ReflectionClassFact:
    attributes: tuple[tuple[str, _ReflectionFact], ...]


@dataclasses.dataclass(frozen=True)
class _ReflectionIteratorFact:
    elements: tuple[_ReflectionFact, ...]


@dataclasses.dataclass(frozen=True)
class _ReflectionItemGetterFact:
    keys: tuple[str | int | None, ...]


@dataclasses.dataclass(frozen=True)
class _ReflectionOperationFact:
    owner: _ReflectionFact | None
    operation: str
    owner_name: str | None
    owner_argument: int | None = None


@dataclasses.dataclass(frozen=True)
class _ReflectionCallableFact:
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda
    captured: tuple[tuple[str, _ReflectionFact], ...]
    reviewed_return: bool = False


@dataclasses.dataclass(frozen=True)
class _ReflectionCallableChoiceFact:
    choices: tuple[_ReflectionCallableFact, ...]


@dataclasses.dataclass(frozen=True)
class _ReflectionLiteralFact:
    value: str | int


_ReflectionFact = (
    str
    | _ReflectionSequenceFact
    | _ReflectionMappingFact
    | _ReflectionClassFact
    | _ReflectionIteratorFact
    | _ReflectionItemGetterFact
    | _ReflectionOperationFact
    | _ReflectionCallableFact
    | _ReflectionCallableChoiceFact
    | _ReflectionLiteralFact
)


def _reflection_primitive_fact(kind: str) -> _ReflectionFact:
    return f"{_REFLECTION_PRIMITIVE_PREFIX}{kind}"


def _reflection_primitive_kind(fact: _ReflectionFact | None) -> str | None:
    if not isinstance(fact, str) or not fact.startswith(_REFLECTION_PRIMITIVE_PREFIX):
        return None
    return fact.removeprefix(_REFLECTION_PRIMITIVE_PREFIX)


def _normalized_ast_dump(node: ast.AST) -> str:
    if _AST_DUMP_SUPPORTS_SHOW_EMPTY:
        return ast.dump(node, include_attributes=False, **{"show_empty": True})
    return ast.dump(node, include_attributes=False)


def _normalized_ast_sha256(node: ast.AST) -> str:
    return hashlib.sha256(_normalized_ast_dump(node).encode("utf-8")).hexdigest()


_ALLOWED_SYNC_CALLSITE_SHAPES = {
    (
        "src/ingestion/sync.py",
        (
            "module",
            "class:SyncCoordinator",
            "async-function:_run_locked",
        ),
    ): ast.dump(
        ast.parse(
            "self._page_client.sync_emails("
            "account_id, scope.sync_folder, expected.cursor, self._page_limit"
            ")",
            mode="eval",
        ).body,
        include_attributes=False,
    ),
    (
        "src/ingestion/cold_start.py",
        ("module", "async-function:_fetch_ordinary_page"),
    ): ast.dump(
        ast.parse(
            "client.sync_emails(account_id, sync_folder, cursor, limit)",
            mode="eval",
        ).body,
        include_attributes=False,
    ),
}

_REVIEWED_NON_SYNC_REFLECTION_KEY = (
    "src/providers/factory.py",
    ("module", "function:_create_oauth_model"),
)
_REVIEWED_NON_SYNC_REFLECTION_FILE_AST_SHA256 = {
    "src/providers/factory.py": (
        "544e80ea67d2894f7eafa5619596058335081d2dd4cad016173e32ae3b87e647"
    ),
}
_REVIEWED_NON_SYNC_REFLECTION_BINDINGS = {
    _REVIEWED_NON_SYNC_REFLECTION_KEY: frozenset(
        {"module_path", "class_name", "module", "cls"}
    )
}
_REVIEWED_NON_SYNC_REFLECTION_ASSIGNMENT_SHAPES = {
    _REVIEWED_NON_SYNC_REFLECTION_KEY: {
        _normalized_ast_dump(
            ast.parse("module_path, class_name = _OAUTH_PROVIDERS[spec.name]").body[0]
        ): 1,
        _normalized_ast_dump(
            ast.parse("module = importlib.import_module(module_path)").body[0]
        ): 1,
        _normalized_ast_dump(ast.parse("cls = getattr(module, class_name)").body[0]): 1,
    }
}
_REVIEWED_NON_SYNC_REFLECTION_CALL_SHAPES = {
    _REVIEWED_NON_SYNC_REFLECTION_KEY: {
        _normalized_ast_dump(
            ast.parse("importlib.import_module(module_path)", mode="eval").body
        ): 1,
        _normalized_ast_dump(
            ast.parse("getattr(module, class_name)", mode="eval").body
        ): 1,
        _normalized_ast_dump(
            ast.parse(
                "cls(model_name=model, temperature=temperature, **kwargs)",
                mode="eval",
            ).body
        ): 1,
    }
}
_REVIEWED_NON_SYNC_REFLECTION_CALL_PATHS = {
    _REVIEWED_NON_SYNC_REFLECTION_KEY: frozenset(
        {"importlib.import_module", "getattr", "cls"}
    )
}


def _constant_text(node: ast.expr) -> str | None:
    """Fold only self-contained string expressions used as reflection keys."""

    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _constant_text(node.left)
        right = _constant_text(node.right)
        if left is not None and right is not None:
            return left + right
        return None
    if not isinstance(node, ast.JoinedStr):
        return None

    parts: list[str] = []
    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parts.append(value.value)
            continue
        if not isinstance(value, ast.FormattedValue):
            return None
        if value.conversion not in {-1, ord("s")} or value.format_spec is not None:
            return None
        folded = _constant_text(value.value)
        if folded is None:
            return None
        parts.append(folded)
    return "".join(parts)


class _SyncCallsiteVisitor(ast.NodeVisitor):
    """Collect direct Sync calls while preserving their exact lexical owner."""

    def __init__(
        self,
        *,
        reviewed_return_owners: frozenset[tuple[str, ...]] = frozenset(),
    ) -> None:
        self._scope: list[str] = ["module"]
        self._reflection_aliases: list[dict[str, _ReflectionFact]] = [{}]
        self._module_late_bindings: dict[str, _ReflectionFact] = {}
        self._module_functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        self._allow_module_late_bindings = True
        self._replayed_module_functions: set[int] = set()
        self._active_callable_evaluations: set[int] = set()
        self._return_facts: list[list[_ReflectionFact]] = []
        self._reviewed_return_owners = reviewed_return_owners
        self.calls: list[tuple[tuple[str, ...], ast.Call]] = []
        self.all_calls: list[tuple[tuple[str, ...], ast.Call]] = []
        self.assignments: list[tuple[tuple[str, ...], ast.Assign]] = []
        self.indirect_references: list[tuple[tuple[str, ...], str]] = []
        self.reflection_violations: list[tuple[tuple[str, ...], str]] = []

    @property
    def _owner(self) -> tuple[str, ...]:
        return tuple(self._scope)

    def _visit_in_scope(
        self,
        scope: str,
        nodes: list[ast.stmt],
        *,
        bound_names: tuple[str, ...] = (),
        bound_facts: dict[str, _ReflectionFact] | None = None,
        collect_returns: bool = False,
    ) -> _ReflectionFact:
        self._scope.append(scope)
        aliases = {name: _REFLECTION_SAFE for name in bound_names}
        if bound_facts is not None:
            aliases.update(bound_facts)
        self._reflection_aliases.append(aliases)
        if collect_returns:
            self._return_facts.append([])
        try:
            for node in nodes:
                self.visit(node)
            if collect_returns:
                return self._merge_reflection_facts(self._return_facts[-1])
            return _REFLECTION_SAFE
        finally:
            if collect_returns:
                self._return_facts.pop()
            self._reflection_aliases.pop()
            self._scope.pop()

    @staticmethod
    def _late_definition_fact(
        node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
    ) -> str:
        primitive_names = {
            "attrgetter",
            "getattr",
            "itemgetter",
            "methodcaller",
            "partial",
            "setattr",
            "vars",
        }
        primitive_attributes = primitive_names | {
            "__dict__",
            "__getattribute__",
            "__setattr__",
        }
        locally_bound = {node.name}
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            arguments = (
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
                node.args.vararg,
                node.args.kwarg,
            )
            locally_bound.update(
                argument.arg for argument in arguments if argument is not None
            )
        locally_bound.update(
            child.id
            for child in ast.walk(node)
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store)
        )
        if any(
            (
                isinstance(child, ast.Name)
                and isinstance(child.ctx, ast.Load)
                and child.id in primitive_names
                and child.id not in locally_bound
            )
            or (isinstance(child, ast.Attribute) and child.attr in primitive_attributes)
            for child in ast.walk(node)
        ):
            return _REFLECTION_FORWARDER
        return _REFLECTION_SAFE

    def visit_Module(self, node: ast.Module) -> None:
        self._module_late_bindings = {
            statement.name: self._late_definition_fact(statement)
            for statement in node.body
            if isinstance(
                statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            )
        }
        for statement in node.body:
            self.visit(statement)

    def _visit_arguments(self, arguments: ast.arguments) -> None:
        positional = (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)
        for argument in positional:
            if argument.annotation is not None:
                self.visit(argument.annotation)
        for argument in (arguments.vararg, arguments.kwarg):
            if argument is not None and argument.annotation is not None:
                self.visit(argument.annotation)
        for default in (*arguments.defaults, *arguments.kw_defaults):
            if default is not None:
                self.visit(default)

    def _visit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        scope_kind: str,
    ) -> _ReflectionFact:
        for decorator in node.decorator_list:
            self.visit(decorator)
        self._visit_arguments(node.args)
        if node.returns is not None:
            self.visit(node.returns)
        for type_parameter in getattr(node, "type_params", ()):
            self.visit(type_parameter)
        positional = (*node.args.posonlyargs, *node.args.args)
        all_arguments = (
            *positional,
            *node.args.kwonlyargs,
            node.args.vararg,
            node.args.kwarg,
        )
        bound_names = tuple(
            argument.arg for argument in all_arguments if argument is not None
        )
        bound_facts = {name: _REFLECTION_SAFE for name in bound_names}
        for argument, default in zip(
            positional[-len(node.args.defaults) :] if node.args.defaults else (),
            node.args.defaults,
            strict=True,
        ):
            bound_facts[argument.arg] = self._reflection_fact(default)
        for argument, default in zip(
            node.args.kwonlyargs,
            node.args.kw_defaults,
            strict=True,
        ):
            if default is not None:
                bound_facts[argument.arg] = self._reflection_fact(default)
        return_fact = self._visit_in_scope(
            f"{scope_kind}:{node.name}",
            node.body,
            bound_names=bound_names,
            bound_facts=bound_facts,
            collect_returns=True,
        )
        function_owner = (*self._owner, f"{scope_kind}:{node.name}")
        if function_owner in self._reviewed_return_owners:
            return_fact = _REFLECTION_SAFE
        return return_fact

    @staticmethod
    def _callable_argument_names(
        node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda,
    ) -> tuple[str, ...]:
        arguments = node.args
        declared = (
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
            arguments.vararg,
            arguments.kwarg,
        )
        return tuple(argument.arg for argument in declared if argument is not None)

    def _callable_fact(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda,
    ) -> _ReflectionCallableFact:
        excluded = set(self._callable_argument_names(node))
        if not isinstance(node, ast.Lambda):
            excluded.add(node.name)
        captured: dict[str, _ReflectionFact] = {}
        for scope, aliases in zip(
            self._scope[1:],
            self._reflection_aliases[1:],
            strict=True,
        ):
            if scope.startswith("class:"):
                continue
            captured.update(
                (name, fact) for name, fact in aliases.items() if name not in excluded
            )
        reviewed_return = False
        if not isinstance(node, ast.Lambda):
            scope_kind = (
                "async-function"
                if isinstance(node, ast.AsyncFunctionDef)
                else "function"
            )
            reviewed_return = (
                *self._owner,
                f"{scope_kind}:{node.name}",
            ) in self._reviewed_return_owners
        return _ReflectionCallableFact(
            node,
            tuple(sorted(captured.items())),
            reviewed_return,
        )

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._reflection_aliases[-1][node.name] = _REFLECTION_SAFE
        self._visit_function(node, scope_kind="function")
        self._reflection_aliases[-1][node.name] = self._callable_fact(node)
        if self._owner == ("module",):
            self._module_functions[node.name] = node

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._reflection_aliases[-1][node.name] = _REFLECTION_SAFE
        self._visit_function(node, scope_kind="async-function")
        self._reflection_aliases[-1][node.name] = self._callable_fact(node)
        if self._owner == ("module",):
            self._module_functions[node.name] = node

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._reflection_aliases[-1][node.name] = _REFLECTION_SAFE
        if self._owner == ("module",):
            self._module_functions.pop(node.name, None)
        for decorator in node.decorator_list:
            self.visit(decorator)
        for base in node.bases:
            self.visit(base)
        for keyword in node.keywords:
            self.visit(keyword.value)
        for type_parameter in getattr(node, "type_params", ()):
            self.visit(type_parameter)
        self._scope.append(f"class:{node.name}")
        self._reflection_aliases.append({})
        try:
            for statement in node.body:
                self.visit(statement)
            attributes = tuple(sorted(self._reflection_aliases[-1].items()))
        finally:
            self._reflection_aliases.pop()
            self._scope.pop()
        self._reflection_aliases[-1][node.name] = _ReflectionClassFact(attributes)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self._visit_arguments(node.args)
        self._scope.append("lambda")
        positional = (*node.args.posonlyargs, *node.args.args)
        arguments = (
            *positional,
            *node.args.kwonlyargs,
            node.args.vararg,
            node.args.kwarg,
        )
        aliases = {
            argument.arg: _REFLECTION_SAFE
            for argument in arguments
            if argument is not None
        }
        for argument, default in zip(
            positional[-len(node.args.defaults) :] if node.args.defaults else (),
            node.args.defaults,
            strict=True,
        ):
            aliases[argument.arg] = self._reflection_fact(default)
        for argument, default in zip(
            node.args.kwonlyargs,
            node.args.kw_defaults,
            strict=True,
        ):
            if default is not None:
                aliases[argument.arg] = self._reflection_fact(default)
        self._reflection_aliases.append(aliases)
        try:
            self.visit(node.body)
        finally:
            self._reflection_aliases.pop()
            self._scope.pop()

    def _visit_comprehension(self, node: ast.AST, scope: str) -> None:
        self._scope.append(scope)
        generators = getattr(node, "generators", ())
        self._reflection_aliases.append({})
        try:
            for generator in generators:
                self.visit(generator.iter)
                self._bind_reflection_alias(
                    generator.target,
                    self._iterable_element_fact(generator.iter),
                )
                for condition in generator.ifs:
                    self.visit(condition)
            if isinstance(node, ast.DictComp):
                self.visit(node.key)
                self.visit(node.value)
            else:
                self.visit(node.elt)
        finally:
            self._reflection_aliases.pop()
            self._scope.pop()

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comprehension(node, "list-comprehension")

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comprehension(node, "set-comprehension")

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self._visit_comprehension(node, "dict-comprehension")

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comprehension(node, "generator-expression")

    def _iterable_element_fact(self, node: ast.expr) -> _ReflectionFact:
        fact = self._reflection_fact(node)
        if isinstance(fact, _ReflectionSequenceFact):
            return self._merge_reflection_facts(list(fact.elements))
        if isinstance(fact, _ReflectionIteratorFact):
            return self._merge_reflection_facts(list(fact.elements))
        if isinstance(fact, _ReflectionMappingFact):
            return _REFLECTION_SAFE
        return _REFLECTION_SAFE

    def _visit_for(self, node: ast.For | ast.AsyncFor) -> None:
        self.visit(node.iter)
        self._bind_reflection_alias(
            node.target,
            self._iterable_element_fact(node.iter),
        )
        for statement in node.body:
            self.visit(statement)
        for statement in node.orelse:
            self.visit(statement)

    def visit_For(self, node: ast.For) -> None:
        self._visit_for(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self._visit_for(node)

    @staticmethod
    def _is_direct_sync_call(node: ast.Call) -> bool:
        return (
            isinstance(node.func, ast.Name) and node.func.id == _SYNC_CALL_NAME
        ) or (
            isinstance(node.func, ast.Attribute) and node.func.attr == _SYNC_CALL_NAME
        )

    @staticmethod
    def _expression_path(node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            owner = _SyncCallsiteVisitor._expression_path(node.value)
            if owner is not None:
                return f"{owner}.{node.attr}"
        return None

    def _lookup_reflection_alias(self, name: str) -> _ReflectionFact | None:
        for aliases in reversed(self._reflection_aliases):
            if name in aliases:
                return aliases[name]
        if (
            self._allow_module_late_bindings
            and len(self._reflection_aliases) > 1
            and name in self._module_late_bindings
        ):
            return self._module_late_bindings[name]
        return None

    @staticmethod
    def _merge_reflection_facts(
        facts: list[_ReflectionFact],
    ) -> _ReflectionFact:
        unique = set(facts)
        if not unique:
            return _REFLECTION_SAFE
        if len(unique) == 1:
            return unique.pop()
        if _REFLECTION_TARGET in unique:
            return _REFLECTION_TARGET
        if unique == {_REFLECTION_SAFE}:
            return _REFLECTION_SAFE
        if all(
            isinstance(fact, (_ReflectionCallableFact, _ReflectionCallableChoiceFact))
            for fact in unique
        ):
            choices = {
                choice
                for fact in unique
                for choice in (
                    fact.choices
                    if isinstance(fact, _ReflectionCallableChoiceFact)
                    else (fact,)
                )
            }
            if len(choices) == 1:
                return choices.pop()
            return _ReflectionCallableChoiceFact(
                tuple(
                    sorted(
                        choices,
                        key=lambda choice: (
                            getattr(choice.node, "lineno", -1),
                            getattr(choice.node, "col_offset", -1),
                            id(choice.node),
                        ),
                    )
                )
            )
        non_safe = unique - {_REFLECTION_SAFE}
        if len(non_safe) == 1:
            candidate = next(iter(non_safe))
            if _reflection_primitive_kind(candidate) is not None:
                return candidate
        return _REFLECTION_UNRESOLVED

    def _module_attribute_is_trusted(
        self,
        node: ast.Attribute,
        *,
        module: str,
        module_fact: str,
    ) -> bool:
        root = node.value
        while isinstance(root, ast.Attribute):
            root = root.value
        if not isinstance(root, ast.Name):
            return False
        bound = self._lookup_reflection_alias(root.id)
        return (root.id == module and bound is None) or bound == module_fact

    def _reflection_expression_kind(self, node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            bound = self._lookup_reflection_alias(node.id)
            primitive = _reflection_primitive_kind(bound)
            if primitive is not None:
                return primitive
            if bound is not None:
                return None
            if node.id in {
                "dict",
                "getattr",
                "iter",
                "list",
                "next",
                "setattr",
                "tuple",
                "vars",
            }:
                return node.id
            if node.id in {
                "attrgetter",
                "getitem",
                "itemgetter",
                "methodcaller",
                "partial",
                "setitem",
            }:
                return node.id
            return None

        if not isinstance(node, ast.Attribute):
            return None
        if node.attr == "__call__":
            return self._reflection_expression_kind(node.value)
        path = self._expression_path(node) or ""
        if node.attr == "__getattribute__":
            if path == "object.__getattribute__":
                return (
                    "object-getattribute"
                    if self._lookup_reflection_alias("object") is None
                    else None
                )
            return "bound-getattribute"
        if node.attr in {
            "getattr",
            "setattr",
            "vars",
        } and self._module_attribute_is_trusted(
            node,
            module="builtins",
            module_fact=_REFLECTION_MODULE_BUILTINS,
        ):
            return node.attr
        if node.attr in {
            "attrgetter",
            "getitem",
            "itemgetter",
            "methodcaller",
            "setitem",
        } and (
            self._module_attribute_is_trusted(
                node,
                module="operator",
                module_fact=_REFLECTION_MODULE_OPERATOR,
            )
        ):
            return node.attr
        if node.attr == "partial" and self._module_attribute_is_trusted(
            node,
            module="functools",
            module_fact=_REFLECTION_MODULE_FUNCTOOLS,
        ):
            return "partial"
        if node.attr == "SimpleNamespace" and self._module_attribute_is_trusted(
            node,
            module="types",
            module_fact=_REFLECTION_MODULE_TYPES,
        ):
            return "SimpleNamespace"
        return None

    def _reflection_call_kind(self, node: ast.Call) -> str | None:
        kind = self._reflection_expression_kind(node.func)
        if kind is not None:
            return kind
        return _reflection_primitive_kind(self._reflection_fact(node.func))

    @staticmethod
    def _call_argument(
        node: ast.Call,
        position: int,
        *keyword_names: str,
    ) -> ast.expr | None:
        if len(node.args) > position:
            argument = node.args[position]
            return argument.value if isinstance(argument, ast.Starred) else argument
        for keyword in node.keywords:
            if keyword.arg in keyword_names:
                return keyword.value
        return None

    def _reflection_name_fact(self, node: ast.expr | None) -> str:
        if node is None:
            return _REFLECTION_UNRESOLVED
        text = _constant_text(node)
        if text is None and isinstance(node, ast.Name):
            bound = self._lookup_reflection_alias(node.id)
            if isinstance(bound, _ReflectionLiteralFact):
                text = bound.value
        if text is None:
            return _REFLECTION_UNRESOLVED
        if text == _SYNC_CALL_NAME:
            return _REFLECTION_TARGET
        return _REFLECTION_SAFE

    @staticmethod
    def _fact_is_sync_selector(fact: _ReflectionFact) -> bool:
        return fact == _REFLECTION_TARGET or (
            isinstance(fact, _ReflectionLiteralFact) and fact.value == _SYNC_CALL_NAME
        )

    def _expanded_call_argument_facts(
        self,
        node: ast.Call,
    ) -> tuple[list[_ReflectionFact], list[tuple[str | None, _ReflectionFact]]]:
        positional: list[_ReflectionFact] = []
        for argument in node.args:
            if not isinstance(argument, ast.Starred):
                positional.append(self._reflection_fact(argument))
                continue
            starred = self._reflection_fact(argument.value)
            if isinstance(starred, _ReflectionSequenceFact):
                positional.extend(starred.elements)
            elif self._fact_is_sync_selector(starred):
                positional.append(starred)

        keywords: list[tuple[str | None, _ReflectionFact]] = []
        for keyword in node.keywords:
            fact = self._reflection_fact(keyword.value)
            if keyword.arg is not None:
                keywords.append((keyword.arg, fact))
            elif isinstance(fact, _ReflectionMappingFact):
                keywords.extend(fact.entries)
            elif self._fact_is_sync_selector(fact):
                keywords.append((None, fact))
        return positional, keywords

    def _unknown_sync_selector_result_fact(
        self,
        node: ast.Call,
        invoked_fact: _ReflectionFact,
    ) -> str | None:
        """Fail closed when an unknown callable may select the Sync method.

        Known reflection primitives, reviewed callables and modeled container
        operations are handled before this backstop.  An otherwise-unproven
        callable that receives a statically-known ``sync_emails`` selector
        after its receiver (or through a selector keyword) produces an
        unresolved fact.  That fact becomes a violation only if the result is
        subsequently used as a callable.
        """

        if invoked_fact != _REFLECTION_SAFE:
            return None
        positional, keywords = self._expanded_call_argument_facts(node)
        selector_keywords = {"attribute", "key", "name", "selector"}
        positional_target = any(
            self._fact_is_sync_selector(fact) for fact in positional[1:]
        )
        keyword_target = any(
            (name in selector_keywords or name is None)
            and self._fact_is_sync_selector(fact)
            for name, fact in keywords
        )
        if positional_target or keyword_target:
            return _REFLECTION_UNRESOLVED
        return None

    def _constant_item_key(self, node: ast.expr | None) -> str | int | None:
        if node is None:
            return None
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            operand = self._constant_item_key(node.operand)
            return -operand if isinstance(operand, int) else None
        if isinstance(node, ast.Name):
            bound = self._lookup_reflection_alias(node.id)
            if isinstance(bound, _ReflectionLiteralFact):
                return bound.value
        if isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                return node.value
            if isinstance(node.value, int) and not isinstance(node.value, bool):
                return node.value
        return None

    def _container_item_fact(
        self,
        container_fact: _ReflectionFact,
        key: str | int | None,
    ) -> _ReflectionFact | None:
        if isinstance(container_fact, _ReflectionSequenceFact):
            if not isinstance(key, int):
                return _REFLECTION_UNRESOLVED if key is None else _REFLECTION_SAFE
            if -len(container_fact.elements) <= key < len(container_fact.elements):
                return container_fact.elements[key]
            return _REFLECTION_UNRESOLVED
        if not isinstance(container_fact, _ReflectionMappingFact):
            return None
        _present, fact = self._mapping_item_state(container_fact, key)
        return fact

    @staticmethod
    def _mapping_item_state(
        mapping_fact: _ReflectionMappingFact,
        key: str | int | None,
    ) -> tuple[bool | None, _ReflectionFact]:
        if not isinstance(key, str):
            return None, _REFLECTION_UNRESOLVED if key is None else _REFLECTION_SAFE
        matches = [fact for candidate, fact in mapping_fact.entries if candidate == key]
        if len(matches) == 1:
            return True, matches[0]
        if matches or any(candidate is None for candidate, _ in mapping_fact.entries):
            return None, _REFLECTION_UNRESOLVED
        return False, _REFLECTION_SAFE

    @staticmethod
    def _updated_mapping_fact(
        mapping_fact: _ReflectionMappingFact,
        entries: list[tuple[str | None, _ReflectionFact]],
    ) -> _ReflectionMappingFact:
        merged = list(mapping_fact.entries)
        for key, fact in entries:
            if key is not None:
                merged = [
                    (candidate, value)
                    for candidate, value in merged
                    if candidate != key
                ]
            merged.append((key, fact))
        return _ReflectionMappingFact(tuple(merged))

    def _mapping_entries_from_iterable(
        self,
        node: ast.expr,
    ) -> list[tuple[str | None, _ReflectionFact]]:
        source_fact = self._reflection_fact(node)
        if isinstance(source_fact, _ReflectionMappingFact):
            return list(source_fact.entries)
        if isinstance(node, ast.Call) and self._reflection_call_kind(node) in {
            "list",
            "tuple",
        }:
            source = self._call_argument(node, 0, "iterable")
            return self._mapping_entries_from_iterable(source) if source else []
        if not isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            return [(None, source_fact)] if source_fact != _REFLECTION_SAFE else []

        entries: list[tuple[str | None, _ReflectionFact]] = []
        for element in node.elts:
            if isinstance(element, ast.Starred):
                entries.append((None, self._reflection_fact(element.value)))
                continue
            if not isinstance(element, (ast.List, ast.Tuple)) or len(element.elts) != 2:
                element_fact = self._reflection_fact(element)
                if element_fact != _REFLECTION_SAFE:
                    entries.append((None, element_fact))
                continue
            key = self._constant_item_key(element.elts[0])
            entries.append(
                (
                    key if isinstance(key, str) else None,
                    self._reflection_fact(element.elts[1]),
                )
            )
        return entries

    def _mapping_constructor_fact(self, node: ast.Call) -> _ReflectionMappingFact:
        fact = _ReflectionMappingFact(())
        if node.args:
            fact = self._updated_mapping_fact(
                fact,
                self._mapping_entries_from_iterable(node.args[0]),
            )
        for keyword in node.keywords:
            value_fact = self._reflection_fact(keyword.value)
            if keyword.arg is None:
                if isinstance(value_fact, _ReflectionMappingFact):
                    fact = self._updated_mapping_fact(fact, list(value_fact.entries))
                else:
                    fact = self._updated_mapping_fact(fact, [(None, value_fact)])
            else:
                fact = self._updated_mapping_fact(
                    fact,
                    [(keyword.arg, value_fact)],
                )
        return fact

    def _binary_reflection_fact(
        self,
        left: _ReflectionFact,
        operator: ast.operator,
        right: _ReflectionFact,
    ) -> _ReflectionFact:
        if (
            isinstance(operator, ast.Add)
            and isinstance(left, _ReflectionSequenceFact)
            and isinstance(right, _ReflectionSequenceFact)
        ):
            return _ReflectionSequenceFact((*left.elements, *right.elements))
        if (
            isinstance(operator, ast.BitOr)
            and isinstance(left, _ReflectionMappingFact)
            and isinstance(right, _ReflectionMappingFact)
        ):
            return self._updated_mapping_fact(left, list(right.entries))
        if left == _REFLECTION_SAFE and right == _REFLECTION_SAFE:
            return _REFLECTION_SAFE
        return self._merge_reflection_facts([left, right])

    def _object_constructor_fact(self, node: ast.Call) -> _ReflectionClassFact:
        attributes: dict[str, _ReflectionFact] = {}
        for keyword in node.keywords:
            value_fact = self._reflection_fact(keyword.value)
            if keyword.arg is None:
                if isinstance(value_fact, _ReflectionMappingFact):
                    attributes.update(
                        (name, fact)
                        for name, fact in value_fact.entries
                        if name is not None
                    )
                continue
            attributes[keyword.arg] = value_fact
        return _ReflectionClassFact(tuple(sorted(attributes.items())))

    def _callable_bound_facts(
        self,
        callable_fact: _ReflectionCallableFact,
        call: ast.Call,
    ) -> dict[str, _ReflectionFact]:
        definition = callable_fact.node
        arguments = definition.args
        positional_parameters = (*arguments.posonlyargs, *arguments.args)
        bound: dict[str, _ReflectionFact] = dict(callable_fact.captured)
        bound.update(
            (name, _REFLECTION_SAFE)
            for name in self._callable_argument_names(definition)
        )
        for parameter, default in zip(
            positional_parameters[-len(arguments.defaults) :]
            if arguments.defaults
            else (),
            arguments.defaults,
            strict=True,
        ):
            bound[parameter.arg] = self._reflection_fact(default)
        for parameter, default in zip(
            arguments.kwonlyargs,
            arguments.kw_defaults,
            strict=True,
        ):
            if default is not None:
                bound[parameter.arg] = self._reflection_fact(default)

        positional_facts: list[_ReflectionFact] = []
        for argument in call.args:
            if not isinstance(argument, ast.Starred):
                positional_facts.append(self._reflection_fact(argument))
                continue
            starred_fact = self._reflection_fact(argument.value)
            if isinstance(starred_fact, _ReflectionSequenceFact):
                positional_facts.extend(starred_fact.elements)
            elif isinstance(starred_fact, _ReflectionIteratorFact):
                positional_facts.extend(starred_fact.elements)
            else:
                positional_facts.append(_REFLECTION_UNRESOLVED)
        for parameter, fact in zip(
            positional_parameters,
            positional_facts,
            strict=False,
        ):
            bound[parameter.arg] = fact

        extra_positional = positional_facts[len(positional_parameters) :]
        if arguments.vararg is not None:
            bound[arguments.vararg.arg] = _ReflectionSequenceFact(
                tuple(extra_positional)
            )

        keyword_parameters = {
            parameter.arg for parameter in (*arguments.args, *arguments.kwonlyargs)
        }
        extra_keywords: list[tuple[str | None, _ReflectionFact]] = []
        for keyword in call.keywords:
            value_fact = self._reflection_fact(keyword.value)
            if keyword.arg is None:
                if isinstance(value_fact, _ReflectionMappingFact):
                    for name, fact in value_fact.entries:
                        if name in keyword_parameters:
                            assert name is not None
                            bound[name] = fact
                        else:
                            extra_keywords.append((name, fact))
                else:
                    extra_keywords.append((None, value_fact))
                continue
            if keyword.arg in keyword_parameters:
                bound[keyword.arg] = value_fact
            else:
                extra_keywords.append((keyword.arg, value_fact))
        if arguments.kwarg is not None:
            bound[arguments.kwarg.arg] = _ReflectionMappingFact(tuple(extra_keywords))
        return bound

    def _callable_return_fact(
        self,
        callable_fact: _ReflectionCallableFact,
        call: ast.Call,
    ) -> _ReflectionFact:
        if callable_fact.reviewed_return:
            return _REFLECTION_SAFE
        definition = callable_fact.node
        identity = id(definition)
        if identity in self._active_callable_evaluations:
            return _REFLECTION_UNRESOLVED
        self._active_callable_evaluations.add(identity)
        try:
            bound_facts = self._callable_bound_facts(callable_fact, call)
            bound_names = self._callable_argument_names(definition)
            if isinstance(definition, ast.Lambda):
                self._scope.append("call:lambda")
                self._reflection_aliases.append(bound_facts)
                try:
                    result = self._reflection_fact(definition.body)
                    self.visit(definition.body)
                    return result
                finally:
                    self._reflection_aliases.pop()
                    self._scope.pop()
            scope_kind = (
                "async-function"
                if isinstance(definition, ast.AsyncFunctionDef)
                else "function"
            )
            return self._visit_in_scope(
                f"call:{scope_kind}:{definition.name}",
                definition.body,
                bound_names=bound_names,
                bound_facts=bound_facts,
                collect_returns=True,
            )
        finally:
            self._active_callable_evaluations.remove(identity)

    def _operation_owner_node(
        self,
        operation: _ReflectionOperationFact,
        node: ast.Call,
    ) -> ast.expr | None:
        if operation.owner_argument is None:
            return None
        return self._call_argument(
            node,
            operation.owner_argument,
            "container",
            "mapping",
            "object",
            "sequence",
        )

    def _operation_owner_name(
        self,
        operation: _ReflectionOperationFact,
        node: ast.Call,
    ) -> str | None:
        if operation.owner_name is not None:
            return operation.owner_name
        owner_node = self._operation_owner_node(operation, node)
        return owner_node.id if isinstance(owner_node, ast.Name) else None

    def _operation_owner_fact(
        self,
        operation: _ReflectionOperationFact,
        node: ast.Call,
    ) -> _ReflectionFact:
        owner_name = self._operation_owner_name(operation, node)
        if owner_name is not None:
            bound = self._lookup_reflection_alias(owner_name)
            if bound is not None:
                return bound
        owner_node = self._operation_owner_node(operation, node)
        if owner_node is not None:
            return self._reflection_fact(owner_node)
        return operation.owner or _REFLECTION_UNRESOLVED

    def _operation_argument(
        self,
        operation: _ReflectionOperationFact,
        node: ast.Call,
        position: int,
        *keyword_names: str,
    ) -> ast.expr | None:
        owner_offset = 1 if operation.owner_argument is not None else 0
        return self._call_argument(
            node,
            position + owner_offset,
            *keyword_names,
        )

    @staticmethod
    def _unbound_operation_fact(
        invoked_fact: _ReflectionFact,
    ) -> _ReflectionOperationFact | None:
        primitive = _reflection_primitive_kind(invoked_fact)
        operation = {
            "getitem": "__getitem__",
            "setitem": "__setitem__",
        }.get(primitive or "")
        if operation is None:
            return None
        return _ReflectionOperationFact(None, operation, None, 0)

    def _operation_result(
        self,
        operation: _ReflectionOperationFact,
        node: ast.Call,
    ) -> _ReflectionFact:
        owner = self._operation_owner_fact(operation, node)
        if operation.operation in {
            "append",
            "extend",
            "insert",
            "update",
            "__setitem__",
            "__setattr__",
        }:
            return _REFLECTION_SAFE

        key_node = self._operation_argument(operation, node, 0, "key", "index")
        key = self._constant_item_key(key_node)
        if operation.operation == "__getitem__":
            return self._container_item_fact(owner, key) or _REFLECTION_SAFE
        if not isinstance(owner, _ReflectionMappingFact):
            return _REFLECTION_SAFE

        present, current = self._mapping_item_state(owner, key)
        default_node = self._operation_argument(operation, node, 1, "default")
        default = (
            self._reflection_fact(default_node)
            if default_node is not None
            else _REFLECTION_SAFE
        )
        if operation.operation == "setdefault":
            if present is True:
                return current
            if present is False:
                return default
            return _REFLECTION_UNRESOLVED
        if operation.operation in {"get", "pop"}:
            if present is True:
                return current
            if present is False:
                return default
            return _REFLECTION_UNRESOLVED
        return _REFLECTION_SAFE

    def _reflection_constructor_fact(
        self,
        node: ast.Call,
    ) -> _ReflectionFact | None:
        invoked_fact = self._reflection_fact(node.func)
        operation = (
            invoked_fact
            if isinstance(invoked_fact, _ReflectionOperationFact)
            else self._unbound_operation_fact(invoked_fact)
        )
        if operation is not None:
            return self._operation_result(operation, node)
        if isinstance(invoked_fact, _ReflectionCallableChoiceFact):
            return self._merge_reflection_facts(
                [
                    self._callable_return_fact(choice, node)
                    for choice in invoked_fact.choices
                ]
            )
        if isinstance(invoked_fact, _ReflectionCallableFact):
            return self._callable_return_fact(invoked_fact, node)
        if isinstance(invoked_fact, _ReflectionItemGetterFact):
            target = self._call_argument(node, 0, "obj", "object")
            if target is None:
                return _REFLECTION_UNRESOLVED
            target_fact = self._reflection_fact(target)
            selected = [
                self._container_item_fact(target_fact, key) or _REFLECTION_SAFE
                for key in invoked_fact.keys
            ]
            if len(selected) == 1:
                return selected[0]
            return _ReflectionSequenceFact(tuple(selected))
        if isinstance(invoked_fact, _ReflectionClassFact):
            attributes = dict(invoked_fact.attributes)
            attributes.update(dict(self._object_constructor_fact(node).attributes))
            return _ReflectionClassFact(tuple(sorted(attributes.items())))
        if invoked_fact == _REFLECTION_FORWARDER:
            names = [
                text
                for argument in (
                    *node.args,
                    *(keyword.value for keyword in node.keywords),
                )
                if (text := _constant_text(argument)) is not None
            ]
            if _SYNC_CALL_NAME in names:
                return _REFLECTION_TARGET
            return _REFLECTION_SAFE if names else _REFLECTION_UNRESOLVED
        if invoked_fact == _REFLECTION_NAMESPACE_GET:
            return self._reflection_name_fact(
                self._call_argument(node, 0, "key", "name")
            )
        if (
            isinstance(node.func, ast.Name)
            and node.func.id in {"staticmethod", "classmethod"}
            and self._lookup_reflection_alias(node.func.id) is None
        ):
            wrapped = self._call_argument(node, 0, "function", "func")
            return (
                self._reflection_fact(wrapped)
                if wrapped is not None
                else _REFLECTION_UNRESOLVED
            )
        if isinstance(node.func, ast.Attribute) and node.func.attr in {"get", "pop"}:
            mapping_fact = self._mapping_lookup_fact(
                node.func.value,
                self._call_argument(node, 0, "key", "name"),
            )
            if mapping_fact is not None:
                return mapping_fact
            if self._reflection_fact(node.func.value) == _REFLECTION_NAMESPACE:
                return self._reflection_name_fact(
                    self._call_argument(node, 0, "key", "name")
                )

        kind = self._reflection_call_kind(node)
        if kind is None:
            unknown_selector = self._unknown_sync_selector_result_fact(
                node,
                invoked_fact,
            )
            if unknown_selector is not None:
                return unknown_selector
        if kind == "dict":
            return self._mapping_constructor_fact(node)
        if kind in {"list", "tuple"}:
            source = self._call_argument(node, 0, "iterable")
            if source is None:
                return _ReflectionSequenceFact(())
            source_fact = self._reflection_fact(source)
            if isinstance(source_fact, _ReflectionSequenceFact):
                return source_fact
            if isinstance(source_fact, _ReflectionIteratorFact):
                return _ReflectionSequenceFact(source_fact.elements)
            return _ReflectionSequenceFact(())
        if kind == "iter":
            source = self._call_argument(node, 0, "iterable")
            if source is None:
                return _ReflectionIteratorFact(())
            source_fact = self._reflection_fact(source)
            if isinstance(source_fact, _ReflectionSequenceFact):
                return _ReflectionIteratorFact(source_fact.elements)
            if isinstance(source_fact, _ReflectionIteratorFact):
                return source_fact
            return _ReflectionIteratorFact(())
        if kind == "next":
            iterator = self._call_argument(node, 0, "iterator")
            if iterator is None:
                return _REFLECTION_UNRESOLVED
            iterator_fact = self._reflection_fact(iterator)
            if isinstance(iterator_fact, _ReflectionIteratorFact):
                if iterator_fact.elements:
                    return iterator_fact.elements[0]
                default = self._call_argument(node, 1, "default")
                return (
                    self._reflection_fact(default)
                    if default is not None
                    else _REFLECTION_SAFE
                )
            return _REFLECTION_SAFE
        if kind == "SimpleNamespace":
            return self._object_constructor_fact(node)
        if kind == "setattr":
            return _REFLECTION_SAFE
        if kind == "itemgetter":
            if not node.args:
                return _REFLECTION_UNRESOLVED
            return _ReflectionItemGetterFact(
                tuple(
                    self._constant_item_key(
                        argument.value
                        if isinstance(argument, ast.Starred)
                        else argument
                    )
                    for argument in node.args
                )
            )
        if kind == "vars":
            return _REFLECTION_NAMESPACE
        if kind == "partial":
            wrapped = self._call_argument(node, 0, "func")
            wrapped_fact = (
                self._reflection_fact(wrapped) if wrapped is not None else None
            )
            if _reflection_primitive_kind(wrapped_fact) is not None or wrapped_fact in {
                _REFLECTION_NAMESPACE,
                _REFLECTION_TARGET,
                _REFLECTION_UNRESOLVED,
            }:
                return _REFLECTION_UNRESOLVED
            return _REFLECTION_SAFE
        if kind == "getattr":
            return self._reflection_name_fact(
                self._call_argument(node, 1, "name", "attribute")
            )
        if kind == "object-getattribute":
            return self._reflection_name_fact(
                self._call_argument(node, 1, "name", "attribute")
            )
        if kind == "bound-getattribute":
            return self._reflection_name_fact(
                self._call_argument(node, 0, "name", "attribute")
            )
        if kind == "methodcaller":
            return self._reflection_name_fact(self._call_argument(node, 0, "name"))
        if kind != "attrgetter":
            return None

        if not node.args:
            return _REFLECTION_UNRESOLVED
        facts = [
            self._reflection_name_fact(
                argument.value if isinstance(argument, ast.Starred) else argument
            )
            for argument in node.args
        ]
        if _REFLECTION_TARGET in facts:
            return _REFLECTION_TARGET
        if _REFLECTION_UNRESOLVED in facts:
            return _REFLECTION_UNRESOLVED
        return _REFLECTION_SAFE

    def _is_vars_call(self, node: ast.expr) -> bool:
        return isinstance(node, ast.Call) and self._reflection_call_kind(node) == "vars"

    def _mapping_lookup_fact(
        self,
        mapping: ast.expr,
        key: ast.expr | None,
    ) -> _ReflectionFact | None:
        if key is None:
            return None
        item_key = self._constant_item_key(key)
        key_text = item_key if isinstance(item_key, str) else None
        mapping_fact = self._reflection_fact(mapping)
        if isinstance(mapping_fact, _ReflectionMappingFact) and key_text is not None:
            matches = [
                fact
                for candidate, fact in mapping_fact.entries
                if candidate == key_text
            ]
            if len(matches) == 1:
                return matches[0]
            if matches or any(
                candidate is None for candidate, _ in mapping_fact.entries
            ):
                return _REFLECTION_UNRESOLVED
            return _REFLECTION_SAFE
        if isinstance(mapping, ast.Attribute) and mapping.attr == "__dict__":
            owner_fact = self._reflection_fact(mapping.value)
            if owner_fact == _REFLECTION_MODULE_BUILTINS:
                if key_text in {"getattr", "vars"}:
                    return _reflection_primitive_fact(key_text)
                return (
                    _REFLECTION_SAFE if key_text is not None else _REFLECTION_UNRESOLVED
                )
        return None

    def _reflection_subscript_fact(self, node: ast.Subscript) -> _ReflectionFact | None:
        sequence_fact = self._reflection_fact(node.value)
        index = self._constant_item_key(node.slice)
        if isinstance(sequence_fact, _ReflectionSequenceFact) and isinstance(
            index, int
        ):
            if -len(sequence_fact.elements) <= index < len(sequence_fact.elements):
                return sequence_fact.elements[index]
            return _REFLECTION_UNRESOLVED
        mapping_fact = self._mapping_lookup_fact(node.value, node.slice)
        if mapping_fact is not None:
            return mapping_fact
        if (
            self._is_vars_call(node.value)
            or self._reflection_fact(node.value) == _REFLECTION_NAMESPACE
            or (isinstance(node.value, ast.Attribute) and node.value.attr == "__dict__")
        ):
            return self._reflection_name_fact(node.slice)
        return None

    def _reflection_fact(self, node: ast.expr) -> _ReflectionFact:
        primitive = self._reflection_expression_kind(node)
        if primitive is not None:
            return _reflection_primitive_fact(primitive)
        if isinstance(node, ast.Constant) and (
            isinstance(node.value, str)
            or (isinstance(node.value, int) and not isinstance(node.value, bool))
        ):
            return _ReflectionLiteralFact(node.value)
        if isinstance(node, ast.Name):
            return self._lookup_reflection_alias(node.id) or _REFLECTION_SAFE
        if isinstance(node, ast.Attribute):
            owner_fact = self._reflection_fact(node.value)
            if node.attr == "__dict__":
                return _REFLECTION_NAMESPACE
            if node.attr == "get" and owner_fact == _REFLECTION_NAMESPACE:
                return _REFLECTION_NAMESPACE_GET
            owner_primitive = _reflection_primitive_kind(owner_fact)
            unbound_operations = {
                "dict": {
                    "__getitem__",
                    "__setitem__",
                    "get",
                    "pop",
                    "setdefault",
                    "update",
                },
                "list": {
                    "__getitem__",
                    "__setitem__",
                    "append",
                    "extend",
                    "insert",
                    "pop",
                },
            }
            if node.attr in unbound_operations.get(owner_primitive or "", set()):
                return _ReflectionOperationFact(None, node.attr, None, 0)
            owner_name = node.value.id if isinstance(node.value, ast.Name) else None
            if isinstance(owner_fact, _ReflectionMappingFact) and node.attr in {
                "__getitem__",
                "get",
                "pop",
                "setdefault",
                "update",
            }:
                return _ReflectionOperationFact(
                    owner_fact,
                    node.attr,
                    owner_name,
                )
            if isinstance(owner_fact, _ReflectionSequenceFact) and node.attr in {
                "append",
                "extend",
                "insert",
            }:
                return _ReflectionOperationFact(
                    owner_fact,
                    node.attr,
                    owner_name,
                )
            if (
                isinstance(owner_fact, _ReflectionClassFact)
                and node.attr == "__setattr__"
            ):
                return _ReflectionOperationFact(
                    owner_fact,
                    node.attr,
                    owner_name,
                )
            if isinstance(owner_fact, _ReflectionClassFact):
                return dict(owner_fact.attributes).get(node.attr, _REFLECTION_SAFE)
            if node.attr == "__call__":
                return owner_fact
        if isinstance(node, ast.Call):
            constructor_fact = self._reflection_constructor_fact(node)
            if constructor_fact is not None:
                return constructor_fact
            return self._reflection_fact(node.func)
        if isinstance(node, ast.Subscript):
            return self._reflection_subscript_fact(node) or _REFLECTION_SAFE
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            return _ReflectionSequenceFact(
                tuple(self._reflection_fact(element) for element in node.elts)
            )
        if isinstance(node, ast.Dict):
            mapping_fact = _ReflectionMappingFact(())
            for key, value in zip(node.keys, node.values, strict=True):
                value_fact = self._reflection_fact(value)
                if key is None and isinstance(value_fact, _ReflectionMappingFact):
                    mapping_fact = self._updated_mapping_fact(
                        mapping_fact,
                        list(value_fact.entries),
                    )
                    continue
                item_key = self._constant_item_key(key) if key is not None else None
                mapping_fact = self._updated_mapping_fact(
                    mapping_fact,
                    [
                        (
                            item_key if isinstance(item_key, str) else None,
                            value_fact,
                        )
                    ],
                )
            return mapping_fact
        if isinstance(node, ast.BinOp):
            return self._binary_reflection_fact(
                self._reflection_fact(node.left),
                node.op,
                self._reflection_fact(node.right),
            )
        if isinstance(node, ast.IfExp):
            return self._merge_reflection_facts(
                [
                    self._reflection_fact(node.body),
                    self._reflection_fact(node.orelse),
                ]
            )
        if isinstance(node, ast.BoolOp):
            return self._merge_reflection_facts(
                [self._reflection_fact(value) for value in node.values]
            )
        if isinstance(node, ast.Lambda):
            return self._callable_fact(node)
        return _REFLECTION_SAFE

    def _record_reflection_violation(self, kind: str) -> None:
        violation = (self._owner, kind)
        if violation not in self.reflection_violations:
            self.reflection_violations.append(violation)

    @staticmethod
    def _bound_target_names(target: ast.expr) -> tuple[str, ...]:
        if isinstance(target, ast.Name):
            return (target.id,)
        if isinstance(target, ast.Starred):
            return _SyncCallsiteVisitor._bound_target_names(target.value)
        if isinstance(target, (ast.List, ast.Tuple)):
            return tuple(
                name
                for element in target.elts
                for name in _SyncCallsiteVisitor._bound_target_names(element)
            )
        return ()

    def _bind_reflection_alias(
        self,
        target: ast.expr,
        fact: _ReflectionFact,
    ) -> None:
        if isinstance(target, ast.Starred):
            self._bind_reflection_alias(target.value, fact)
            return
        if isinstance(target, (ast.List, ast.Tuple)):
            if isinstance(fact, _ReflectionSequenceFact):
                starred = [
                    index
                    for index, element in enumerate(target.elts)
                    if isinstance(element, ast.Starred)
                ]
                if not starred and len(target.elts) == len(fact.elements):
                    for element, element_fact in zip(
                        target.elts,
                        fact.elements,
                        strict=True,
                    ):
                        self._bind_reflection_alias(element, element_fact)
                    return
                if len(starred) == 1 and len(fact.elements) >= len(target.elts) - 1:
                    star_index = starred[0]
                    suffix_size = len(target.elts) - star_index - 1
                    for element, element_fact in zip(
                        target.elts[:star_index],
                        fact.elements[:star_index],
                        strict=True,
                    ):
                        self._bind_reflection_alias(element, element_fact)
                    middle_end = len(fact.elements) - suffix_size
                    star_target = target.elts[star_index]
                    assert isinstance(star_target, ast.Starred)
                    self._bind_reflection_alias(
                        star_target.value,
                        _ReflectionSequenceFact(fact.elements[star_index:middle_end]),
                    )
                    if suffix_size:
                        for element, element_fact in zip(
                            target.elts[-suffix_size:],
                            fact.elements[-suffix_size:],
                            strict=True,
                        ):
                            self._bind_reflection_alias(element, element_fact)
                    return
            for element in target.elts:
                self._bind_reflection_alias(element, fact)
            return
        if isinstance(target, ast.Subscript):
            self._bind_mutated_subscript(target, fact)
            return
        if isinstance(target, ast.Attribute):
            self._bind_mutated_attribute(target, fact)
            return
        if not isinstance(target, ast.Name):
            return
        self._reflection_aliases[-1][target.id] = fact
        if self._owner == ("module",):
            self._module_functions.pop(target.id, None)

    def _replace_bound_alias(self, name: str, fact: _ReflectionFact) -> None:
        for aliases in reversed(self._reflection_aliases):
            if name in aliases:
                aliases[name] = fact
                return

    def _bind_mutated_subscript(
        self,
        target: ast.Subscript,
        fact: _ReflectionFact,
    ) -> None:
        if not isinstance(target.value, ast.Name):
            return
        owner_fact = self._lookup_reflection_alias(target.value.id)
        key = self._constant_item_key(target.slice)
        if isinstance(owner_fact, _ReflectionMappingFact):
            entries = list(owner_fact.entries)
            if isinstance(key, str):
                entries = [
                    (candidate, value)
                    for candidate, value in entries
                    if candidate != key
                ]
            entries.append((key if isinstance(key, str) else None, fact))
            self._replace_bound_alias(
                target.value.id,
                _ReflectionMappingFact(tuple(entries)),
            )
            return
        if not isinstance(owner_fact, _ReflectionSequenceFact) or not isinstance(
            key, int
        ):
            return
        if not (-len(owner_fact.elements) <= key < len(owner_fact.elements)):
            return
        elements = list(owner_fact.elements)
        elements[key] = fact
        self._replace_bound_alias(
            target.value.id,
            _ReflectionSequenceFact(tuple(elements)),
        )

    def _bind_mutated_attribute(
        self,
        target: ast.Attribute,
        fact: _ReflectionFact,
    ) -> None:
        if not isinstance(target.value, ast.Name):
            return
        owner_fact = self._lookup_reflection_alias(target.value.id)
        if not isinstance(owner_fact, _ReflectionClassFact):
            return
        attributes = dict(owner_fact.attributes)
        attributes[target.attr] = fact
        self._replace_bound_alias(
            target.value.id,
            _ReflectionClassFact(tuple(sorted(attributes.items()))),
        )

    def _mapping_update_entries_from_call(
        self,
        node: ast.Call,
        *,
        argument_offset: int = 0,
    ) -> list[tuple[str | None, _ReflectionFact]]:
        entries: list[tuple[str | None, _ReflectionFact]] = []
        if len(node.args) > argument_offset:
            entries.extend(
                self._mapping_entries_from_iterable(node.args[argument_offset])
            )
        for keyword in node.keywords:
            value_fact = self._reflection_fact(keyword.value)
            if keyword.arg is None and isinstance(value_fact, _ReflectionMappingFact):
                entries.extend(value_fact.entries)
            else:
                entries.append((keyword.arg, value_fact))
        return entries

    def _apply_operation_mutation(
        self,
        operation: _ReflectionOperationFact,
        node: ast.Call,
    ) -> None:
        owner_name = self._operation_owner_name(operation, node)
        if owner_name is None:
            return
        owner = self._operation_owner_fact(operation, node)
        if isinstance(owner, _ReflectionMappingFact):
            if operation.operation == "update":
                self._replace_bound_alias(
                    owner_name,
                    self._updated_mapping_fact(
                        owner,
                        self._mapping_update_entries_from_call(
                            node,
                            argument_offset=(
                                1 if operation.owner_argument is not None else 0
                            ),
                        ),
                    ),
                )
                return
            key = self._constant_item_key(
                self._operation_argument(operation, node, 0, "key", "index")
            )
            if operation.operation == "__setitem__":
                value = self._operation_argument(operation, node, 1, "value")
                if value is not None:
                    self._replace_bound_alias(
                        owner_name,
                        self._updated_mapping_fact(
                            owner,
                            [
                                (
                                    key if isinstance(key, str) else None,
                                    self._reflection_fact(value),
                                )
                            ],
                        ),
                    )
                return
            if operation.operation == "setdefault":
                present, _current = self._mapping_item_state(owner, key)
                if present is False:
                    default = self._operation_argument(
                        operation,
                        node,
                        1,
                        "default",
                    )
                    self._replace_bound_alias(
                        owner_name,
                        self._updated_mapping_fact(
                            owner,
                            [
                                (
                                    key if isinstance(key, str) else None,
                                    self._reflection_fact(default)
                                    if default is not None
                                    else _REFLECTION_SAFE,
                                )
                            ],
                        ),
                    )
                return
            if operation.operation == "pop" and isinstance(key, str):
                self._replace_bound_alias(
                    owner_name,
                    _ReflectionMappingFact(
                        tuple(
                            (candidate, fact)
                            for candidate, fact in owner.entries
                            if candidate != key
                        )
                    ),
                )
                return
        if isinstance(owner, _ReflectionSequenceFact):
            elements = list(owner.elements)
            if operation.operation == "append":
                value = self._operation_argument(operation, node, 0, "object")
                if value is not None:
                    elements.append(self._reflection_fact(value))
            elif operation.operation == "extend":
                value = self._operation_argument(operation, node, 0, "iterable")
                value_fact = self._reflection_fact(value) if value is not None else None
                if isinstance(value_fact, _ReflectionSequenceFact):
                    elements.extend(value_fact.elements)
                elif isinstance(value_fact, _ReflectionIteratorFact):
                    elements.extend(value_fact.elements)
            elif operation.operation == "insert":
                index = self._constant_item_key(
                    self._operation_argument(operation, node, 0, "index")
                )
                value = self._operation_argument(operation, node, 1, "object")
                if value is not None:
                    value_fact = self._reflection_fact(value)
                    if isinstance(index, int):
                        bounded_index = max(
                            0,
                            min(
                                index if index >= 0 else len(elements) + index,
                                len(elements),
                            ),
                        )
                        elements.insert(bounded_index, value_fact)
                    else:
                        elements.append(value_fact)
            elif operation.operation == "__setitem__":
                index = self._constant_item_key(
                    self._operation_argument(operation, node, 0, "index")
                )
                value = self._operation_argument(operation, node, 1, "value")
                if (
                    isinstance(index, int)
                    and -len(elements) <= index < len(elements)
                    and value is not None
                ):
                    elements[index] = self._reflection_fact(value)
            else:
                return
            self._replace_bound_alias(
                owner_name,
                _ReflectionSequenceFact(tuple(elements)),
            )
            return
        if (
            isinstance(owner, _ReflectionClassFact)
            and operation.operation == "__setattr__"
        ):
            name = self._constant_item_key(self._call_argument(node, 0, "name"))
            value = self._call_argument(node, 1, "value")
            if not isinstance(name, str) or value is None:
                return
            attributes = dict(owner.attributes)
            attributes[name] = self._reflection_fact(value)
            self._replace_bound_alias(
                owner_name,
                _ReflectionClassFact(tuple(sorted(attributes.items()))),
            )

    def _apply_builtin_setattr_mutation(self, node: ast.Call) -> None:
        owner = self._call_argument(node, 0, "object", "obj")
        name = self._constant_item_key(self._call_argument(node, 1, "name"))
        value = self._call_argument(node, 2, "value")
        if (
            not isinstance(owner, ast.Name)
            or not isinstance(name, str)
            or value is None
        ):
            return
        owner_fact = self._lookup_reflection_alias(owner.id)
        if not isinstance(owner_fact, _ReflectionClassFact):
            return
        attributes = dict(owner_fact.attributes)
        attributes[name] = self._reflection_fact(value)
        self._replace_bound_alias(
            owner.id,
            _ReflectionClassFact(tuple(sorted(attributes.items()))),
        )

    def _apply_call_mutation(
        self,
        node: ast.Call,
        invoked_fact: _ReflectionFact,
    ) -> None:
        operation = (
            invoked_fact
            if isinstance(invoked_fact, _ReflectionOperationFact)
            else self._unbound_operation_fact(invoked_fact)
        )
        if operation is not None:
            self._apply_operation_mutation(operation, node)
            return
        if self._reflection_call_kind(node) == "setattr":
            self._apply_builtin_setattr_mutation(node)

    def _module_function_aliases(
        self,
        target: ast.expr,
        value: ast.expr,
    ) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
        if self._owner != ("module",):
            return {}
        if isinstance(target, ast.Name) and isinstance(value, ast.Name):
            function = self._module_functions.get(value.id)
            return {target.id: function} if function is not None else {}
        if (
            isinstance(target, (ast.List, ast.Tuple))
            and isinstance(value, (ast.List, ast.Tuple))
            and len(target.elts) == len(value.elts)
        ):
            aliases: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
            for element, element_value in zip(target.elts, value.elts, strict=True):
                aliases.update(self._module_function_aliases(element, element_value))
            return aliases
        return {}

    def _replay_module_function_at_call(self, node: ast.Call) -> None:
        if not isinstance(node.func, ast.Name):
            return
        if self._owner != ("module",) and self._allow_module_late_bindings:
            return
        function = self._module_functions.get(node.func.id)
        if function is None or id(function) in self._replayed_module_functions:
            return

        module_aliases = self._reflection_aliases[0]
        had_binding = node.func.id in module_aliases
        saved_binding = module_aliases.get(node.func.id)
        saved_late_binding_mode = self._allow_module_late_bindings
        self._allow_module_late_bindings = False
        self._replayed_module_functions.add(id(function))
        try:
            self._visit_function(
                function,
                scope_kind=(
                    "async-function"
                    if isinstance(function, ast.AsyncFunctionDef)
                    else "function"
                ),
            )
        finally:
            self._replayed_module_functions.remove(id(function))
            self._allow_module_late_bindings = saved_late_binding_mode
            if had_binding:
                assert saved_binding is not None
                module_aliases[node.func.id] = saved_binding
            else:
                module_aliases.pop(node.func.id, None)

    def visit_Call(self, node: ast.Call) -> None:
        self.all_calls.append((self._owner, node))
        self._replay_module_function_at_call(node)
        if self._is_direct_sync_call(node):
            self.calls.append((self._owner, node))
            if isinstance(node.func, ast.Attribute):
                self.visit(node.func.value)
            for argument in node.args:
                self.visit(argument)
            for keyword in node.keywords:
                self.visit(keyword.value)
            return

        invoked_fact = self._reflection_fact(node.func)
        if invoked_fact in {_REFLECTION_TARGET, _REFLECTION_UNRESOLVED}:
            self._record_reflection_violation(invoked_fact)
        constructor_fact = self._reflection_constructor_fact(node)
        if constructor_fact == _REFLECTION_TARGET:
            self._record_reflection_violation(_REFLECTION_TARGET)
        self._apply_call_mutation(node, invoked_fact)
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if self._reflection_subscript_fact(node) == _REFLECTION_TARGET:
            self._record_reflection_violation(_REFLECTION_TARGET)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.assignments.append((self._owner, node))
        fact = self._reflection_fact(node.value)
        self.visit(node.value)
        for target in node.targets:
            function_aliases = self._module_function_aliases(target, node.value)
            self.visit(target)
            self._bind_reflection_alias(target, fact)
            self._module_functions.update(function_aliases)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.visit(node.annotation)
        fact = _REFLECTION_SAFE
        if node.value is not None:
            fact = self._reflection_fact(node.value)
            self.visit(node.value)
        function_aliases = (
            self._module_function_aliases(node.target, node.value)
            if node.value is not None
            else {}
        )
        self.visit(node.target)
        self._bind_reflection_alias(node.target, fact)
        self._module_functions.update(function_aliases)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        fact = self._reflection_fact(node.value)
        self.visit(node.value)
        function_aliases = self._module_function_aliases(node.target, node.value)
        self.visit(node.target)
        self._bind_reflection_alias(node.target, fact)
        self._module_functions.update(function_aliases)

    def visit_Return(self, node: ast.Return) -> None:
        if node.value is None:
            fact = _REFLECTION_SAFE
        else:
            fact = self._reflection_fact(node.value)
            self.visit(node.value)
        if self._return_facts:
            self._return_facts[-1].append(fact)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        left_fact = self._reflection_fact(node.target)
        right_fact = self._reflection_fact(node.value)
        self.visit(node.target)
        self.visit(node.value)
        self._bind_reflection_alias(
            node.target,
            self._binary_reflection_fact(left_fact, node.op, right_fact),
        )

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr == _SYNC_CALL_NAME:
            self.indirect_references.append((self._owner, "attribute-alias"))
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id == _SYNC_CALL_NAME:
            self.indirect_references.append((self._owner, "name-alias"))

    def visit_Import(self, node: ast.Import) -> None:
        if any(
            alias.name.rsplit(".", 1)[-1] == _SYNC_CALL_NAME for alias in node.names
        ):
            self.indirect_references.append((self._owner, "import-alias"))
        module_facts = {
            "builtins": _REFLECTION_MODULE_BUILTINS,
            "functools": _REFLECTION_MODULE_FUNCTOOLS,
            "operator": _REFLECTION_MODULE_OPERATOR,
            "types": _REFLECTION_MODULE_TYPES,
        }
        for alias in node.names:
            root = alias.name.split(".", 1)[0]
            binding = alias.asname or root
            self._reflection_aliases[-1][binding] = module_facts.get(
                root,
                _REFLECTION_SAFE,
            )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if any(alias.name == _SYNC_CALL_NAME for alias in node.names):
            self.indirect_references.append((self._owner, "import-alias"))
        primitive_modules = {
            "builtins": {
                "dict",
                "getattr",
                "iter",
                "list",
                "next",
                "setattr",
                "tuple",
                "vars",
            },
            "functools": {"partial"},
            "operator": {
                "attrgetter",
                "getitem",
                "itemgetter",
                "methodcaller",
                "setitem",
            },
            "types": {"SimpleNamespace"},
        }
        approved = primitive_modules.get(node.module or "", set())
        for alias in node.names:
            if alias.name == "*":
                continue
            binding = alias.asname or alias.name
            self._reflection_aliases[-1][binding] = (
                _reflection_primitive_fact(alias.name)
                if alias.name in approved
                else _REFLECTION_SAFE
            )


def _normalized_call_shape(call: ast.Call) -> str:
    return ast.dump(call, include_attributes=False)


def _cursor_argument(call: ast.Call) -> ast.expr | None:
    if any(isinstance(argument, ast.Starred) for argument in call.args):
        return None
    bound = {
        parameter: argument
        for parameter, argument in zip(
            _SYNC_POSITIONAL_PARAMETERS,
            call.args,
            strict=False,
        )
    }
    for keyword in call.keywords:
        if keyword.arg in {"cursor", "sync_state"}:
            bound["cursor"] = keyword.value
    return bound.get("cursor")


def _is_static_reset_sentinel(node: ast.expr) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        value = node.value
    elif isinstance(node, ast.Name):
        value = node.id
    elif isinstance(node, ast.Attribute):
        value = node.attr
    else:
        return False
    tokens = value.lower().replace("-", "_").replace(".", "_").split("_")
    return "reset" in tokens


def _static_cursor_violation(cursor: ast.expr) -> str | None:
    if isinstance(cursor, ast.Constant):
        if cursor.value is None:
            return "none"
        if isinstance(cursor.value, str) and not cursor.value.strip():
            return "empty"
    if _is_static_reset_sentinel(cursor):
        return "reset"
    return None


def _reviewed_non_sync_reflection_owners(
    tree: ast.Module,
    *,
    relative_path: str,
    visitor: _SyncCallsiteVisitor,
) -> set[tuple[str, ...]]:
    expected_file_hash = _REVIEWED_NON_SYNC_REFLECTION_FILE_AST_SHA256.get(
        relative_path
    )
    if expected_file_hash is None:
        return set()
    if _normalized_ast_sha256(tree) != expected_file_hash:
        return set()

    approved: set[tuple[str, ...]] = set()
    for (
        key,
        expected_assignments,
    ) in _REVIEWED_NON_SYNC_REFLECTION_ASSIGNMENT_SHAPES.items():
        path, owner = key
        if path != relative_path:
            continue
        binding_names = _REVIEWED_NON_SYNC_REFLECTION_BINDINGS[key]
        actual_assignments = Counter(
            _normalized_ast_dump(assignment)
            for assignment_owner, assignment in visitor.assignments
            if assignment_owner == owner
            and any(
                binding_names.intersection(
                    visitor._bound_target_names(target)  # noqa: SLF001
                )
                for target in assignment.targets
            )
        )
        call_paths = _REVIEWED_NON_SYNC_REFLECTION_CALL_PATHS[key]
        actual_calls = Counter(
            _normalized_ast_dump(call)
            for call_owner, call in visitor.all_calls
            if call_owner == owner
            and visitor._expression_path(call.func)  # noqa: SLF001
            in call_paths
        )
        if actual_assignments == Counter(
            expected_assignments
        ) and actual_calls == Counter(_REVIEWED_NON_SYNC_REFLECTION_CALL_SHAPES[key]):
            approved.add(owner)
    return approved


def _sync_callsite_violations(source: str, *, relative_path: str) -> list[str]:
    tree = ast.parse(source, filename=relative_path)
    expected_file_hash = _REVIEWED_NON_SYNC_REFLECTION_FILE_AST_SHA256.get(
        relative_path
    )
    reviewed_return_owners = frozenset(
        owner
        for path, owner in _REVIEWED_NON_SYNC_REFLECTION_BINDINGS
        if path == relative_path
        and expected_file_hash is not None
        and _normalized_ast_sha256(tree) == expected_file_hash
    )
    visitor = _SyncCallsiteVisitor(
        reviewed_return_owners=reviewed_return_owners,
    )
    visitor.visit(tree)
    reviewed_non_sync_owners = _reviewed_non_sync_reflection_owners(
        tree,
        relative_path=relative_path,
        visitor=visitor,
    )
    violations = [
        f"indirect_reference:{'/'.join(owner)}:{kind}"
        for owner, kind in visitor.indirect_references
    ]
    violations.extend(
        f"{kind}:{'/'.join(owner)}"
        for owner, kind in visitor.reflection_violations
        if not (kind == _REFLECTION_UNRESOLVED and owner in reviewed_non_sync_owners)
    )
    exact_occurrences = {
        key: 0 for key in _ALLOWED_SYNC_CALLSITE_SHAPES if key[0] == relative_path
    }
    for owner, call in visitor.calls:
        key = (relative_path, owner)
        cursor = _cursor_argument(call)
        if cursor is None:
            violations.append(f"cursor_omitted:{'/'.join(owner)}")
        else:
            cursor_violation = _static_cursor_violation(cursor)
            if cursor_violation is not None:
                violations.append(f"cursor_{cursor_violation}:{'/'.join(owner)}")
        expected_shape = _ALLOWED_SYNC_CALLSITE_SHAPES.get(key)
        if expected_shape is None:
            violations.append(f"unapproved_call:{'/'.join(owner)}")
            continue
        if _normalized_call_shape(call) != expected_shape:
            violations.append(f"call_shape:{'/'.join(owner)}")
            continue
        exact_occurrences[key] += 1
    for (_path, owner), count in exact_occurrences.items():
        if count != 1:
            violations.append(f"exact_occurrences:{'/'.join(owner)}:{count}")
    return violations


def _project_sync_callsite_violations() -> list[str]:
    violations: list[str] = []
    expected_paths = {path for path, _owner in _ALLOWED_SYNC_CALLSITE_SHAPES}
    for relative_path in sorted(expected_paths):
        if not (PROJECT_ROOT / relative_path).is_file():
            violations.append(f"missing_source:{relative_path}")
    for source_path in sorted((PROJECT_ROOT / "src").rglob("*.py")):
        relative_path = source_path.relative_to(PROJECT_ROOT).as_posix()
        violations.extend(
            f"{relative_path}:{violation}"
            for violation in _sync_callsite_violations(
                source_path.read_text(encoding="utf-8"),
                relative_path=relative_path,
            )
        )
    return violations


def test_sync_batch_exposes_only_frozen_v2_page_fields() -> None:
    assert [field.name for field in dataclasses.fields(SyncBatch)] == [
        "contract_version",
        "cursor",
        "changes",
        "includes_last",
    ]
    assert SyncBatch.__dataclass_params__.frozen is True
    assert SyncBatch.__slots__ == (
        "contract_version",
        "cursor",
        "changes",
        "includes_last",
    )
    batch = SyncBatch("exchange_sync_contract_v2", "cursor-1", (), False)
    assert not hasattr(batch, "__dict__")


def test_sync_client_has_no_permission_probe_or_test_transport_mutator() -> None:
    assert callable(getattr(ExchangeClient, "sync_emails", None))
    assert not hasattr(ExchangeClient, "validate_sync_permission")
    assert not hasattr(ExchangeClient, "replace_transport_for_test")


def test_sync_client_boundary_imports_no_policy_database_or_repository_layer() -> None:
    source_path = PROJECT_ROOT / "src/utils/exchange_api.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    forbidden_prefixes = {
        "src.db",
        "src.ingestion.policy",
        "src.ingestion.repository",
        "src.maintenance.checkpoint_repository",
        "src.utils.db_async",
        "src.utils.notification_policy",
        "psycopg",
        "sqlalchemy",
    }
    assert not any(
        imported_name == prefix or imported_name.startswith(f"{prefix}.")
        for imported_name in imported
        for prefix in forbidden_prefixes
    )


def test_sync_client_call_sites_match_exact_dormant_allowlist() -> None:
    assert _project_sync_callsite_violations() == []


@pytest.mark.parametrize(
    ("relative_path", "source"),
    (
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    return await self._page_client.sync_emails(
                        account_id,
                        scope.sync_folder,
                        expected.cursor,
                        self._page_limit,
                    )
            """,
        ),
        (
            "src/ingestion/cold_start.py",
            """
            async def _fetch_ordinary_page():
                return await client.sync_emails(
                    account_id,
                    sync_folder,
                    cursor,
                    limit,
                )
            """,
        ),
    ),
    ids=("ordinary-sync", "approved-boundary-apply"),
)
def test_sync_callsite_detector_accepts_only_frozen_shapes(
    relative_path: str,
    source: str,
) -> None:
    assert (
        _sync_callsite_violations(
            textwrap.dedent(source),
            relative_path=relative_path,
        )
        == []
    )


@pytest.mark.parametrize(
    ("relative_path", "source", "expected_violation"),
    (
        (
            "src/ingestion/rogue.py",
            """
            async def poll(client):
                return await client.sync_emails(account_id, folder, cursor, limit)
            """,
            "unapproved_call:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def another_owner(self):
                    return await self._page_client.sync_emails(
                        account_id,
                        scope.sync_folder,
                        expected.cursor,
                        self._page_limit,
                    )
            """,
            "unapproved_call:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    return await self._page_client.sync_emails(
                        account_id,
                        scope.sync_folder,
                        limit=self._page_limit,
                    )
            """,
            "cursor_omitted:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    return await self._page_client.sync_emails(
                        account_id,
                        scope.sync_folder,
                        None,
                        self._page_limit,
                    )
            """,
            "cursor_none:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    return await self._page_client.sync_emails(
                        account_id,
                        scope.sync_folder,
                        "",
                        self._page_limit,
                    )
            """,
            "cursor_empty:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    return await self._page_client.sync_emails(
                        account_id,
                        scope.sync_folder,
                        RESET_SENTINEL,
                        self._page_limit,
                    )
            """,
            "cursor_reset:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    return await self._page_client.sync_emails(
                        account_id=account_id,
                        folder=scope.sync_folder,
                        sync_state=expected.cursor,
                        limit=self._page_limit,
                    )
            """,
            "call_shape:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    async def nested():
                        return await self._page_client.sync_emails(
                            account_id,
                            scope.sync_folder,
                            expected.cursor,
                            self._page_limit,
                        )
                    return await nested()
            """,
            "unapproved_call:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    call = self._page_client.sync_emails
                    return await call(
                        account_id,
                        scope.sync_folder,
                        expected.cursor,
                        self._page_limit,
                    )
            """,
            "indirect_reference:",
        ),
        (
            "src/ingestion/rogue.py",
            """
            from transport import sync_emails as call

            async def poll():
                return await call(account_id, folder, cursor, limit)
            """,
            "indirect_reference:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    call = getattr(self._page_client, "sync_emails")
                    return await call(
                        account_id,
                        scope.sync_folder,
                        expected.cursor,
                        self._page_limit,
                    )
            """,
            "reflection_target:",
        ),
        (
            "src/ingestion/sync.py",
            """
            class SyncCoordinator:
                async def _run_locked(self):
                    await self._page_client.sync_emails(
                        account_id,
                        scope.sync_folder,
                        expected.cursor,
                        self._page_limit,
                    )
                    return await self._page_client.sync_emails(
                        account_id,
                        scope.sync_folder,
                        expected.cursor,
                        self._page_limit,
                    )
            """,
            "exact_occurrences:",
        ),
        (
            "src/ingestion/cold_start.py",
            """
            async def _fetch_ordinary_page():
                return await sync_emails(account_id, sync_folder, cursor, limit)
            """,
            "call_shape:",
        ),
    ),
    ids=(
        "third-file-call",
        "wrong-owner",
        "cursor-omitted",
        "cursor-none",
        "cursor-empty",
        "cursor-reset-sentinel",
        "keyword-arguments",
        "nested-owner",
        "attribute-alias",
        "import-alias",
        "reflective-alias",
        "duplicate-exact-call",
        "name-call",
    ),
)
def test_sync_callsite_detector_rejects_reasonable_bypasses(
    relative_path: str,
    source: str,
    expected_violation: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path=relative_path,
    )
    assert any(item.startswith(expected_violation) for item in violations), violations


@pytest.mark.parametrize(
    ("source", "expected_violation"),
    (
        (
            """
            async def poll(client):
                return await getattr(client, "sync_" + "emails")(
                    account_id,
                    folder,
                    cursor,
                    limit,
                )
            """,
            "reflection_target:",
        ),
        (
            """
            async def poll(client):
                return await getattr(client, f"{'sync_'}emails")(
                    account_id,
                    folder,
                    cursor,
                    limit,
                )
            """,
            "reflection_target:",
        ),
        (
            """
            async def poll(client):
                call = object.__getattribute__(client, "sync_emails")
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            async def poll(client):
                call = vars(client)["sync_emails"]
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            async def poll(client):
                call = client.__dict__["sync_emails"]
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            from operator import attrgetter

            async def poll(client):
                call = attrgetter("sync_emails")(client)
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            from operator import methodcaller

            async def poll(client):
                return await methodcaller(
                    "sync_emails",
                    account_id,
                    folder,
                    cursor,
                    limit,
                )(client)
            """,
            "reflection_target:",
        ),
        (
            """
            async def poll(client):
                reflected = getattr(client, "sync_" + "emails")
                call = reflected
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            async def poll(client, method_name):
                reflected = getattr(client, method_name)
                call = reflected
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_unresolved:",
        ),
    ),
    ids=(
        "getattr-folded-concatenation",
        "getattr-folded-f-string",
        "object-getattribute",
        "vars-subscript",
        "dict-subscript",
        "attrgetter-result-alias",
        "methodcaller",
        "reflection-result-assignment-alias",
        "reflection-unresolved-name-called",
    ),
)
def test_sync_callsite_detector_rejects_reflection_bypasses(
    source: str,
    expected_violation: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )
    assert any(item.startswith(expected_violation) for item in violations), violations


@pytest.mark.parametrize(
    ("source", "expected_violation"),
    (
        (
            """
            async def poll(client):
                reflect = getattr
                call = reflect(client, "sync_emails")
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            async def poll(client):
                reflect = object.__getattribute__
                call = reflect(client, "sync_emails")
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            import operator

            async def poll(client):
                reflect = operator.attrgetter
                call = reflect("sync_emails")(client)
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            async def poll(client):
                expose = vars
                call = expose(client)["sync_emails"]
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            async def poll(client):
                call = client.__dict__.get("sync_emails")
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            from functools import partial

            async def poll(client):
                reflect = partial(getattr, client)
                call = reflect("sync_emails")
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_",
        ),
        (
            """
            async def poll(client, method_name):
                reflect = lambda value, name: getattr(value, name)
                call = reflect(client, method_name)
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_unresolved:",
        ),
        (
            """
            from builtins import getattr as reflect

            async def poll(client):
                call = reflect(client, "sync_emails")
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
        (
            """
            from operator import attrgetter as reflect

            async def poll(client):
                call = reflect("sync_emails")(client)
                return await call(account_id, folder, cursor, limit)
            """,
            "reflection_target:",
        ),
    ),
    ids=(
        "assigned-getattr-primitive",
        "assigned-object-getattribute-primitive",
        "assigned-operator-attrgetter-primitive",
        "assigned-vars-primitive",
        "dunder-dict-get",
        "partial-getattr-forwarder",
        "lambda-getattr-forwarder",
        "builtins-getattr-import-alias",
        "operator-attrgetter-import-alias",
    ),
)
def test_sync_callsite_detector_rejects_aliased_reflection_primitives(
    source: str,
    expected_violation: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )
    assert any(item.startswith(expected_violation) for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            async def poll(client, reflect=getattr):
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="default-parameter",
        ),
        pytest.param(
            """
            async def poll(client, enabled):
                reflect = getattr if enabled else getattr
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="if-expression",
        ),
        pytest.param(
            """
            async def poll(client):
                reflect = (getattr,)[0]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="tuple-subscript",
        ),
        pytest.param(
            """
            async def poll(client):
                reflect = {"lookup": getattr}.get("lookup")
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="mapping-get",
        ),
        pytest.param(
            """
            import builtins

            async def poll(client):
                reflect = builtins.__dict__["getattr"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="builtins-dunder-dict",
        ),
    ),
)
def test_sync_callsite_detector_rejects_composed_reflection_primitive_aliases(
    source: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            async def poll(client):
                reflect = lambda value, name, primitive=getattr: primitive(value, name)
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="lambda-default-forwarder",
        ),
        pytest.param(
            """
            def reflect(value, name):
                return getattr(value, name)

            async def poll(client):
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="ordinary-function-forwarder",
        ),
    ),
)
def test_sync_callsite_detector_rejects_reflection_forwarders(source: str) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_") for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            async def poll(client):
                reflect = getattr or getattr
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="boolean-or",
        ),
        pytest.param(
            """
            async def poll(client):
                for reflect in (getattr,):
                    call = reflect(client, "sync_emails")
                    return await call(8, "INBOX", "cursor", 10)
            """,
            id="for-binding",
        ),
        pytest.param(
            """
            async def poll(client):
                calls = [
                    reflect(client, "sync_emails")
                    for reflect in (getattr,)
                ]
                return await calls[0](8, "INBOX", "cursor", 10)
            """,
            id="comprehension-binding",
        ),
        pytest.param(
            """
            async def poll(client):
                box = (getattr,)
                reflect = box[0]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="assigned-tuple",
        ),
        pytest.param(
            """
            async def poll(client):
                box = [getattr]
                reflect = box[0]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="assigned-list",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {"pick": getattr}
                reflect = box["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="assigned-dict",
        ),
        pytest.param(
            """
            async def poll(client):
                box = [{"pick": getattr}]
                call = box[0]["pick"](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="nested-assigned-containers",
        ),
        pytest.param(
            """
            async def poll(client):
                (reflect,) = (getattr,)
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="tuple-unpacking-assignment",
        ),
        pytest.param(
            """
            async def poll(client):
                for (reflect,) in ((getattr,),):
                    call = reflect(client, "sync_emails")
                    return await call(8, "INBOX", "cursor", 10)
            """,
            id="tuple-unpacking-for",
        ),
        pytest.param(
            """
            async def poll(client):
                calls = [
                    reflect(client, "sync_emails")
                    for (reflect,) in ((getattr,),)
                ]
                return await calls[0](8, "INBOX", "cursor", 10)
            """,
            id="tuple-unpacking-comprehension",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {**{"pick": getattr}}
                reflect = box["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="mapping-unpacking",
        ),
    ),
)
def test_sync_callsite_detector_propagates_reflection_through_control_flow_and_containers(
    source: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            async def poll(client):
                reflect = dict(pick=getattr)["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="dict-constructor",
        ),
        pytest.param(
            """
            from types import SimpleNamespace

            async def poll(client):
                reflect = SimpleNamespace(pick=getattr).pick
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="simple-namespace-constructor",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {}
                box["pick"] = getattr
                reflect = box["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="mapping-item-mutation",
        ),
        pytest.param(
            """
            async def poll(client):
                reflect = {"pick": getattr}.pop("pick")
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="mapping-pop",
        ),
        pytest.param(
            """
            async def poll(client):
                reflect = list((getattr,))[0]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="list-constructor",
        ),
        pytest.param(
            """
            async def poll(client):
                reflect = next(iter((getattr,)))
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="iterator-next",
        ),
        pytest.param(
            """
            async def poll(client):
                first, *rest = (None, getattr)
                reflect = rest[0]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="starred-rest-index",
        ),
        pytest.param(
            """
            from operator import itemgetter

            async def poll(client):
                reflect = itemgetter(0)((getattr,))
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="operator-itemgetter",
        ),
        pytest.param(
            """
            class Box:
                pass

            async def poll(client):
                box = Box()
                box.pick = getattr
                reflect = box.pick
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="instance-attribute-mutation",
        ),
    ),
)
def test_sync_callsite_detector_propagates_reflection_through_one_hop_values(
    source: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            import copy

            async def poll(client):
                reflect = copy.copy({"pick": getattr})["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="copy-module-copy",
        ),
        pytest.param(
            """
            from copy import deepcopy

            async def poll(client):
                reflect = deepcopy({"pick": getattr})["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="copy-deepcopy-import",
        ),
        pytest.param(
            """
            from types import MappingProxyType

            async def poll(client):
                reflect = MappingProxyType({"pick": getattr})["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="types-mapping-proxy",
        ),
        pytest.param(
            """
            from collections import ChainMap

            async def poll(client):
                reflect = ChainMap({"pick": getattr})["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="collections-chain-map",
        ),
        pytest.param(
            """
            from collections import UserDict

            async def poll(client):
                reflect = UserDict({"pick": getattr})["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="collections-user-dict",
        ),
        pytest.param(
            """
            import operator

            async def poll(client):
                reflect = operator.or_({}, {"pick": getattr})["pick"]
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="operator-or-mapping-union",
        ),
    ),
)
def test_sync_callsite_detector_fails_closed_for_unknown_sync_selector_results(
    source: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_unresolved:") for item in violations), (
        violations
    )


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            async def poll(client, choose):
                call = choose(client, selector="sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="selector-keyword",
        ),
        pytest.param(
            """
            async def poll(client, choose):
                call = choose(client, **{"name": "sync_emails"})
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="static-keyword-unpacking",
        ),
        pytest.param(
            """
            async def poll(client, choose):
                call = choose(*(client, "sync_emails"))
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="static-full-positional-unpacking",
        ),
        pytest.param(
            """
            async def poll(client, choose):
                call = choose(client, *("sync_emails",))
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="static-trailing-positional-unpacking",
        ),
    ),
)
def test_sync_callsite_detector_expands_static_unknown_selector_arguments(
    source: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_unresolved:") for item in violations), (
        violations
    )


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            async def poll(client):
                box = {}
                box.update(pick=getattr)
                call = box["pick"](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="mapping-update",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {}
                box.setdefault("pick", getattr)
                call = box["pick"](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="mapping-setdefault",
        ),
        pytest.param(
            """
            class Box:
                pass

            async def poll(client):
                box = Box()
                setattr(box, "pick", getattr)
                call = box.pick(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="setattr-mutation",
        ),
        pytest.param(
            """
            class Box:
                pass

            async def poll(client):
                box = Box()
                box.__setattr__("pick", getattr)
                call = box.pick(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="bound-dunder-setattr",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {} | {"pick": getattr}
                call = box["pick"](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="mapping-union",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {}
                box |= {"pick": getattr}
                call = box["pick"](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="mapping-in-place-union",
        ),
        pytest.param(
            """
            async def poll(client):
                box = [] + [getattr]
                call = box[0](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="list-concatenation",
        ),
        pytest.param(
            """
            async def poll(client):
                box = []
                box += [getattr]
                call = box[0](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="list-in-place-concatenation",
        ),
        pytest.param(
            """
            async def poll(client):
                box = []
                box.append(getattr)
                call = box[0](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="list-append-index",
        ),
        pytest.param(
            """
            async def poll(client):
                box = []
                box.append(getattr)
                call = box[-1](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="list-append-negative-index",
        ),
        pytest.param(
            """
            async def poll(client):
                box = []
                box.extend((getattr,))
                for reflect in box:
                    call = reflect(client, "sync_emails")
                    return await call(8, "INBOX", "cursor", 10)
            """,
            id="list-extend-for",
        ),
        pytest.param(
            """
            async def poll(client):
                box = []
                box.insert(0, getattr)
                reflect = next(iter(box))
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="list-insert-next-iter",
        ),
        pytest.param(
            """
            async def poll(client):
                box = dict((("pick", getattr),))
                call = box["pick"](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="mapping-pair-constructor",
        ),
    ),
)
def test_sync_callsite_detector_propagates_mutable_container_and_object_facts(
    source: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            async def poll(client):
                box = {"pick": getattr}
                fetch = box.get
                call = fetch("pick")(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="bound-mapping-get",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {"pick": getattr}
                fetch = box.pop
                call = fetch("pick")(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="bound-mapping-pop",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {"pick": getattr}
                fetch = box.__getitem__
                call = fetch("pick")(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="bound-mapping-dunder-getitem",
        ),
    ),
)
def test_sync_callsite_detector_propagates_bound_mapping_accessors(
    source: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            async def poll(client):
                box = {"pick": getattr}
                reflect = dict.get(box, "pick")
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="unbound-dict-get",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {"pick": getattr}
                reflect = dict.pop(box, "pick")
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="unbound-dict-pop",
        ),
        pytest.param(
            """
            async def poll(client):
                box = {"pick": getattr}
                reflect = dict.__getitem__(box, "pick")
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="unbound-dict-dunder-getitem",
        ),
        pytest.param(
            """
            from operator import getitem

            async def poll(client):
                box = {"pick": getattr}
                reflect = getitem(box, "pick")
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="operator-getitem",
        ),
        pytest.param(
            """
            import operator

            async def poll(client):
                box = {}
                operator.setitem(box, "pick", getattr)
                call = box["pick"](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="operator-setitem",
        ),
        pytest.param(
            """
            from operator import setitem

            async def poll(client):
                box = [None]
                setitem(box, 0, getattr)
                call = box[0](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="operator-setitem-sequence",
        ),
        pytest.param(
            """
            async def poll(client):
                box = []
                list.append(box, getattr)
                call = box[0](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="unbound-list-append",
        ),
        pytest.param(
            """
            async def poll(client):
                box = []
                list.extend(box, (getattr,))
                call = box[-1](client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="unbound-list-extend",
        ),
        pytest.param(
            """
            async def poll(client):
                box = []
                list.insert(box, 0, getattr)
                reflect = next(iter(box))
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="unbound-list-insert",
        ),
    ),
)
def test_sync_callsite_detector_propagates_unbound_container_operations(
    source: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            def identity(value):
                return value

            async def poll(client):
                reflect = identity(getattr)
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="identity-positional-argument",
        ),
        pytest.param(
            """
            def identity(value):
                return value

            async def poll(client):
                reflect = identity(value=getattr)
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="identity-keyword-argument",
        ),
        pytest.param(
            """
            def outer():
                def identity(value):
                    return value

                return identity

            async def poll(client):
                reflect = outer()(getattr)
                call = reflect(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="nested-returned-wrapper",
        ),
    ),
)
def test_sync_callsite_detector_binds_wrapper_call_arguments(source: str) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            def size(value):
                return len(value)

            def inspect_values():
                box = []
                box.append(len)
                return box[0]((1, 2, 3))
            """,
            id="safe-list-append",
        ),
        pytest.param(
            """
            def inspect_values():
                box = []
                box.extend((len,))
                return next(iter(box))((1, 2, 3))
            """,
            id="safe-list-extend",
        ),
        pytest.param(
            """
            def inspect_values():
                box = []
                box.insert(0, len)
                for size in box:
                    return size((1, 2, 3))
            """,
            id="safe-list-insert",
        ),
        pytest.param(
            """
            def inspect_values():
                box = dict((("size", len),))
                return box["size"]((1, 2, 3))
            """,
            id="safe-mapping-pair-constructor",
        ),
    ),
)
def test_sync_callsite_detector_allows_safe_values_through_mutation_paths(
    source: str,
) -> None:
    assert (
        _sync_callsite_violations(
            textwrap.dedent(source),
            relative_path="src/ingestion/benign.py",
        )
        == []
    )


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            def inspect_values(values):
                box = {"size": len}
                return dict.get(box, "size")(values)
            """,
            id="safe-unbound-dict-get",
        ),
        pytest.param(
            """
            from operator import getitem

            def inspect_values(values):
                box = {"size": len}
                return getitem(box, "size")(values)
            """,
            id="safe-operator-getitem",
        ),
        pytest.param(
            """
            def inspect_values(values):
                box = {"size": len}
                return dict.pop(box, "size")(values)
            """,
            id="safe-unbound-dict-pop",
        ),
        pytest.param(
            """
            def inspect_values(values):
                box = {"size": len}
                return dict.__getitem__(box, "size")(values)
            """,
            id="safe-unbound-dict-dunder-getitem",
        ),
        pytest.param(
            """
            from operator import setitem

            def inspect_values(values):
                box = {}
                setitem(box, "size", len)
                return box["size"](values)
            """,
            id="safe-operator-setitem",
        ),
        pytest.param(
            """
            from operator import setitem

            def inspect_values(values):
                box = [None]
                setitem(box, 0, len)
                return box[0](values)
            """,
            id="safe-operator-setitem-sequence",
        ),
        pytest.param(
            """
            def inspect_values(values):
                box = []
                list.append(box, len)
                return box[0](values)
            """,
            id="safe-unbound-list-append",
        ),
        pytest.param(
            """
            def inspect_values(values):
                box = []
                list.extend(box, (len,))
                return box[-1](values)
            """,
            id="safe-unbound-list-extend",
        ),
        pytest.param(
            """
            def inspect_values(values):
                box = []
                list.insert(box, 0, len)
                return next(iter(box))(values)
            """,
            id="safe-unbound-list-insert",
        ),
    ),
)
def test_sync_callsite_detector_allows_safe_unbound_container_operations(
    source: str,
) -> None:
    assert (
        _sync_callsite_violations(
            textwrap.dedent(source),
            relative_path="src/ingestion/benign.py",
        )
        == []
    )


@pytest.mark.parametrize(
    "source",
    (
        pytest.param(
            """
            async def poll(client):
                namespace = vars(client)
                fetch = namespace.get
                call = fetch("sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="vars-get-method-alias",
        ),
        pytest.param(
            """
            async def poll(client):
                namespace = client.__dict__
                fetch = namespace.get
                call = fetch("sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="dunder-dict-get-method-alias",
        ),
        pytest.param(
            """
            async def poll(client):
                call = getattr.__call__(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="getattr-dunder-call",
        ),
        pytest.param(
            """
            class Reflector:
                pick = staticmethod(
                    lambda value, name: getattr(value, name)
                )

            async def poll(client):
                call = Reflector.pick(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """,
            id="staticmethod-class-wrapper",
        ),
    ),
)
def test_sync_callsite_detector_propagates_reflection_through_callable_wrappers(
    source: str,
) -> None:
    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


def test_sync_callsite_detector_uses_module_bindings_at_early_call_time() -> None:
    source = """
    import asyncio

    async def poll(client):
        call = getattr(client, "sync_emails")
        return await call(8, "INBOX", "cursor", 10)

    result = asyncio.run(poll(client))

    def getattr(value, name):
        return (value, name)
    """

    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


@pytest.mark.parametrize(
    "early_invocation",
    (
        pytest.param(
            """
            runner = poll
            result = asyncio.run(runner(client))
            """,
            id="function-alias",
        ),
        pytest.param(
            """
            async def run(client):
                return await poll(client)

            result = asyncio.run(run(client))
            """,
            id="function-wrapper",
        ),
    ),
)
def test_sync_callsite_detector_replays_indirect_early_module_calls(
    early_invocation: str,
) -> None:
    source = (
        textwrap.dedent(
            """
            import asyncio

            async def poll(client):
                call = getattr(client, "sync_emails")
                return await call(8, "INBOX", "cursor", 10)
            """
        )
        + textwrap.dedent(early_invocation)
        + textwrap.dedent(
            """
            def getattr(value, name):
                return (value, name)
            """
        )
    )

    violations = _sync_callsite_violations(
        source,
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


def test_sync_callsite_detector_allows_module_call_after_benign_shadow() -> None:
    source = """
    import asyncio

    async def poll(client):
        return getattr(client, "sync_emails")

    def getattr(value, name):
        return (value, name)

    result = asyncio.run(poll(client))
    """

    assert (
        _sync_callsite_violations(
            textwrap.dedent(source),
            relative_path="src/ingestion/benign.py",
        )
        == []
    )


def test_sync_callsite_detector_allows_benign_boolop_reflection_key() -> None:
    source = """
    async def close_transport(client, enabled):
        reflect = enabled and getattr or getattr
        return reflect(client, "close")()
    """

    assert (
        _sync_callsite_violations(
            textwrap.dedent(source),
            relative_path="src/ingestion/benign.py",
        )
        == []
    )


def test_sync_callsite_detector_allows_late_shadow_with_primitive_parameter_name() -> (
    None
):
    source = """
    import asyncio

    async def poll(client):
        call = getattr(client, "sync_emails")
        return call

    def getattr(vars, name):
        return (vars, name)

    result = asyncio.run(poll(client))
    """

    assert (
        _sync_callsite_violations(
            textwrap.dedent(source),
            relative_path="src/ingestion/benign.py",
        )
        == []
    )


def test_sync_callsite_detector_allows_late_module_shadow_of_builtin_name() -> None:
    source = """
    async def poll(client):
        value = getattr(client, "sync_emails")
        return value

    def getattr(value, name):
        return (value, name)
    """

    assert (
        _sync_callsite_violations(
            textwrap.dedent(source),
            relative_path="src/ingestion/benign.py",
        )
        == []
    )


def test_sync_callsite_detector_rejects_late_reflection_forwarder_shadow() -> None:
    source = """
    async def poll(client):
        call = getattr(client, "sync_emails")
        return await call(8, "INBOX", "cursor", 10)

    def getattr(value, name):
        return object.__getattribute__(value, name)
    """

    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/ingestion/rogue.py",
    )

    assert any(item.startswith("reflection_target:") for item in violations), violations


@pytest.mark.parametrize(
    "source",
    (
        "SYNC_METHOD_LABEL = 'sync_emails'",
        """
        def explain_failure():
            raise RuntimeError("sync_emails")
        """,
        """
        def close_transport(client):
            return getattr(client, "close")()
        """,
    ),
    ids=("ordinary-string", "error-message", "benign-getattr-close"),
)
def test_sync_callsite_detector_allows_benign_non_sync_text_and_reflection(
    source: str,
) -> None:
    assert (
        _sync_callsite_violations(
            textwrap.dedent(source),
            relative_path="src/ingestion/benign.py",
        )
        == []
    )


def test_reviewed_oauth_reflection_exception_manifest_is_exact() -> None:
    key = (
        "src/providers/factory.py",
        ("module", "function:_create_oauth_model"),
    )
    assert _REVIEWED_NON_SYNC_REFLECTION_KEY == key
    assert set(_REVIEWED_NON_SYNC_REFLECTION_BINDINGS) == {key}
    assert set(_REVIEWED_NON_SYNC_REFLECTION_ASSIGNMENT_SHAPES) == {key}
    assert set(_REVIEWED_NON_SYNC_REFLECTION_CALL_SHAPES) == {key}
    assert set(_REVIEWED_NON_SYNC_REFLECTION_CALL_PATHS) == {key}
    assert _REVIEWED_NON_SYNC_REFLECTION_BINDINGS[key] == {
        "module_path",
        "class_name",
        "module",
        "cls",
    }
    assert _REVIEWED_NON_SYNC_REFLECTION_CALL_PATHS[key] == {
        "importlib.import_module",
        "getattr",
        "cls",
    }
    assert Counter(_REVIEWED_NON_SYNC_REFLECTION_ASSIGNMENT_SHAPES[key].values()) == {
        1: 3
    }
    assert Counter(_REVIEWED_NON_SYNC_REFLECTION_CALL_SHAPES[key].values()) == {1: 3}
    assert _REVIEWED_NON_SYNC_REFLECTION_FILE_AST_SHA256 == {
        "src/providers/factory.py": (
            "544e80ea67d2894f7eafa5619596058335081d2dd4cad016173e32ae3b87e647"
        )
    }

    source_path = PROJECT_ROOT / key[0]
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    assert _normalized_ast_sha256(tree) == (
        "544e80ea67d2894f7eafa5619596058335081d2dd4cad016173e32ae3b87e647"
    )


def test_reviewed_oauth_reflection_exception_accepts_only_frozen_source() -> None:
    relative_path = "src/providers/factory.py"
    source = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")

    assert _sync_callsite_violations(source, relative_path=relative_path) == []


def test_reviewed_oauth_reflection_shapes_do_not_approve_synthetic_source() -> None:
    source = """
        def _create_oauth_model(spec, model, temperature, **kwargs):
            module_path, class_name = _OAUTH_PROVIDERS[spec.name]
            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)
            return cls(model_name=model, temperature=temperature, **kwargs)
    """

    violations = _sync_callsite_violations(
        textwrap.dedent(source),
        relative_path="src/providers/factory.py",
    )
    assert any(item.startswith("reflection_unresolved:") for item in violations), (
        violations
    )


@pytest.mark.parametrize(
    ("before", "after"),
    (
        (
            '"CodexChatModel"',
            '"ReviewedCodexChatModel"',
        ),
        (
            "module = importlib.import_module(module_path)",
            "module = importlib.import_module(name=module_path)",
        ),
        (
            "cls = getattr(module, class_name)",
            "cls = object.__getattribute__(module, class_name)",
        ),
        (
            "return cls(model_name=model, temperature=temperature, **kwargs)",
            "return cls(model=model, temperature=temperature, **kwargs)",
        ),
        (
            "return cls(model_name=model, temperature=temperature, **kwargs)",
            "cls(model_name=model, temperature=temperature, **kwargs)\n"
            "    return cls(model_name=model, temperature=temperature, **kwargs)",
        ),
        (
            "def _create_oauth_model(",
            "def _create_reviewed_oauth_model(",
        ),
    ),
    ids=(
        "provider-table",
        "import-assignment-shape",
        "getattr-assignment-shape",
        "alias-call-shape",
        "extra-alias-call",
        "lexical-owner",
    ),
)
def test_reviewed_oauth_reflection_exception_rejects_ast_mutations(
    before: str,
    after: str,
) -> None:
    relative_path = "src/providers/factory.py"
    source = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")
    assert source.count(before) == 1
    mutated = source.replace(before, after, 1)

    violations = _sync_callsite_violations(mutated, relative_path=relative_path)
    assert any(item.startswith("reflection_unresolved:") for item in violations), (
        violations
    )


def test_reviewed_oauth_reflection_exception_rejects_path_mutation() -> None:
    source = (PROJECT_ROOT / "src/providers/factory.py").read_text(encoding="utf-8")

    violations = _sync_callsite_violations(
        source,
        relative_path="src/providers/factory_copy.py",
    )
    assert any(item.startswith("reflection_unresolved:") for item in violations), (
        violations
    )


def test_sync_exception_ast_normalization_keeps_empty_fields() -> None:
    normalized = _normalized_ast_dump(ast.parse("def reviewed():\n    return cls()\n"))

    assert "type_ignores=[]" in normalized
    assert "args=[]" in normalized
    assert "keywords=[]" in normalized
    assert "decorator_list=[]" in normalized
    assert "type_params=[]" in normalized


def _reachable_sync_nodes() -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    source_path = PROJECT_ROOT / "src/utils/exchange_api.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    module_functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    exchange_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "ExchangeClient"
    )
    methods = {
        node.name: node
        for node in exchange_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    pending = [methods["sync_emails"]]
    reachable: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    seen: set[tuple[str, str]] = set()
    while pending:
        node = pending.pop()
        namespace = (
            "method"
            if node.name in methods and methods[node.name] is node
            else "module"
        )
        identity = (namespace, node.name)
        if identity in seen:
            continue
        seen.add(identity)
        reachable.append(node)
        for call in (child for child in ast.walk(node) if isinstance(child, ast.Call)):
            if isinstance(call.func, ast.Name) and call.func.id in module_functions:
                pending.append(module_functions[call.func.id])
            elif (
                isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id == "self"
                and call.func.attr in methods
            ):
                pending.append(methods[call.func.attr])
    return reachable


def test_sync_call_graph_has_one_bounded_stream_and_no_unbounded_response_api() -> None:
    reachable = _reachable_sync_nodes()
    stream_calls = [
        node
        for root in reachable
        for node in ast.walk(root)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "stream"
    ]
    bounded_reader_calls = [
        node
        for root in reachable
        for node in ast.walk(root)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "read_json_limited"
    ]
    forbidden_response_access = [
        node
        for root in reachable
        for node in ast.walk(root)
        if isinstance(node, ast.Attribute) and node.attr in {"json", "aread", "text"}
    ]
    loops = [
        node
        for root in reachable
        for node in ast.walk(root)
        if isinstance(node, (ast.For, ast.AsyncFor, ast.While))
    ]

    assert len(stream_calls) == 1
    assert len(bounded_reader_calls) == 1
    assert forbidden_response_access == []
    assert loops == []


def test_strict_reader_uses_incremental_bytearray_and_no_unbounded_response_api() -> (
    None
):
    from src.safety.http_response import read_json_limited

    tree = ast.parse(textwrap.dedent(inspect.getsource(read_json_limited)))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]

    assert any(
        isinstance(node.func, ast.Name) and node.func.id == "bytearray"
        for node in calls
    )
    assert any(
        isinstance(node.func, ast.Attribute) and node.func.attr == "aiter_bytes"
        for node in calls
    )
    assert not any(
        isinstance(node, ast.Attribute) and node.attr in {"json", "aread", "text"}
        for node in ast.walk(tree)
    )
