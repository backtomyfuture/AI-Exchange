from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest


_SQL_EXECUTION_METHODS = frozenset(
    {"copy", "copy_expert", "execute", "executemany", "exec_driver_sql"}
)
_SQL_TEXT_CONSTRUCTORS = frozenset({"SQL", "text"})
_SQL_IDENTIFIER_CONSTRUCTORS = frozenset({"Identifier", "_table"})
_EVENT_INBOX_MUTATION = re.compile(
    r"""
    (?:
        \b(?:
            insert\s+into
            |
            update\s+(?:only\s+)?
            |
            delete\s+from\s+(?:only\s+)?
            |
            merge\s+into\s+(?:only\s+)?
            |
            truncate\s+(?:table\s+)?(?:only\s+)?
        )
        \s*
        (?:(?:"[^"]+"|[a-z_][a-z0-9_$]*)\s*\.\s*)?
        (?:"event_inbox"|event_inbox\b)
        |
        \bcopy\s+
        (?:(?:"[^"]+"|[a-z_][a-z0-9_$]*)\s*\.\s*)?
        (?:"event_inbox"|event_inbox\b)
        (?:\s*\([^)]*\))?\s+from\b
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _call_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _scope_nodes(root: ast.AST):
    """Yield one lexical scope without descending into child scopes."""
    pending = list(ast.iter_child_nodes(root))
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            pending.extend(node.decorator_list)
            pending.extend(node.args.defaults)
            pending.extend(
                default for default in node.args.kw_defaults if default is not None
            )
            continue
        if isinstance(node, ast.Lambda):
            pending.extend(node.args.defaults)
            pending.extend(
                default for default in node.args.kw_defaults if default is not None
            )
            continue
        if isinstance(node, ast.ClassDef):
            pending.extend(node.decorator_list)
            pending.extend(node.bases)
            pending.extend(keyword.value for keyword in node.keywords)
            pending.extend(node.body)
            continue
        yield node
        pending.extend(ast.iter_child_nodes(node))


def _scope_bindings(root: ast.AST) -> dict[str, list[ast.expr]]:
    bindings: dict[str, list[ast.expr]] = {}
    for node in _scope_nodes(root):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    bindings.setdefault(target.id, []).append(node.value)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.value is not None:
                bindings.setdefault(node.target.id, []).append(node.value)
        elif isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name):
            bindings.setdefault(node.target.id, []).append(node.value)
    return bindings


def _bound_target_names(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Starred):
        return _bound_target_names(node.value)
    if isinstance(node, (ast.List, ast.Tuple)):
        return {name for element in node.elts for name in _bound_target_names(element)}
    if isinstance(node, ast.MatchAs):
        names = {node.name} if node.name is not None else set()
        if node.pattern is not None:
            names.update(_bound_target_names(node.pattern))
        return names
    if isinstance(node, ast.MatchStar):
        return {node.name} if node.name is not None else set()
    if isinstance(node, ast.MatchMapping):
        names = {
            name for pattern in node.patterns for name in _bound_target_names(pattern)
        }
        if node.rest is not None:
            names.add(node.rest)
        return names
    if isinstance(node, ast.MatchSequence):
        return {
            name for pattern in node.patterns for name in _bound_target_names(pattern)
        }
    if isinstance(node, ast.MatchClass):
        return {
            name
            for pattern in (*node.patterns, *node.kwd_patterns)
            for name in _bound_target_names(pattern)
        }
    if isinstance(node, ast.MatchOr):
        return {
            name for pattern in node.patterns for name in _bound_target_names(pattern)
        }
    return set()


def _scope_bound_names(root: ast.AsyncFunctionDef) -> set[str]:
    names = {
        argument.arg
        for argument in (
            *root.args.posonlyargs,
            *root.args.args,
            *root.args.kwonlyargs,
        )
    }
    if root.args.vararg is not None:
        names.add(root.args.vararg.arg)
    if root.args.kwarg is not None:
        names.add(root.args.kwarg.arg)
    names.update(_scope_bindings(root))

    pending = list(ast.iter_child_nodes(root))
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
            names.add(node.name)
            continue
        if isinstance(node, ast.Lambda):
            continue
        if isinstance(node, ast.Assign):
            for target in node.targets:
                names.update(_bound_target_names(target))
        elif isinstance(node, (ast.AnnAssign, ast.NamedExpr)):
            names.update(_bound_target_names(node.target))
        elif isinstance(node, (ast.For, ast.AsyncFor, ast.comprehension)):
            names.update(_bound_target_names(node.target))
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                if item.optional_vars is not None:
                    names.update(_bound_target_names(item.optional_vars))
        elif isinstance(node, ast.ExceptHandler) and node.name is not None:
            names.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update(
                alias.asname or alias.name.split(".", 1)[0] for alias in node.names
            )
        elif isinstance(node, ast.pattern):
            names.update(_bound_target_names(node))
        pending.extend(ast.iter_child_nodes(node))
    return names


def _render_sql(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]

    if isinstance(node, ast.JoinedStr):
        if any(not isinstance(part, ast.Constant) for part in node.values):
            return []
        return [
            "".join(part.value for part in node.values if isinstance(part.value, str))
        ]

    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _render_sql(node.left, bindings, resolving=resolving)
        right = _render_sql(node.right, bindings, resolving=resolving)
        return [left_part + right_part for left_part in left for right_part in right]

    if isinstance(node, ast.IfExp):
        return [
            *_render_sql(node.body, bindings, resolving=resolving),
            *_render_sql(node.orelse, bindings, resolving=resolving),
        ]

    if isinstance(node, ast.Name):
        if node.id in resolving:
            return []
        return [
            rendered
            for value in bindings.get(node.id, [])
            for rendered in _render_sql(
                value,
                bindings,
                resolving=resolving | {node.id},
            )
        ]

    if not isinstance(node, ast.Call):
        return []

    call_name = _call_name(node.func)
    if call_name in _SQL_TEXT_CONSTRUCTORS and node.args:
        return _render_sql(node.args[0], bindings, resolving=resolving)

    if call_name in _SQL_IDENTIFIER_CONSTRUCTORS:
        parts = [
            rendered
            for argument in node.args
            for rendered in _render_sql(argument, bindings, resolving=resolving)
        ]
        return [".".join(parts)] if parts else ["dynamic_identifier"]

    if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        templates = _render_sql(node.func.value, bindings, resolving=resolving)
        rendered: list[str] = []
        for template in templates:
            statement = template
            for argument in node.args:
                values = _render_sql(argument, bindings, resolving=resolving)
                statement = statement.replace(
                    "{}",
                    values[0] if values else "dynamic_identifier",
                    1,
                )
            for keyword in node.keywords:
                if keyword.arg is None:
                    continue
                values = _render_sql(keyword.value, bindings, resolving=resolving)
                statement = statement.replace(
                    "{" + keyword.arg + "}",
                    values[0] if values else "dynamic_identifier",
                )
            rendered.append(statement)
        return rendered

    return []


def _execution_query(call: ast.Call) -> ast.expr | None:
    if _call_name(call.func) not in _SQL_EXECUTION_METHODS:
        return None
    if call.args:
        return call.args[0]
    for keyword in call.keywords:
        if keyword.arg in {"query", "sql", "statement"}:
            return keyword.value
    return None


def _find_event_inbox_mutations(source: str, *, filename: str) -> list[int]:
    tree = ast.parse(source, filename=filename)
    violations: list[int] = []

    def inspect_scope(
        root: ast.AST,
        inherited_bindings: dict[str, list[ast.expr]],
    ) -> None:
        bindings = {name: list(values) for name, values in inherited_bindings.items()}
        for name, values in _scope_bindings(root).items():
            bindings[name] = values

        for node in _scope_nodes(root):
            if not isinstance(node, ast.Call):
                continue
            query = _execution_query(node)
            if query is None:
                continue
            if any(
                _EVENT_INBOX_MUTATION.search(statement)
                for statement in _render_sql(query, bindings)
            ):
                violations.append(node.lineno)

        for child in ast.iter_child_nodes(root):
            if isinstance(child, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
                inspect_scope(child, bindings)

    inspect_scope(tree, {})
    return sorted(set(violations))


def test_detector_handles_composed_sql_without_matching_explanatory_text() -> None:
    source = '''
async def write_rows(cursor, sql, _table):
    statement = sql.SQL(
        "InSeRt\\n    InTo {} (id) VALUES (%s)"
    ).format(_table("event_inbox"))
    await cursor.execute(statement)
    await cursor.execute(
        """
        UpDaTe
            "runtime" . "event_inbox"
        SET status = 'completed'
        """
    )
    logger.info("UPDATE event_inbox is restricted to the repository")
'''

    assert _find_event_inbox_mutations(source, filename="<detector-contract>") == [
        6,
        7,
    ]


def test_detector_covers_every_runtime_mutation_form_and_ignores_copy_to() -> None:
    source = """
async def mutate_rows(cursor, sql, _table):
    await cursor.execute('DELETE FROM ONLY "runtime"."event_inbox" WHERE id = %s')
    await cursor.execute(sql.SQL("MERGE INTO {} USING source ON false WHEN NOT MATCHED THEN INSERT DEFAULT VALUES").format(sql.Identifier("runtime", "event_inbox")))
    await cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(_table("event_inbox")))
    await cursor.execute('COPY "runtime"."event_inbox" (id) FROM STDIN')
    await cursor.execute('COPY "runtime"."event_inbox" (id) TO STDOUT')
    async with cursor.copy('COPY "runtime"."event_inbox" (id) FROM STDIN') as writer:
        await writer.write_row((1,))
    async with cursor.copy('COPY "runtime"."event_inbox" (id) TO STDOUT') as reader:
        await reader.read_row()
    await cursor.copy_expert('COPY "runtime"."event_inbox" (id) FROM STDIN')
    await cursor.copy_expert('COPY "runtime"."event_inbox" (id) TO STDOUT')
    await cursor.copy_expert(sql='COPY "runtime"."event_inbox" (id) FROM STDIN', file=stream)
    await cursor.copy_expert(sql='COPY "runtime"."event_inbox" (id) TO STDOUT', file=stream)
"""

    assert _find_event_inbox_mutations(source, filename="<mutation-contract>") == [
        3,
        4,
        5,
        6,
        8,
        12,
        14,
    ]


def test_event_inbox_mutations_are_owned_by_the_repository() -> None:
    project_root = Path(__file__).resolve().parents[2]
    allowed = project_root / "src" / "ingestion" / "repository.py"
    candidates = list((project_root / "src").rglob("*.py"))
    scripts = project_root / "scripts"
    if scripts.is_dir():
        candidates.extend(scripts.rglob("*.py"))

    violations: list[str] = []
    for path in sorted(candidates):
        if path == allowed:
            continue
        source = path.read_text(encoding="utf-8")
        violations.extend(
            f"{path.relative_to(project_root)}:{line}"
            for line in _find_event_inbox_mutations(source, filename=str(path))
        )

    assert violations == [], (
        "event_inbox mutations are reserved for "
        f"src/ingestion/repository.py: {violations}"
    )


def _class_method(
    tree: ast.Module,
    class_name: str,
    method_name: str,
) -> ast.AsyncFunctionDef:
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        for child in node.body:
            if isinstance(child, ast.AsyncFunctionDef) and child.name == method_name:
                return child
    raise AssertionError(f"missing {class_name}.{method_name}")


def _class_scope_nodes(node: ast.ClassDef):
    pending = list(node.body)
    while pending:
        child = pending.pop()
        yield child
        if isinstance(
            child,
            (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef, ast.Lambda),
        ):
            continue
        pending.extend(ast.iter_child_nodes(child))


def _pinned_class_method(
    tree: ast.Module,
    *,
    class_name: str,
    method_name: str,
) -> tuple[ast.AsyncFunctionDef | None, list[str]]:
    violations: list[str] = []
    classes = [
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    ]
    if len(classes) != 1:
        violations.append(f"class_definition_count:{class_name}:{len(classes)}")
    if not classes:
        return None, violations
    selected_class = classes[0]
    if selected_class.decorator_list or selected_class.bases or selected_class.keywords:
        violations.append(f"class_contract:{class_name}")

    definitions = [
        node
        for node in selected_class.body
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and node.name == method_name
    ]
    if len(definitions) != 1:
        violations.append(f"insert_definition_count:{len(definitions)}")

    direct_definition = definitions[0] if definitions else None
    for node in _class_scope_nodes(selected_class):
        if node is direct_definition:
            continue
        if (
            isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef))
            and node.name == method_name
        ):
            if node not in definitions:
                violations.append("insert_rebound")
            continue
        if isinstance(node, ast.pattern) and method_name in _bound_target_names(node):
            violations.append("insert_rebound")
        if isinstance(node, ast.ExceptHandler) and node.name == method_name:
            violations.append("insert_rebound")
        if (
            isinstance(node, ast.Name)
            and isinstance(node.ctx, (ast.Store, ast.Del))
            and node.id == method_name
        ):
            violations.append("insert_rebound")
        if isinstance(node, (ast.Import, ast.ImportFrom)) and any(
            (alias.asname or alias.name.split(".", 1)[0]) == method_name
            for alias in node.names
        ):
            violations.append("insert_rebound")
    if not isinstance(direct_definition, ast.AsyncFunctionDef):
        if direct_definition is not None:
            violations.append("insert_not_async")
        return None, sorted(set(violations))
    return direct_definition, sorted(set(violations))


def _has_exact_insert_signature(method: ast.AsyncFunctionDef) -> bool:
    arguments = method.args
    return (
        not arguments.posonlyargs
        and tuple(argument.arg for argument in arguments.args)
        == ("self", "event", "generation", "fencing_token")
        and tuple(
            ast.unparse(argument.annotation)
            if argument.annotation is not None
            else None
            for argument in arguments.args
        )
        == (None, "NormalizedIngressEvent", "int", "int")
        and not arguments.kwonlyargs
        and arguments.vararg is None
        and arguments.kwarg is None
        and not arguments.defaults
        and not any(arguments.kw_defaults)
        and method.returns is not None
        and ast.unparse(method.returns) == "IngressReceipt"
        and method.type_comment is None
    )


def _has_exact_transaction_factory(
    method: ast.AsyncFunctionDef | ast.FunctionDef,
) -> bool:
    arguments = method.args
    if (
        not isinstance(method, ast.FunctionDef)
        or method.decorator_list
        or arguments.posonlyargs
        or tuple(argument.arg for argument in arguments.args) != ("self", "connection")
        or tuple(
            ast.unparse(argument.annotation)
            if argument.annotation is not None
            else None
            for argument in arguments.args
        )
        != (None, "psycopg.AsyncConnection[Any]")
        or tuple(argument.arg for argument in arguments.kwonlyargs)
        != ("for_key_share",)
        or tuple(
            ast.unparse(argument.annotation)
            if argument.annotation is not None
            else None
            for argument in arguments.kwonlyargs
        )
        != ("bool",)
        or arguments.vararg is not None
        or arguments.kwarg is not None
        or arguments.defaults
        or len(arguments.kw_defaults) != 1
        or not isinstance(arguments.kw_defaults[0], ast.Constant)
        or arguments.kw_defaults[0].value is not True
        or method.returns is None
        or ast.unparse(method.returns) != "EmailEventTransaction"
        or method.type_comment is not None
        or len(method.body) != 2
    ):
        return False
    guard, result = method.body
    if (
        not isinstance(guard, ast.If)
        or guard.orelse
        or not isinstance(guard.test, ast.Compare)
        or not isinstance(guard.test.left, ast.Call)
        or not isinstance(guard.test.left.func, ast.Name)
        or guard.test.left.func.id != "type"
        or len(guard.test.left.args) != 1
        or not isinstance(guard.test.left.args[0], ast.Name)
        or guard.test.left.args[0].id != "for_key_share"
        or len(guard.test.ops) != 1
        or not isinstance(guard.test.ops[0], ast.IsNot)
        or len(guard.test.comparators) != 1
        or not isinstance(guard.test.comparators[0], ast.Name)
        or guard.test.comparators[0].id != "bool"
        or len(guard.body) != 1
        or not isinstance(guard.body[0], ast.Raise)
        or not isinstance(guard.body[0].exc, ast.Call)
        or not isinstance(guard.body[0].exc.func, ast.Name)
        or guard.body[0].exc.func.id != "ValueError"
        or len(guard.body[0].exc.args) != 1
        or not isinstance(guard.body[0].exc.args[0], ast.Constant)
        or guard.body[0].exc.args[0].value != "for_key_share must be an exact boolean"
        or guard.body[0].cause is not None
        or not isinstance(result, ast.Return)
        or not isinstance(result.value, ast.Call)
    ):
        return False
    constructor = result.value
    return (
        isinstance(constructor.func, ast.Name)
        and constructor.func.id == "EmailEventTransaction"
        and len(constructor.keywords) == 1
        and constructor.keywords[0].arg == "for_key_share"
        and isinstance(constructor.keywords[0].value, ast.Name)
        and constructor.keywords[0].value.id == "for_key_share"
        and tuple(
            argument.id if isinstance(argument, ast.Name) else None
            for argument in constructor.args
        )
        == ("self", "connection")
    )


def _is_exact_lock_mode_guard(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.If)
        and not node.orelse
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Call)
        and isinstance(node.test.left.func, ast.Name)
        and node.test.left.func.id == "type"
        and len(node.test.left.args) == 1
        and isinstance(node.test.left.args[0], ast.Name)
        and node.test.left.args[0].id == "for_key_share"
        and len(node.test.ops) == 1
        and isinstance(node.test.ops[0], ast.IsNot)
        and len(node.test.comparators) == 1
        and isinstance(node.test.comparators[0], ast.Name)
        and node.test.comparators[0].id == "bool"
        and len(node.body) == 1
        and isinstance(node.body[0], ast.Raise)
        and isinstance(node.body[0].exc, ast.Call)
        and isinstance(node.body[0].exc.func, ast.Name)
        and node.body[0].exc.func.id == "ValueError"
        and len(node.body[0].exc.args) == 1
        and isinstance(node.body[0].exc.args[0], ast.Constant)
        and node.body[0].exc.args[0].value == "for_key_share must be an exact boolean"
        and node.body[0].cause is None
    )


def _is_exact_self_assignment(
    node: ast.stmt,
    *,
    attribute: str,
    source: str,
) -> bool:
    return (
        isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Attribute)
        and _attribute_chain(node.targets[0].value) == ("self",)
        and node.targets[0].attr == attribute
        and isinstance(node.value, ast.Name)
        and node.value.id == source
    )


def _has_exact_transaction_constructor(
    method: ast.AsyncFunctionDef | ast.FunctionDef,
) -> bool:
    arguments = method.args
    if (
        not isinstance(method, ast.FunctionDef)
        or method.name != "__init__"
        or method.decorator_list
        or arguments.posonlyargs
        or tuple(argument.arg for argument in arguments.args)
        != ("self", "repository", "connection")
        or tuple(
            ast.unparse(argument.annotation)
            if argument.annotation is not None
            else None
            for argument in arguments.args
        )
        != (None, "InboxRepository", "psycopg.AsyncConnection[Any]")
        or tuple(argument.arg for argument in arguments.kwonlyargs)
        != ("for_key_share",)
        or tuple(
            ast.unparse(argument.annotation)
            if argument.annotation is not None
            else None
            for argument in arguments.kwonlyargs
        )
        != ("bool",)
        or arguments.vararg is not None
        or arguments.kwarg is not None
        or arguments.defaults
        or arguments.kw_defaults != [None]
        or method.returns is None
        or ast.unparse(method.returns) != "None"
        or method.type_comment is not None
        or len(method.body) != 5
    ):
        return False
    transaction_id = method.body[4]
    return (
        _is_exact_lock_mode_guard(method.body[0])
        and _is_exact_self_assignment(
            method.body[1],
            attribute="_repository",
            source="repository",
        )
        and _is_exact_self_assignment(
            method.body[2],
            attribute="_connection",
            source="connection",
        )
        and _is_exact_self_assignment(
            method.body[3],
            attribute="_for_key_share",
            source="for_key_share",
        )
        and isinstance(transaction_id, ast.AnnAssign)
        and isinstance(transaction_id.target, ast.Attribute)
        and _attribute_chain(transaction_id.target.value) == ("self",)
        and transaction_id.target.attr == "_transaction_id"
        and ast.unparse(transaction_id.annotation) == "str | None"
        and isinstance(transaction_id.value, ast.Constant)
        and transaction_id.value.value is None
        and transaction_id.simple == 0
    )


def _transaction_constructor_violations(tree: ast.Module) -> list[str]:
    methods, class_count = _class_methods(tree, "EmailEventTransaction")
    constructor = methods.get("__init__")
    if class_count != 1 or constructor is None:
        return ["transaction_constructor_shape"]
    if not _has_exact_transaction_constructor(constructor):
        return ["transaction_constructor_shape"]
    return []


_TRANSACTION_LIFECYCLE_ATTRIBUTES = frozenset(
    {
        "autocommit",
        "commit",
        "connection",
        "rollback",
        "savepoint",
        "set_autocommit",
        "transaction",
    }
)
_TRANSACTION_CONTROL_SQL = re.compile(
    r"^\s*(?:"
    r"BEGIN\b|START\s+TRANSACTION\b|COMMIT\b|END\b|"
    r"ROLLBACK\b|ABORT\b|SAVEPOINT\b|"
    r"RELEASE(?:\s+SAVEPOINT)?\b|PREPARE\s+TRANSACTION\b|"
    r"SET\s+SESSION\s+CHARACTERISTICS\s+AS\s+TRANSACTION\b|"
    r"SET\s+(?:(?:LOCAL|SESSION)\s+)?TRANSACTION\b"
    r")",
    re.IGNORECASE,
)
_CALLER_OWNED_REPOSITORY_HELPERS = frozenset(
    {
        "_acquire_account_lock",
        "_append_audit",
        "_duplicate_receipt",
        "_table",
        "_validate_insert_inputs",
    }
)
_CALLER_OWNED_GLOBAL_CALLS = frozenset(
    {
        "IngressReceipt",
        "Jsonb",
        "StaleFence",
        "ValueError",
        "_database_error",
        "_invariant_error",
        "_row_values",
        "isinstance",
        "str",
        "type",
        "uuid4",
    }
)
_CALLER_OWNED_SELF_CALLS = frozenset({"_assert_transaction_identity"})
_CALLER_OWNED_CURSOR_CALLS = frozenset({"inserted_cursor", "ownership_cursor"})
_POOL_WRAPPER_SELF_CALLS = frozenset(
    {"_configure_transaction", "_validate_insert_inputs", "transaction"}
)
_POOL_WRAPPER_GLOBAL_CALLS = frozenset({"_database_error"})
_MODULE_IMPORT_PROVENANCE = {
    "DatabaseOperationError": ("src.domain.errors", "DatabaseOperationError"),
    "IngressReceipt": ("src.ingestion.models", "IngressReceipt"),
    "Jsonb": ("psycopg.types.json", "Jsonb"),
    "NormalizedIngressEvent": (
        "src.ingestion.models",
        "NormalizedIngressEvent",
    ),
    "PoolTimeout": ("psycopg_pool", "PoolTimeout"),
    "StaleFence": ("src.domain.errors", "StaleFence"),
    "TransactionStatus": ("psycopg.pq", "TransactionStatus"),
    "ownership_advisory_lock_key": (
        "src.ingestion.ownership",
        "ownership_advisory_lock_key",
    ),
    "sql": ("psycopg", "sql"),
    "uuid4": ("uuid", "uuid4"),
}
_MODULE_FUNCTION_PROVENANCE = frozenset(
    {"_database_error", "_invariant_error", "_row_values"}
)
_MODULE_BUILTIN_PROVENANCE = frozenset(
    {
        "RuntimeError",
        "ValueError",
        "bool",
        "dict",
        "int",
        "isinstance",
        "len",
        "list",
        "range",
        "staticmethod",
        "str",
        "tuple",
        "type",
    }
)
_MODULE_DIRECT_IMPORT_PROVENANCE = frozenset({"psycopg"})
_MODULE_CLASS_PROVENANCE = {"_AuditInvariantError": ("DatabaseOperationError",)}
_HELPER_SAFE_EXTERNAL_NAME_CALLS = frozenset(
    {
        "DatabaseOperationError",
        "EmailEventTransaction",
        "IngressReceipt",
        "Jsonb",
        "RuntimeError",
        "StaleFence",
        "ValueError",
        "_AuditInvariantError",
        "bool",
        "dict",
        "int",
        "isinstance",
        "len",
        "list",
        "ownership_advisory_lock_key",
        "range",
        "str",
        "tuple",
        "type",
        "uuid4",
    }
)
_DATABASE_EXCEPTION_BINDING = "_DATABASE_EXCEPTIONS"
_HELPER_MODULE_ALLOWLIST = (
    _HELPER_SAFE_EXTERNAL_NAME_CALLS - {"EmailEventTransaction"}
) | frozenset(
    {
        "NormalizedIngressEvent",
        "TransactionStatus",
        "staticmethod",
    }
)
_CALLER_MODULE_ALLOWLIST = (
    _CALLER_OWNED_GLOBAL_CALLS
    | frozenset(
        {
            "DatabaseOperationError",
            "PoolTimeout",
            "RuntimeError",
            "ValueError",
            "_DATABASE_EXCEPTIONS",
            "psycopg",
            "sql",
        }
    )
    | _HELPER_MODULE_ALLOWLIST
)
_POOL_MODULE_ALLOWLIST = (
    _POOL_WRAPPER_GLOBAL_CALLS
    | frozenset(
        {
            "DatabaseOperationError",
            "PoolTimeout",
            "RuntimeError",
            "StaleFence",
            "ValueError",
            "_DATABASE_EXCEPTIONS",
            "psycopg",
        }
    )
    | _HELPER_MODULE_ALLOWLIST
)
_UNTERMINATED_SQL = "<UNTERMINATED_SQL>"
_PASSTHROUGH_EXCEPTIONS = (
    "StaleFence",
    "DatabaseOperationError",
    "ValueError",
    "RuntimeError",
)
_RUNTIME_NAMESPACE_CALLS = frozenset(
    {
        "__delattr__",
        "__setattr__",
        "delattr",
        "eval",
        "exec",
        "globals",
        "locals",
        "setattr",
        "vars",
    }
)
_RUNTIME_TARGET_CLASSES = ("EmailEventTransaction", "InboxRepository")


def _attribute_chain(node: ast.expr) -> tuple[str, ...] | None:
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, ast.Attribute):
        parent = _attribute_chain(node.value)
        if parent is not None:
            return (*parent, node.attr)
    return None


def _module_scope_nodes(tree: ast.Module):
    pending = list(tree.body)
    while pending:
        node = pending.pop()
        yield node
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
            continue
        if isinstance(node, (ast.Lambda, ast.comprehension)):
            continue
        pending.extend(ast.iter_child_nodes(node))


def _definition_time_expressions(node: ast.AST) -> list[ast.expr]:
    if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
        expressions = [*node.decorator_list, *node.args.defaults]
        expressions.extend(
            default for default in node.args.kw_defaults if default is not None
        )
        return expressions
    if isinstance(node, ast.Lambda):
        expressions = list(node.args.defaults)
        expressions.extend(
            default for default in node.args.kw_defaults if default is not None
        )
        return expressions
    if isinstance(node, ast.ClassDef):
        return [
            *node.decorator_list,
            *node.bases,
            *(keyword.value for keyword in node.keywords),
        ]
    return []


def _runtime_lexical_nodes(
    root: ast.Module | ast.ClassDef,
    *,
    descend_class_bodies: bool = False,
):
    pending = list(root.body)
    while pending:
        node = pending.pop()
        yield node
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.Lambda)):
            pending.extend(_definition_time_expressions(node))
            continue
        if isinstance(node, ast.ClassDef):
            pending.extend(_definition_time_expressions(node))
            if descend_class_bodies:
                pending.extend(node.body)
            continue
        if isinstance(node, ast.comprehension):
            pending.append(node.iter)
            pending.extend(node.ifs)
            continue
        pending.extend(ast.iter_child_nodes(node))


def _has_runtime_namespace_effect(node: ast.AST) -> bool:
    if isinstance(node, (ast.Attribute, ast.Subscript)) and isinstance(
        node.ctx,
        (ast.Store, ast.Del),
    ):
        return True
    return (
        isinstance(node, ast.Call) and _call_name(node.func) in _RUNTIME_NAMESPACE_CALLS
    )


def _class_binding_names(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
        return {node.id}
    if isinstance(node, ast.pattern):
        return _bound_target_names(node)
    if isinstance(node, ast.ExceptHandler) and node.name is not None:
        return {node.name}
    if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
        return {node.name}
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        return {alias.asname or alias.name.split(".", 1)[0] for alias in node.names}
    return set()


def _runtime_binding_violations(tree: ast.Module) -> list[str]:
    violations: list[str] = []
    module_nodes = tuple(_runtime_lexical_nodes(tree, descend_class_bodies=True))
    local_function_names = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
    }
    local_function_call = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in local_function_names
        for node in module_nodes
    )
    local_decorator_reference = any(
        isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef))
        and any(
            isinstance(decorator, ast.Name) and decorator.id in local_function_names
            for decorator in node.decorator_list
        )
        for node in module_nodes
    )
    if (
        any(_has_runtime_namespace_effect(node) for node in module_nodes)
        or local_function_call
        or local_decorator_reference
    ):
        violations.append("module_runtime_binding_mutation")

    direct_classes: dict[str, ast.ClassDef] = {}
    for class_name in _RUNTIME_TARGET_CLASSES:
        records = _module_binding_records(tree, class_name)
        valid = [
            node
            for kind, node in records
            if kind == "class"
            and isinstance(node, ast.ClassDef)
            and node in tree.body
            and not node.decorator_list
            and not node.bases
            and not node.keywords
        ]
        if len(records) != 1 or len(valid) != 1:
            violations.append("module_runtime_binding_mutation")
        if len(valid) == 1:
            direct_classes[class_name] = valid[0]

    for class_name, class_node in direct_classes.items():
        direct_methods = [
            node
            for node in class_node.body
            if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        ]
        protected_names = {node.name for node in direct_methods}
        direct_counts = {
            name: sum(method.name == name for method in direct_methods)
            for name in protected_names
        }
        class_mutation = any(count != 1 for count in direct_counts.values())
        direct_method_nodes = set(direct_methods)
        for node in _runtime_lexical_nodes(class_node):
            if _has_runtime_namespace_effect(node) or isinstance(
                node,
                (ast.Global, ast.Nonlocal),
            ):
                class_mutation = True
            names = _class_binding_names(node) & protected_names
            if not names:
                continue
            if node in direct_method_nodes:
                continue
            class_mutation = True
        if class_mutation:
            violations.append(f"class_runtime_binding_mutation:{class_name}")
    return sorted(set(violations))


def _module_binding_records(
    tree: ast.Module,
    name: str,
) -> list[tuple[str, ast.AST]]:
    records: list[tuple[str, ast.AST]] = []
    direct_nodes = set(tree.body)
    for node in _module_scope_nodes(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
            if node.name == name:
                kind = "direct_function" if node in direct_nodes else "nested_function"
                if isinstance(node, ast.ClassDef):
                    kind = "class"
                records.append((kind, node))
            continue
        if isinstance(node, ast.Import):
            if any(
                (alias.asname or alias.name.split(".", 1)[0]) == name
                for alias in node.names
            ):
                records.append(("import", node))
            continue
        if isinstance(node, ast.ImportFrom):
            if any(
                alias.name != "*" and (alias.asname or alias.name) == name
                for alias in node.names
            ):
                records.append(
                    (
                        "direct_import_from"
                        if node in direct_nodes
                        else "nested_import",
                        node,
                    )
                )
            continue
        if isinstance(node, ast.ExceptHandler) and node.name == name:
            records.append(("write", node))
        if isinstance(node, ast.pattern) and name in _bound_target_names(node):
            records.append(("write", node))
        if (
            isinstance(node, ast.Name)
            and isinstance(node.ctx, (ast.Store, ast.Del))
            and node.id == name
        ):
            records.append(("write", node))
    return records


def _module_allowlist_provenance_violations(
    tree: ast.Module,
    allowlist: frozenset[str],
) -> list[str]:
    violations: list[str] = []
    if any(
        isinstance(node, ast.ImportFrom)
        and any(alias.name == "*" for alias in node.names)
        for node in _module_scope_nodes(tree)
    ):
        violations.append("module_allowlist_wildcard_import")

    for name in sorted(allowlist):
        records = _module_binding_records(tree, name)
        if name == _DATABASE_EXCEPTION_BINDING:
            valid_assignments = [
                node
                for node in tree.body
                if isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == name
                and isinstance(node.value, ast.Tuple)
                and len(node.value.elts) == 2
                and _attribute_chain(node.value.elts[0]) == ("psycopg", "Error")
                and isinstance(node.value.elts[1], ast.Name)
                and node.value.elts[1].id == "PoolTimeout"
            ]
            if len(records) != 1 or len(valid_assignments) != 1:
                violations.append(f"module_allowlist_override:{name}")
            continue
        if name in _MODULE_BUILTIN_PROVENANCE:
            if records:
                violations.append(f"module_allowlist_override:{name}")
            continue
        if name in _MODULE_FUNCTION_PROVENANCE:
            expected_arguments = {
                "_database_error": ("operation", "error"),
                "_invariant_error": ("message",),
                "_row_values": ("row", "columns"),
            }[name]
            valid = [
                node
                for kind, node in records
                if kind == "direct_function"
                and isinstance(node, ast.FunctionDef)
                and not node.decorator_list
                and not node.args.defaults
                and not any(node.args.kw_defaults)
                and not node.args.posonlyargs
                and tuple(argument.arg for argument in node.args.args)
                == expected_arguments
                and not node.args.kwonlyargs
                and node.args.vararg is None
                and node.args.kwarg is None
            ]
            if len(records) != 1 or len(valid) != 1:
                violations.append(f"module_allowlist_override:{name}")
            continue
        if name in _MODULE_CLASS_PROVENANCE:
            expected_bases = _MODULE_CLASS_PROVENANCE[name]
            valid_classes = [
                node
                for kind, node in records
                if kind == "class"
                and isinstance(node, ast.ClassDef)
                and node in tree.body
                and not node.decorator_list
                and not node.keywords
                and tuple(ast.unparse(base) for base in node.bases) == expected_bases
            ]
            if len(records) != 1 or len(valid_classes) != 1:
                violations.append(f"module_allowlist_override:{name}")
            continue
        if name in _MODULE_DIRECT_IMPORT_PROVENANCE:
            valid_imports = [
                node
                for kind, node in records
                if kind == "import"
                and isinstance(node, ast.Import)
                and len(node.names) == 1
                and node.names[0].name == name
                and node.names[0].asname is None
                and node in tree.body
            ]
            if len(records) != 1 or len(valid_imports) != 1:
                violations.append(f"module_allowlist_override:{name}")
            continue
        expected = _MODULE_IMPORT_PROVENANCE.get(name)
        if expected is None:
            violations.append(f"module_allowlist_unpinned:{name}")
            continue
        valid_imports = []
        for kind, node in records:
            if kind != "direct_import_from" or not isinstance(node, ast.ImportFrom):
                continue
            if node.module != expected[0] or node.level != 0:
                continue
            matching = [
                alias
                for alias in node.names
                if alias.name == expected[1] and (alias.asname or alias.name) == name
            ]
            if len(matching) == 1:
                valid_imports.append(node)
        if len(records) != 1 or len(valid_imports) != 1:
            violations.append(f"module_allowlist_override:{name}")
    return violations


def _definition_contract_violations(
    node: ast.AsyncFunctionDef | ast.FunctionDef,
    *,
    prefix: str,
) -> list[str]:
    violations: list[str] = []
    if node.decorator_list:
        violations.append(f"{prefix}_decorator")
    if node.args.defaults or any(node.args.kw_defaults):
        violations.append(f"{prefix}_default")
    return violations


def _is_bare_raise(node: ast.AST) -> bool:
    return isinstance(node, ast.Raise) and node.exc is None and node.cause is None


def _is_exact_passthrough_handler(node: ast.ExceptHandler) -> bool:
    return (
        node.name is None
        and isinstance(node.type, ast.Tuple)
        and tuple(
            item.id if isinstance(item, ast.Name) else None for item in node.type.elts
        )
        == _PASSTHROUGH_EXCEPTIONS
        and len(node.body) == 1
        and _is_bare_raise(node.body[0])
    )


def _is_exact_database_handler(
    node: ast.ExceptHandler,
    *,
    operation: str,
) -> bool:
    if (
        not isinstance(node.type, ast.Name)
        or node.type.id != "_DATABASE_EXCEPTIONS"
        or node.name != "error"
        or len(node.body) != 1
        or not isinstance(node.body[0], ast.Raise)
    ):
        return False
    statement = node.body[0]
    error_call = statement.exc
    return (
        isinstance(error_call, ast.Call)
        and isinstance(error_call.func, ast.Name)
        and error_call.func.id == "_database_error"
        and not error_call.keywords
        and len(error_call.args) == 2
        and isinstance(error_call.args[0], ast.Constant)
        and error_call.args[0].value == operation
        and isinstance(error_call.args[1], ast.Name)
        and error_call.args[1].id == "error"
        and isinstance(statement.cause, ast.Constant)
        and statement.cause.value is None
    )


def _has_exact_insert_exception_handlers(node: ast.Try) -> bool:
    return (
        not node.orelse
        and not node.finalbody
        and len(node.handlers) == 2
        and _is_exact_passthrough_handler(node.handlers[0])
        and _is_exact_database_handler(
            node.handlers[1],
            operation="insert_event_inbox",
        )
    )


def _lex_sql_statements(statement: str) -> tuple[list[str], bool]:
    """Split SQL outside PostgreSQL quotes/comments; return completeness."""

    statements: list[str] = []
    current: list[str] = []
    index = 0
    length = len(statement)
    state = "normal"
    block_depth = 0
    dollar_tag = ""

    def identifier_continue(character: str) -> bool:
        return character == "_" or character == "$" or character.isalnum()

    while index < length:
        character = statement[index]
        following = statement[index + 1] if index + 1 < length else ""

        if state == "normal":
            if character == "-" and following == "-":
                current.append(" ")
                state = "line_comment"
                index += 2
                continue
            if character == "/" and following == "*":
                current.append(" ")
                state = "block_comment"
                block_depth = 1
                index += 2
                continue
            if character == "'":
                current.append(" ")
                escaped = (
                    index > 0
                    and statement[index - 1] in {"e", "E"}
                    and (index < 2 or not identifier_continue(statement[index - 2]))
                )
                state = "escape_string" if escaped else "single_quote"
                index += 1
                continue
            if character == '"':
                current.append(" ")
                state = "double_quote"
                index += 1
                continue
            if character == "$":
                match = re.match(
                    r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$",
                    statement[index:],
                )
                eligible = index == 0 or not identifier_continue(statement[index - 1])
                if match is not None and eligible:
                    dollar_tag = match.group(0)
                    current.append(" ")
                    state = "dollar_quote"
                    index += len(dollar_tag)
                    continue
                possible_end = statement.find("$", index + 1)
                possible_tag = statement[index + 1 : possible_end]
                if (
                    eligible
                    and possible_end >= 0
                    and possible_tag
                    and not possible_tag.isascii()
                ):
                    state = "invalid"
                    index = length
                    continue
            if character == ";":
                statements.append("".join(current))
                current = []
                index += 1
                continue
            current.append(character)
            index += 1
            continue

        if state == "line_comment":
            if character in "\r\n":
                current.append(character)
                state = "normal"
            index += 1
            continue

        if state == "block_comment":
            if character == "/" and following == "*":
                block_depth += 1
                index += 2
                continue
            if character == "*" and following == "/":
                block_depth -= 1
                index += 2
                if block_depth == 0:
                    state = "normal"
                continue
            index += 1
            continue

        if state == "single_quote":
            if character == "\\":
                state = "invalid"
                index = length
                continue
            if character == "'":
                if following == "'":
                    index += 2
                    continue
                state = "normal"
            index += 1
            continue

        if state == "escape_string":
            if character == "\\" and following:
                index += 2
                continue
            if character == "'":
                if following == "'":
                    index += 2
                    continue
                state = "normal"
            index += 1
            continue

        if state == "double_quote":
            if character == '"':
                if following == '"':
                    index += 2
                    continue
                state = "normal"
            index += 1
            continue

        if state == "dollar_quote":
            if statement.startswith(dollar_tag, index):
                index += len(dollar_tag)
                state = "normal"
                dollar_tag = ""
                continue
            index += 1

    complete = state in {"normal", "line_comment"}
    statements.append("".join(current))
    return statements, complete


def _transaction_control_tokens(statement: str) -> list[str]:
    statements, complete = _lex_sql_statements(statement)
    if not complete:
        return [_UNTERMINATED_SQL]
    return [
        match.group(0).strip().upper()
        for part in statements
        if (match := _TRANSACTION_CONTROL_SQL.search(part)) is not None
    ]


def _is_exact_xid_guard(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Await)
        and isinstance(node.value.value, ast.Call)
        and not node.value.value.args
        and not node.value.value.keywords
        and isinstance(node.value.value.func, ast.Attribute)
        and node.value.value.func.attr == "_assert_transaction_identity"
        and _attribute_chain(node.value.value.func.value) == ("self",)
    )


def _is_exact_caller_validation_assign(node: ast.stmt) -> bool:
    if (
        not isinstance(node, ast.Assign)
        or len(node.targets) != 1
        or not isinstance(node.targets[0], ast.Tuple)
        or tuple(
            element.id if isinstance(element, ast.Name) else None
            for element in node.targets[0].elts
        )
        != ("event", "generation", "fencing_token")
        or not isinstance(node.value, ast.Call)
        or node.value.keywords
        or not isinstance(node.value.func, ast.Attribute)
        or node.value.func.attr != "_validate_insert_inputs"
        or _attribute_chain(node.value.func.value) != ("self", "_repository")
    ):
        return False
    return tuple(
        argument.id if isinstance(argument, ast.Name) else None
        for argument in node.value.args
    ) == ("event", "generation", "fencing_token")


def _is_exact_lock_mode_snapshot(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "for_key_share"
        and isinstance(node.value, ast.Attribute)
        and _attribute_chain(node.value.value) == ("self",)
        and node.value.attr == "_for_key_share"
    )


def _is_exact_payload_assign(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "payload"
        and isinstance(node.value, ast.Call)
        and not node.value.args
        and not node.value.keywords
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "payload_for_storage"
        and _attribute_chain(node.value.func.value) == ("event",)
    )


def _caller_has_exact_body(method: ast.AsyncFunctionDef) -> bool:
    return (
        len(method.body) == 5
        and _is_exact_lock_mode_snapshot(method.body[0])
        and _is_exact_lock_mode_guard(method.body[1])
        and _is_exact_caller_validation_assign(method.body[2])
        and _is_exact_payload_assign(method.body[3])
        and isinstance(method.body[4], ast.Try)
    )


def _is_exact_lock_mode_fragment(node: ast.AST) -> bool:
    if (
        not isinstance(node, ast.IfExp)
        or not isinstance(node.test, ast.Name)
        or node.test.id != "for_key_share"
    ):
        return False

    def sql_literal(expression: ast.expr, expected: str) -> bool:
        return (
            isinstance(expression, ast.Call)
            and not expression.keywords
            and len(expression.args) == 1
            and isinstance(expression.args[0], ast.Constant)
            and expression.args[0].value == expected
            and isinstance(expression.func, ast.Attribute)
            and expression.func.attr == "SQL"
            and _attribute_chain(expression.func.value) == ("sql",)
        )

    return sql_literal(node.body, " FOR KEY SHARE") and sql_literal(node.orelse, "")


def _is_sql_format_call(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "SQL"
        and _attribute_chain(node.func.value) == ("sql",)
    )


def _caller_call_violation(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        if node.func.id not in _CALLER_OWNED_GLOBAL_CALLS:
            return f"restricted_global_call:{node.func.id}"
        return None
    if not isinstance(node.func, ast.Attribute):
        return "restricted_dynamic_call"
    receiver = _attribute_chain(node.func.value)
    if receiver == ("self",):
        if node.func.attr not in _CALLER_OWNED_SELF_CALLS:
            return f"restricted_self_call:{node.func.attr}"
        return None
    if receiver == ("self", "_repository"):
        if node.func.attr not in _CALLER_OWNED_REPOSITORY_HELPERS:
            return f"restricted_repository_call:{node.func.attr}"
        return None
    if receiver == ("self", "_connection"):
        if node.func.attr != "execute":
            return f"restricted_connection_call:{node.func.attr}"
        return None
    if receiver == ("event",) and node.func.attr == "payload_for_storage":
        return None
    if receiver == ("sql",) and node.func.attr == "SQL":
        return None
    if (
        receiver is not None
        and len(receiver) == 1
        and receiver[0] in _CALLER_OWNED_CURSOR_CALLS
        and node.func.attr == "fetchone"
    ):
        return None
    if node.func.attr == "format" and _is_sql_format_call(node.func.value):
        return None
    rendered_receiver = ".".join(receiver or ("dynamic",))
    return f"restricted_receiver_call:{rendered_receiver}.{node.func.attr}"


def _protected_binding_violations(
    nodes: tuple[ast.AST, ...],
    *,
    attributes: frozenset[tuple[str, ...]],
    bindings: dict[str, list[ast.expr]],
) -> list[str]:
    aliases: dict[str, tuple[str, ...]] = {"self": ("self",)}

    def resolve(node: ast.expr) -> tuple[str, ...] | None:
        if isinstance(node, ast.Name):
            return aliases.get(node.id, (node.id,))
        if isinstance(node, ast.Attribute):
            parent = resolve(node.value)
            return (*parent, node.attr) if parent is not None else None
        return None

    changed = True
    while changed:
        changed = False
        for name, values in bindings.items():
            resolved = {resolve(value) for value in values}
            resolved.discard(None)
            if len(resolved) != 1:
                continue
            value = next(iter(resolved))
            if value[0] != "self" or aliases.get(name) == value:
                continue
            aliases[name] = value
            changed = True

    violations: list[str] = []
    for node in nodes:
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            if node.id == "self":
                violations.append("protected_binding_write:self")
        if isinstance(node, ast.pattern) and "self" in _bound_target_names(node):
            violations.append("protected_binding_write:self")
        if isinstance(node, ast.ExceptHandler) and node.name == "self":
            violations.append("protected_binding_write:self")
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
            if node.name == "self":
                violations.append("protected_binding_write:self")
        if isinstance(node, (ast.Import, ast.ImportFrom)) and any(
            (alias.asname or alias.name.split(".", 1)[0]) == "self"
            for alias in node.names
        ):
            violations.append("protected_binding_write:self")
        if isinstance(node, ast.Subscript) and isinstance(
            node.ctx, (ast.Store, ast.Del)
        ):
            base = resolve(node.value)
            if base in {("self", "__dict__"), ("self", "__class__", "__dict__")}:
                violations.append("protected_binding_write:self.__dict__")
        if not isinstance(node, ast.Attribute) or not isinstance(
            node.ctx, (ast.Store, ast.Del)
        ):
            continue
        chain = resolve(node)
        if chain in attributes:
            violations.append("protected_binding_write:" + ".".join(chain))
    return violations


def _nested_scope_violations(method: ast.AsyncFunctionDef) -> list[str]:
    return sorted(
        {
            f"nested_scope:{type(node).__name__}"
            for node in ast.walk(method)
            if node is not method
            and isinstance(
                node,
                (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef, ast.Lambda),
            )
        }
    )


def _class_methods(
    tree: ast.Module,
    class_name: str,
) -> tuple[dict[str, ast.AsyncFunctionDef | ast.FunctionDef], int]:
    classes = [
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    ]
    if len(classes) != 1:
        return {}, len(classes)
    methods: dict[str, ast.AsyncFunctionDef | ast.FunctionDef] = {}
    duplicates: set[str] = set()
    for node in classes[0].body:
        if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            continue
        if node.name in methods:
            duplicates.add(node.name)
        methods[node.name] = node
    for name in duplicates:
        methods.pop(name, None)
    return methods, 1


def _protected_capability_paths(
    tree: ast.Module,
    class_name: str,
) -> frozenset[tuple[str, ...]]:
    methods, _ = _class_methods(tree, class_name)
    paths = {
        ("self", "_connection"),
        ("self", "_pool"),
        ("self", "_repository"),
        *(("self", method_name) for method_name in methods),
    }
    repository_helpers = {
        "_acquire_account_lock",
        "_append_audit",
        "_configure_transaction",
        "_duplicate_receipt",
        "_table",
        "_validate_insert_inputs",
        "insert",
        "transaction",
    }
    if class_name == "EmailEventTransaction":
        paths.update(
            {
                ("self", "_assert_transaction_identity"),
                ("self", "_for_key_share"),
                ("self", "_require_transaction"),
            }
        )
        repository_methods, _ = _class_methods(tree, "InboxRepository")
        paths.update(
            ("self", "_repository", method_name)
            for method_name in repository_methods.keys() | repository_helpers
        )
    elif class_name == "InboxRepository":
        paths.update(("self", method_name) for method_name in repository_helpers)
    return frozenset(paths)


def _expression_uses_connection_capability(
    node: ast.AST,
    tainted_names: set[str],
) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and child.id in tainted_names:
            return True
        if isinstance(child, ast.Attribute):
            chain = _attribute_chain(child)
            if chain is not None and chain[:2] == ("self", "_connection"):
                return True
    return False


def _helper_tainted_names(
    method: ast.AsyncFunctionDef | ast.FunctionDef,
    nodes: tuple[ast.AST, ...],
) -> set[str]:
    tainted = {
        argument.arg
        for argument in (*method.args.posonlyargs, *method.args.args)
        if argument.arg in {"connection", "cursor"}
    }
    changed = True
    while changed:
        changed = False
        for node in nodes:
            value: ast.expr | None = None
            targets: list[ast.AST] = []
            if isinstance(node, ast.Assign):
                value = node.value
                targets = list(node.targets)
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                value = node.value
                targets = [node.target]
            elif isinstance(node, ast.NamedExpr):
                value = node.value
                targets = [node.target]
            if value is None:
                continue
            unwrapped = value.value if isinstance(value, ast.Await) else value
            capability_value = isinstance(
                unwrapped,
                (
                    ast.Attribute,
                    ast.Dict,
                    ast.DictComp,
                    ast.GeneratorExp,
                    ast.List,
                    ast.ListComp,
                    ast.Name,
                    ast.Set,
                    ast.SetComp,
                    ast.Subscript,
                    ast.Tuple,
                ),
            ) and _expression_uses_connection_capability(unwrapped, tainted)
            execute_result = (
                isinstance(unwrapped, ast.Call)
                and isinstance(unwrapped.func, ast.Attribute)
                and unwrapped.func.attr in _SQL_EXECUTION_METHODS
                and _expression_uses_connection_capability(
                    unwrapped.func.value,
                    tainted,
                )
            )
            if not capability_value and not execute_result:
                continue
            for target in targets:
                for name in _bound_target_names(target):
                    if name not in tainted:
                        tainted.add(name)
                        changed = True
    return tainted


def _callee_parameter_accepts_connection(
    method: ast.AsyncFunctionDef | ast.FunctionDef,
    *,
    class_name: str,
    position: int | None,
    keyword: str | None,
) -> bool:
    parameters = [
        *method.args.posonlyargs,
        *method.args.args,
    ]
    if class_name != "<module>" and parameters and parameters[0].arg == "self":
        parameters = parameters[1:]
    if keyword is not None:
        return keyword in {"connection", "cursor"} and any(
            argument.arg == keyword
            for argument in (*parameters, *method.args.kwonlyargs)
        )
    return (
        position is not None
        and position < len(parameters)
        and parameters[position].arg in {"connection", "cursor"}
    )


def _helper_boundary_violations(tree: ast.Module) -> list[str]:
    roots = (
        ("EmailEventTransaction", "_assert_transaction_identity"),
        ("InboxRepository", "_acquire_account_lock"),
        ("InboxRepository", "_configure_transaction"),
        ("InboxRepository", "_duplicate_receipt"),
        ("InboxRepository", "_append_audit"),
        ("InboxRepository", "_validate_insert_inputs"),
        ("InboxRepository", "transaction"),
    )
    class_maps: dict[
        str,
        dict[str, ast.AsyncFunctionDef | ast.FunctionDef],
    ] = {}
    violations: list[str] = []
    for class_name in {class_name for class_name, _ in roots}:
        methods, class_count = _class_methods(tree, class_name)
        class_maps[class_name] = methods
        if class_count != 1:
            violations.append(f"helper_class_count:{class_name}:{class_count}")

    module_methods: dict[str, ast.AsyncFunctionDef | ast.FunctionDef] = {}
    duplicate_module_methods: set[str] = set()
    for node in tree.body:
        if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            continue
        if node.name in module_methods:
            duplicate_module_methods.add(node.name)
        module_methods[node.name] = node
    for name in duplicate_module_methods:
        module_methods.pop(name, None)
    class_maps["<module>"] = module_methods

    pending = list(roots)
    visited: set[tuple[str, str]] = set()
    while pending:
        class_name, method_name = pending.pop()
        key = (class_name, method_name)
        if key in visited:
            continue
        visited.add(key)
        method = class_maps.get(class_name, {}).get(method_name)
        if method is None:
            violations.append(f"helper_missing_callee:{class_name}.{method_name}")
            continue
        if (
            class_name == "InboxRepository"
            and method_name == "transaction"
            and not _has_exact_transaction_factory(method)
        ):
            violations.append("transaction_factory_shape")
        definition_violations = _definition_contract_violations(
            method,
            prefix="definition",
        )
        if (
            class_name == "InboxRepository"
            and method_name == "transaction"
            and _has_exact_transaction_factory(method)
        ):
            definition_violations = [
                item for item in definition_violations if item != "definition_default"
            ]
        if (
            class_name == "InboxRepository"
            and method_name == "_validate_insert_inputs"
            and len(method.decorator_list) == 1
            and isinstance(method.decorator_list[0], ast.Name)
            and method.decorator_list[0].id == "staticmethod"
        ):
            definition_violations = [
                item for item in definition_violations if item != "definition_decorator"
            ]
        violations.extend(
            f"helper_{item}:{class_name}.{method_name}"
            for item in definition_violations
        )
        violations.extend(
            f"helper_{item}:{class_name}.{method_name}"
            for item in _nested_scope_violations(method)
        )
        nodes = tuple(_scope_nodes(method))
        bindings = _scope_bindings(method)
        if class_name != "<module>":
            violations.extend(
                "helper_" + item
                for item in _protected_binding_violations(
                    tuple(ast.walk(method)),
                    attributes=_protected_capability_paths(tree, class_name),
                    bindings=bindings,
                )
            )
        bound_names = _scope_bound_names(method)
        tainted_names = _helper_tainted_names(method, nodes)
        for node in nodes:
            if (
                isinstance(node, ast.Attribute)
                and node.attr in _TRANSACTION_LIFECYCLE_ATTRIBUTES
            ):
                violations.append(f"helper_lifecycle_attribute:{node.attr}")
            if isinstance(node, ast.Attribute):
                chain = _attribute_chain(node)
                if chain is not None and chain[:2] == ("self", "_pool"):
                    violations.append("helper_pool_access")
            if isinstance(node, ast.Return) and node.value is not None:
                if _expression_uses_connection_capability(node.value, tainted_names):
                    is_transaction_wrapper = (
                        class_name == "InboxRepository"
                        and method_name == "transaction"
                        and isinstance(node.value, ast.Call)
                        and isinstance(node.value.func, ast.Name)
                        and node.value.func.id == "EmailEventTransaction"
                    )
                    if not is_transaction_wrapper:
                        violations.append("helper_tainted_connection_escape")
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
                assigned_value = node.value
                if _expression_uses_connection_capability(
                    assigned_value,
                    tainted_names,
                ):
                    targets = (
                        list(node.targets)
                        if isinstance(node, ast.Assign)
                        else [node.target]
                    )
                    if any(not isinstance(target, ast.Name) for target in targets):
                        violations.append("helper_tainted_connection_escape")
            if not isinstance(node, ast.Call):
                continue

            callee: tuple[str, str] | None = None
            if isinstance(node.func, ast.Name) and node.func.id in bound_names:
                violations.append(f"helper_dynamic_call:{node.func.id}")
            elif isinstance(node.func, ast.Name) and node.func.id in module_methods:
                callee = ("<module>", node.func.id)
                pending.append(callee)
            elif (
                isinstance(node.func, ast.Name)
                and node.func.id not in _HELPER_SAFE_EXTERNAL_NAME_CALLS
            ):
                violations.append(f"helper_unresolved_call:{node.func.id}")
            elif not isinstance(node.func, (ast.Name, ast.Attribute)):
                violations.append("helper_dynamic_call")

            query = _execution_query(node)
            if query is None:
                if _call_name(node.func) in _SQL_EXECUTION_METHODS:
                    violations.append("helper_dynamic_sql")
            else:
                statements = _render_sql(query, bindings)
                if len(statements) != 1 or "dynamic_identifier" in statements[0]:
                    violations.append("helper_dynamic_sql")
                else:
                    for token in _transaction_control_tokens(statements[0]):
                        if token == _UNTERMINATED_SQL:
                            violations.append("helper_dynamic_sql")
                        elif (
                            class_name == "InboxRepository"
                            and method_name == "_configure_transaction"
                            and statements[0].strip().upper()
                            == "SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED"
                            and token == "SET LOCAL TRANSACTION"
                        ):
                            continue
                        else:
                            violations.append("helper_transaction_control_sql:" + token)

            receiver = None
            if isinstance(node.func, ast.Attribute):
                receiver = _attribute_chain(node.func.value)
                callee_class: str | None = None
                if receiver == ("self",):
                    callee_class = class_name
                elif class_name == "EmailEventTransaction" and receiver == (
                    "self",
                    "_repository",
                ):
                    callee_class = "InboxRepository"
                if callee_class is not None:
                    callee = (callee_class, node.func.attr)
                    if node.func.attr not in class_maps.get(callee_class, {}):
                        violations.append(
                            f"helper_missing_callee:{callee_class}.{node.func.attr}"
                        )
                        callee = None
                    else:
                        pending.append(callee)
                elif (
                    receiver is not None
                    and receiver[0] == "self"
                    and receiver[:2]
                    not in {
                        ("self", "_connection"),
                        ("self", "_pool"),
                    }
                ):
                    violations.append("helper_dynamic_call:self_receiver")

            receiver_tainted = (
                isinstance(node.func, ast.Name) and node.func.id in tainted_names
            ) or (
                isinstance(node.func, ast.Attribute)
                and _expression_uses_connection_capability(
                    node.func.value,
                    tainted_names,
                )
            )
            if receiver_tainted:
                allowed_effect = isinstance(
                    node.func, ast.Attribute
                ) and node.func.attr in {
                    *_SQL_EXECUTION_METHODS,
                    "fetchall",
                    "fetchmany",
                    "fetchone",
                }
                if not allowed_effect:
                    violations.append("helper_tainted_connection_escape")

            tainted_arguments = [
                (position, None)
                for position, argument in enumerate(node.args)
                if _expression_uses_connection_capability(argument, tainted_names)
            ]
            tainted_arguments.extend(
                (None, keyword.arg)
                for keyword in node.keywords
                if _expression_uses_connection_capability(
                    keyword.value,
                    tainted_names,
                )
            )
            if tainted_arguments:
                constructor_allowed = (
                    class_name == "InboxRepository"
                    and method_name == "transaction"
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "EmailEventTransaction"
                )
                callee_method = (
                    class_maps.get(callee[0], {}).get(callee[1])
                    if callee is not None
                    else None
                )
                parameters_accept = callee_method is not None and all(
                    _callee_parameter_accepts_connection(
                        callee_method,
                        class_name=callee[0],
                        position=position,
                        keyword=keyword,
                    )
                    for position, keyword in tainted_arguments
                )
                if not constructor_allowed and not parameters_accept:
                    violations.append("helper_tainted_connection_escape")
    return sorted(set(violations))


def _caller_owned_insert_violations(source: str) -> list[str]:
    tree = ast.parse(source, filename="<repository-boundary-contract>")
    violations = _runtime_binding_violations(tree)
    violations.extend(
        _module_allowlist_provenance_violations(
            tree,
            _CALLER_MODULE_ALLOWLIST,
        )
    )
    violations.extend(_helper_boundary_violations(tree))
    violations.extend(_transaction_constructor_violations(tree))
    method, class_violations = _pinned_class_method(
        tree,
        class_name="EmailEventTransaction",
        method_name="insert",
    )
    violations.extend(class_violations)
    if method is None:
        return sorted(set(violations))
    violations.extend(_definition_contract_violations(method, prefix="insert"))
    if not _has_exact_insert_signature(method):
        violations.append("insert_signature")
    if not _caller_has_exact_body(method):
        violations.append("caller_body_shape")
    violations.extend(_nested_scope_violations(method))
    scoped_nodes = tuple(_scope_nodes(method))
    bindings = _scope_bindings(method)
    lock_mode_reads = [
        node
        for node in scoped_nodes
        if isinstance(node, ast.Attribute)
        and isinstance(node.ctx, ast.Load)
        and _attribute_chain(node) == ("self", "_for_key_share")
    ]
    if len(lock_mode_reads) != 1:
        violations.append(f"lock_mode_field_read_count:{len(lock_mode_reads)}")
    lock_mode_bindings = bindings.get("for_key_share", [])
    if (
        len(lock_mode_bindings) != 1
        or not isinstance(lock_mode_bindings[0], ast.Attribute)
        or _attribute_chain(lock_mode_bindings[0]) != ("self", "_for_key_share")
    ):
        violations.append("lock_mode_snapshot_shape")
    lock_mode_loads = [
        node
        for node in scoped_nodes
        if isinstance(node, ast.Name)
        and isinstance(node.ctx, ast.Load)
        and node.id == "for_key_share"
    ]
    if len(lock_mode_loads) != 2:
        violations.append(f"lock_mode_local_load_count:{len(lock_mode_loads)}")
    lock_mode_fragments = [
        node for node in scoped_nodes if _is_exact_lock_mode_fragment(node)
    ]
    if len(lock_mode_fragments) != 1:
        violations.append(f"lock_mode_fragment_count:{len(lock_mode_fragments)}")
    violations.extend(
        _protected_binding_violations(
            tuple(ast.walk(method)),
            attributes=_protected_capability_paths(
                tree,
                "EmailEventTransaction",
            ),
            bindings=bindings,
        )
    )
    bound_names = _scope_bound_names(method)
    xid_calls = [
        node
        for node in scoped_nodes
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_assert_transaction_identity"
        and _attribute_chain(node.func.value) == ("self",)
    ]
    if len(xid_calls) != 1:
        violations.append(f"xid_guard_count:{len(xid_calls)}")
    direct_tries = [node for node in method.body if isinstance(node, ast.Try)]
    guard = None
    if (
        len(direct_tries) == 1
        and direct_tries[0].body
        and _is_exact_xid_guard(direct_tries[0].body[0])
    ):
        guard = direct_tries[0].body[0]
    else:
        violations.append("xid_guard_not_first")
    if len(direct_tries) != 1 or not _has_exact_insert_exception_handlers(
        direct_tries[0]
    ):
        violations.append("exception_handlers_shape")

    for node in scoped_nodes:
        if isinstance(node, ast.Attribute):
            if node.attr in _TRANSACTION_LIFECYCLE_ATTRIBUTES:
                violations.append(f"lifecycle_attribute:{node.attr}")
            receiver = _attribute_chain(node.value)
            if (
                receiver == ("self", "_repository")
                and node.attr not in _CALLER_OWNED_REPOSITORY_HELPERS
            ):
                violations.append(f"restricted_repository_call:{node.attr}")

        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id in bound_names:
            violations.append(f"shadowed_callable:{node.func.id}")
        if _call_name(node.func) == "getattr":
            attribute_name = (
                node.args[1].value
                if len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
                else None
            )
            if attribute_name in _TRANSACTION_LIFECYCLE_ATTRIBUTES:
                violations.append(f"lifecycle_getattr:{attribute_name}")
            elif attribute_name is None:
                violations.append("lifecycle_getattr:dynamic")
        call_violation = _caller_call_violation(node)
        if call_violation is not None:
            violations.append(call_violation)
        receiver = (
            _attribute_chain(node.func.value)
            if isinstance(node.func, ast.Attribute)
            else None
        )
        database_call = receiver == ("self", "_connection") or (
            receiver == ("self", "_repository")
            and node.func.attr
            in {"_acquire_account_lock", "_append_audit", "_duplicate_receipt"}
        )
        if database_call and guard is not None and node.lineno < guard.lineno:
            violations.append("database_call_before_xid")

    for node in scoped_nodes:
        if not isinstance(node, ast.Call):
            continue
        query = _execution_query(node)
        if query is None:
            if _call_name(node.func) in _SQL_EXECUTION_METHODS:
                violations.append("dynamic_sql")
            continue
        statements = _render_sql(query, bindings)
        if len(statements) != 1 or "dynamic_identifier" in statements[0]:
            violations.append("dynamic_sql")
            continue
        for token in _transaction_control_tokens(statements[0]):
            if token == _UNTERMINATED_SQL:
                violations.append("dynamic_sql")
            else:
                violations.append("transaction_control_sql:" + token)

    return sorted(set(violations))


def _is_exact_pool_delegation(node: ast.AST) -> bool:
    if not isinstance(node, ast.Return) or not isinstance(node.value, ast.Await):
        return False
    insert_call = node.value.value
    if (
        not isinstance(insert_call, ast.Call)
        or insert_call.keywords
        or not isinstance(insert_call.func, ast.Attribute)
        or insert_call.func.attr != "insert"
        or [
            argument.id if isinstance(argument, ast.Name) else None
            for argument in insert_call.args
        ]
        != ["event", "generation", "fencing_token"]
    ):
        return False
    transaction_call = insert_call.func.value
    return (
        isinstance(transaction_call, ast.Call)
        and not transaction_call.keywords
        and len(transaction_call.args) == 1
        and isinstance(transaction_call.args[0], ast.Name)
        and transaction_call.args[0].id == "connection"
        and isinstance(transaction_call.func, ast.Attribute)
        and transaction_call.func.attr == "transaction"
        and isinstance(transaction_call.func.value, ast.Name)
        and transaction_call.func.value.id == "self"
    )


def _async_with_calls(
    node: ast.AsyncWith,
    receiver: tuple[str, ...],
    method_name: str,
) -> bool:
    if len(node.items) != 1:
        return False
    context = node.items[0].context_expr
    return (
        isinstance(context, ast.Call)
        and not context.args
        and not context.keywords
        and isinstance(context.func, ast.Attribute)
        and context.func.attr == method_name
        and _attribute_chain(context.func.value) == receiver
    )


def _is_exact_configure_call(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Await)
        and isinstance(node.value.value, ast.Call)
        and isinstance(node.value.value.func, ast.Attribute)
        and node.value.value.func.attr == "_configure_transaction"
        and _attribute_chain(node.value.value.func.value) == ("self",)
        and len(node.value.value.args) == 1
        and isinstance(node.value.value.args[0], ast.Name)
        and node.value.value.args[0].id == "connection"
    )


def _is_exact_pool_validation_call(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and not node.value.keywords
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "_validate_insert_inputs"
        and _attribute_chain(node.value.func.value) == ("self",)
        and [
            argument.id if isinstance(argument, ast.Name) else None
            for argument in node.value.args
        ]
        == ["event", "generation", "fencing_token"]
    )


def _is_transaction_insert_call(node: ast.Call) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "insert"
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Attribute)
        and node.func.value.func.attr == "transaction"
        and _attribute_chain(node.func.value.func.value) == ("self",)
    )


def _pool_call_violation(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        if node.func.id not in _POOL_WRAPPER_GLOBAL_CALLS:
            return f"restricted_global_call:{node.func.id}"
        return None
    if not isinstance(node.func, ast.Attribute):
        return "restricted_dynamic_call"
    receiver = _attribute_chain(node.func.value)
    if receiver == ("self",):
        if node.func.attr not in _POOL_WRAPPER_SELF_CALLS:
            return f"restricted_self_call:{node.func.attr}"
        return None
    if receiver == ("self", "_pool") and node.func.attr == "connection":
        return None
    if receiver == ("connection",) and node.func.attr == "transaction":
        return None
    if _is_transaction_insert_call(node):
        return None
    rendered_receiver = ".".join(receiver or ("dynamic",))
    return f"restricted_receiver_call:{rendered_receiver}.{node.func.attr}"


def _pool_insert_delegation_violations(source: str) -> list[str]:
    tree = ast.parse(source, filename="<repository-boundary-contract>")
    violations = _runtime_binding_violations(tree)
    violations.extend(
        _module_allowlist_provenance_violations(
            tree,
            _POOL_MODULE_ALLOWLIST,
        )
    )
    violations.extend(_helper_boundary_violations(tree))
    violations.extend(_transaction_constructor_violations(tree))
    method, class_violations = _pinned_class_method(
        tree,
        class_name="InboxRepository",
        method_name="insert",
    )
    violations.extend(class_violations)
    if method is None:
        return sorted(set(violations))
    violations.extend(_definition_contract_violations(method, prefix="insert"))
    if not _has_exact_insert_signature(method):
        violations.append("insert_signature")
    violations.extend(_nested_scope_violations(method))
    scoped_nodes = tuple(_scope_nodes(method))
    bindings = _scope_bindings(method)
    violations.extend(
        _protected_binding_violations(
            tuple(ast.walk(method)),
            attributes=_protected_capability_paths(tree, "InboxRepository"),
            bindings=bindings,
        )
    )
    bound_names = _scope_bound_names(method)
    for node in scoped_nodes:
        if isinstance(node, (ast.AsyncFor, ast.For, ast.If, ast.Match, ast.While)):
            violations.append(f"unexpected_control_flow:{type(node).__name__}")
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in bound_names:
                violations.append(f"shadowed_callable:{node.func.id}")
            call_violation = _pool_call_violation(node)
            if call_violation is not None:
                violations.append(call_violation)

    exact_returns = [node for node in scoped_nodes if _is_exact_pool_delegation(node)]
    returns = [node for node in scoped_nodes if isinstance(node, ast.Return)]
    if len(returns) != 1:
        violations.append(f"return_count:{len(returns)}")
    delegations = [
        node
        for node in scoped_nodes
        if isinstance(node, ast.Call) and _is_transaction_insert_call(node)
    ]
    if len(delegations) != 1:
        violations.append(f"delegation_count:{len(delegations)}")
    if len(exact_returns) != 1:
        violations.append(f"exact_delegation_count:{len(exact_returns)}")
        return sorted(set(violations))

    target = exact_returns[0]
    parents = {
        child: parent
        for parent in ast.walk(method)
        for child in ast.iter_child_nodes(parent)
    }
    ancestors: list[ast.AST] = []
    current: ast.AST = target
    while current in parents:
        current = parents[current]
        ancestors.append(current)

    conditional_types = (ast.AsyncFor, ast.For, ast.If, ast.Match, ast.While)
    if any(isinstance(ancestor, conditional_types) for ancestor in ancestors):
        violations.append("delegation_not_on_live_path")

    pool_contexts = [
        node
        for node in scoped_nodes
        if isinstance(node, ast.AsyncWith)
        and _async_with_calls(node, ("self", "_pool"), "connection")
    ]
    if len(pool_contexts) != 1:
        violations.append(f"pool_context_count:{len(pool_contexts)}")
        pool_context = None
    else:
        pool_context = pool_contexts[0]
        binding = pool_context.items[0].optional_vars
        if not isinstance(binding, ast.Name) or binding.id != "connection":
            violations.append("pool_connection_binding")

    transaction_contexts = [
        node
        for node in scoped_nodes
        if isinstance(node, ast.AsyncWith)
        and len(node.items) == 1
        and isinstance(node.items[0].context_expr, ast.Call)
        and isinstance(node.items[0].context_expr.func, ast.Attribute)
        and node.items[0].context_expr.func.attr == "transaction"
    ]
    if len(transaction_contexts) != 1:
        violations.append(f"transaction_context_count:{len(transaction_contexts)}")
        transaction_context = None
    else:
        transaction_context = transaction_contexts[0]
        if (
            not _async_with_calls(
                transaction_context,
                ("connection",),
                "transaction",
            )
            or transaction_context.items[0].optional_vars is not None
        ):
            violations.append("transaction_connection_binding")

    try_node = method.body[1] if len(method.body) == 2 else None
    if (
        len(method.body) != 2
        or not _is_exact_pool_validation_call(method.body[0])
        or not isinstance(try_node, ast.Try)
    ):
        violations.append("method_body_shape")
    elif (
        try_node.orelse
        or try_node.finalbody
        or len(try_node.body) != 1
        or try_node.body[0] is not pool_context
    ):
        violations.append("try_body_shape")

    if pool_context is None or pool_context.body != [transaction_context]:
        violations.append("pool_body_shape")
    if (
        transaction_context is None
        or len(transaction_context.body) != 2
        or transaction_context.body[1] is not target
        or not _is_exact_configure_call(transaction_context.body[0])
    ):
        violations.append("transaction_body_shape")

    if not isinstance(try_node, ast.Try) or not _has_exact_insert_exception_handlers(
        try_node
    ):
        violations.append("exception_handlers_shape")

    return sorted(set(violations))


def test_caller_owned_insert_pins_xid_without_managing_transaction_lifecycle() -> None:
    project_root = Path(__file__).resolve().parents[2]
    path = project_root / "src" / "ingestion" / "repository.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    method = _class_method(tree, "EmailEventTransaction", "insert")
    called_attributes = {
        node.func.attr
        for node in ast.walk(method)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }

    assert "_assert_transaction_identity" in called_attributes
    assert _caller_owned_insert_violations(source) == []


def test_pool_owned_insert_delegates_to_the_caller_owned_primitive() -> None:
    project_root = Path(__file__).resolve().parents[2]
    path = project_root / "src" / "ingestion" / "repository.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    method = _class_method(tree, "InboxRepository", "insert")

    delegated = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "insert"
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Attribute)
        and node.func.value.func.attr == "transaction"
        for node in ast.walk(method)
    )
    called_attributes = {
        node.func.attr
        for node in ast.walk(method)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }

    assert delegated is True
    assert _pool_insert_delegation_violations(source) == []
    assert called_attributes.isdisjoint(
        {"execute", "_acquire_account_lock", "_duplicate_receipt", "_append_audit"}
    )


def test_caller_owned_detector_rejects_alias_getattr_and_transaction_sql() -> None:
    source = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        finish = self._connection.commit
        await finish()
        await getattr(self._connection, "rollback")()
        first = "SET " + "TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        await self._connection.execute(first)
        await self._connection.execute("BEGIN")
        await self._connection.execute("SAVEPOINT nested")
        await self._connection.execute("RELEASE SAVEPOINT nested")
        await self._connection.execute("ROLLBACK TO SAVEPOINT nested")
"""

    violations = _caller_owned_insert_violations(source)

    assert "lifecycle_attribute:commit" in violations
    assert "lifecycle_getattr:rollback" in violations
    assert sum(item.startswith("transaction_control_sql:") for item in violations) == 5


def test_caller_owned_detector_rejects_transaction_helper_bypass() -> None:
    source = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        await self._repository._run_in_new_transaction(event)
"""

    assert (
        "restricted_repository_call:_run_in_new_transaction"
        in _caller_owned_insert_violations(source)
    )


def test_pool_wrapper_detector_rejects_dead_or_non_exact_delegation() -> None:
    dead = """
class InboxRepository:
    async def insert(self, event, generation, fencing_token):
        if False:
            return await self.transaction(connection).insert(
                event, generation, fencing_token
            )
        return IngressReceipt("forged", False)
"""
    aliased = """
class InboxRepository:
    async def insert(self, event, generation, fencing_token):
        delegate = self.transaction(connection).insert
        return await delegate(event, generation, fencing_token)
"""
    nested = """
class InboxRepository:
    async def insert(self, event, generation, fencing_token):
        async def unreachable(connection):
            return await self.transaction(connection).insert(
                event, generation, fencing_token
            )
        return IngressReceipt("forged", False)
"""

    assert "delegation_not_on_live_path" in _pool_insert_delegation_violations(dead)
    assert "exact_delegation_count:0" in _pool_insert_delegation_violations(aliased)
    assert "exact_delegation_count:0" in _pool_insert_delegation_violations(nested)


def test_caller_owned_detector_rejects_unknown_helpers_and_execute_alias() -> None:
    source = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        execute = self._connection.execute
        await execute("COMMIT")
        await run_with_connection(self._connection, event)
        await self._run_with_connection(self._connection, event)
"""

    violations = _caller_owned_insert_violations(source)

    assert "transaction_control_sql:COMMIT" in violations
    assert "restricted_global_call:execute" in violations
    assert "restricted_global_call:run_with_connection" in violations
    assert "restricted_self_call:_run_with_connection" in violations


def test_pool_wrapper_detector_rejects_extra_control_and_helpers() -> None:
    source = """
class InboxRepository:
    async def insert(self, event, generation, fencing_token):
        self._validate_insert_inputs(event, generation, fencing_token)
        async with self._pool.connection() as connection:
            async with connection.transaction():
                if event:
                    await self._shadow_insert(connection, event)
                await record_connection(connection)
                await self._configure_transaction(connection)
                return await self.transaction(connection).insert(
                    event, generation, fencing_token
                )
"""

    violations = _pool_insert_delegation_violations(source)

    assert "unexpected_control_flow:If" in violations
    assert "restricted_self_call:_shadow_insert" in violations
    assert "restricted_global_call:record_connection" in violations


def test_caller_owned_detector_rejects_shadow_dynamic_sql_and_dead_xid() -> None:
    shadowed = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            str = self._connection.execute
            await str("COMMIT")
        except Exception:
            raise
"""
    dynamic_sql = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            command = "COMMIT"
            await self._connection.execute(f"{command}")
        except Exception:
            raise
"""
    dead_xid = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        if False:
            await self._assert_transaction_identity()
"""
    local_function = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            async def str(query):
                return await self._connection.execute(query)
            await str("COMMIT")
        except Exception:
            raise
"""
    database_first = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        await self._connection.execute("SELECT 1")
        try:
            await self._assert_transaction_identity()
        except Exception:
            raise
"""

    assert "shadowed_callable:str" in _caller_owned_insert_violations(shadowed)
    assert "dynamic_sql" in _caller_owned_insert_violations(dynamic_sql)
    assert "xid_guard_not_first" in _caller_owned_insert_violations(dead_xid)
    assert "shadowed_callable:str" in _caller_owned_insert_violations(local_function)
    assert "database_call_before_xid" in _caller_owned_insert_violations(database_first)


def test_caller_owned_detector_rejects_commented_and_multistatement_control_sql() -> (
    None
):
    commented = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            await self._connection.execute("/* guard */ COMMIT")
        except Exception:
            raise
"""
    multistatement = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            await self._connection.execute("SELECT 1; COMMIT")
        except Exception:
            raise
"""

    assert "transaction_control_sql:COMMIT" in _caller_owned_insert_violations(
        commented
    )
    assert "transaction_control_sql:COMMIT" in _caller_owned_insert_violations(
        multistatement
    )


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            """
class InboxRepository:
    async def insert(self, event, generation, fencing_token):
        self._validate_insert_inputs(event, generation, fencing_token)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    return None
                    return await self.transaction(connection).insert(
                        event, generation, fencing_token
                    )
        except Exception:
            raise
""",
            "return_count:2",
        ),
        (
            """
class InboxRepository:
    async def insert(self, event, generation, fencing_token):
        self._validate_insert_inputs(event, generation, fencing_token)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self.transaction(connection).insert(
                        event, generation, fencing_token
                    )
                    return await self.transaction(connection).insert(
                        event, generation, fencing_token
                    )
        except Exception:
            raise
""",
            "delegation_count:2",
        ),
        (
            """
class InboxRepository:
    async def insert(self, event, generation, fencing_token):
        self._validate_insert_inputs(event, generation, fencing_token)
        connection = forged
        try:
            async with self._pool.connection() as pool_connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    return await self.transaction(connection).insert(
                        event, generation, fencing_token
                    )
        except Exception:
            raise
""",
            "pool_connection_binding",
        ),
        (
            """
class InboxRepository:
    async def insert(self, event, generation, fencing_token):
        self._validate_insert_inputs(event, generation, fencing_token)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    async with connection.transaction():
                        pass
                    return await self.transaction(connection).insert(
                        event, generation, fencing_token
                    )
        except Exception:
            raise
""",
            "transaction_context_count:2",
        ),
    ],
)
def test_pool_wrapper_detector_requires_exact_single_skeleton(
    source: str,
    expected: str,
) -> None:
    assert expected in _pool_insert_delegation_violations(source)


@pytest.mark.parametrize(
    ("write", "expected"),
    [
        ("self = forged", "protected_binding_write:self"),
        (
            "self._connection = forged",
            "protected_binding_write:self._connection",
        ),
        (
            "self._assert_transaction_identity = forged",
            "protected_binding_write:self._assert_transaction_identity",
        ),
    ],
)
def test_caller_owned_detector_rejects_protected_binding_rewrites(
    write: str,
    expected: str,
) -> None:
    source = f"""
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            {write}
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert expected in _caller_owned_insert_violations(source)


@pytest.mark.parametrize(
    ("pattern", "call", "name"),
    [
        ("_ as str", "str(event)", "str"),
        ("[*Jsonb]", "Jsonb(event)", "Jsonb"),
        ("{**uuid4}", "uuid4()", "uuid4"),
    ],
)
def test_caller_owned_detector_rejects_match_pattern_allowlist_shadowing(
    pattern: str,
    call: str,
    name: str,
) -> None:
    source = f"""
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            match event:
                case {pattern}:
                    pass
            {call}
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert f"shadowed_callable:{name}" in _caller_owned_insert_violations(source)


def test_caller_owned_detector_rejects_module_allowlist_override() -> None:
    source = """
str = forged

class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            str(event)
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert "module_allowlist_override:str" in _caller_owned_insert_violations(source)


@pytest.mark.parametrize(
    ("class_name", "detector"),
    [
        ("EmailEventTransaction", _caller_owned_insert_violations),
        ("InboxRepository", _pool_insert_delegation_violations),
    ],
)
def test_insert_detector_rejects_decorators(class_name: str, detector: object) -> None:
    source = f"""
class {class_name}:
    @transactional
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert "insert_decorator" in detector(source)  # type: ignore[operator]


def test_caller_owned_detector_checks_nested_definition_time_effects() -> None:
    source = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            def nested(value=self._connection.commit()):
                pass
            callback = lambda value=self._connection.rollback(): value
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    violations = _caller_owned_insert_violations(source)

    assert "lifecycle_attribute:commit" in violations
    assert "lifecycle_attribute:rollback" in violations


def test_caller_owned_detector_checks_allowlisted_helpers_transitively() -> None:
    source = """
class InboxRepository:
    async def _acquire_account_lock(self, connection, account_id):
        await self._lock_impl(connection)

    async def _lock_impl(self, connection):
        await connection.execute("SELECT 1; COMMIT")

class EmailEventTransaction:
    async def _assert_transaction_identity(self):
        await self._identity_impl()

    async def _identity_impl(self):
        self._connection.rollback()

    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            await self._repository._acquire_account_lock(
                self._connection, event.account_id
            )
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    violations = _caller_owned_insert_violations(source)

    assert "helper_transaction_control_sql:COMMIT" in violations
    assert "helper_lifecycle_attribute:rollback" in violations


def test_transaction_sql_detector_is_quote_aware() -> None:
    quoted = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            await self._connection.execute("SELECT '; COMMIT'")
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""
    real = quoted.replace("SELECT '; COMMIT'", "SELECT '; COMMIT'; COMMIT")

    assert not any(
        item.startswith("transaction_control_sql:")
        for item in _caller_owned_insert_violations(quoted)
    )
    assert "transaction_control_sql:COMMIT" in _caller_owned_insert_violations(real)


@pytest.mark.parametrize(
    "handler",
    [
        "except BaseException:\n            return forged",
        "except asyncio.CancelledError:\n            return forged",
        (
            "except Exception:\n"
            "            await self._connection.execute('SELECT 1')\n"
            "            raise"
        ),
    ],
)
def test_caller_owned_detector_requires_exact_exception_handlers(handler: str) -> None:
    source = f"""
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        {handler}
"""

    assert "exception_handlers_shape" in _caller_owned_insert_violations(source)


def test_pool_detector_requires_exact_exception_handlers_without_checkout() -> None:
    source = """
class InboxRepository:
    async def insert(self, event, generation, fencing_token):
        self._validate_insert_inputs(event, generation, fencing_token)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    return await self.transaction(connection).insert(
                        event, generation, fencing_token
                    )
        except BaseException:
            async with self._pool.connection() as ignored:
                pass
            raise
"""

    assert "exception_handlers_shape" in _pool_insert_delegation_violations(source)


def test_caller_detector_rejects_duplicate_class_and_insert_bindings() -> None:
    duplicate_class = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None

class EmailEventTransaction:
    insert = forged
"""
    duplicate_insert = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None

    async def insert(self, event, generation, fencing_token):
        return forged
"""
    rebound_insert = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None

    insert = forged
"""

    assert "class_definition_count:EmailEventTransaction:2" in (
        _caller_owned_insert_violations(duplicate_class)
    )
    assert "insert_definition_count:2" in _caller_owned_insert_violations(
        duplicate_insert
    )
    assert "insert_rebound" in _caller_owned_insert_violations(rebound_insert)


@pytest.mark.parametrize(
    "class_header",
    [
        "@decorate\nclass EmailEventTransaction:",
        "class EmailEventTransaction(Base):",
        "class EmailEventTransaction(metaclass=Meta):",
    ],
)
def test_caller_detector_rejects_class_customization(class_header: str) -> None:
    source = f"""
{class_header}
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert "class_contract:EmailEventTransaction" in (
        _caller_owned_insert_violations(source)
    )


def test_insert_detectors_require_exact_signatures() -> None:
    caller = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token, extra):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""
    pool = caller.replace("EmailEventTransaction", "InboxRepository")

    assert "insert_signature" in _caller_owned_insert_violations(caller)
    assert "insert_signature" in _pool_insert_delegation_violations(pool)


def test_caller_detector_rejects_dead_or_extra_prelude() -> None:
    source = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        return forged
        event, generation, fencing_token = self._repository._validate_insert_inputs(
            event, generation, fencing_token
        )
        payload = event.payload_for_storage()
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert "caller_body_shape" in _caller_owned_insert_violations(source)


def test_caller_detector_rejects_nested_scope_even_when_never_called() -> None:
    source = """
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            def hidden():
                self._connection.commit()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert "nested_scope:FunctionDef" in _caller_owned_insert_violations(source)


@pytest.mark.parametrize(
    ("write", "expected"),
    [
        (
            "self.__dict__['_connection'] = forged",
            "protected_binding_write:self.__dict__",
        ),
        (
            "alias = self\n            alias._connection = forged",
            "protected_binding_write:self._connection",
        ),
        (
            "match event:\n                case _ as self:\n                    pass",
            "protected_binding_write:self",
        ),
    ],
)
def test_caller_detector_rejects_indirect_protected_writes(
    write: str,
    expected: str,
) -> None:
    source = f"""
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
            {write}
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert expected in _caller_owned_insert_violations(source)


def test_module_provenance_pins_database_exception_tuple() -> None:
    source = """
_DATABASE_EXCEPTIONS = (BaseException,)

class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert "module_allowlist_override:_DATABASE_EXCEPTIONS" in (
        _caller_owned_insert_violations(source)
    )


def test_transaction_sql_lexer_handles_postgres_quotes_and_nested_comments() -> None:
    safe_queries = (
        'SELECT "semi; COMMIT"',
        "SELECT $$semi; COMMIT$$",
        "SELECT $tag$semi; COMMIT$tag$",
        "SELECT 1 /* outer /* ; COMMIT */ still outer */",
        "SELECT E'escaped\\'; COMMIT'",
    )

    for query in safe_queries:
        assert _transaction_control_tokens(query) == []
    assert _transaction_control_tokens("SELECT $$; COMMIT$$; COMMIT") == ["COMMIT"]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 'unterminated",
        'SELECT "unterminated',
        "SELECT $tag$unterminated",
        "SELECT /* unterminated",
    ],
)
def test_transaction_sql_lexer_fails_closed_on_unterminated_input(
    query: str,
) -> None:
    assert _transaction_control_tokens(query) == [_UNTERMINATED_SQL]


def test_helper_graph_rejects_decorator_default_dynamic_and_missing_callee() -> None:
    source = """
class InboxRepository:
    @decorate
    async def _acquire_account_lock(self, connection, account_id=side_effect()):
        callback = self._missing
        await callback(connection)

class EmailEventTransaction:
    async def _assert_transaction_identity(self):
        await self._missing_identity()

    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    violations = _caller_owned_insert_violations(source)

    assert any(item.startswith("helper_definition_decorator:") for item in violations)
    assert any(item.startswith("helper_definition_default:") for item in violations)
    assert "helper_dynamic_call:callback" in violations
    assert any(item.startswith("helper_missing_callee:") for item in violations)


@pytest.mark.parametrize(
    "tail", ["else:\n            pass", "finally:\n            pass"]
)
def test_caller_exception_contract_rejects_else_and_finally(tail: str) -> None:
    source = f"""
class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
        {tail}
"""

    assert "exception_handlers_shape" in _caller_owned_insert_violations(source)


def test_helper_graph_includes_pool_wrapper_helpers_and_local_functions() -> None:
    source = """
def local_effect(connection):
    return connection.execute("SELECT 1; COMMIT")

class InboxRepository:
    async def _configure_transaction(self, connection):
        await local_effect(connection)

    async def _acquire_account_lock(self, connection, account_id):
        pass

    async def _duplicate_receipt(self, connection, event):
        pass

    async def _append_audit(self, connection, **kwargs):
        pass

    @staticmethod
    def _validate_insert_inputs(event, generation, fencing_token):
        return event, generation, fencing_token

    def transaction(self, connection):
        return EmailEventTransaction(self, connection)

class EmailEventTransaction:
    async def _assert_transaction_identity(self):
        pass

    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert "helper_transaction_control_sql:COMMIT" in (
        _caller_owned_insert_violations(source)
    )


def test_helper_graph_rejects_tainted_connection_escape_and_pool_checkout() -> None:
    source = """
class InboxRepository:
    async def _acquire_account_lock(self, connection, account_id):
        await external(connection)
        async with self._pool.connection() as second_connection:
            pass

class EmailEventTransaction:
    async def _assert_transaction_identity(self):
        pass

    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    violations = _caller_owned_insert_violations(source)

    assert "helper_tainted_connection_escape" in violations
    assert "helper_pool_access" in violations


def test_helper_graph_rejects_nested_closure() -> None:
    source = """
class InboxRepository:
    async def _acquire_account_lock(self, connection, account_id):
        def hidden():
            connection.commit()

class EmailEventTransaction:
    async def _assert_transaction_identity(self):
        pass

    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert any(
        item.startswith("helper_nested_scope:FunctionDef")
        for item in _caller_owned_insert_violations(source)
    )


def test_sql_lexer_fails_closed_for_ambiguous_quotes_and_tags() -> None:
    assert _transaction_control_tokens("SELECT 'ambiguous\\\\value'") == [
        _UNTERMINATED_SQL
    ]
    assert _transaction_control_tokens("SELECT $é$hidden$é$") == [_UNTERMINATED_SQL]
    assert _transaction_control_tokens("SELECT identifier$tag$") == []


def test_module_provenance_rejects_sql_rebinding() -> None:
    source = """
from psycopg import sql
sql = forged

class EmailEventTransaction:
    async def insert(self, event, generation, fencing_token):
        try:
            await self._assert_transaction_identity()
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None
"""

    assert "module_allowlist_override:sql" in _caller_owned_insert_violations(source)


def _production_repository_source() -> str:
    project_root = Path(__file__).resolve().parents[2]
    return (project_root / "src" / "ingestion" / "repository.py").read_text(
        encoding="utf-8"
    )


@pytest.mark.parametrize(
    "mutation",
    [
        'globals()["str"] = lambda value: value',
        "EmailEventTransaction = InboxRepository",
        "EmailEventTransaction.insert = forged",
        "del EmailEventTransaction.insert",
        'setattr(EmailEventTransaction, "insert", forged)',
        "InboxRepository._acquire_account_lock = forged",
        "EmailEventTransaction._assert_transaction_identity = forged",
    ],
)
def test_runtime_binding_gate_rejects_module_namespace_mutation(
    mutation: str,
) -> None:
    source = _production_repository_source() + "\n" + mutation + "\n"

    assert "module_runtime_binding_mutation" in _caller_owned_insert_violations(source)
    assert "module_runtime_binding_mutation" in _pool_insert_delegation_violations(
        source
    )


@pytest.mark.parametrize(
    "injection",
    [
        "    _acquire_account_lock = forged\n",
        "    from somewhere import forged as _acquire_account_lock\n",
        (
            "    match forged:\n"
            "        case _ as _acquire_account_lock:\n"
            "            pass\n"
        ),
        (
            "    try:\n"
            "        pass\n"
            "    except Exception as _acquire_account_lock:\n"
            "        pass\n"
        ),
        '    locals()["_acquire_account_lock"] = forged\n',
        '    registry["_acquire_account_lock"] = forged\n',
        "    del registry._acquire_account_lock\n",
        '    setattr(registry, "_acquire_account_lock", forged)\n',
    ],
)
def test_runtime_binding_gate_rejects_class_namespace_mutation(
    injection: str,
) -> None:
    source = _production_repository_source()
    marker = "\n\nclass EmailEventTransaction:"
    assert marker in source
    source = source.replace(marker, "\n" + injection + marker, 1)

    assert "class_runtime_binding_mutation:InboxRepository" in (
        _caller_owned_insert_violations(source)
    )
    assert "class_runtime_binding_mutation:InboxRepository" in (
        _pool_insert_delegation_violations(source)
    )


def test_runtime_binding_gate_rejects_email_helper_rebinding() -> None:
    source = _production_repository_source()
    prefix, email_class = source.split("class EmailEventTransaction:", 1)
    marker = "\n    async def insert("
    assert marker in email_class
    email_class = email_class.replace(
        marker,
        "\n    _assert_transaction_identity = forged\n" + marker,
        1,
    )
    source = prefix + "class EmailEventTransaction:" + email_class

    assert "class_runtime_binding_mutation:EmailEventTransaction" in (
        _caller_owned_insert_violations(source)
    )


@pytest.mark.parametrize(
    "class_body",
    [
        "    EmailEventTransaction.insert = InboxRepository.insert\n",
        '    globals()["str"] = lambda value: value\n',
    ],
)
def test_module_runtime_gate_enters_immediately_executed_class_bodies(
    class_body: str,
) -> None:
    source = _production_repository_source() + "\nclass Mutator:\n" + class_body

    assert "module_runtime_binding_mutation" in _caller_owned_insert_violations(source)


@pytest.mark.parametrize(
    "replacement",
    [
        (
            "        return EmailEventTransaction(\n"
            "            self,\n"
            "            forged_connection,\n"
            "            for_key_share=for_key_share,\n"
            "        )"
        ),
        (
            "        return EmailEventTransaction(\n"
            "            self,\n"
            "            connection,\n"
            "            for_key_share=forged_lock_mode,\n"
            "        )"
        ),
    ],
    ids=["connection", "lock-mode"],
)
def test_transaction_factory_requires_exact_connection_forwarding(
    replacement: str,
) -> None:
    original = (
        "        return EmailEventTransaction(\n"
        "            self,\n"
        "            connection,\n"
        "            for_key_share=for_key_share,\n"
        "        )"
    )
    production = _production_repository_source()
    assert production.count(original) == 1
    source = production.replace(original, replacement, 1)
    assert source != production

    assert "transaction_factory_shape" in _caller_owned_insert_violations(source)
    assert "transaction_factory_shape" in _pool_insert_delegation_violations(source)


@pytest.mark.parametrize(
    "replacement",
    [
        "        self._repository = repository\n",
        (
            "        self._repository = repository\n"
            "        if type(for_key_share) is not bool:\n"
            '            raise ValueError("for_key_share must be an exact boolean")\n'
        ),
    ],
    ids=["deleted", "delayed"],
)
def test_transaction_constructor_guard_precedes_all_state_binding(
    replacement: str,
) -> None:
    original = (
        "        if type(for_key_share) is not bool:\n"
        '            raise ValueError("for_key_share must be an exact boolean")\n'
        "        self._repository = repository\n"
    )
    production = _production_repository_source()
    assert production.count(original) == 1
    source = production.replace(original, replacement, 1)
    assert source != production

    assert "transaction_constructor_shape" in _caller_owned_insert_violations(source)
    assert "transaction_constructor_shape" in _pool_insert_delegation_violations(source)


@pytest.mark.parametrize(
    "variant",
    ["deleted", "delayed", "truthiness"],
)
def test_transaction_insert_guard_is_exact_and_precedes_input_consumption(
    variant: str,
) -> None:
    snapshot = "        for_key_share = self._for_key_share\n"
    guard = (
        "        if type(for_key_share) is not bool:\n"
        '            raise ValueError("for_key_share must be an exact boolean")\n'
    )
    truthiness_guard = (
        "        if not for_key_share:\n"
        '            raise ValueError("for_key_share must be an exact boolean")\n'
    )
    validation = (
        "        event, generation, fencing_token = "
        "self._repository._validate_insert_inputs(\n"
        "            event,\n"
        "            generation,\n"
        "            fencing_token,\n"
        "        )\n"
    )
    suffix = (
        "        payload = event.payload_for_storage()\n"
        "        try:\n"
        "            await self._assert_transaction_identity()\n"
    )
    original = snapshot + guard + validation + suffix
    replacement = {
        "deleted": snapshot + validation + suffix,
        "delayed": snapshot + validation + guard + suffix,
        "truthiness": snapshot + truthiness_guard + validation + suffix,
    }[variant]
    production = _production_repository_source()
    assert production.count(original) == 1
    source = production.replace(original, replacement, 1)
    assert source != production

    assert "caller_body_shape" in _caller_owned_insert_violations(source)


def test_transaction_insert_rejects_post_await_field_reread() -> None:
    production = _production_repository_source()
    original = 'if for_key_share else sql.SQL("")'
    replacement = 'if self._for_key_share else sql.SQL("")'
    assert production.count(original) == 1
    source = production.replace(original, replacement, 1)
    assert source != production

    violations = _caller_owned_insert_violations(source)
    assert "lock_mode_field_read_count:2" in violations
    assert "lock_mode_fragment_count:0" in violations


@pytest.mark.parametrize(
    ("write", "expected"),
    [
        (
            "            self._for_key_share = for_key_share\n",
            "protected_binding_write:self._for_key_share",
        ),
        (
            "            for_key_share = False\n",
            "lock_mode_snapshot_shape",
        ),
    ],
    ids=["field-write", "local-rebind"],
)
def test_transaction_insert_rejects_lock_mode_write_bypass(
    write: str,
    expected: str,
) -> None:
    production = _production_repository_source()
    marker = (
        "        payload = event.payload_for_storage()\n"
        "        try:\n"
        "            await self._assert_transaction_identity()\n"
    )
    assert production.count(marker) == 1
    source = production.replace(marker, marker + write, 1)
    assert source != production

    assert expected in _caller_owned_insert_violations(source)


@pytest.mark.parametrize(
    ("write", "expected"),
    [
        (
            "            self._repository._acquire_account_lock = forged\n",
            "protected_binding_write:self._repository._acquire_account_lock",
        ),
        (
            "            self.insert = forged\n",
            "protected_binding_write:self.insert",
        ),
    ],
)
def test_caller_protects_repository_and_insert_capabilities(
    write: str,
    expected: str,
) -> None:
    source = _production_repository_source()
    prefix, email_class = source.split("class EmailEventTransaction:", 1)
    guard = "        try:\n            await self._assert_transaction_identity()\n"
    assert guard in email_class
    email_class = email_class.replace(guard, guard + write, 1)
    source = prefix + "class EmailEventTransaction:" + email_class

    assert expected in _caller_owned_insert_violations(source)


def test_reachable_helper_protects_repository_capabilities() -> None:
    source = _production_repository_source()
    marker = "        self._require_transaction()\n"
    assert marker in source
    source = source.replace(
        marker,
        marker + "        self._repository._acquire_account_lock = forged\n",
        1,
    )

    assert (
        "helper_protected_binding_write:self._repository._acquire_account_lock"
        in _caller_owned_insert_violations(source)
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("type", "module_allowlist_override:type"),
        ("staticmethod", "module_allowlist_override:staticmethod"),
        (
            "ownership_advisory_lock_key",
            "module_allowlist_override:ownership_advisory_lock_key",
        ),
        ("_AuditInvariantError", "module_allowlist_override:_AuditInvariantError"),
    ],
)
def test_helper_safe_names_have_exact_module_provenance(
    name: str,
    expected: str,
) -> None:
    source = _production_repository_source() + f"\n{name} = forged\n"

    assert expected in _caller_owned_insert_violations(source)


@pytest.mark.parametrize(
    "name",
    ["NormalizedIngressEvent", "TransactionStatus"],
)
def test_transaction_boundary_dependencies_have_exact_module_provenance(
    name: str,
) -> None:
    source = _production_repository_source() + f"\n{name} = forged\n"
    expected = f"module_allowlist_override:{name}"

    assert expected in _caller_owned_insert_violations(source)
    assert expected in _pool_insert_delegation_violations(source)


@pytest.mark.parametrize(
    "container",
    [
        "[connection]",
        "[item for item in (connection,)]",
    ],
)
def test_helper_taint_propagates_through_container_and_subscript(
    container: str,
) -> None:
    source = _production_repository_source()
    marker = """    async def _acquire_account_lock(
        self,
        connection: psycopg.AsyncConnection[Any],
        account_id: int,
    ) -> None:
"""
    assert marker in source
    source = source.replace(
        marker,
        marker + f"        box = {container}\n" + "        self._connection = box[0]\n",
        1,
    )

    assert "helper_tainted_connection_escape" in _caller_owned_insert_violations(source)


@pytest.mark.parametrize(
    "trigger",
    [
        "class Trigger:\n    mutate()\n",
        "class Outer:\n    class Inner:\n        mutate()\n",
        "@mutate\nclass Trigger:\n    pass\n",
    ],
)
def test_module_runtime_gate_rejects_import_time_local_function_effects(
    trigger: str,
) -> None:
    source = (
        _production_repository_source()
        + "\ndef mutate(target):\n"
        + "    EmailEventTransaction.insert = InboxRepository.insert\n"
        + trigger.replace("mutate()", "mutate(EmailEventTransaction)")
    )

    assert "module_runtime_binding_mutation" in _caller_owned_insert_violations(source)


def test_transaction_sql_detector_rejects_session_characteristics() -> None:
    assert _transaction_control_tokens(
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE"
    ) == ["SET SESSION CHARACTERISTICS AS TRANSACTION"]
