from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass
from pathlib import Path

from tests.architecture.test_email_state_repository_boundary import (
    _DYNAMIC_IDENTIFIER,
    _TASK5_REPOSITORY_STRUCTURAL_AST_SHA256,
    _TRUSTED_TABLE_MARKER_ATTRIBUTE,
    _execution_query,
    _is_sql_execution,
    _normalized_ast_dump,
    _render_sql,
    _scope_bindings,
    _scope_nodes,
)


_ROOTS = frozenset(
    {
        "EmailEventTransaction.apply_email_event",
        "InboxRepository.apply_email_event",
    }
)
_SQL_EXECUTION_METHODS = frozenset(
    {"copy", "copy_expert", "execute", "executemany", "exec_driver_sql"}
)
_DATABASE_RECEIVER_METHODS = _SQL_EXECUTION_METHODS | {
    "commit",
    "cursor",
    "fetchall",
    "fetchmany",
    "fetchone",
    "rollback",
    "transaction",
}
_PHASE3_RELATIONS = frozenset(
    {
        "approval_actions",
        "approval_resources",
        "card_resources",
        "draft_versions",
        "legacy_lark_card_invalidations",
        "mailbox_action_outbox",
        "mailbox_outbox",
        "notification_cards",
        "notification_outbox",
        "pipeline_activation_barrier_successors",
        "pipeline_activation_consumptions",
        "pipeline_legacy_effects",
        "send_intents",
        "send_outbox",
        "send_resolution_actions",
    }
)
_ALLOWED_MUTATIONS = frozenset(
    {
        ("insert", "audit_events"),
        ("insert", "emails"),
        ("update", "emails"),
    }
)
_FORBIDDEN_SQL_COMMANDS = frozenset(
    {
        "alter",
        "call",
        "cluster",
        "comment",
        "copy",
        "create",
        "do",
        "drop",
        "grant",
        "reindex",
        "refresh",
        "revoke",
        "vacuum",
    }
)
_SAFE_SQL_FUNCTIONS = frozenset(
    {
        "any",
        "coalesce",
        "pg_catalog.clock_timestamp",
        "pg_catalog.current_setting",
        "pg_catalog.pg_advisory_xact_lock_shared",
        "pg_catalog.pg_current_xact_id",
        "pg_catalog.set_config",
    }
)
_FORBIDDEN_EXTERNAL_TOKENS = (
    "content_store",
    "contentstore",
    "exchange",
    "lark",
    "qdrant",
)
_FORBIDDEN_EXTERNAL_CALLS = frozenset(
    {
        "ainvoke",
        "begin_effect",
        "complete",
        "create_card",
        "fail",
        "generate",
        "invoke",
        "notify",
        "renew",
        "send",
        "send_card",
        "update_card",
    }
)
_SAFE_NAME_CALLS = frozenset(
    {
        "DatabaseOperationError",
        "EmailEventApplication",
        "EmailEventDecision",
        "EmailEventTransaction",
        "EmailStatus",
        "InboxLease",
        "Jsonb",
        "ManualReviewRequired",
        "NormalizedIngressEvent",
        "PipelineGenerationState",
        "RuntimeError",
        "StaleFence",
        "UUID",
        "ValueError",
        "_EmailRow",
        "all",
        "decide_email_event",
        "dict",
        "isinstance",
        "len",
        "ownership_advisory_lock_key",
        "range",
        "set",
        "sorted",
        "str",
        "tuple",
        "uuid4",
        "zip",
    }
)
_BUILTIN_SAFE_NAME_CALLS = frozenset(
    {
        "RuntimeError",
        "ValueError",
        "all",
        "dict",
        "isinstance",
        "len",
        "range",
        "set",
        "sorted",
        "str",
        "tuple",
        "zip",
    }
)
_TRUSTED_OWNER_BUILTIN_NAME_CALLS = frozenset(
    {
        ("InboxRepository.transaction", "type"),
    }
)
_PROVENANCE_GUARDED_BUILTIN_NAME_CALLS = _BUILTIN_SAFE_NAME_CALLS | frozenset(
    name for _, name in _TRUSTED_OWNER_BUILTIN_NAME_CALLS
)
_TRUSTED_SAFE_IMPORT_SOURCES = {
    "DatabaseOperationError": "src.domain.errors",
    "EmailEventApplication": "src.ingestion.email_events",
    "EmailEventDecision": "src.ingestion.email_events",
    "EmailStatus": "src.ingestion.email_events",
    "InboxLease": "src.ingestion.models",
    "Jsonb": "psycopg.types.json",
    "ManualReviewRequired": "src.domain.errors",
    "NormalizedIngressEvent": "src.ingestion.models",
    "PipelineGenerationState": "src.domain.email_state",
    "StaleFence": "src.domain.errors",
    "UUID": "uuid",
    "decide_email_event": "src.ingestion.email_events",
    "ownership_advisory_lock_key": "src.ingestion.ownership",
    "uuid4": "uuid",
}
_TRUSTED_SAFE_LOCAL_CLASSES = frozenset({"EmailEventTransaction", "_EmailRow"})
_IDENTITY_BREAKING_CALL_PATHS = frozenset(
    {
        "all",
        "any",
        "bool",
        "bytes",
        "float",
        "frozenset",
        "hash",
        "int",
        "isinstance",
        "len",
        "range",
        "repr",
        "str",
        "sum",
        "type",
    }
)
_SAFE_ATTRIBUTE_CALL_PATHS = frozenset(
    {
        "connection.execute",
        "connection.transaction",
        "cursor.execute",
        "cursor.fetchall",
        "cursor.fetchone",
        "email_id.encode",
        "event.payload.get",
        "hashlib.sha256",
        "hashlib.sha256().hexdigest",
        "inbox_id.encode",
        "item.get",
        "left.keys",
        "right.keys",
        "selected_cursor.fetchone",
        "self._connection.execute",
        "self._pool.connection",
        "sql.composed",
        "sql.identifier",
        "sql.sql",
        "sql.sql().format",
        "sql.sql().join",
        "str().encode",
        "transaction_id.isascii",
        "transaction_id.isdigit",
    }
)
_TRUSTED_SAFE_ATTRIBUTE_PARAMETER_PATHS = frozenset(
    {
        ("InboxRepository._acquire_account_lock", "connection.execute"),
        ("InboxRepository._configure_transaction", "connection.execute"),
        ("_json_values_equal", "left.keys"),
        ("_json_values_equal", "right.keys"),
        ("_processing_attempt_event_key", "inbox_id.encode"),
        ("_processing_attempt_fingerprint", "email_id.encode"),
        ("_processing_attempt_fingerprint", "inbox_id.encode"),
        ("_source_is_read", "event.payload.get"),
    }
)


def _call_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _definitions(
    tree: ast.Module,
) -> dict[str, ast.AsyncFunctionDef | ast.FunctionDef]:
    definitions: dict[str, ast.AsyncFunctionDef | ast.FunctionDef] = {}
    for node in tree.body:
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            definitions[node.name] = node
        elif isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, (ast.AsyncFunctionDef, ast.FunctionDef)):
                    definitions[f"{node.name}.{child.name}"] = child
    return definitions


def _self_attribute(node: ast.expr) -> tuple[str, ...] | None:
    path = _attribute_path(node)
    if not path or path[0] != "self":
        return None
    return path[1:]


def _attribute_path(node: ast.expr) -> tuple[str, ...] | None:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return None
    parts.append(current.id)
    return tuple(reversed(parts))


def _expression_path(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        value = _expression_path(node.value)
        return f"{value}.{node.attr}" if value else None
    if isinstance(node, ast.Call):
        function = _expression_path(node.func)
        return f"{function}()" if function else None
    return None


def _identifier_start(character: str) -> bool:
    return character == "_" or character.isalpha()


def _identifier_continue(character: str) -> bool:
    return _identifier_start(character) or character.isdigit() or character == "$"


def _sql_tokens(statement: str) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []
    index = 0
    while index < len(statement):
        character = statement[index]
        if character.isspace():
            index += 1
            continue
        if statement.startswith("--", index):
            newline = statement.find("\n", index + 2)
            index = len(statement) if newline < 0 else newline + 1
            continue
        if statement.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(statement) and depth:
                if statement.startswith("/*", index):
                    depth += 1
                    index += 2
                elif statement.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            continue
        if (
            character in {"e", "E"}
            and index + 1 < len(statement)
            and statement[index + 1] == "'"
            and (index == 0 or not _identifier_continue(statement[index - 1]))
        ):
            index += 2
            value = ""
            while index < len(statement):
                if statement[index] == "'":
                    if index + 1 < len(statement) and statement[index + 1] == "'":
                        value += "'"
                        index += 2
                        continue
                    index += 1
                    break
                if statement[index] == "\\" and index + 1 < len(statement):
                    value += statement[index : index + 2]
                    index += 2
                else:
                    value += statement[index]
                    index += 1
            tokens.append(("string", value))
            continue
        if character == "'":
            index += 1
            value = ""
            while index < len(statement):
                if statement[index] == "'":
                    if index + 1 < len(statement) and statement[index + 1] == "'":
                        value += "'"
                        index += 2
                        continue
                    index += 1
                    break
                value += statement[index]
                index += 1
            tokens.append(("string", value))
            continue
        if character == "$":
            end = index + 1
            while end < len(statement) and (
                statement[end].isalnum() or statement[end] == "_"
            ):
                end += 1
            if end < len(statement) and statement[end] == "$":
                delimiter = statement[index : end + 1]
                closing = statement.find(delimiter, end + 1)
                index = len(statement) if closing < 0 else closing + len(delimiter)
                continue
        if (
            character in {"u", "U"}
            and statement.startswith('&"', index + 1)
            and (index == 0 or not _identifier_continue(statement[index - 1]))
        ):
            index += 3
            while index < len(statement):
                if statement[index] == '"':
                    if index + 1 < len(statement) and statement[index + 1] == '"':
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            tokens.append(("unsupported", "unicode_escaped_identifier"))
            continue
        if character == '"':
            index += 1
            value = ""
            while index < len(statement):
                if statement[index] == '"':
                    if index + 1 < len(statement) and statement[index + 1] == '"':
                        value += '"'
                        index += 2
                        continue
                    index += 1
                    break
                value += statement[index]
                index += 1
            tokens.append(("quoted", value))
            continue
        if _identifier_start(character):
            end = index + 1
            while end < len(statement) and _identifier_continue(statement[end]):
                end += 1
            tokens.append(("word", statement[index:end].lower()))
            index = end
            continue
        if statement.startswith("%s", index):
            tokens.append(("placeholder", "%s"))
            index += 2
            continue
        if character in ".,();=":
            tokens.append(("symbol", character))
        index += 1
    return tokens


def _is_word(tokens: list[tuple[str, str]], index: int, value: str) -> bool:
    return (
        0 <= index < len(tokens)
        and tokens[index][0] == "word"
        and tokens[index][1] == value
    )


def _qualified_identifier(
    tokens: list[tuple[str, str]],
    index: int,
) -> tuple[tuple[str, ...], int] | None:
    if index >= len(tokens) or tokens[index][0] not in {"word", "quoted"}:
        return None
    parts = [tokens[index][1]]
    index += 1
    while (
        index + 1 < len(tokens)
        and tokens[index] == ("symbol", ".")
        and tokens[index + 1][0] in {"word", "quoted"}
    ):
        parts.append(tokens[index + 1][1])
        index += 2
    return tuple(parts), index


def _after_parentheses(tokens: list[tuple[str, str]], index: int) -> int:
    if index >= len(tokens) or tokens[index] != ("symbol", "("):
        return index
    depth = 0
    while index < len(tokens):
        if tokens[index] == ("symbol", "("):
            depth += 1
        elif tokens[index] == ("symbol", ")"):
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return len(tokens)


def _row_lock_update(tokens: list[tuple[str, str]], index: int) -> bool:
    return _is_word(tokens, index - 1, "for") or (
        _is_word(tokens, index - 3, "for")
        and _is_word(tokens, index - 2, "no")
        and _is_word(tokens, index - 1, "key")
    )


def _mutation_details(
    tokens: list[tuple[str, str]],
) -> list[tuple[str, tuple[str, ...]]]:
    mutations: list[tuple[str, tuple[str, ...]]] = []
    for index, token in enumerate(tokens):
        if token[0] != "word":
            continue
        operation = token[1]
        target_index: int | None = None
        if operation == "insert":
            target_index = index + 2 if _is_word(tokens, index + 1, "into") else None
        elif operation == "update" and not _row_lock_update(tokens, index):
            target_index = index + 1
            if _is_word(tokens, target_index, "only"):
                target_index += 1
        elif operation == "delete":
            target_index = index + 2 if _is_word(tokens, index + 1, "from") else None
            if target_index is not None and _is_word(tokens, target_index, "only"):
                target_index += 1
        elif operation == "merge":
            target_index = index + 2 if _is_word(tokens, index + 1, "into") else None
            if target_index is not None and _is_word(tokens, target_index, "only"):
                target_index += 1
        elif operation == "truncate":
            target_index = index + 1
            if _is_word(tokens, target_index, "table"):
                target_index += 1
            if _is_word(tokens, target_index, "only"):
                target_index += 1
        elif operation == "copy":
            parsed = _qualified_identifier(tokens, index + 1)
            if parsed is None:
                continue
            parts, after_target = parsed
            after_target = _after_parentheses(tokens, after_target)
            if _is_word(tokens, after_target, "from"):
                mutations.append((operation, parts))
            continue
        else:
            continue

        if target_index is None:
            mutations.append(("unresolved", (_DYNAMIC_IDENTIFIER,)))
            continue
        parsed = _qualified_identifier(tokens, target_index)
        if parsed is None:
            mutations.append(("unresolved", (_DYNAMIC_IDENTIFIER,)))
            continue
        parts, after_target = parsed
        mutations.append((operation, parts))
        if operation == "truncate":
            while after_target < len(tokens) and tokens[after_target] == (
                "symbol",
                ",",
            ):
                parsed = _qualified_identifier(tokens, after_target + 1)
                if parsed is None:
                    mutations.append(("unresolved", (_DYNAMIC_IDENTIFIER,)))
                    break
                parts, after_target = parsed
                mutations.append((operation, parts))
    return mutations


def _mutation_operation_targets(statement: str) -> list[tuple[str, str]]:
    return [
        (operation, ".".join(parts))
        for operation, parts in _mutation_details(_sql_tokens(statement))
    ]


def _phase3_relation_names(tokens: list[tuple[str, str]]) -> set[str]:
    return {
        value
        for kind, value in tokens
        if kind in {"quoted", "word"} and value in _PHASE3_RELATIONS
    }


@dataclass(frozen=True, slots=True)
class _SqlFunction:
    name: str
    quoted: bool
    open_index: int


def _cte_column_list_open_indices(
    tokens: list[tuple[str, str]],
) -> set[int]:
    openings: set[int] = set()
    for with_index, token in enumerate(tokens):
        if token != ("word", "with"):
            continue
        cursor = with_index + 1
        if _is_word(tokens, cursor, "recursive"):
            cursor += 1
        while cursor < len(tokens):
            parsed = _qualified_identifier(tokens, cursor)
            if parsed is None:
                break
            _, cursor = parsed
            if cursor < len(tokens) and tokens[cursor] == ("symbol", "("):
                openings.add(cursor)
                cursor = _after_parentheses(tokens, cursor)
            if not _is_word(tokens, cursor, "as"):
                break
            cursor += 1
            if _is_word(tokens, cursor, "not") and _is_word(
                tokens,
                cursor + 1,
                "materialized",
            ):
                cursor += 2
            elif _is_word(tokens, cursor, "materialized"):
                cursor += 1
            if cursor >= len(tokens) or tokens[cursor] != ("symbol", "("):
                break
            cursor = _after_parentheses(tokens, cursor)
            if cursor >= len(tokens) or tokens[cursor] != ("symbol", ","):
                break
            cursor += 1
    return openings


def _sql_functions(tokens: list[tuple[str, str]]) -> set[_SqlFunction]:
    functions: set[_SqlFunction] = set()
    cte_column_lists = _cte_column_list_open_indices(tokens)
    index = 0
    syntax = {
        "array",
        "as",
        "cast",
        "conflict",
        "exists",
        "filter",
        "in",
        "on",
        "over",
        "row",
        "set",
        "sets",
        "using",
        "values",
    }
    structural_prefixes = {"as", "copy", "into"}
    while index < len(tokens):
        parsed = _qualified_identifier(tokens, index)
        if parsed is None:
            index += 1
            continue
        parts, after_path = parsed
        if after_path < len(tokens) and tokens[after_path] == ("symbol", "("):
            previous = (
                tokens[index - 1][1] if index and tokens[index - 1][0] == "word" else ""
            )
            quoted = any(kind == "quoted" for kind, _ in tokens[index:after_path])
            if (
                (quoted or parts[-1] not in syntax)
                and previous not in structural_prefixes
                and after_path not in cte_column_lists
            ):
                functions.add(
                    _SqlFunction(
                        name=".".join(parts),
                        quoted=quoted,
                        open_index=after_path,
                    )
                )
        index = max(after_path, index + 1)
    return functions


def _function_arguments(
    tokens: list[tuple[str, str]],
    open_index: int,
) -> list[list[tuple[str, str]]] | None:
    arguments: list[list[tuple[str, str]]] = [[]]
    depth = 0
    for token in tokens[open_index + 1 :]:
        if token == ("symbol", "("):
            depth += 1
        elif token == ("symbol", ")"):
            if depth == 0:
                return arguments
            depth -= 1
        if token == ("symbol", ",") and depth == 0:
            arguments.append([])
        else:
            arguments[-1].append(token)
    return None


def _safe_sql_function(
    function: _SqlFunction,
    tokens: list[tuple[str, str]],
) -> bool:
    if function.quoted:
        return False
    if function.name != "pg_catalog.set_config":
        return function.name in _SAFE_SQL_FUNCTIONS
    arguments = _function_arguments(tokens, function.open_index)
    return arguments in [
        [[("string", key)], [("placeholder", "%s")], [("word", "true")]]
        for key in (
            "idle_in_transaction_session_timeout",
            "lock_timeout",
            "statement_timeout",
        )
    ]


def _statement_commands(tokens: list[tuple[str, str]]) -> set[str]:
    commands: set[str] = set()
    statements: list[list[tuple[str, str]]] = []
    statement: list[tuple[str, str]] = []
    depth = 0
    for token in tokens:
        if token == ("symbol", "("):
            depth += 1
        elif token == ("symbol", ")"):
            depth = max(depth - 1, 0)
        if token == ("symbol", ";") and depth == 0:
            if statement:
                statements.append(statement)
            statement = []
        else:
            statement.append(token)
    if statement:
        statements.append(statement)

    allowed_roots = {"insert", "select", "update", "with"}
    allowed_set = (
        "set",
        "local",
        "transaction",
        "isolation",
        "level",
        "read",
        "committed",
    )
    for candidate in statements:
        words = tuple(value for kind, value in candidate if kind == "word")
        if not words:
            commands.add("unresolved")
        elif words[0] not in allowed_roots and words != allowed_set:
            commands.add(words[0])
    return commands


def _trusted_logical_target(
    parts: tuple[str, ...],
    trusted_markers: frozenset[str],
) -> str | None:
    if len(parts) != 2 or parts[0] not in trusted_markers:
        return None
    return parts[1] or None


def _local_call_targets(
    owner: str,
    call: ast.Call,
    definitions: dict[str, ast.AsyncFunctionDef | ast.FunctionDef],
) -> set[str]:
    targets: set[str] = set()
    class_name = owner.partition(".")[0] if "." in owner else None
    if isinstance(call.func, ast.Name) and call.func.id in definitions:
        targets.add(call.func.id)
        return targets
    if not isinstance(call.func, ast.Attribute):
        return targets

    chain = _self_attribute(call.func)
    if chain and len(chain) == 1 and class_name is not None:
        candidate = f"{class_name}.{chain[0]}"
        if candidate in definitions:
            targets.add(candidate)
    elif chain and len(chain) == 2 and chain[0] == "_repository":
        candidate = f"InboxRepository.{chain[1]}"
        if candidate in definitions:
            targets.add(candidate)

    if (
        call.func.attr == "apply_email_event"
        and isinstance(call.func.value, ast.Call)
        and isinstance(call.func.value.func, ast.Attribute)
        and _self_attribute(call.func.value.func) == ("transaction",)
        and "EmailEventTransaction.apply_email_event" in definitions
    ):
        targets.add("EmailEventTransaction.apply_email_event")
    return targets


def _reachable_definitions(
    tree: ast.Module,
) -> tuple[
    dict[str, ast.AsyncFunctionDef | ast.FunctionDef],
    frozenset[str],
]:
    definitions = _definitions(tree)
    missing = _ROOTS - definitions.keys()
    if missing:
        raise AssertionError(f"missing Task-5 call-graph roots: {sorted(missing)}")
    reachable: set[str] = set()
    pending = list(_ROOTS)
    while pending:
        owner = pending.pop()
        if owner in reachable:
            continue
        reachable.add(owner)
        for node in ast.walk(definitions[owner]):
            if isinstance(node, ast.Call):
                pending.extend(
                    _local_call_targets(owner, node, definitions) - reachable
                )
    return definitions, frozenset(reachable)


def _table_factory_is_exact(
    definitions: dict[str, ast.AsyncFunctionDef | ast.FunctionDef],
) -> bool:
    factory = definitions.get("InboxRepository._table")
    if not isinstance(factory, ast.FunctionDef) or factory.decorator_list:
        return False
    arguments = factory.args
    if (
        [argument.arg for argument in arguments.posonlyargs + arguments.args]
        != ["self", "name"]
        or arguments.vararg is not None
        or arguments.kwarg is not None
        or arguments.kwonlyargs
        or arguments.defaults
        or arguments.kw_defaults
        or len(factory.body) != 1
        or not isinstance(factory.body[0], ast.Return)
    ):
        return False
    returned = factory.body[0].value
    return (
        isinstance(returned, ast.Call)
        and not returned.keywords
        and len(returned.args) == 2
        and isinstance(returned.func, ast.Attribute)
        and returned.func.attr == "Identifier"
        and isinstance(returned.func.value, ast.Name)
        and returned.func.value.id == "sql"
        and isinstance(returned.args[0], ast.Attribute)
        and returned.args[0].attr == "_schema"
        and isinstance(returned.args[0].value, ast.Name)
        and returned.args[0].value.id == "self"
        and isinstance(returned.args[1], ast.Name)
        and returned.args[1].id == "name"
    )


def _written_targets(node: ast.AST) -> list[ast.expr]:
    if isinstance(node, ast.Assign):
        return list(node.targets)
    if isinstance(node, (ast.AnnAssign, ast.AugAssign, ast.NamedExpr)):
        return [node.target]
    if isinstance(node, ast.Delete):
        return list(node.targets)
    if isinstance(node, (ast.AsyncFor, ast.For, ast.comprehension)):
        return [node.target]
    if isinstance(node, (ast.AsyncWith, ast.With)):
        return [
            item.optional_vars for item in node.items if item.optional_vars is not None
        ]
    return []


def _target_attribute_paths(target: ast.expr) -> set[tuple[str, ...]]:
    if isinstance(target, (ast.List, ast.Tuple)):
        return {
            path for element in target.elts for path in _target_attribute_paths(element)
        }
    path = _attribute_path(target)
    return {path} if path is not None else set()


def _argument_names(
    function: ast.AsyncFunctionDef | ast.FunctionDef,
) -> tuple[str, ...]:
    arguments = function.args
    names = [
        argument.arg
        for argument in (
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
        )
    ]
    if arguments.vararg is not None:
        names.append(arguments.vararg.arg)
    if arguments.kwarg is not None:
        names.append(arguments.kwarg.arg)
    return tuple(names)


def _import_binding_names(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Import):
        return {alias.asname or alias.name.partition(".")[0] for alias in node.names}
    if isinstance(node, ast.ImportFrom):
        return {alias.asname or alias.name for alias in node.names}
    return set()


def _is_star_import(node: ast.AST) -> bool:
    return isinstance(node, ast.ImportFrom) and any(
        alias.name == "*" for alias in node.names
    )


def _node_binding_names(node: ast.AST) -> set[str]:
    names = _import_binding_names(node)
    if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
        names.add(node.name)
    elif isinstance(node, ast.ExceptHandler) and node.name is not None:
        names.add(node.name)
    elif isinstance(node, (ast.MatchAs, ast.MatchStar)) and node.name is not None:
        names.add(node.name)
    elif isinstance(node, ast.MatchMapping) and node.rest is not None:
        names.add(node.rest)
    for target in _written_targets(node):
        names.update(
            path[0] for path in _target_attribute_paths(target) if len(path) == 1
        )
    return names


def _module_scope_nodes(
    root: ast.Module | ast.AsyncFunctionDef | ast.FunctionDef,
):
    pending = list(root.body)
    while pending:
        node = pending.pop()
        yield node
        if isinstance(
            node,
            (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef, ast.Lambda),
        ):
            continue
        pending.extend(ast.iter_child_nodes(node))


def _is_trusted_sql_import(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.ImportFrom)
        and node.module == "psycopg"
        and node.level == 0
        and len(node.names) == 1
        and node.names[0].name == "sql"
        and node.names[0].asname is None
    )


def _is_trusted_hashlib_import(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Import)
        and len(node.names) == 1
        and node.names[0].name == "hashlib"
        and node.names[0].asname is None
    )


def _is_trusted_safe_import(name: str, node: ast.AST) -> bool:
    source = _TRUSTED_SAFE_IMPORT_SOURCES.get(name)
    return (
        source is not None
        and isinstance(node, ast.ImportFrom)
        and node.module == source
        and node.level == 0
        and any(alias.name == name and alias.asname is None for alias in node.names)
    )


def _is_trusted_safe_local_class(name: str, node: ast.AST) -> bool:
    return (
        name in _TRUSTED_SAFE_LOCAL_CLASSES
        and isinstance(node, ast.ClassDef)
        and node.name == name
    )


def _untrusted_safe_module_names(tree: ast.Module) -> frozenset[str]:
    nodes = list(_module_scope_nodes(tree))
    has_star_import = any(_is_star_import(node) for node in nodes)
    binding_nodes = [node for node in nodes if not isinstance(node, ast.comprehension)]
    untrusted = {
        name
        for name in _PROVENANCE_GUARDED_BUILTIN_NAME_CALLS
        if has_star_import
        or any(name in _node_binding_names(node) for node in binding_nodes)
    }
    for name in _SAFE_NAME_CALLS - _BUILTIN_SAFE_NAME_CALLS:
        safe_bindings = [
            node for node in binding_nodes if name in _node_binding_names(node)
        ]
        if safe_bindings and (
            len(safe_bindings) != 1
            or not (
                _is_trusted_safe_import(name, safe_bindings[0])
                or _is_trusted_safe_local_class(name, safe_bindings[0])
            )
        ):
            untrusted.add(name)
    hashlib_bindings = [
        node for node in binding_nodes if "hashlib" in _node_binding_names(node)
    ]
    if (
        has_star_import
        or len(hashlib_bindings) != 1
        or not _is_trusted_hashlib_import(hashlib_bindings[0])
    ):
        untrusted.add("hashlib")
    return frozenset(untrusted)


def _sql_receiver_parameter_names(
    function: ast.AsyncFunctionDef | ast.FunctionDef,
) -> frozenset[str]:
    parameters = frozenset(_argument_names(function)) - {"cls", "self"}
    return frozenset(
        path[0]
        for node in _scope_nodes(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _DATABASE_RECEIVER_METHODS
        for path in [_attribute_path(node.func)]
        if path is not None and path[0] in parameters
    )


def _explicit_call_argument_for_parameter(
    function: ast.AsyncFunctionDef | ast.FunctionDef,
    call: ast.Call,
    parameter: str,
) -> ast.expr | None:
    for keyword in call.keywords:
        if keyword.arg == parameter:
            return keyword.value

    all_positional = [
        argument.arg for argument in (*function.args.posonlyargs, *function.args.args)
    ]
    positional = list(all_positional)
    if positional and positional[0] in {"cls", "self"}:
        positional = positional[1:]
    expanded_arguments: list[ast.expr] = []
    for argument in call.args:
        if not isinstance(argument, ast.Starred):
            expanded_arguments.append(argument)
            continue
        if not isinstance(argument.value, (ast.List, ast.Tuple)):
            expanded_arguments = []
            break
        expanded_arguments.extend(argument.value.elts)
    if parameter in positional:
        index = positional.index(parameter)
        if index < len(expanded_arguments):
            return expanded_arguments[index]
    return None


def _parameter_default(
    function: ast.AsyncFunctionDef | ast.FunctionDef,
    parameter: str,
) -> ast.expr | None:
    all_positional = [
        argument.arg for argument in (*function.args.posonlyargs, *function.args.args)
    ]

    positional_defaults = (
        dict(
            zip(
                all_positional[-len(function.args.defaults) :],
                function.args.defaults,
                strict=True,
            )
        )
        if function.args.defaults
        else {}
    )
    if parameter in positional_defaults:
        return positional_defaults[parameter]
    kwonly_defaults = {
        argument.arg: default
        for argument, default in zip(
            function.args.kwonlyargs,
            function.args.kw_defaults,
            strict=True,
        )
        if default is not None
    }
    if parameter in kwonly_defaults:
        return kwonly_defaults[parameter]
    return None


def _call_argument_for_parameter(
    function: ast.AsyncFunctionDef | ast.FunctionDef,
    call: ast.Call,
    parameter: str,
) -> ast.expr | None:
    explicit = _explicit_call_argument_for_parameter(function, call, parameter)
    return explicit if explicit is not None else _parameter_default(function, parameter)


def _is_trusted_pool_connection_target(
    function: ast.AsyncFunctionDef | ast.FunctionDef,
    argument: ast.expr,
) -> bool:
    if not isinstance(argument, ast.Name):
        return False
    for node in _scope_nodes(function):
        if not isinstance(node, (ast.AsyncWith, ast.With)):
            continue
        for item in node.items:
            if (
                isinstance(item.optional_vars, ast.Name)
                and item.optional_vars.id == argument.id
                and isinstance(item.context_expr, ast.Call)
                and _expression_path(item.context_expr.func) == "self._pool.connection"
                and not item.context_expr.args
                and not item.context_expr.keywords
            ):
                return True
    return False


def _trusted_sql_receiver_parameters(
    definitions: dict[str, ast.AsyncFunctionDef | ast.FunctionDef],
    reachable: frozenset[str],
) -> frozenset[tuple[str, str]]:
    trusted: set[tuple[str, str]] = set()
    for callee in reachable:
        function = definitions[callee]
        for parameter in _sql_receiver_parameter_names(function):
            calls: list[tuple[str, ast.Call]] = []
            for caller in reachable:
                for node in _scope_nodes(definitions[caller]):
                    if isinstance(node, ast.Call) and callee in _local_call_targets(
                        caller, node, definitions
                    ):
                        calls.append((caller, node))
            sources = [
                (
                    caller,
                    _call_argument_for_parameter(function, call, parameter),
                )
                for caller, call in calls
            ]
            if sources and all(
                argument is not None
                and (
                    _expression_path(argument) == "self._connection"
                    or (
                        (callee, parameter)
                        == ("InboxRepository._configure_transaction", "connection")
                        and caller == "InboxRepository.apply_email_event"
                        and _is_trusted_pool_connection_target(
                            definitions[caller],
                            argument,
                        )
                    )
                )
                for caller, argument in sources
            ):
                trusted.add((callee, parameter))
    return frozenset(trusted)


def _factory_path_is_protected(
    path: tuple[str, ...],
    protected_paths: set[tuple[str, ...]] | frozenset[tuple[str, ...]] = frozenset(),
) -> bool:
    return (
        path in protected_paths
        or path[0] == "sql"
        or any(part in {"_schema", "_table"} for part in path[1:])
    )


def _binding_is_protected_factory_object(
    name: str,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> bool:
    return _expression_is_protected_factory_object(
        ast.Name(id=name, ctx=ast.Load()),
        bindings,
        resolving=resolving,
    )


def _expression_is_protected_factory_object(
    expression: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> bool:
    if isinstance(expression, ast.Name):
        if expression.id == "sql":
            return True
        if expression.id in resolving:
            return False
        return any(
            _expression_is_protected_factory_object(
                value,
                bindings,
                resolving=resolving | {expression.id},
            )
            for value in bindings.get(expression.id, [])
        )
    path = _attribute_path(expression)
    if path is not None and _factory_path_is_protected(path):
        return True
    return any(
        _expression_is_protected_factory_object(
            source,
            bindings,
            resolving=resolving,
        )
        for source in _identity_source_expressions(expression)
    )


def _factory_written_path_is_protected(
    path: tuple[str, ...],
    bindings: dict[str, list[ast.expr]],
    protected_paths: set[tuple[str, ...]] | frozenset[tuple[str, ...]] = frozenset(),
) -> bool:
    return _factory_path_is_protected(
        path,
        protected_paths,
    ) or _binding_is_protected_factory_object(path[0], bindings)


def _factory_state_is_written(
    definitions: dict[str, ast.AsyncFunctionDef | ast.FunctionDef],
    reachable: frozenset[str],
) -> bool:
    protected = {
        "EmailEventTransaction": {
            ("self", "_repository"),
            ("self", "_repository", "_schema"),
            ("self", "_repository", "_table"),
        },
        "InboxRepository": {
            ("self", "_schema"),
            ("self", "_table"),
        },
    }
    for owner in reachable:
        function = definitions[owner]
        if "sql" in _argument_names(function):
            return True
        class_name = owner.partition(".")[0]
        protected_paths = protected.get(class_name, set())
        bindings = _scope_bindings(function)
        scope_nodes = list(_module_scope_nodes(function))
        for node in scope_nodes:
            if _is_star_import(node) or "sql" in _node_binding_names(node):
                return True
            for target in _written_targets(node):
                for path in _target_attribute_paths(target):
                    if _factory_written_path_is_protected(
                        path,
                        bindings,
                        protected_paths,
                    ):
                        return True
    return False


def _module_factory_state_is_written(tree: ast.Module) -> bool:
    bindings = _scope_bindings(tree)
    sql_binding_nodes = [
        node
        for node in _module_scope_nodes(tree)
        if "sql" in _import_binding_names(node)
    ]
    if len(sql_binding_nodes) > 1 or any(
        not _is_trusted_sql_import(node) for node in sql_binding_nodes
    ):
        return True
    for node in _module_scope_nodes(tree):
        if _is_star_import(node):
            return True
        if not isinstance(
            node, (ast.Import, ast.ImportFrom)
        ) and "sql" in _node_binding_names(node):
            return True
        for target in _written_targets(node):
            for path in _target_attribute_paths(target):
                if _factory_written_path_is_protected(path, bindings):
                    return True
    return False


def _mark_trusted_table_calls(
    tree: ast.Module,
    definitions: dict[str, ast.AsyncFunctionDef | ast.FunctionDef],
    reachable: frozenset[str],
) -> tuple[frozenset[str], bool]:
    state_is_written = _factory_state_is_written(
        definitions,
        reachable,
    ) or _module_factory_state_is_written(tree)
    exact_factory = _table_factory_is_exact(definitions) and not state_is_written
    uses_factory = False
    markers: set[str] = set()
    for owner in reachable:
        allowed_path = (
            "self._table"
            if owner.startswith("InboxRepository.")
            else "self._repository._table"
        )
        for node in _scope_nodes(definitions[owner]):
            if not isinstance(node, ast.Call) or _call_name(node.func) != "_table":
                continue
            uses_factory = True
            if (
                not exact_factory
                or _expression_path(node.func) != allowed_path
                or len(node.args) != 1
                or node.keywords
                or not isinstance(node.args[0], ast.Constant)
                or not isinstance(node.args[0].value, str)
            ):
                continue
            marker = f"__task5_trusted_{id(node):x}__"
            setattr(node, _TRUSTED_TABLE_MARKER_ATTRIBUTE, marker)
            markers.add(marker)
    invalid_factory = (
        "InboxRepository._table" in definitions or uses_factory or state_is_written
    ) and not exact_factory
    return frozenset(markers), invalid_factory


def _module_alias_roots(
    name: str,
    bindings: dict[str, list[ast.expr]],
    module_names: frozenset[str],
    *,
    resolving: frozenset[str] = frozenset(),
) -> set[str]:
    if name in resolving:
        return set()
    roots = {name} if name in module_names else set()
    for value in bindings.get(name, []):
        if isinstance(value, ast.Name) and value.id != _DYNAMIC_IDENTIFIER:
            roots.update(
                _module_alias_roots(
                    value.id,
                    bindings,
                    module_names,
                    resolving=resolving | {name},
                )
            )
    return roots


def _identity_source_expressions(expression: ast.expr) -> tuple[ast.expr, ...]:
    if isinstance(expression, (ast.Attribute, ast.Starred, ast.Subscript)):
        return (expression.value,)
    if isinstance(expression, (ast.List, ast.Set, ast.Tuple)):
        return tuple(expression.elts)
    if isinstance(expression, ast.Dict):
        return tuple(expression.values)
    if isinstance(expression, ast.IfExp):
        return (expression.body, expression.orelse)
    if isinstance(expression, ast.BoolOp):
        return tuple(expression.values)
    if isinstance(expression, ast.NamedExpr):
        return (expression.value,)
    if isinstance(expression, ast.Call):
        call_path = (_expression_path(expression.func) or "").lower()
        if call_path in _IDENTITY_BREAKING_CALL_PATHS:
            return ()
        return (*expression.args, *(keyword.value for keyword in expression.keywords))
    return ()


def _expression_parameter_roots(
    expression: ast.expr,
    bindings: dict[str, list[ast.expr]],
    parameters: frozenset[str],
    *,
    resolving: frozenset[str] = frozenset(),
) -> set[str]:
    if isinstance(expression, ast.Name):
        if expression.id in parameters:
            return {expression.id}
        if expression.id in resolving:
            return set()
        return {
            root
            for value in bindings.get(expression.id, [])
            for root in _expression_parameter_roots(
                value,
                bindings,
                parameters,
                resolving=resolving | {expression.id},
            )
        }
    return {
        root
        for source in _identity_source_expressions(expression)
        for root in _expression_parameter_roots(
            source,
            bindings,
            parameters,
            resolving=resolving,
        )
    }


def _binding_parameter_roots(
    name: str,
    bindings: dict[str, list[ast.expr]],
    parameters: frozenset[str],
    *,
    resolving: frozenset[str] = frozenset(),
) -> set[str]:
    return _expression_parameter_roots(
        ast.Name(id=name, ctx=ast.Load()),
        bindings,
        parameters,
        resolving=resolving,
    )


def _expression_module_mapping_roots(
    expression: ast.expr,
    bindings: dict[str, list[ast.expr]],
    module_mapping_names: frozenset[str],
    *,
    resolving: frozenset[str] = frozenset(),
    shadowed_module_names: frozenset[str] = frozenset(),
) -> set[str]:
    if isinstance(expression, ast.Name):
        roots = (
            {expression.id}
            if expression.id in module_mapping_names
            and expression.id not in shadowed_module_names
            else set()
        )
        if expression.id in resolving:
            return roots
        for value in bindings.get(expression.id, []):
            roots.update(
                _expression_module_mapping_roots(
                    value,
                    bindings,
                    module_mapping_names,
                    resolving=resolving | {expression.id},
                    shadowed_module_names=shadowed_module_names,
                )
            )
        return roots
    return {
        root
        for source in _identity_source_expressions(expression)
        for root in _expression_module_mapping_roots(
            source,
            bindings,
            module_mapping_names,
            resolving=resolving,
            shadowed_module_names=shadowed_module_names,
        )
    }


def _binding_can_be_mapping(
    name: str,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> bool:
    if name in resolving:
        return False
    return any(
        isinstance(value, ast.Dict)
        or isinstance(value, ast.Name)
        and _binding_can_be_mapping(
            value.id,
            bindings,
            resolving=resolving | {name},
        )
        for value in bindings.get(name, [])
    )


def _tainted_module_names(
    definitions: dict[str, ast.AsyncFunctionDef | ast.FunctionDef],
    reachable: frozenset[str],
    module_bindings: dict[str, list[ast.expr]],
) -> frozenset[str]:
    module_names = frozenset(module_bindings)
    module_mapping_names = frozenset(
        name for name in module_names if _binding_can_be_mapping(name, module_bindings)
    )
    tainted: set[str] = set()
    for owner in reachable:
        function = definitions[owner]
        scope_nodes = list(_scope_nodes(function))
        bindings = _scope_bindings(function)
        dynamic_names = {
            name
            for name, values in bindings.items()
            if any(
                isinstance(value, ast.Name) and value.id == _DYNAMIC_IDENTIFIER
                for value in values
            )
        }
        for name in dynamic_names:
            tainted.update(_module_alias_roots(name, bindings, module_names))

        parameters = frozenset(_argument_names(function)) - {"cls", "self"}
        mutated_parameters = {
            parameter
            for name in dynamic_names
            for parameter in _binding_parameter_roots(
                name,
                bindings,
                parameters,
            )
        }

        for parameter in mutated_parameters:
            for caller in reachable:
                caller_function = definitions[caller]
                caller_bindings = _scope_bindings(caller_function)
                caller_global_names = {
                    name
                    for candidate in _scope_nodes(caller_function)
                    if isinstance(candidate, ast.Global)
                    for name in candidate.names
                }
                for node in _scope_nodes(caller_function):
                    if not isinstance(
                        node, ast.Call
                    ) or owner not in _local_call_targets(caller, node, definitions):
                        continue
                    argument = _explicit_call_argument_for_parameter(
                        function,
                        node,
                        parameter,
                    )
                    if argument is None:
                        argument = _parameter_default(function, parameter)
                        argument_bindings = {
                            name: list(values)
                            for name, values in module_bindings.items()
                        }
                        shadowed_module_names = frozenset()
                    else:
                        argument_bindings = {
                            name: list(values)
                            for name, values in module_bindings.items()
                        }
                        argument_bindings.update(caller_bindings)
                        comprehension_names = _comprehension_bound_names(
                            caller_function,
                            node,
                        )
                        for name in comprehension_names:
                            argument_bindings[name] = [
                                ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())
                            ]
                        shadowed_module_names = frozenset(caller_bindings) | (
                            comprehension_names
                        )
                        shadowed_module_names -= caller_global_names
                    if argument is not None:
                        tainted.update(
                            _expression_module_mapping_roots(
                                argument,
                                argument_bindings,
                                module_mapping_names,
                                shadowed_module_names=shadowed_module_names,
                            )
                        )

        global_names = {
            name
            for node in scope_nodes
            if isinstance(node, ast.Global)
            for name in node.names
        }
        for node in scope_nodes:
            for target in _written_targets(node):
                for path in _target_attribute_paths(target):
                    if len(path) == 1 and path[0] in global_names & module_names:
                        tainted.add(path[0])
    return frozenset(tainted)


def _function_bound_names(
    function: ast.AsyncFunctionDef | ast.FunctionDef,
) -> frozenset[str]:
    names = set(_argument_names(function)) - {"cls", "self"}
    for node in _module_scope_nodes(function):
        if not isinstance(node, ast.comprehension):
            names.update(_node_binding_names(node))
    return frozenset(names)


def _comprehension_bound_names(
    function: ast.AsyncFunctionDef | ast.FunctionDef,
    call: ast.Call,
) -> frozenset[str]:
    names: set[str] = set()
    for expression in _scope_nodes(function):
        if not isinstance(
            expression,
            (ast.DictComp, ast.GeneratorExp, ast.ListComp, ast.SetComp),
        ) or not any(node is call for node in ast.walk(expression)):
            continue
        for generator in expression.generators:
            names.update(
                path[0]
                for path in _target_attribute_paths(generator.target)
                if len(path) == 1
            )
    return frozenset(names)


def _side_effect_violations(source: str, *, filename: str) -> list[str]:
    tree = ast.parse(source, filename=filename)
    definitions, reachable = _reachable_definitions(tree)
    untrusted_safe_module_names = _untrusted_safe_module_names(tree)
    trusted_markers, invalid_factory = _mark_trusted_table_calls(
        tree,
        definitions,
        reachable,
    )
    module_bindings = _scope_bindings(tree)
    tainted_module_names = _tainted_module_names(
        definitions,
        reachable,
        module_bindings,
    )
    trusted_sql_receiver_parameters = _trusted_sql_receiver_parameters(
        definitions,
        reachable,
    )
    violations: list[str] = (
        ["factory:InboxRepository._table:untrusted"] if invalid_factory else []
    )
    for owner in sorted(reachable):
        function = definitions[owner]
        function_parameters = frozenset(_argument_names(function)) - {"cls", "self"}
        local_bindings = _scope_bindings(function)
        function_bound_names = _function_bound_names(function)
        bindings = {name: list(values) for name, values in module_bindings.items()}
        bindings.update(local_bindings)
        receiver_parameters = _sql_receiver_parameter_names(function)
        untrusted_receiver_parameters = {
            parameter
            for parameter in receiver_parameters
            if (owner, parameter) not in trusted_sql_receiver_parameters
        }
        for parameter in untrusted_receiver_parameters:
            bindings.setdefault(parameter, []).append(
                ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())
            )
        for name in tainted_module_names:
            bindings[name] = [ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())]
        for node in _scope_nodes(function):
            if not isinstance(node, ast.Call):
                continue
            call_bindings = {name: list(values) for name, values in bindings.items()}
            for name in _comprehension_bound_names(function, node):
                call_bindings.setdefault(name, []).append(
                    ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())
                )
            query = _execution_query(node, call_bindings)
            if query is None and _is_sql_execution(node, call_bindings):
                violations.append(
                    f"{owner}:{node.lineno}:mutation:unresolved:dynamic_identifier"
                )
            if query is not None:
                statements = _render_sql(query, call_bindings)
                if not statements:
                    violations.append(
                        f"{owner}:{node.lineno}:mutation:unresolved:dynamic_identifier"
                    )
                for statement in statements:
                    if statement == _DYNAMIC_IDENTIFIER:
                        violations.append(
                            f"{owner}:{node.lineno}:mutation:unresolved:dynamic_identifier"
                        )
                        continue
                    tokens = _sql_tokens(statement)
                    if any(
                        kind in {"quoted", "word"} and value == _DYNAMIC_IDENTIFIER
                        for kind, value in tokens
                    ) or any(kind == "unsupported" for kind, _ in tokens):
                        violations.append(
                            f"{owner}:{node.lineno}:mutation:unresolved:dynamic_identifier"
                        )
                    mutations = _mutation_details(tokens)
                    for operation, parts in mutations:
                        logical_target = _trusted_logical_target(parts, trusted_markers)
                        if (
                            logical_target is not None
                            and (
                                operation,
                                logical_target,
                            )
                            in _ALLOWED_MUTATIONS
                        ):
                            continue
                        target = logical_target or ".".join(parts)
                        violations.append(
                            f"{owner}:{node.lineno}:mutation:{operation}:{target}"
                        )
                    violations.extend(
                        f"{owner}:{node.lineno}:relation:{relation}"
                        for relation in _phase3_relation_names(tokens)
                    )
                    violations.extend(
                        f"{owner}:{node.lineno}:statement:{command}"
                        for command in _statement_commands(tokens)
                    )
                    violations.extend(
                        f"{owner}:{node.lineno}:function:{function.name}"
                        for function in _sql_functions(tokens)
                        if not _safe_sql_function(function, tokens)
                    )
            if (
                isinstance(node.func, ast.Name)
                and node.func.id not in definitions
                and (
                    (
                        node.func.id not in _SAFE_NAME_CALLS
                        and (owner, node.func.id)
                        not in _TRUSTED_OWNER_BUILTIN_NAME_CALLS
                    )
                    or node.func.id in call_bindings
                    or node.func.id in function_bound_names
                    or node.func.id in untrusted_safe_module_names
                )
            ):
                violations.append(f"{owner}:{node.lineno}:call:{node.func.id.lower()}")
            if not isinstance(node.func, (ast.Attribute, ast.Name)):
                violations.append(f"{owner}:{node.lineno}:call:dynamic")
            call_path = (_expression_path(node.func) or "").lower()
            call_name = (_call_name(node.func) or "").lower()
            local_targets = _local_call_targets(owner, node, definitions)
            attribute_path = _attribute_path(node.func)
            untrusted_safe_attribute_parameter = (
                attribute_path is not None
                and attribute_path[0] in function_parameters
                and call_path in _SAFE_ATTRIBUTE_CALL_PATHS
                and (owner, call_path) not in _TRUSTED_SAFE_ATTRIBUTE_PARAMETER_PATHS
            )
            rebound_sql_receiver = (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in _DATABASE_RECEIVER_METHODS
                and attribute_path is not None
                and (
                    node.func.attr in _SQL_EXECUTION_METHODS
                    and attribute_path[0] in call_bindings
                    or attribute_path[0] in untrusted_receiver_parameters
                )
            )
            if (
                isinstance(node.func, ast.Attribute)
                and not local_targets
                and (
                    call_path not in _SAFE_ATTRIBUTE_CALL_PATHS
                    or rebound_sql_receiver
                    or untrusted_safe_attribute_parameter
                    or attribute_path is not None
                    and attribute_path[0] in untrusted_safe_module_names
                )
            ):
                violations.append(
                    f"{owner}:{node.lineno}:call:{call_path or call_name}"
                )
            if call_name in _FORBIDDEN_EXTERNAL_CALLS or any(
                token in call_path for token in _FORBIDDEN_EXTERNAL_TOKENS
            ):
                violations.append(
                    f"{owner}:{node.lineno}:call:{call_path or call_name}"
                )
    return sorted(set(violations))


def test_detector_follows_only_task5_call_graph_helpers() -> None:
    source = """
async def external_effect():
    await lark.send_card()

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

    async def legacy_delivery(self):
        await exchange.send()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._record_delete()
        await external_effect()

    async def _record_delete(self):
        await cursor.execute('UPDATE send_outbox SET cancelled = true')

    async def unrelated_legacy(self):
        await cursor.execute("UPDATE notification_cards SET state = 'closed'")
"""

    violations = _side_effect_violations(source, filename="<call-graph-contract>")

    assert any("mutation:update:send_outbox" in violation for violation in violations)
    assert any("call:lark.send_card" in violation for violation in violations)
    assert all("legacy_delivery" not in violation for violation in violations)
    assert all("unrelated_legacy" not in violation for violation in violations)


def test_detector_rejects_external_client_paths_and_unknown_imported_helpers() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._deliver()

    async def _deliver(self):
        await self._exchange_client.request("POST", "/send")
        await self._lark_client.post("/cards")
        await self._qdrant_client.upsert("emails")
        await self._remote_client.request("POST", "/opaque-effect")
        await helper.request("POST", "/opaque-helper-effect")
        await imported_delivery_helper()

    async def unrelated_legacy(self):
        await self._exchange_client.request("POST", "/legacy-send")
"""

    violations = _side_effect_violations(
        source,
        filename="<external-call-contract>",
    )

    assert {
        violation.rpartition(":call:")[2]
        for violation in violations
        if ":call:" in violation
    } == {
        "imported_delivery_helper",
        "self._exchange_client.request",
        "self._lark_client.post",
        "self._qdrant_client.upsert",
        "self._remote_client.request",
        "helper.request",
    }
    assert all("unrelated_legacy" not in violation for violation in violations)


def test_detector_fail_closes_every_non_allowlisted_mutation() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._mutate()

    async def _mutate(self):
        table = unresolved_table
        await cursor.execute('INSERT INTO "runtime"."pipeline_legacy_effects" (id) VALUES (%s)')
        await cursor.execute('UPDATE runtime.outbound_jobs SET status = %s')
        await cursor.execute('UPDATE ONLY "audit"."audit_events" SET result = %s')
        await cursor.execute('DELETE FROM ONLY runtime.audit_events WHERE id = %s')
        await cursor.execute('DELETE FROM "runtime"."emails" WHERE id = %s')
        await cursor.execute(f'UPDATE {table} SET status = %s')
        await cursor.execute('MERGE INTO runtime.emails USING incoming ON false')
        await cursor.execute('TRUNCATE TABLE ONLY "runtime"."emails"')
        await cursor.execute('COPY "runtime"."emails" (id) FROM STDIN')
"""

    violations = _side_effect_violations(
        source,
        filename="<mutation-allowlist-contract>",
    )

    assert {
        violation.partition(":mutation:")[2]
        for violation in violations
        if ":mutation:" in violation
    } == {
        "copy:runtime.emails",
        "delete:runtime.audit_events",
        "delete:runtime.emails",
        "insert:runtime.pipeline_legacy_effects",
        "merge:runtime.emails",
        "truncate:runtime.emails",
        "unresolved:dynamic_identifier",
        "update:audit.audit_events",
        "update:dynamic_identifier",
        "update:runtime.outbound_jobs",
    }


def test_detector_allows_only_the_task5_mutation_manifest() -> None:
    source = """
class InboxRepository:
    def _table(self, name):
        return sql.Identifier(self._schema, name)

    def transaction(self):
        return EmailEventTransaction(self)

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    def __init__(self, repository):
        self._repository = repository

    async def apply_email_event(self):
        await self._mutate()

    async def _mutate(self):
        await cursor.execute(sql.SQL('INSERT INTO {} (id) VALUES (%s)').format(self._repository._table('emails')))
        await cursor.execute(sql.SQL('UPDATE {} SET status = %s').format(self._repository._table('emails')))
        await cursor.execute(sql.SQL('INSERT INTO {} (id) VALUES (%s)').format(self._repository._table('audit_events')))
        await cursor.execute('SELECT * FROM outbound_jobs')
"""

    assert [
        mutation
        for statement in (
            "INSERT INTO __task5_trusted_schema__.emails (id) VALUES (%s)",
            "UPDATE __task5_trusted_schema__.emails SET status = %s",
            "INSERT INTO __task5_trusted_schema__.audit_events (id) VALUES (%s)",
        )
        for mutation in _mutation_operation_targets(statement)
    ] == [
        ("insert", "__task5_trusted_schema__.emails"),
        ("update", "__task5_trusted_schema__.emails"),
        ("insert", "__task5_trusted_schema__.audit_events"),
    ]
    assert _mutation_operation_targets("SELECT * FROM emails FOR UPDATE") == []
    assert _side_effect_violations(source, filename="<allowed-mutation-contract>") == []


def test_sql_lexer_ignores_comments_strings_and_row_lock_clauses() -> None:
    source = '''
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._read()

    async def _read(self):
        await cursor.execute("""
            -- INSERT INTO outbound_jobs VALUES (1)
            /* UPDATE outbound_jobs SET state = 'x';
               /* DELETE FROM pipeline_legacy_effects */
               MERGE INTO outbound_jobs USING x ON false;
               TRUNCATE outbound_jobs;
               COPY outbound_jobs FROM STDIN; */
            SELECT 'DELETE FROM notification_outbox',
                   $$UPDATE pipeline_legacy_effects SET state = 1$$,
                   $body$MERGE INTO outbound_jobs USING x ON false$body$
            FROM emails AS e
            FOR UPDATE OF e SKIP LOCKED
        """)
        await cursor.execute('SELECT * FROM emails AS e FOR NO KEY UPDATE OF e NOWAIT')
'''

    assert _side_effect_violations(source, filename="<sql-lexical-skip-contract>") == []


def test_sql_lexer_finds_cte_mutations_and_restricted_reads() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._queries()

    async def _queries(self):
        await cursor.execute('WITH removed AS (DELETE FROM outbound_jobs RETURNING id), recorded AS (INSERT INTO pipeline_legacy_effects (id) SELECT id FROM removed RETURNING id) SELECT * FROM recorded')
        await cursor.execute('SELECT * FROM ONLY emails, notification_outbox')
        await cursor.execute('WITH seen AS (SELECT * FROM pipeline_legacy_effects) SELECT * FROM seen')
"""

    violations = _side_effect_violations(source, filename="<cte-sql-contract>")

    assert any("mutation:delete:outbound_jobs" in item for item in violations)
    assert any("mutation:insert:pipeline_legacy_effects" in item for item in violations)
    assert any("relation:notification_outbox" in item for item in violations)
    assert any("relation:pipeline_legacy_effects" in item for item in violations)


def test_sql_lexer_rejects_commands_ddl_and_unknown_select_functions() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._commands()

    async def _commands(self):
        await cursor.execute('CALL deliver_mail()')
        await cursor.execute('DROP TABLE emails')
        await cursor.execute('DO $$ BEGIN PERFORM deliver_mail(); END $$')
        await cursor.execute('COPY emails TO STDOUT')
        await cursor.execute('SELECT dangerous_side_effect()')
        await cursor.execute('SELECT pg_catalog.clock_timestamp()')
"""

    violations = _side_effect_violations(source, filename="<sql-command-contract>")

    assert any("statement:call" in item for item in violations)
    assert any("statement:drop" in item for item in violations)
    assert any("statement:do" in item for item in violations)
    assert any("statement:copy" in item for item in violations)
    assert any("function:dangerous_side_effect" in item for item in violations)
    assert all("clock_timestamp" not in item for item in violations)


def test_sql_lexer_rejects_unknown_functions_inside_allowed_dml() -> None:
    source = '''
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction(self)

    def _table(self, name):
        return sql.Identifier(self._schema, name)

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    def __init__(self, repository):
        self._repository = repository

    async def apply_email_event(self):
        await self._mutate()

    async def _mutate(self):
        await cursor.execute(sql.SQL("""
            UPDATE {} SET status = cancel_send_outbox(),
            updated_at = coalesce(updated_at, pg_catalog.clock_timestamp())
        """).format(self._repository._table('emails')))
'''

    violations = _side_effect_violations(source, filename="<dml-function-contract>")

    assert any("function:cancel_send_outbox" in item for item in violations)
    assert all("function:coalesce" not in item for item in violations)
    assert all("function:pg_catalog.clock_timestamp" not in item for item in violations)


def test_execution_kwargs_are_resolved_or_fail_closed() -> None:
    source = """
RESTRICTED_QUERY = {"query": "SELECT * FROM send_outbox"}
SAFE_QUERY = {"query": "SELECT * FROM emails"}

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._queries()

    async def _queries(self):
        await cursor.execute(**RESTRICTED_QUERY)
        await cursor.execute(**SAFE_QUERY)
        await cursor.execute(**dynamic_execution_kwargs)
"""

    violations = _side_effect_violations(source, filename="<execution-kwargs-contract>")

    assert any("relation:send_outbox" in item for item in violations)
    assert (
        sum("mutation:unresolved:dynamic_identifier" in item for item in violations)
        == 1
    )


def test_partial_dynamic_sql_fragments_fail_closed() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction(self)

    def _table(self, name):
        return sql.Identifier(self._schema, name)

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    def __init__(self, repository):
        self._repository = repository

    async def apply_email_event(self):
        await self._queries()

    async def _queries(self):
        await cursor.execute(sql.SQL('SELECT {}').format(unknown_select_fragment))
        await cursor.execute(sql.SQL('UPDATE {} SET {}').format(self._repository._table('emails'), unknown_set_fragment))
        await cursor.execute("SELECT 'dynamic_identifier', dynamic_identifier_column")
"""

    violations = _side_effect_violations(source, filename="<partial-dynamic-contract>")

    assert (
        sum("mutation:unresolved:dynamic_identifier" in item for item in violations)
        == 2
    )


def test_mutation_allowlist_requires_trusted_table_origin() -> None:
    source = """
SCHEMA = unresolved_schema

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction(self)

    def _table(self, name):
        return sql.Identifier(self._schema, name)

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    def __init__(self, repository):
        self._repository = repository

    async def apply_email_event(self):
        await self._mutate()

    async def _mutate(self):
        await cursor.execute(sql.SQL('INSERT INTO {} (id) VALUES (%s)').format(self._repository._table('emails')))
        await cursor.execute(sql.Composed([sql.SQL('INSERT INTO '), self._repository._table('audit_events'), sql.SQL(' (id) VALUES (%s)')]))
        await cursor.execute(sql.SQL(' ').join([sql.SQL('UPDATE'), self._repository._table('emails'), sql.SQL('SET status = %s')]))
        await cursor.execute('UPDATE emails SET status = %s')
        await cursor.execute('INSERT INTO runtime.emails (id) VALUES (%s)')
        await cursor.execute('UPDATE notification_outbox.emails SET status = %s')
        await cursor.execute('UPDATE 业务.邮件 SET 状态 = %s')
        await cursor.execute(sql.SQL('UPDATE {} SET status = %s').format(sql.Identifier(SCHEMA, 'emails')))
"""

    violations = _side_effect_violations(
        source,
        filename="<trusted-table-origin-contract>",
    )
    mutations = {
        item.partition(":mutation:")[2] for item in violations if ":mutation:" in item
    }

    assert mutations == {
        "insert:runtime.emails",
        "unresolved:dynamic_identifier",
        "update:dynamic_identifier.emails",
        "update:emails",
        "update:notification_outbox.emails",
        "update:业务.邮件",
    }
    assert all("sql.composed" not in item for item in violations)
    assert all("sql.sql().join" not in item for item in violations)


def test_detector_recognizes_identifier_safe_phase3_relation_names() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._record_delete()

    async def _record_delete(self):
        await cursor.execute('INSERT INTO draft_versions (id) VALUES (%s)')
        await cursor.execute('UPDATE notification_outbox SET status = %s')
        await cursor.execute('UPDATE mailbox_action_outbox SET status = %s')
        await cursor.execute('UPDATE mailbox_outbox SET status = %s')
        await cursor.execute('UPDATE send_outbox SET status = %s')
        await cursor.execute('DELETE FROM approval_actions WHERE email_id = %s')
        await cursor.execute('DELETE FROM approval_resources WHERE email_id = %s')
        await cursor.execute('UPDATE send_intents SET status = %s')
        await cursor.execute('DELETE FROM send_resolution_actions WHERE id = %s')
        await cursor.execute('UPDATE card_resources SET status = %s')
        await cursor.execute('UPDATE notification_cards SET status = %s')
        await cursor.execute('UPDATE legacy_lark_card_invalidations SET status = %s')
        await cursor.execute('UPDATE pipeline_activation_barrier_successors SET superseded_by = %s')
        await cursor.execute('DELETE FROM pipeline_activation_consumptions WHERE id = %s')
        await cursor.execute('UPDATE pipeline_legacy_effects SET result = %s')
"""

    violations = _side_effect_violations(
        source,
        filename="<phase3-relation-contract>",
    )

    assert {
        violation.rpartition(":")[2]
        for violation in violations
        if ":mutation:" in violation
    } == _PHASE3_RELATIONS


def test_detector_resolves_module_relation_constants_in_psycopg_identifiers() -> None:
    source = """
DRAFT_TABLE = "draft_versions"
SEND_TABLE = "send_outbox"
CARD_TABLE = "legacy_lark_card_invalidations"

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._record_delete()

    async def _record_delete(self):
        await cursor.execute(sql.SQL("INSERT INTO {} (id) VALUES (%s)").format(sql.Identifier(DRAFT_TABLE)))
        await cursor.execute(sql.SQL("UPDATE {} SET status = %s").format(sql.Identifier(SEND_TABLE)))
        await cursor.execute(sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(CARD_TABLE)))
"""

    violations = _side_effect_violations(
        source,
        filename="<module-relation-binding-contract>",
    )

    assert {
        violation.rpartition(":")[2]
        for violation in violations
        if ":mutation:" in violation
    } == {
        "draft_versions",
        "legacy_lark_card_invalidations",
        "send_outbox",
    }


def _task5_sql_source(*statements: str) -> str:
    executions = "\n".join(
        f"        await cursor.execute({statement!r})" for statement in statements
    )
    return f"""
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._queries()

    async def _queries(self):
{executions}
"""


def test_sql_lexer_distinguishes_standard_escape_strings_and_unicode_identifiers() -> (
    None
):
    standard = _side_effect_violations(
        _task5_sql_source("SELECT '\\'; UPDATE send_outbox SET status='x'"),
        filename="<standard-string-contract>",
    )
    escaped = _side_effect_violations(
        _task5_sql_source("SELECT E'\\'; UPDATE send_outbox SET status='x'"),
        filename="<escape-string-contract>",
    )
    unicode_identifier = _side_effect_violations(
        _task5_sql_source('SELECT * FROM U&"send\\005Foutbox"'),
        filename="<unicode-identifier-contract>",
    )

    assert any("mutation:update:send_outbox" in item for item in standard)
    assert any("relation:send_outbox" in item for item in standard)
    assert escaped == []
    assert any(
        "mutation:unresolved:dynamic_identifier" in item for item in unicode_identifier
    )


def test_sql_statement_roots_use_a_positive_allowlist_with_one_set_exception() -> None:
    source = _task5_sql_source(
        "EXECUTE prepared_mutation",
        "PREPARE harmless AS SELECT 1",
        "DEALLOCATE harmless",
        "NOTIFY task5_channel, 'payload'",
        "LISTEN task5_channel",
        "UNLISTEN task5_channel",
        "SET LOCAL search_path = attacker, public",
        "RESET ALL",
        "LOCK TABLE emails IN ACCESS EXCLUSIVE MODE",
        "DISCARD ALL",
        "SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED",
    )
    violations = _side_effect_violations(source, filename="<statement-root-contract>")

    assert {
        item.partition(":statement:")[2] for item in violations if ":statement:" in item
    } == {
        "deallocate",
        "discard",
        "execute",
        "listen",
        "lock",
        "notify",
        "prepare",
        "reset",
        "set",
        "unlisten",
    }


def test_sql_function_allowlist_rejects_quoted_names_and_unsafe_set_config() -> None:
    source = _task5_sql_source(
        'SELECT "coalesce"(), "any"()',
        "SELECT pg_catalog.set_config('search_path', 'attacker', false)",
        "SELECT pg_catalog.set_config('lock_timeout', '0', true)",
        "SELECT pg_catalog.set_config('lock_timeout', %s, true)",
    )
    violations = _side_effect_violations(source, filename="<sql-function-shape>")
    functions = [
        item.partition(":function:")[2] for item in violations if ":function:" in item
    ]

    assert functions.count("coalesce") == 1
    assert functions.count("any") == 1
    assert functions.count("pg_catalog.set_config") == 2


def test_ast_binding_mutations_and_rebound_safe_calls_fail_closed() -> None:
    source = """
OPTIONS = {"query": "SELECT 1"}

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._queries()

    async def _queries(self):
        text_query = "SELECT 1; "
        text_query += "UPDATE send_outbox SET status = 1"
        await cursor.execute(text_query)
        composed_query = sql.SQL("SELECT 1; ")
        composed_query += sql.SQL("UPDATE send_outbox SET status = 1")
        await cursor.execute(composed_query)
        OPTIONS["query"] = "UPDATE send_outbox SET status = 1"
        await cursor.execute(**OPTIONS)
        all = cursor.execute
        await all("UPDATE send_outbox SET status = 1")
        len = cursor.execute
        await len("SELECT 1")
        execute = len
        await execute("SELECT 1")
        cursor = harmless
        await cursor.execute("SELECT 1")
"""
    violations = _side_effect_violations(source, filename="<binding-mutation-contract>")

    assert (
        sum("mutation:unresolved:dynamic_identifier" in item for item in violations)
        == 3
    )
    assert any("call:all" in item for item in violations)
    assert any("call:len" in item for item in violations)
    assert any("call:execute" in item for item in violations)
    assert any("call:cursor.execute" in item for item in violations)


def test_safe_callable_names_require_unshadowed_builtin_provenance() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._parameter(evil)
        await self._imported()
        await self._aliased_import()
        await self._nested()

    async def _parameter(self, all):
        await all()

    async def _imported(self):
        from evil import all
        await all()

    async def _aliased_import(self):
        import evil as all
        await all()

    async def _nested(self):
        def all():
            return True
        await all()
"""
    violations = _side_effect_violations(source, filename="<safe-call-provenance>")

    assert sum("call:all" in item for item in violations) == 4


def test_repository_transaction_type_guard_requires_exact_builtin_provenance() -> None:
    clean = """
class InboxRepository:
    def transaction(self, value):
        return type(value)

    async def apply_email_event(self):
        return self.transaction(True)

class EmailEventTransaction:
    async def apply_email_event(self):
        return None
"""
    module_shadow = "type = forged\n" + clean
    local_rebind = clean.replace(
        "    def transaction(self, value):\n        return type(value)",
        (
            "    def transaction(self, value):\n"
            "        type = forged\n"
            "        return type(value)"
        ),
    )
    wrong_owner = clean.replace(
        "    async def apply_email_event(self):\n        return None",
        (
            "    def _probe(self, value):\n"
            "        return type(value)\n\n"
            "    async def apply_email_event(self):\n"
            "        return self._probe(True)"
        ),
    )

    clean_violations = _side_effect_violations(
        clean,
        filename="<trusted-owner-builtin-type>",
    )
    assert not any("call:type" in item for item in clean_violations)

    for label, source in (
        ("module-shadow", module_shadow),
        ("local-rebind", local_rebind),
        ("wrong-owner", wrong_owner),
    ):
        violations = _side_effect_violations(
            source,
            filename=f"<trusted-owner-builtin-type-{label}>",
        )
        assert any("call:type" in item for item in violations), label


def test_safe_callable_allowlist_rejects_untrusted_module_binders() -> None:
    cases = (
        (
            "builtin",
            "from evil import all",
            "await all()",
            "call:all",
        ),
        (
            "module",
            "import evil as hashlib",
            'hashlib.sha256(b"payload")',
            "call:hashlib.sha256",
        ),
        (
            "imported-callable",
            "from evil import Jsonb",
            "Jsonb({})",
            "call:jsonb",
        ),
    )
    missed: list[str] = []
    for label, module_binding, invocation, expected in cases:
        source = f"""
{module_binding}

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        {invocation}
"""
        violations = _side_effect_violations(
            source,
            filename=f"<safe-call-module-binding-{label}>",
        )
        if not any(expected in item for item in violations):
            missed.append(label)

    assert missed == []


def test_project_local_safe_callables_have_structural_ratchet_coverage() -> None:
    callable_sources = {
        "decide_email_event": "src/ingestion/email_events.py",
        "ownership_advisory_lock_key": "src/ingestion/ownership.py",
    }
    class_exemption_sources = {
        "DatabaseOperationError": "src/domain/errors.py",
        "EmailEventApplication": "src/ingestion/email_events.py",
        "EmailEventDecision": "src/ingestion/email_events.py",
        "EmailStatus": "src/ingestion/email_events.py",
        "InboxLease": "src/ingestion/models.py",
        "ManualReviewRequired": "src/domain/errors.py",
        "NormalizedIngressEvent": "src/ingestion/models.py",
        "PipelineGenerationState": "src/domain/email_state.py",
        "StaleFence": "src/domain/errors.py",
    }
    project_local_imports = {
        name: source
        for name, source in _TRUSTED_SAFE_IMPORT_SOURCES.items()
        if source.startswith("src.")
    }
    external_imports = {
        "Jsonb": "psycopg.types.json",
        "UUID": "uuid",
        "uuid4": "uuid",
    }

    assert set(project_local_imports) == set(callable_sources) | set(
        class_exemption_sources
    )
    assert {
        name: source
        for name, source in _TRUSTED_SAFE_IMPORT_SOURCES.items()
        if not source.startswith("src.")
    } == external_imports
    assert set(_SAFE_NAME_CALLS) == (
        set(project_local_imports)
        | set(external_imports)
        | set(_BUILTIN_SAFE_NAME_CALLS)
        | set(_TRUSTED_SAFE_LOCAL_CLASSES)
    )
    assert _TRUSTED_OWNER_BUILTIN_NAME_CALLS == {
        ("InboxRepository.transaction", "type")
    }
    assert _PROVENANCE_GUARDED_BUILTIN_NAME_CALLS == (
        _BUILTIN_SAFE_NAME_CALLS | {"type"}
    )
    reviewed_sources = set(callable_sources.values()) | set(
        class_exemption_sources.values()
    )
    missing = reviewed_sources - set(_TASK5_REPOSITORY_STRUCTURAL_AST_SHA256)
    assert missing == set()

    project_root = Path(__file__).resolve().parents[2]
    for relative in reviewed_sources:
        tree = ast.parse(
            (project_root / relative).read_text(encoding="utf-8"),
            filename=relative,
        )
        tree.body.append(ast.Pass())
        mutated = _normalized_ast_dump(tree)
        mutated_digest = hashlib.sha256(mutated.encode("utf-8")).hexdigest()
        assert mutated_digest != _TASK5_REPOSITORY_STRUCTURAL_AST_SHA256[relative]


def test_comprehension_targets_do_not_rebind_outer_safe_names() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        values = [all for all in callbacks]
        return all(values)
"""
    violations = _side_effect_violations(source, filename="<comprehension-scope>")

    assert all("call:all" not in item for item in violations)


def test_module_comprehension_targets_do_not_rebind_outer_safe_names() -> None:
    source = """
values = [all for all in callbacks]

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        return all(values)
"""
    violations = _side_effect_violations(
        source,
        filename="<module-comprehension-scope>",
    )

    assert violations == []


def test_trusted_table_provenance_cannot_be_forged() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction(self)

    def _table(self, name):
        return sql.Identifier(self._schema, name)

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    def __init__(self, repository):
        self._repository = repository

    async def apply_email_event(self):
        await self._mutate()

    async def _mutate(self):
        await cursor.execute('UPDATE __task5_trusted_schema__.emails SET status = 1')
        await cursor.execute(sql.SQL('UPDATE {} SET status = 1').format(sql.Identifier('__task5_trusted_schema__', 'emails')))
        await cursor.execute(sql.SQL('UPDATE {} SET status = 1').format(other._table('emails')))
        await cursor.execute(sql.Composed([sql.SQL('UPDATE __task5_trusted_schema__.emails SET status = 1; UPDATE '), self._repository._table('emails'), sql.SQL(' SET status = 1')]))
"""
    violations = _side_effect_violations(
        source, filename="<trusted-provenance-contract>"
    )

    assert (
        sum(
            "mutation:update:__task5_trusted_schema__.emails" in item
            for item in violations
        )
        == 4
    )


def test_trusted_table_factory_must_match_production_implementation() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction(self)

    def _table(self, name):
        return sql.Identifier('public', 'send_outbox')

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    def __init__(self, repository):
        self._repository = repository

    async def apply_email_event(self):
        await self._mutate()

    async def _mutate(self):
        await cursor.execute(sql.SQL('UPDATE {} SET status = 1').format(self._repository._table('emails')))
"""
    violations = _side_effect_violations(source, filename="<trusted-factory-contract>")

    assert "factory:InboxRepository._table:untrusted" in violations


def test_safe_sql_syntax_is_not_misclassified_as_function_calls() -> None:
    source = _task5_sql_source(
        "SELECT * FROM emails JOIN audit_events USING(id)",
        "WITH RECURSIVE a(id) AS (SELECT 1), b(id) AS (SELECT id FROM a) SELECT * FROM b",
        "SELECT CAST(1 AS pg_catalog.int8)",
        "SELECT 1 WHERE EXISTS (SELECT 1)",
        "SELECT DISTINCT ON (account_id) account_id FROM emails",
    )

    assert _side_effect_violations(source, filename="<safe-sql-syntax>") == []


def test_function_alias_is_not_mistaken_for_a_cte_column_list() -> None:
    violations = _side_effect_violations(
        _task5_sql_source(
            "SELECT dangerous_side_effect() AS result",
            "SELECT * FROM dangerous_side_effect() AS result",
        ),
        filename="<function-alias-contract>",
    )

    assert sum("function:dangerous_side_effect" in item for item in violations) == 2


def test_reachable_factory_attribute_writes_invalidate_trusted_provenance() -> None:
    cases = (
        ("transaction", "self._repository = other"),
        ("transaction", "self._repository._table = other._table"),
        ("transaction", "self._repository._schema = 'attacker'"),
        ("transaction", "self._repository._schema += '_attacker'"),
        ("transaction", "self._repository._schema, value = pair"),
        ("transaction", "del self._repository._table"),
        (
            "transaction",
            "repo = self._repository; repo._table = other._table",
        ),
        (
            "transaction",
            "repo = self._repository; repo._schema = 'attacker'",
        ),
        ("transaction", "sql.Identifier = evil"),
        ("transaction", "sql.Text = evil"),
        ("transaction", "driver = sql; driver.SQL = evil"),
        ("transaction", "driver = sql; child = driver; child.Text = evil"),
        (
            "transaction",
            "drivers = (sql,); driver = drivers[0]; driver.SQL = evil",
        ),
        ("transaction", "global sql; sql = evil"),
        ("repository", "self._table = other._table"),
        ("repository", "self._schema = 'attacker'"),
    )
    for owner, mutation in cases:
        repository_mutation = mutation if owner == "repository" else "pass"
        transaction_mutation = mutation if owner == "transaction" else "pass"
        source = f"""
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction(self)

    def _table(self, name):
        return sql.Identifier(self._schema, name)

    async def apply_email_event(self):
        {repository_mutation}
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    def __init__(self, repository):
        self._repository = repository

    async def apply_email_event(self):
        await self._mutate()

    async def _mutate(self):
        {transaction_mutation}
        await cursor.execute(sql.SQL('UPDATE {{}} SET status = 1').format(self._repository._table('emails')))
"""

        violations = _side_effect_violations(
            source,
            filename=f"<factory-write-{owner}>",
        )

        assert "factory:InboxRepository._table:untrusted" in violations


def test_reachable_sql_bindings_invalidate_trusted_provenance() -> None:
    cases = (
        ("parameter", ", sql", "evil", "pass"),
        ("assignment", "", "", "sql = evil"),
        ("nested-def", "", "", "def sql():\n            pass"),
        (
            "except-target",
            "",
            "",
            "try:\n            value = 1\n        except Exception as sql:\n            pass",
        ),
        (
            "match-target",
            "",
            "",
            "match value:\n            case sql:\n                pass",
        ),
        ("sql-constructor", "", "", "sql.SQL = evil"),
        ("composed-constructor", "", "", "sql.Composed = evil"),
        ("identifier-constructor", "", "", "sql.Identifier = evil"),
        ("import-from", "", "", "from evil import sql"),
        ("import-alias", "", "", "import evil as sql"),
        ("star-import", "", "", "from evil import *"),
    )
    missed: list[str] = []
    for label, parameter, call_argument, mutation in cases:
        source = f"""
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction(self)

    def _table(self, name):
        return sql.Identifier(self._schema, name)

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    def __init__(self, repository):
        self._repository = repository

    async def apply_email_event(self):
        await self._mutate({call_argument})

    async def _mutate(self{parameter}):
        {mutation}
        await cursor.execute(sql.SQL('UPDATE {{}} SET status = 1').format(self._repository._table('emails')))
"""
        violations = _side_effect_violations(
            source,
            filename=f"<reachable-sql-binding-{label}>",
        )
        if "factory:InboxRepository._table:untrusted" not in violations:
            missed.append(label)

    assert missed == []


def test_module_factory_namespace_writes_invalidate_trusted_provenance() -> None:
    cases = (
        ("factory", "InboxRepository._table = evil"),
        ("factory-child", "InboxRepository._table.helper = evil"),
        (
            "factory-alias-child",
            "factory = InboxRepository._table; factory.helper = evil",
        ),
        ("assignment", "sql = evil"),
        ("sql-child", "sql.Text = evil"),
        ("sql-alias-child", "driver = sql\ndriver.Text = evil"),
        ("import-from", "from evil import sql"),
        ("import-alias", "import evil as sql"),
        ("second-import", "from psycopg import sql\nfrom evil import sql"),
        ("second-alias", "from psycopg import sql\nimport evil as sql"),
        ("star-import", "from psycopg import sql\nfrom evil import *"),
        ("class", "class sql:\n    pass"),
        ("function", "def sql():\n    pass"),
        (
            "except-target",
            "try:\n    value = 1\nexcept Exception as sql:\n    pass",
        ),
        ("match-target", "match value:\n    case sql:\n        pass"),
    )
    missed: list[str] = []
    for label, mutation in cases:
        source = f"""
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction(self)

    def _table(self, name):
        return sql.Identifier(self._schema, name)

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

{mutation}

class EmailEventTransaction:
    def __init__(self, repository):
        self._repository = repository

    async def apply_email_event(self):
        await cursor.execute(sql.SQL('UPDATE {{}} SET status = 1').format(self._repository._table('emails')))
"""

        violations = _side_effect_violations(
            source,
            filename=f"<module-factory-write-{label}>",
        )
        if "factory:InboxRepository._table:untrusted" not in violations:
            missed.append(label)

    assert missed == []


def test_repository_freezes_psycopg_sql_import_and_module_bindings() -> None:
    repository = (
        Path(__file__).resolve().parents[2] / "src" / "ingestion" / "repository.py"
    )
    tree = ast.parse(repository.read_text(encoding="utf-8"), filename=str(repository))
    sql_imports = [
        node
        for node in _module_scope_nodes(tree)
        if "sql" in _import_binding_names(node)
    ]
    trusted_imports = [node for node in sql_imports if _is_trusted_sql_import(node)]
    protected_writes = [
        node
        for node in _module_scope_nodes(tree)
        if isinstance(node, ast.ClassDef)
        and node.name == "sql"
        or any(
            path
            in {
                ("InboxRepository", "_table"),
                ("sql",),
                ("sql", "Composed"),
                ("sql", "Identifier"),
                ("sql", "SQL"),
            }
            for target in _written_targets(node)
            for path in _target_attribute_paths(target)
        )
    ]

    assert len(sql_imports) == 1
    assert trusted_imports == sql_imports
    assert protected_writes == []


def test_loop_and_unpack_execution_receivers_fail_closed() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._queries()
        await self._unpack()
        await self._with_cursor()
        await self._comprehension()

    async def _queries(self):
        for cursor in connections:
            await cursor.execute("SELECT 1")
        async for cursor in async_connections:
            await cursor.execute("SELECT 1")
        all, query = pair
        await all(query)

    async def _unpack(self):
        cursor, = connections
        await cursor.execute("SELECT 1")

    async def _with_cursor(self):
        async with self._context as cursor:
            await cursor.execute("SELECT 1")

    async def _comprehension(self):
        return [await cursor.execute("SELECT 1") async for cursor in async_connections]
"""
    violations = _side_effect_violations(source, filename="<receiver-binding-contract>")

    assert sum("call:cursor.execute" in item for item in violations) == 5
    assert any("call:all" in item for item in violations)


def test_helper_parameter_and_exception_receivers_fail_closed() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._use(self._remote_client)
        await self._queries()

    async def _use(self, cursor):
        await cursor.execute("SELECT 1")

    async def _queries(self):
        try:
            value = 1
        except Exception as cursor:
            await cursor.execute("SELECT 1")
"""
    violations = _side_effect_violations(source, filename="<helper-receiver-contract>")

    assert sum("call:cursor.execute" in item for item in violations) == 2


def test_database_receiver_parameters_require_trusted_call_sources() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._one(self._remote_client)
        await self._many(self._remote_client)
        await self._transaction(self._remote_client)

    async def _one(self, cursor):
        await cursor.fetchone()

    async def _many(self, cursor):
        await cursor.fetchall()

    async def _transaction(self, connection):
        async with connection.transaction():
            pass
"""
    violations = _side_effect_violations(source, filename="<database-receiver-source>")

    assert any("call:cursor.fetchone" in item for item in violations)
    assert any("call:cursor.fetchall" in item for item in violations)
    assert any("call:connection.transaction" in item for item in violations)


def test_safe_attribute_parameters_require_trusted_call_sources() -> None:
    source = """
class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await self._read(self._remote_client)

    async def _read(self, item):
        item.get("status")
"""
    violations = _side_effect_violations(
        source,
        filename="<safe-attribute-parameter-source>",
    )

    assert any("call:item.get" in item for item in violations)


def test_subscript_callables_and_saved_execute_receivers_fail_closed() -> None:
    source = """
CALLS = {"run": remote.send}

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        calls = {"run": cursor.execute}
        await calls["run"]("UPDATE send_outbox SET status = 1")
        await self._helper()

    async def _helper(self):
        await CALLS["run"]()
"""
    violations = _side_effect_violations(source, filename="<subscript-call-contract>")

    assert sum("call:dynamic" in item for item in violations) == 2


def test_reachable_helper_mapping_mutation_taints_module_binding() -> None:
    mutations = (
        'OPTIONS["query"] = "UPDATE send_outbox SET status = 1"',
        'global OPTIONS\n    OPTIONS = {"query": "UPDATE send_outbox SET status = 1"}',
        'alias = OPTIONS\n    alias["query"] = "UPDATE send_outbox SET status = 1"',
        'alias = OPTIONS\n    second = alias\n    second["query"] = "UPDATE send_outbox SET status = 1"',
    )
    missed: list[str] = []
    for index, mutation in enumerate(mutations):
        source = f"""
OPTIONS = {{"query": "SELECT 1"}}

async def poison():
    {mutation}

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await poison()
        await cursor.execute(**OPTIONS)
"""
        violations = _side_effect_violations(
            source,
            filename=f"<helper-binding-contract-{index}>",
        )
        if not any(
            "mutation:unresolved:dynamic_identifier" in item for item in violations
        ):
            missed.append(str(index))

    assert missed == []


def test_helper_parameter_mapping_mutation_taints_call_argument() -> None:
    cases = (
        (
            "direct",
            "options",
            "OPTIONS",
            'options["query"] = "UPDATE send_outbox SET status = 1"',
        ),
        (
            "alias",
            "options",
            "OPTIONS",
            'alias = options\n    alias["query"] = "UPDATE send_outbox SET status = 1"',
        ),
        (
            "container-alias",
            "options",
            "OPTIONS",
            'container = [options]\n    container[0]["query"] = "UPDATE send_outbox SET status = 1"',
        ),
        (
            "star",
            "options",
            "*[OPTIONS]",
            'options["query"] = "UPDATE send_outbox SET status = 1"',
        ),
        (
            "keyword",
            "options",
            "options=OPTIONS",
            'options["query"] = "UPDATE send_outbox SET status = 1"',
        ),
        (
            "default",
            "options=OPTIONS",
            "",
            'options["query"] = "UPDATE send_outbox SET status = 1"',
        ),
        (
            "container",
            "options",
            "CONTAINER[0]",
            'options["query"] = "UPDATE send_outbox SET status = 1"',
        ),
        (
            "conditional",
            "options",
            "OPTIONS if choose else {}",
            'options["query"] = "UPDATE send_outbox SET status = 1"',
        ),
        (
            "unknown-call",
            "options",
            "identity(OPTIONS)",
            'options["query"] = "UPDATE send_outbox SET status = 1"',
        ),
    )
    missed: list[str] = []
    for label, parameter, call_arguments, mutation in cases:
        source = f"""
OPTIONS = {{"query": "SELECT 1"}}
CONTAINER = [OPTIONS]

async def poison({parameter}):
    {mutation}

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await poison({call_arguments})
        await cursor.execute(**OPTIONS)
"""
        violations = _side_effect_violations(
            source,
            filename=f"<parameter-mapping-contract-{label}>",
        )
        if not any(
            "mutation:unresolved:dynamic_identifier" in item for item in violations
        ):
            missed.append(label)

    assert missed == []


def test_unrelated_parameter_container_writes_do_not_taint_module_mapping() -> None:
    source = """
OPTIONS = {"query": "SELECT 1"}

async def touch(value):
    container = [value]
    container[0] = None

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await touch(1)
        await cursor.execute(**OPTIONS)
"""
    violations = _side_effect_violations(
        source,
        filename="<unrelated-parameter-container>",
    )

    assert violations == []


def test_scalar_call_results_do_not_retain_parameter_identity() -> None:
    source = """
OPTIONS = {"query": "SELECT 1"}

async def touch(options):
    container = [len(options)]
    container[0] = None

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        await touch(OPTIONS)
        await cursor.execute(**OPTIONS)
"""
    violations = _side_effect_violations(
        source,
        filename="<scalar-call-identity>",
    )

    assert violations == []


def test_local_mapping_shadow_does_not_taint_same_named_module_mapping() -> None:
    source = """
OPTIONS = {"query": "SELECT 1"}

async def poison(options):
    options["touched"] = True

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        OPTIONS = {"query": "SELECT 2"}
        await poison(OPTIONS)
        await self._read_module_mapping()

    async def _read_module_mapping(self):
        await cursor.execute(**OPTIONS)
"""
    violations = _side_effect_violations(
        source,
        filename="<local-mapping-shadow>",
    )

    assert violations == []


def test_mapping_default_is_resolved_in_definition_scope() -> None:
    source = """
OPTIONS = {"query": "SELECT 1"}

async def poison(options=OPTIONS):
    options["touched"] = True

class InboxRepository:
    def transaction(self):
        return EmailEventTransaction()

    async def apply_email_event(self):
        return await self.transaction().apply_email_event()

class EmailEventTransaction:
    async def apply_email_event(self):
        OPTIONS = {"query": "SELECT 2"}
        await poison()
        await self._read_module_mapping()

    async def _read_module_mapping(self):
        await cursor.execute(**OPTIONS)
"""
    violations = _side_effect_violations(
        source,
        filename="<mapping-default-definition-scope>",
    )

    assert any("mutation:unresolved:dynamic_identifier" in item for item in violations)


def test_task5_email_event_call_graph_has_no_phase3_or_external_side_effect() -> None:
    repository = (
        Path(__file__).resolve().parents[2] / "src" / "ingestion" / "repository.py"
    )

    violations = _side_effect_violations(
        repository.read_text(encoding="utf-8"),
        filename=str(repository),
    )

    assert violations == [], (
        "Task-5 email-event application may only mutate emails and its exact "
        f"processing receipt; found {violations}"
    )
