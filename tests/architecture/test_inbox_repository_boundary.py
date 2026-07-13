from __future__ import annotations

import ast
import re
from pathlib import Path


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
        if isinstance(
            node,
            (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef, ast.Lambda),
        ):
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


def _render_sql(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]

    if isinstance(node, ast.JoinedStr):
        rendered = ""
        for part in node.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                rendered += part.value
            else:
                rendered += "dynamic_identifier"
        return [rendered]

    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _render_sql(node.left, bindings, resolving=resolving)
        right = _render_sql(node.right, bindings, resolving=resolving)
        return [left_part + right_part for left_part in left for right_part in right]

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
        if keyword.arg in {"query", "statement"}:
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
"""

    assert _find_event_inbox_mutations(source, filename="<mutation-contract>") == [
        3,
        4,
        5,
        6,
        8,
        12,
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
