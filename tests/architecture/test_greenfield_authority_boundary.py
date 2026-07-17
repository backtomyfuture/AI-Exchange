from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GREENFIELD_MODULES = (
    ROOT / "src" / "ingestion" / "runtime_capability.py",
    ROOT / "src" / "ingestion" / "runtime_authority.py",
    ROOT / "src" / "ingestion" / "recovery.py",
)
AUTHORITY_SQL_MODULES = (
    ROOT / "src" / "ingestion" / "runtime_authority.py",
    ROOT / "src" / "ingestion" / "recovery.py",
)
COMMAND_RECEIPTS = ROOT / "src" / "ingestion" / "command_receipts.py"
GREENFIELD_MIGRATION = (
    ROOT / "alembic" / "versions" / "20260716_0006_greenfield_runtime_authority.py"
)
PRODUCTION_STARTUP = (
    ROOT / "src" / "main.py",
    ROOT / "src" / "init_app.py",
    ROOT / "src" / "server.py",
)

_BANNED_IMPORT_PREFIXES = (
    "aiohttp",
    "fastapi",
    "httpx",
    "langgraph",
    "lark_oapi",
    "openai",
    "qdrant_client",
    "requests",
    "src.config",
    "src.clients",
    "src.exchange_service",
    "src.graph",
    "src.init_app",
    "src.main",
    "src.nodes",
    "src.router",
    "src.server",
    "src.settings",
    "src.utils.email_processor",
    "src.utils.exchange_api",
    "src.utils.lark_app",
    "src.utils.retriever",
)
_BANNED_IMPORT_SEGMENTS = frozenset(
    {
        "backfill",
        "cutover",
        "legacy",
        "shadow",
    }
)
_BANNED_IMPORT_SYMBOLS = frozenset(
    {
        "ChatOpenAI",
        "DurableInboxWorker",
        "ExchangeClient",
        "LegacyProcessingAdapter",
        "QdrantClient",
        "WebhookWorker",
        "get_settings",
        "lark_app",
        "Settings",
    }
)
_DYNAMIC_IMPORT_CALLS = frozenset({"__import__", "import_module"})
_SQL_EXECUTION_METHODS = frozenset(
    {"copy", "copy_expert", "execute", "executemany", "exec_driver_sql"}
)
_SQL_WRAPPERS = frozenset({"SQL", "text"})
_GOVERNED_RELATIONS = (
    "audit_events",
    "emails",
    "event_inbox",
    "pipeline_command_receipts",
    "pipeline_folder_scopes",
    "pipeline_initializations",
    "pipeline_ownership",
    "pipeline_runtime_authority",
    "pipeline_runtime_capabilities",
    "pipeline_runtime_instances",
    "sync_cold_start_plans",
    "sync_cursors",
)
_RAW_DML = re.compile(
    r"\b(?:insert\s+into|update|delete\s+from|merge\s+into|"
    r"truncate(?:\s+table)?|copy)\s+(?:\"?public\"?\s*\.\s*)?"
    r"\"?(?:" + "|".join(_GOVERNED_RELATIONS) + r")\"?\b",
    re.IGNORECASE,
)
_FIXED_GREENFIELD_CALL = re.compile(
    r"\Aselect\b.+?\bfrom\s+"
    r"(?P<function>public\.greenfield_[a-z][a-z0-9_]*)\s*\([^()]*\)\s*\Z",
    re.IGNORECASE | re.DOTALL,
)
_CREATE_FUNCTION = re.compile(
    r"\bcreate\s+(?:or\s+replace\s+)?function\s+"
    r"\"?public\"?\s*\.\s*\"?(greenfield_[a-z][a-z0-9_]*)\"?\s*\(",
    re.IGNORECASE,
)
_FIXED_SEARCH_PATH = re.compile(
    r"\bset\s+search_path\s*(?:=|to)\s*pg_catalog\b",
    re.IGNORECASE,
)
_PUBLIC_ACTIVE_API_MARKERS = frozenset({"activate", "activation", "active", "worker"})
_FORBIDDEN_GENERIC_COMMANDS = frozenset(
    {
        "inbox.requeue",
        "runtime.initialize",
        "runtime.pause",
        "runtime.resume_ingress",
    }
)


def _parse(source: str, *, filename: str) -> ast.Module:
    return ast.parse(source, filename=filename)


def _call_name(expression: ast.expr) -> str | None:
    if isinstance(expression, ast.Name):
        return expression.id
    if isinstance(expression, ast.Attribute):
        return expression.attr
    return None


def _import_violations(source: str, *, filename: str) -> list[str]:
    tree = _parse(source, filename=filename)
    violations: list[str] = []

    def inspect_module(module: str, lineno: int) -> None:
        segments = frozenset(
            part for part in re.split(r"[._]", module.casefold()) if part
        )
        if module.startswith(_BANNED_IMPORT_PREFIXES):
            violations.append(f"{lineno}:banned_import:{module}")
        if segments & _BANNED_IMPORT_SEGMENTS:
            violations.append(f"{lineno}:obsolete_import:{module}")

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                inspect_module(alias.name, node.lineno)
        elif isinstance(node, ast.ImportFrom):
            if node.module is not None:
                inspect_module(node.module, node.lineno)
            for alias in node.names:
                if (
                    alias.name in _BANNED_IMPORT_SYMBOLS
                    or frozenset(alias.name.casefold().split("_"))
                    & _BANNED_IMPORT_SEGMENTS
                ):
                    violations.append(f"{node.lineno}:banned_symbol:{alias.name}")
        elif isinstance(node, ast.Call) and _call_name(node.func) in (
            _DYNAMIC_IMPORT_CALLS
        ):
            violations.append(f"{node.lineno}:dynamic_import")

    return sorted(set(violations))


def _name_parts(name: str) -> frozenset[str]:
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).casefold()
    return frozenset(part for part in snake.split("_") if part)


def _public_phase2_api_violations(source: str, *, filename: str) -> list[str]:
    tree = _parse(source, filename=filename)
    violations: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
            markers = _name_parts(node.name) & _PUBLIC_ACTIVE_API_MARKERS
            if markers:
                violations.append(
                    f"{node.lineno}:public_class:{node.name}:{','.join(sorted(markers))}"
                )
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith("_"):
            continue
        markers = _name_parts(node.name) & _PUBLIC_ACTIVE_API_MARKERS
        if markers:
            violations.append(
                f"{node.lineno}:public_callable:{node.name}:{','.join(sorted(markers))}"
            )
        for candidate in ast.walk(node):
            if (
                isinstance(candidate, ast.Attribute)
                and candidate.attr == "ACTIVE"
                and isinstance(candidate.value, ast.Name)
                and candidate.value.id == "RuntimeAuthorityState"
            ):
                violations.append(f"{candidate.lineno}:active_authority_reference")
            elif isinstance(candidate, ast.Call) and _call_name(candidate.func) == (
                "DurableInboxWorker"
            ):
                violations.append(f"{candidate.lineno}:worker_construction")

    return sorted(set(violations))


def _module_bindings(tree: ast.Module) -> dict[str, list[ast.expr]]:
    bindings: dict[str, list[ast.expr]] = {}
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if value is None:
                continue
            for target in targets:
                if isinstance(target, ast.Name):
                    bindings.setdefault(target.id, []).append(value)
    return bindings


def _static_strings(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> set[str] | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}
    if isinstance(node, ast.JoinedStr):
        if not all(
            isinstance(part, ast.Constant) and isinstance(part.value, str)
            for part in node.values
        ):
            return None
        return {"".join(part.value for part in node.values)}
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_strings(node.left, bindings, resolving=resolving)
        right = _static_strings(node.right, bindings, resolving=resolving)
        if left is None or right is None:
            return None
        return {a + b for a in left for b in right}
    if isinstance(node, ast.Name):
        values = bindings.get(node.id, [])
        if node.id in resolving or len(values) != 1:
            return None
        return _static_strings(
            values[0],
            bindings,
            resolving=resolving | {node.id},
        )
    if isinstance(node, ast.Call) and _call_name(node.func) in _SQL_WRAPPERS:
        if len(node.args) != 1 or node.keywords:
            return None
        return _static_strings(node.args[0], bindings, resolving=resolving)
    return None


def _execution_query(call: ast.Call) -> ast.expr | None:
    if _call_name(call.func) not in _SQL_EXECUTION_METHODS:
        return None
    if call.args:
        return call.args[0]
    for keyword in call.keywords:
        if keyword.arg in {"query", "sql", "statement"}:
            return keyword.value
    return None


def _database_call_contract(
    source: str,
    *,
    filename: str,
) -> tuple[set[str], list[str]]:
    tree = _parse(source, filename=filename)
    bindings = _module_bindings(tree)
    functions: set[str] = set()
    violations: list[str] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        query = _execution_query(node)
        if query is None:
            continue
        statements = _static_strings(query, bindings)
        if statements is None or len(statements) != 1:
            violations.append(f"{node.lineno}:dynamic_sql")
            continue
        statement = next(iter(statements))
        normalized = " ".join(statement.split()).casefold()
        if _RAW_DML.search(normalized):
            violations.append(f"{node.lineno}:raw_governed_dml")
            continue
        if ";" in normalized:
            violations.append(f"{node.lineno}:multi_statement_sql")
            continue
        match = _FIXED_GREENFIELD_CALL.search(normalized)
        if match is None:
            violations.append(f"{node.lineno}:non_greenfield_function_sql")
            continue
        functions.add(match.group("function").casefold())

    return functions, sorted(set(violations))


def _security_definer_greenfield_functions(source: str) -> set[str]:
    tree = _parse(source, filename="<greenfield-migration>")
    functions: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        sql = node.value
        matches = list(_CREATE_FUNCTION.finditer(sql))
        for index, match in enumerate(matches):
            end = matches[index + 1].start() if index + 1 < len(matches) else len(sql)
            definition = sql[match.start() : end]
            if "security definer" not in definition.casefold():
                continue
            if _FIXED_SEARCH_PATH.search(definition) is None:
                continue
            functions.add(f"public.{match.group(1).casefold()}")

    generated_template: str | None = None
    generated_contracts: object = None
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        value = node.value
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            if target.id == "_GREENFIELD_TRANSITION_TEMPLATE":
                try:
                    rendered = ast.literal_eval(value)
                except (TypeError, ValueError):
                    rendered = None
                if isinstance(rendered, str):
                    generated_template = rendered
            elif target.id == "_GREENFIELD_TRANSITION_CONTRACTS":
                try:
                    generated_contracts = ast.literal_eval(value)
                except (TypeError, ValueError):
                    generated_contracts = None
    if (
        generated_template is not None
        and "CREATE FUNCTION public.__ROUTINE_NAME__(" in generated_template
        and "security definer" in generated_template.casefold()
        and _FIXED_SEARCH_PATH.search(generated_template) is not None
        and isinstance(generated_contracts, tuple)
    ):
        for contract in generated_contracts:
            if (
                not isinstance(contract, tuple)
                or len(contract) != 4
                or not all(isinstance(item, str) for item in contract)
            ):
                continue
            routine_name = contract[0]
            if re.fullmatch(r"greenfield_[a-z][a-z0-9_]*", routine_name):
                functions.add(f"public.{routine_name}")
    return functions


def _literal_collection(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> set[str] | None:
    static = _static_strings(node, bindings, resolving=resolving)
    if static is not None:
        return static
    if isinstance(node, (ast.List, ast.Set, ast.Tuple)):
        values: set[str] = set()
        for element in node.elts:
            rendered = _literal_collection(element, bindings, resolving=resolving)
            if rendered is None:
                return None
            values.update(rendered)
        return values
    if isinstance(node, ast.Call) and _call_name(node.func) in {
        "frozenset",
        "list",
        "set",
        "tuple",
    }:
        if len(node.args) != 1 or node.keywords:
            return None
        return _literal_collection(node.args[0], bindings, resolving=resolving)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        left = _literal_collection(node.left, bindings, resolving=resolving)
        right = _literal_collection(node.right, bindings, resolving=resolving)
        if left is None or right is None:
            return None
        return left | right
    if isinstance(node, ast.Name):
        values = bindings.get(node.id, [])
        if node.id in resolving or len(values) != 1:
            return None
        return _literal_collection(
            values[0],
            bindings,
            resolving=resolving | {node.id},
        )
    return None


def _generic_command_namespace_violations(
    source: str,
    *,
    filename: str,
) -> list[str]:
    tree = _parse(source, filename=filename)
    bindings = _module_bindings(tree)
    definitions = bindings.get("_COMMAND_NAMES", [])
    if len(definitions) != 1:
        return ["command_allowlist_not_fixed"]
    commands = _literal_collection(definitions[0], bindings)
    if commands is None:
        return ["command_allowlist_not_static"]
    forbidden = sorted(
        command
        for command in commands
        if command.startswith("runtime.") or command == "inbox.requeue"
    )
    unexpected = sorted(set(forbidden) | (commands & _FORBIDDEN_GENERIC_COMMANDS))
    return [f"generic_command:{command}" for command in unexpected]


def _durable_worker_startup_violations(source: str, *, filename: str) -> list[str]:
    tree = _parse(source, filename=filename)
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "src.ingestion.worker":
                    violations.append(f"{node.lineno}:worker_module_import")
        elif isinstance(node, ast.ImportFrom):
            if node.module == "src.ingestion.worker":
                violations.append(f"{node.lineno}:worker_module_import")
            if any(alias.name == "DurableInboxWorker" for alias in node.names):
                violations.append(f"{node.lineno}:worker_symbol_import")
        elif isinstance(node, ast.Call) and _call_name(node.func) == (
            "DurableInboxWorker"
        ):
            violations.append(f"{node.lineno}:worker_construction")
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and (
                "src.ingestion.worker" in node.value
                or "DurableInboxWorker" in node.value
            )
        ):
            violations.append(f"{node.lineno}:worker_dynamic_reference")
    return sorted(set(violations))


def test_greenfield_modules_do_not_import_legacy_or_external_runtime() -> None:
    violations: list[str] = []
    for path in GREENFIELD_MODULES:
        assert path.is_file(), f"missing frozen Task10G module: {path}"
        violations.extend(
            f"{path.relative_to(ROOT)}:{violation}"
            for violation in _import_violations(
                path.read_text(encoding="utf-8"),
                filename=str(path),
            )
        )

    assert violations == []


def test_phase2_greenfield_modules_expose_no_worker_or_activation_api() -> None:
    violations: list[str] = []
    for path in GREENFIELD_MODULES:
        assert path.is_file(), f"missing frozen Task10G module: {path}"
        violations.extend(
            f"{path.relative_to(ROOT)}:{violation}"
            for violation in _public_phase2_api_violations(
                path.read_text(encoding="utf-8"),
                filename=str(path),
            )
        )

    assert violations == []


def test_authority_and_recovery_call_only_migration_owned_greenfield_functions() -> (
    None
):
    assert GREENFIELD_MIGRATION.is_file(), (
        "Task10G must define the fixed greenfield SECURITY DEFINER boundary"
    )
    security_definer_functions = _security_definer_greenfield_functions(
        GREENFIELD_MIGRATION.read_text(encoding="utf-8")
    )
    called_functions: set[str] = set()
    violations: list[str] = []
    for path in AUTHORITY_SQL_MODULES:
        assert path.is_file(), f"missing frozen Task10G module: {path}"
        functions, module_violations = _database_call_contract(
            path.read_text(encoding="utf-8"),
            filename=str(path),
        )
        called_functions.update(functions)
        violations.extend(
            f"{path.relative_to(ROOT)}:{violation}" for violation in module_violations
        )

    missing_definers = sorted(called_functions - security_definer_functions)
    assert violations == []
    assert "public.greenfield_requeue_inbox" in called_functions
    assert missing_definers == [], (
        "Python may call only schema-qualified functions proved SECURITY DEFINER "
        f"with a fixed pg_catalog search path: {missing_definers}"
    )


def test_generic_command_receipt_insert_does_not_gain_greenfield_namespaces() -> None:
    assert COMMAND_RECEIPTS.is_file()
    assert (
        _generic_command_namespace_violations(
            COMMAND_RECEIPTS.read_text(encoding="utf-8"),
            filename=str(COMMAND_RECEIPTS),
        )
        == []
    )


def test_production_startup_does_not_construct_the_dormant_durable_worker() -> None:
    violations: list[str] = []
    for path in PRODUCTION_STARTUP:
        assert path.is_file()
        violations.extend(
            f"{path.relative_to(ROOT)}:{violation}"
            for violation in _durable_worker_startup_violations(
                path.read_text(encoding="utf-8"),
                filename=str(path),
            )
        )

    assert violations == []


def test_boundary_detectors_reject_mutated_examples_and_accept_safe_controls() -> None:
    safe_imports = (
        "from src.ingestion.runtime_capability import RuntimeCapabilityManifest"
    )
    assert _import_violations(safe_imports, filename="<safe-import>") == []
    hostile_imports = (
        "from src.config import get_settings\n"
        "from src.ingestion import shadow_router\n"
        "from src.ingestion.legacy_adapter import LegacyProcessingAdapter\n"
        "from src.utils.exchange_api import ExchangeClient\n"
        "import src.ingestion.backfill_service\n"
        "import src.ingestion.cutover\n"
        "import_module('src.server')\n"
    )
    import_violations = _import_violations(
        hostile_imports,
        filename="<mutated-imports>",
    )
    assert len(import_violations) >= 7

    safe_phase2_api = (
        "class RuntimeWorkload:\n"
        "    WORKER = 'worker'\n\n"
        "def require_phase2_ingress_authority(value):\n"
        "    return value\n"
    )
    assert (
        _public_phase2_api_violations(
            safe_phase2_api,
            filename="<safe-phase2-api>",
        )
        == []
    )
    hostile_phase2_api = (
        "class WorkerSession:\n"
        "    pass\n\n"
        "def transition_authority():\n"
        "    return RuntimeAuthorityState.ACTIVE\n\n"
        "async def activate_runtime():\n"
        "    return 'active'\n"
    )
    phase2_violations = _public_phase2_api_violations(
        hostile_phase2_api,
        filename="<mutated-phase2-api>",
    )
    assert any("public_class:WorkerSession" in item for item in phase2_violations)
    assert any("active_authority_reference" in item for item in phase2_violations)
    assert any("public_callable:activate_runtime" in item for item in phase2_violations)

    safe_sql = (
        "_SQL = 'SELECT id FROM public.greenfield_requeue_inbox(%s)'\n"
        "async def run(connection):\n"
        "    return await connection.execute(_SQL, (1,))\n"
    )
    called, sql_violations = _database_call_contract(
        safe_sql,
        filename="<safe-greenfield-sql>",
    )
    assert called == {"public.greenfield_requeue_inbox"}
    assert sql_violations == []
    hostile_sql = (
        "async def raw(connection):\n"
        "    await connection.execute('UPDATE event_inbox SET status = %s', ('x',))\n\n"
        "async def unqualified(connection):\n"
        "    await connection.execute('SELECT * FROM greenfield_requeue_inbox(%s)', (1,))\n\n"
        "async def dynamic(connection, function_name):\n"
        "    await connection.execute(f'SELECT * FROM public.{function_name}(%s)', (1,))\n"
    )
    _, hostile_sql_violations = _database_call_contract(
        hostile_sql,
        filename="<mutated-greenfield-sql>",
    )
    assert any("raw_governed_dml" in item for item in hostile_sql_violations)
    assert any("non_greenfield_function_sql" in item for item in hostile_sql_violations)
    assert any("dynamic_sql" in item for item in hostile_sql_violations)

    safe_migration = (
        "def upgrade():\n"
        "    op.execute('''\n"
        "    CREATE FUNCTION public.greenfield_requeue_inbox(value bigint)\n"
        "    RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER\n"
        "    SET search_path = pg_catalog AS $$ BEGIN RETURN value; END $$\n"
        "    ''')\n"
    )
    assert _security_definer_greenfield_functions(safe_migration) == {
        "public.greenfield_requeue_inbox"
    }
    hostile_migration = (
        "def upgrade():\n"
        "    op.execute('''\n"
        "    CREATE FUNCTION public.greenfield_missing_definer(value bigint)\n"
        "    RETURNS bigint LANGUAGE sql AS $$ SELECT value $$\n"
        "    ''')\n"
        "    op.execute('''\n"
        "    CREATE FUNCTION greenfield_unqualified(value bigint)\n"
        "    RETURNS bigint LANGUAGE sql SECURITY DEFINER\n"
        "    SET search_path = pg_catalog AS $$ SELECT value $$\n"
        "    ''')\n"
    )
    assert _security_definer_greenfield_functions(hostile_migration) == set()

    safe_commands = "_COMMAND_NAMES = frozenset({'cold_start.preview'})"
    assert (
        _generic_command_namespace_violations(
            safe_commands,
            filename="<safe-command-names>",
        )
        == []
    )
    hostile_commands = (
        "_COMMAND_NAMES = frozenset({"
        "'cold_start.preview', 'runtime.initialize', 'inbox.requeue'})"
    )
    assert _generic_command_namespace_violations(
        hostile_commands,
        filename="<mutated-command-names>",
    ) == [
        "generic_command:inbox.requeue",
        "generic_command:runtime.initialize",
    ]

    safe_startup = "async def start(runtime):\n    await runtime.start_ingress()\n"
    assert (
        _durable_worker_startup_violations(
            safe_startup,
            filename="<safe-startup>",
        )
        == []
    )
    hostile_startup = (
        "from src.ingestion.worker import DurableInboxWorker\n"
        "worker = DurableInboxWorker(repository=None)\n"
    )
    startup_violations = _durable_worker_startup_violations(
        hostile_startup,
        filename="<mutated-startup>",
    )
    assert any("worker_module_import" in item for item in startup_violations)
    assert any("worker_construction" in item for item in startup_violations)
