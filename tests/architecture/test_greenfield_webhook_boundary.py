from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WEBHOOK = ROOT / "src" / "ingestion" / "webhook.py"
SERVER = ROOT / "src" / "server.py"

_BANNED_IMPORT_PREFIXES = (
    "fastapi",
    "langgraph",
    "psycopg",
    "psycopg_pool",
    "qdrant_client",
    "src.config",
    "src.db",
    "src.exchange_service",
    "src.graph",
    "src.nodes",
    "src.router",
    "src.utils.email_processor",
    "src.utils.exchange_api",
    "src.utils.lark_app",
    "src.utils.retriever",
)
_BANNED_RUNTIME_SYMBOLS = frozenset(
    {
        "ExchangeClient",
        "QdrantClient",
        "WebhookWorker",
        "_webhook_queue",
        "ainvoke",
        "create_task",
        "enqueue_exchange_webhook",
        "enqueue_webhook_event",
        "get_email",
        "graph_app",
        "invoke",
        "lark_app",
        "list_emails",
        "mark_as_read",
        "process_and_archive_email",
        "send_approval_card",
        "upsert",
    }
)


def _tree(path: Path) -> ast.Module:
    assert path.is_file(), f"missing frozen Task9G production module: {path}"
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _function(tree: ast.Module, name: str) -> ast.AsyncFunctionDef:
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"missing async function {name}")


def _imported_modules(tree: ast.AST) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
    return modules


def _referenced_symbols(tree: ast.AST) -> set[str]:
    symbols = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    symbols.update(
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    )
    return symbols


def test_typed_webhook_service_has_no_external_or_http_dependency() -> None:
    tree = _tree(WEBHOOK)
    imports = _imported_modules(tree)
    violations = sorted(
        module for module in imports if module.startswith(_BANNED_IMPORT_PREFIXES)
    )

    assert violations == []
    assert _referenced_symbols(tree).isdisjoint(_BANNED_RUNTIME_SYMBOLS)


def test_typed_webhook_service_owns_no_pool_global_context_or_background_task() -> None:
    _tree(WEBHOOK)
    source = WEBHOOK.read_text(encoding="utf-8")

    assert "get_settings" not in source
    assert "database_url" not in source
    assert "AsyncConnectionPool" not in source
    assert "Request" not in source
    assert "HTTPException" not in source
    assert "asyncio.Queue" not in source


def test_exchange_http_route_cannot_reach_legacy_queue_or_business_effects() -> None:
    tree = _tree(SERVER)
    route = _function(tree, "exchange_webhook")
    route_symbols = _referenced_symbols(route)
    source = SERVER.read_text(encoding="utf-8")

    assert route_symbols.isdisjoint(_BANNED_RUNTIME_SYMBOLS)
    assert "enqueue_webhook_event" not in source
    assert "_webhook_queue" not in source
    assert "queue_full" not in source


def test_exchange_http_route_has_no_external_mail_model_graph_lark_or_qdrant_call() -> (
    None
):
    route = _function(_tree(SERVER), "exchange_webhook")
    called_names = {
        node.func.id
        for node in ast.walk(route)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    called_attributes = {
        node.func.attr
        for node in ast.walk(route)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }

    assert (called_names | called_attributes).isdisjoint(
        {
            "get_email",
            "list_emails",
            "ainvoke",
            "invoke",
            "send_approval_card",
            "upsert",
            "mark_as_read",
            "process_and_archive_email",
        }
    )
