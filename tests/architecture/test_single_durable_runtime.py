from __future__ import annotations

import ast
from pathlib import Path


_ROOT = Path(__file__).resolve().parents[2]


def _source(relative: str) -> str:
    return (_ROOT / relative).read_text(encoding="utf-8")


def _async_functions(relative: str) -> list[str]:
    tree = ast.parse(_source(relative), filename=relative)
    return [
        node.name for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)
    ]


def test_server_defines_the_only_application_lifespan() -> None:
    main_source = _source("src/main.py")
    server_functions = _async_functions("src/server.py")

    assert server_functions.count("application_lifespan") == 1
    assert "secure_service_lifespan" not in server_functions
    assert "async def main" not in main_source
    assert "lifespan_context" not in main_source
    assert "lifespan=application_lifespan" in _source("src/server.py")


def test_live_startup_has_one_lark_wiring_and_no_legacy_worker_reachability() -> None:
    live_sources = "\n".join(
        _source(path)
        for path in (
            "src/main.py",
            "src/server.py",
            "src/ingestion/runtime.py",
        )
    )
    forbidden = (
        "exchange_start_worker",
        "exchange_stop_worker",
        "start_worker(",
        "run_polling_loop",
        "SelfHealer",
        "build_graph",
        "MemoryConsolidator",
        "run_scheduler",
        "process_and_archive_email",
    )

    assert [token for token in forbidden if token in live_sources] == []
    server_tree = ast.parse(_source("src/server.py"), filename="src/server.py")
    lark_start_calls = [
        node
        for node in ast.walk(server_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "lark_app"
        and node.func.attr == "start_lark_ws"
    ]
    assert len(lark_start_calls) == 1
    assert [keyword.arg for keyword in lark_start_calls[0].keywords] == ["fail_stop"]


def test_runtime_factory_has_one_worker_wiring_and_no_sync_constructor() -> None:
    tree = ast.parse(_source("src/ingestion/runtime.py"))
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    names = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }

    factory = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "build_ingestion_runtime"
    )
    worker_imports = [
        node
        for node in ast.walk(factory)
        if isinstance(node, ast.ImportFrom) and node.module == "src.ingestion.worker"
    ]
    worker_constructors = [
        node
        for node in ast.walk(factory)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "DurableInboxWorker"
    ]

    assert "src.ingestion.worker" in imported_modules
    assert len(worker_imports) == 1
    assert len(worker_constructors) == 1
    assert "src.ingestion.sync" not in imported_modules
    assert not names.intersection(
        {"activate", "start_worker", "start_sync", "run_polling"}
    )


def test_polling_only_server_exposes_no_webhook_sink_or_legacy_queue_fallback() -> None:
    source = _source("src/server.py")

    assert '"/webhooks/exchange"' not in source
    assert "exchange_webhook" not in source
    assert "enqueue_webhook_event" not in source
    assert "WebhookWorker" not in source
    assert "queue_full" not in source
