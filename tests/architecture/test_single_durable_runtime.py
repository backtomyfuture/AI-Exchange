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


def test_live_startup_has_no_legacy_effect_runtime_reachability() -> None:
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
        "start_lark_ws",
        "build_graph",
        "MemoryConsolidator",
        "run_scheduler",
        "process_and_archive_email",
    )

    assert [token for token in forbidden if token in live_sources] == []


def test_phase2_runtime_has_no_worker_sync_or_activation_constructor() -> None:
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

    assert "src.ingestion.worker" not in imported_modules
    assert "src.ingestion.sync" not in imported_modules
    assert not names.intersection(
        {"activate", "start_worker", "claim_batch", "start_sync", "run_polling"}
    )


def test_webhook_endpoint_has_one_production_sink_and_no_legacy_queue_fallback() -> (
    None
):
    source = _source("src/server.py")

    assert source.count(".webhook_ingress_service") >= 3
    assert source.count('getattr(request.app.state, "webhook_ingress_service"') == 1
    assert "enqueue_webhook_event" not in source
    assert "WebhookWorker" not in source
    assert "queue_full" not in source
