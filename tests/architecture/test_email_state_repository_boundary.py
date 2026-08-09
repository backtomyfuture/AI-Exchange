"""Architecture boundaries for the polling-only ingestion pipeline."""

from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
INGESTION = ROOT / "src" / "ingestion"
BASELINE_SQL = ROOT / "alembic" / "versions" / "20260808_0001_polling_baseline.sql"

_RETIRED_MODULES = (
    "webhook.py",
    "cold_start.py",
    "sync.py",
    "command_receipts.py",
)
_RETIRED_SYMBOLS = (
    "WebhookIngress",
    "IngressSource.WEBHOOK",
    "cold_start",
    "sync_cold_start",
    "greenfield_insert_webhook_event",
)


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _imports(path: Path) -> set[str]:
    tree = ast.parse(_source(path), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_retired_ingress_modules_are_not_shipped() -> None:
    assert [name for name in _RETIRED_MODULES if (INGESTION / name).exists()] == []


def test_runtime_path_has_no_retired_ingress_imports() -> None:
    runtime_files = (
        INGESTION / "runtime.py",
        INGESTION / "repository.py",
        INGESTION / "worker.py",
        ROOT / "src" / "server.py",
    )
    imports = set().union(*(_imports(path) for path in runtime_files))

    assert not {
        module
        for module in imports
        if any(token in module.casefold() for token in ("webhook", "cold_start"))
    }


def test_runtime_source_has_no_retired_ingress_symbols() -> None:
    sources = "\n".join(
        _source(path)
        for path in (
            INGESTION / "runtime.py",
            INGESTION / "repository.py",
            INGESTION / "worker.py",
            INGESTION / "policy.py",
            INGESTION / "normalization.py",
        )
    ).casefold()

    assert all(symbol.casefold() not in sources for symbol in _RETIRED_SYMBOLS)


def test_the_database_snapshot_excludes_retired_ingress_objects() -> None:
    source = _source(BASELINE_SQL).casefold()

    assert "greenfield_insert_webhook_event" not in source
    assert "cold_start" not in source
    assert "webhook_ids" not in source
    assert "greenfield_commit_sync_page" in source


def test_runtime_only_calls_the_fixed_polling_database_routines() -> None:
    polling_source = _source(INGESTION / "polling.py")
    called = set(re.findall(r"public\.(greenfield_[a-z_]+)", polling_source))

    assert "greenfield_commit_sync_page" in called
    assert "greenfield_insert_webhook_event" not in called
    assert called == {"greenfield_commit_sync_page"}


def test_runtime_modules_do_not_execute_dynamic_python() -> None:
    for path in (
        INGESTION / "runtime.py",
        INGESTION / "repository.py",
        INGESTION / "worker.py",
        INGESTION / "processing.py",
        INGESTION / "email_pipeline.py",
    ):
        tree = ast.parse(_source(path), filename=str(path))
        calls = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert not {"eval", "exec", "compile"} & calls, path


def test_access_contract_has_no_historical_revision_literals() -> None:
    source = _source(ROOT / "src" / "db" / "access_contract.py")

    assert "202607" not in source
    assert "20260805" not in source
    assert 'DATABASE_REVISION: Final = "20260808_0001"' in source
