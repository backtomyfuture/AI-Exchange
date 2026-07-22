from __future__ import annotations

import ast
import re
from pathlib import Path

from src.ingestion import command_receipts


ROOT = Path(__file__).resolve().parents[2]
WORKER = ROOT / "src" / "ingestion" / "worker.py"
RUNTIME = ROOT / "src" / "ingestion" / "runtime.py"
LIVE_ROOTS = (
    ROOT / "src" / "main.py",
    ROOT / "src" / "server.py",
    ROOT / "src" / "init_app.py",
    ROOT / "src" / "config.py",
    ROOT / "src" / "exchange_service.py",
)
TASK8_PRODUCTION = (
    ROOT / "src" / "exchange_service.py",
    ROOT / "src" / "ingestion" / "processing.py",
    ROOT / "src" / "ingestion" / "legacy_adapter.py",
    WORKER,
    ROOT / "src" / "ingestion" / "repository.py",
)
MIGRATIONS = ROOT / "alembic" / "versions"
SYNC_CONTROL_MIGRATION = MIGRATIONS / "20260713_0005_sync_reconciliation_control.py"
TASK7_MIGRATION_MANIFEST = frozenset(
    {
        "20260710_0001_existing_schema.py",
        "20260710_0002_p0_alignment.py",
        "20260710_0003_durable_ingestion.py",
        "20260713_0004_ingestion_policy_ignored.py",
        "20260713_0005_sync_reconciliation_control.py",
    }
)
TASK10G_MIGRATION = "20260716_0006_greenfield_runtime_authority.py"
TASK10G_MIGRATION_MANIFEST = TASK7_MIGRATION_MANIFEST | {TASK10G_MIGRATION}
TASK7_COMMAND_NAMES = frozenset(
    {
        "cold_start.preview",
        "cold_start.approve",
        "cold_start.apply_page",
    }
)
WORKER_SYMBOLS = frozenset(
    {
        "DurableInboxWorker",
        "LeaseAuthority",
        "LeaseAuthorityLost",
    }
)


def _python_files(path: Path):
    if path.is_file():
        yield path
        return
    yield from sorted(path.rglob("*.py"))


def _imports_worker(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "src.ingestion.worker":
            return True
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "src.ingestion"
            and any(alias.name in WORKER_SYMBOLS for alias in node.names)
        ):
            return True
        if isinstance(node, ast.Import) and any(
            alias.name == "src.ingestion.worker" for alias in node.names
        ):
            return True
        if isinstance(node, ast.Name) and node.id in WORKER_SYMBOLS:
            return True
        if isinstance(node, ast.Attribute) and node.attr in WORKER_SYMBOLS:
            return True
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and (
                "src.ingestion.worker" in node.value
                or any(symbol in node.value for symbol in WORKER_SYMBOLS)
            )
        ):
            return True
    return False


def _method_node(tree: ast.Module, class_name: str, method_name: str):
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
                    item.name == method_name
                ):
                    return item
    raise AssertionError(f"missing {class_name}.{method_name}")


def test_durable_worker_has_exactly_one_runtime_factory_wiring() -> None:
    violations = [
        str(path.relative_to(ROOT))
        for root in (*LIVE_ROOTS, RUNTIME)
        for path in _python_files(root)
        if _imports_worker(path)
    ]

    assert violations == ["src/ingestion/runtime.py"]
    tree = ast.parse(RUNTIME.read_text(encoding="utf-8"), filename=str(RUNTIME))
    constructors = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "DurableInboxWorker"
    ]
    assert len(constructors) == 1


def test_worker_has_no_module_level_task_creation_or_configuration_gate() -> None:
    source = WORKER.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(WORKER))
    top_level_calls = [
        node
        for statement in tree.body
        for node in ast.walk(statement)
        if not isinstance(
            statement, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        )
        and isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "asyncio"
        and node.func.attr == "create_task"
    ]

    assert top_level_calls == []
    assert "get_settings" not in source
    assert "src.config" not in source
    assert "DURABLE_INBOX_ENABLED" not in source
    assert "logger.exception" not in source


def test_worker_depends_only_on_router_protocol_not_concrete_legacy_adapter() -> None:
    source = WORKER.read_text(encoding="utf-8")

    assert "LegacyProcessingAdapter" not in source
    assert "legacy_adapter" not in source
    assert "exchange_service" not in source
    assert "self._router.select(" in source
    assert "adapter.process(" in source


def test_worker_cannot_bypass_aggregate_effect_marker() -> None:
    source = WORKER.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(WORKER))
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }

    assert "begin_processing_effect" in called_attributes
    assert "begin_effect" not in called_attributes


def test_consumers_claim_and_process_inline_without_per_lease_processing_tasks() -> (
    None
):
    tree = ast.parse(WORKER.read_text(encoding="utf-8"), filename=str(WORKER))
    run_once = _method_node(tree, "DurableInboxWorker", "run_once")
    consume = _method_node(tree, "DurableInboxWorker", "_consume")

    run_once_calls = {
        node.func.attr
        for node in ast.walk(run_once)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    consume_calls = {
        node.func.attr
        for node in ast.walk(consume)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }

    assert "claim_batch" in run_once_calls
    assert "process_lease" in run_once_calls
    assert "create_task" not in run_once_calls
    assert "run_once" in consume_calls
    assert "process_lease" not in consume_calls
    assert "create_task" not in consume_calls


def test_task8_namespace_stays_frozen_while_task10g_adds_one_migration() -> None:
    migration_manifest = frozenset(path.name for path in MIGRATIONS.glob("*.py"))
    assert migration_manifest == TASK10G_MIGRATION_MANIFEST
    assert migration_manifest - TASK7_MIGRATION_MANIFEST == {TASK10G_MIGRATION}
    assert command_receipts._COMMAND_NAMES == TASK7_COMMAND_NAMES

    migration_source = SYNC_CONTROL_MIGRATION.read_text(encoding="utf-8")
    command_checks = re.findall(
        r"command_name\s+IN\s*\((.*?)\)",
        migration_source,
        flags=re.DOTALL,
    )
    assert len(command_checks) == 2
    assert all(
        frozenset(re.findall(r"'([^']+)'", check)) == TASK7_COMMAND_NAMES
        for check in command_checks
    )
    for path in TASK8_PRODUCTION:
        source = path.read_text(encoding="utf-8")
        assert "pipeline_command_receipts" not in source, path
        assert "CommandReceipt" not in source, path
